from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
import threading
import unicodedata
import time
import xml.etree.ElementTree as ET

import requests
from plexapi.server import PlexServer as PlexApiServer
from .media_identity_service import media_candidate_titles, media_year

PLEX_REQUEST_TIMEOUT = 20
PLEX_FULL_SCAN_REQUEST_TIMEOUT = 180
PLEX_TITLE_SEARCH_LIMIT = 12
PLEX_PERSON_SEARCH_LIMIT = 300
PLEX_MIN_REQUEST_INTERVAL = 0.2
PLEX_FAST_INDEX_PAGE_SIZE = 1000


@dataclass
class PlexMatch:
	item: object
	title: str
	year: int | None
	match_source: str = "unknown"


_PLEX_RATE_LOCK = threading.Lock()
_PLEX_LAST_REQUEST_AT_BY_BASE: dict[str, float] = {}


def _normalize_plex_base_url(url: str | None) -> str:
	return (url or "").strip().rstrip("/")


def _apply_plex_rate_limit(base_url: str) -> None:
	if not base_url:
		return

	with _PLEX_RATE_LOCK:
		last_request_at = _PLEX_LAST_REQUEST_AT_BY_BASE.get(base_url, 0.0)
		now = time.monotonic()
		elapsed = now - last_request_at

		if elapsed < PLEX_MIN_REQUEST_INTERVAL:
			time.sleep(PLEX_MIN_REQUEST_INTERVAL - elapsed)

		_PLEX_LAST_REQUEST_AT_BY_BASE[base_url] = time.monotonic()


class TimeoutSession(requests.Session):
	def __init__(self, timeout: int, base_url: str, verify_ssl: bool = True):
		super().__init__()
		self._default_timeout = timeout
		self._plex_base_url = _normalize_plex_base_url(base_url)
		self.verify = verify_ssl

	def request(self, method, url, **kwargs):
		kwargs.setdefault("timeout", self._default_timeout)
		_apply_plex_rate_limit(self._plex_base_url)
		return super().request(method, url, **kwargs)


def _normalize_text(value: str | None) -> str:
	if not value:
		return ""

	value = unicodedata.normalize("NFKD", value)
	value = "".join(ch for ch in value if not unicodedata.combining(ch))
	value = "".join(ch.lower() if ch.isalnum() else " " for ch in value)
	return " ".join(value.split())


def _safe_year(value) -> int | None:
	try:
		return int(value) if value is not None else None
	except (TypeError, ValueError):
		return None


def _default_collection_sort_title(title: str) -> str:
	clean_title = (title or "").strip()
	if not clean_title:
		return "_"
	if clean_title.startswith("_"):
		return clean_title
	return f"_{clean_title}"


class PlexService:
	def __init__(self, base_url: str, token: str, verify_ssl: bool = True):
		self.base_url = base_url.rstrip("/")
		self.token = token
		self.verify_ssl = verify_ssl
		self.request_timeout = PLEX_REQUEST_TIMEOUT

		self.session = TimeoutSession(
			timeout=self.request_timeout,
			base_url=self.base_url,
			verify_ssl=verify_ssl,
		)

		if not verify_ssl:
			import urllib3
			urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

		self.server = PlexApiServer(
			self.base_url,
			self.token,
			session=self.session,
			timeout=self.request_timeout,
		)
		self._section_cache: dict[str, object] = {}

	def list_library_sections(self) -> list[dict]:
		sections = []
		for section in self.server.library.sections():
			if section.type in {"movie", "show"}:
				sections.append({"title": section.title, "type": section.type})
		return sections

	def _get_section(self, section_name: str):
		section = self._section_cache.get(section_name)
		if section is None:
			section = self.server.library.section(section_name)
			self._section_cache[section_name] = section
		return section

	@contextmanager
	def _temporary_request_timeout(self, timeout: int):
		previous_timeout = self.session._default_timeout
		self.session._default_timeout = max(int(timeout or previous_timeout), 1)

		try:
			yield
		finally:
			self.session._default_timeout = previous_timeout

	def _item_titles(self, item) -> set[str]:
		titles = set()

		for attr in ("title", "originalTitle", "grandparentTitle"):
			raw = getattr(item, attr, None)
			norm = _normalize_text(raw)
			if norm:
				titles.add(norm)

		return titles

	def _item_year(self, item) -> int | None:
		return _safe_year(getattr(item, "year", None))

	def _item_person_names(self, item) -> set[str]:
		people_names: set[str] = set()

		for attr in ("roles", "actors"):
			raw_people = getattr(item, attr, None) or []
			for person in raw_people:
				raw_name = getattr(person, "tag", None) or getattr(person, "name", None)
				normalized_name = _normalize_text(raw_name)
				if normalized_name:
					people_names.add(normalized_name)

		return people_names

	def _item_external_ids(self, item) -> dict[str, set[str]]:
		external_ids = {
			'tmdb': set(),
			'imdb': set(),
			'tvdb': set(),
		}

		raw_guids = getattr(item, 'guids', None) or []

		for guid in raw_guids:
			raw_id = getattr(guid, 'id', None) or ''
			raw_id = str(raw_id).strip()
			if not raw_id or '://' not in raw_id:
				continue

			provider, provider_value = raw_id.split('://', 1)
			provider = provider.strip().lower()
			provider_value = provider_value.strip()

			if not provider_value:
				continue

			if provider in external_ids:
				external_ids[provider].add(provider_value)

		return external_ids

	def _credit_external_ids(self, credit: dict) -> dict[str, set[str]]:
		external_ids = {
			'tmdb': set(),
			'imdb': set(),
			'tvdb': set(),
		}

		tmdb_id = credit.get('id')
		if tmdb_id is not None:
			clean_tmdb_id = str(tmdb_id).strip()
			if clean_tmdb_id:
				external_ids['tmdb'].add(clean_tmdb_id)

		imdb_id = credit.get('imdb_id')
		if imdb_id:
			external_ids['imdb'].add(str(imdb_id).strip())

		tvdb_id = credit.get('tvdb_id')
		if tvdb_id is not None:
			clean_tvdb_id = str(tvdb_id).strip()
			if clean_tvdb_id:
				external_ids['tvdb'].add(clean_tvdb_id)

		return external_ids

	def _section_libtype(self, section) -> str:
		return "movie" if getattr(section, "type", None) == "movie" else "show"

	def list_title_keys(
		self,
		section_name: str,
	) -> tuple[
		set[tuple[str, int | None]],
		set[str],
		dict[str, set[str]],
	]:
		section = self._get_section(section_name)
		keys_with_year: set[tuple[str, int | None]] = set()
		keys_without_year: set[str] = set()
		external_ids = {
			'tmdb': set(),
			'imdb': set(),
			'tvdb': set(),
		}

		with self._temporary_request_timeout(PLEX_FULL_SCAN_REQUEST_TIMEOUT):
			for item in section.all():
				item_year = self._item_year(item)

				for title in self._item_titles(item):
					keys_without_year.add(title)
					keys_with_year.add((title, item_year))

				item_external_ids = self._item_external_ids(item)
				for provider, values in item_external_ids.items():
					external_ids.setdefault(provider, set()).update(values)

		return keys_with_year, keys_without_year, external_ids

	def _parse_fast_index_guids(self, xml_item) -> dict[str, set[str]]:
		external_ids = {
			"tmdb": set(),
			"imdb": set(),
			"tvdb": set(),
		}

		raw_guid = str(xml_item.attrib.get("guid") or "").strip()
		raw_guids = [raw_guid] if raw_guid else []

		for guid_node in xml_item.findall("Guid"):
			node_id = str(guid_node.attrib.get("id") or "").strip()
			if node_id:
				raw_guids.append(node_id)

		for raw_id in raw_guids:
			if not raw_id or "://" not in raw_id:
				continue

			provider, provider_value = raw_id.split("://", 1)
			provider = provider.strip().lower()
			provider_value = provider_value.strip()

			if provider in external_ids and provider_value:
				external_ids[provider].add(provider_value)

		return external_ids

	def _build_fast_index_item(self, xml_item, section_media_type: str) -> dict | None:
		attrs = xml_item.attrib

		rating_key = str(attrs.get("ratingKey") or attrs.get("key") or "").strip()
		if not rating_key:
			return None

		raw_titles: list[str] = []
		seen_raw_titles: set[str] = set()

		for attr in ("title", "originalTitle", "grandparentTitle"):
			raw_title = str(attrs.get(attr) or "").strip()
			if not raw_title:
				continue

			normalized_raw_title = _normalize_text(raw_title)
			if not normalized_raw_title or normalized_raw_title in seen_raw_titles:
				continue

			seen_raw_titles.add(normalized_raw_title)
			raw_titles.append(raw_title)

		normalized_titles = sorted(
			_normalize_text(title)
			for title in raw_titles
			if _normalize_text(title)
		)

		item_external_ids = self._parse_fast_index_guids(xml_item)

		tmdb_values = sorted(item_external_ids.get("tmdb") or [])
		imdb_values = sorted(item_external_ids.get("imdb") or [])
		tvdb_values = sorted(item_external_ids.get("tvdb") or [])

		return {
			"rating_key": rating_key,
			"media_type": section_media_type,
			"title": str(attrs.get("title") or "").strip() or "Unknown",
			"original_title": str(attrs.get("originalTitle") or "").strip() or None,
			"year": _safe_year(attrs.get("year")),
			"tmdb_id": tmdb_values[0] if tmdb_values else None,
			"imdb_id": imdb_values[0] if imdb_values else None,
			"tvdb_id": tvdb_values[0] if tvdb_values else None,
			"raw_titles": raw_titles,
			"normalized_titles": normalized_titles,
			"normalized_people": [],
			"external_ids": {
				"tmdb": tmdb_values,
				"imdb": imdb_values,
				"tvdb": tvdb_values,
			},
		}

	def _list_library_items_for_index_via_plexapi(
		self,
		section_name: str,
		*,
		include_people: bool = False,
	):
		section = self._get_section(section_name)
		section_media_type = "movie" if getattr(section, "type", None) == "movie" else "show"

		with self._temporary_request_timeout(PLEX_FULL_SCAN_REQUEST_TIMEOUT):
			for item in section.all():
				rating_key = str(getattr(item, "ratingKey", "") or "").strip()
				if not rating_key:
					continue

				raw_titles: list[str] = []
				seen_raw_titles: set[str] = set()

				for attr in ("title", "originalTitle", "grandparentTitle"):
					raw_title = str(getattr(item, attr, "") or "").strip()
					if not raw_title:
						continue

					normalized_raw_title = _normalize_text(raw_title)
					if not normalized_raw_title or normalized_raw_title in seen_raw_titles:
						continue

					seen_raw_titles.add(normalized_raw_title)
					raw_titles.append(raw_title)

				normalized_people: list[str] = []
				if include_people:
					seen_people: set[str] = set()

					for attr in ("roles", "actors"):
						raw_people = getattr(item, attr, None) or []
						for person in raw_people:
							raw_name = getattr(person, "tag", None) or getattr(person, "name", None)
							normalized_name = _normalize_text(raw_name)
							if not normalized_name or normalized_name in seen_people:
								continue

							seen_people.add(normalized_name)
							normalized_people.append(normalized_name)

				item_external_ids = self._item_external_ids(item)

				tmdb_values = sorted(item_external_ids.get("tmdb") or [])
				imdb_values = sorted(item_external_ids.get("imdb") or [])
				tvdb_values = sorted(item_external_ids.get("tvdb") or [])

				yield {
					"rating_key": rating_key,
					"media_type": section_media_type,
					"title": str(getattr(item, "title", "") or "").strip() or "Unknown",
					"original_title": str(getattr(item, "originalTitle", "") or "").strip() or None,
					"year": self._item_year(item),
					"tmdb_id": tmdb_values[0] if tmdb_values else None,
					"imdb_id": imdb_values[0] if imdb_values else None,
					"tvdb_id": tvdb_values[0] if tvdb_values else None,
					"raw_titles": raw_titles,
					"normalized_titles": sorted(self._item_titles(item)),
					"normalized_people": normalized_people,
					"external_ids": {
						"tmdb": tmdb_values,
						"imdb": imdb_values,
						"tvdb": tvdb_values,
					},
				}

	def list_library_items_for_index(
		self,
		section_name: str,
		*,
		include_people: bool = False,
	):
		if include_people:
			yield from self._list_library_items_for_index_via_plexapi(
				section_name,
				include_people=include_people,
			)
			return

		section = self._get_section(section_name)
		section_key = str(getattr(section, "key", "") or "").strip()
		section_type = getattr(section, "type", None)

		if not section_key:
			yield from self._list_library_items_for_index_via_plexapi(
				section_name,
				include_people=include_people,
			)
			return

		plex_type = "1" if section_type == "movie" else "2"
		section_media_type = "movie" if section_type == "movie" else "show"
		start = 0

		while True:
			url = f"{self.base_url}/library/sections/{section_key}/all"
			params = {
				"type": plex_type,
				"includeGuids": "1",
				"X-Plex-Token": self.token,
			}
			headers = {
				"X-Plex-Container-Start": str(start),
				"X-Plex-Container-Size": str(PLEX_FAST_INDEX_PAGE_SIZE),
				"Accept": "application/xml",
			}

			response = self.session.get(
				url,
				params=params,
				headers=headers,
				timeout=PLEX_FULL_SCAN_REQUEST_TIMEOUT,
			)
			response.raise_for_status()

			root = ET.fromstring(response.content)
			xml_items = [
				child
				for child in root
				if child.tag in {"Video", "Directory"}
			]

			if not xml_items:
				break

			for xml_item in xml_items:
				index_item = self._build_fast_index_item(xml_item, section_media_type)
				if index_item:
					yield index_item

			start += len(xml_items)

			total_size = _safe_year(root.attrib.get("totalSize"))
			if total_size is not None and start >= total_size:
				break

			if len(xml_items) < PLEX_FAST_INDEX_PAGE_SIZE:
				break

	def resolve_local_cache_entries_to_items(
		self,
		section_name: str,
		entries: list[dict],
		media_type: str | None = None,
		limit: int | None = None,
	) -> list[PlexMatch]:
		section = self._get_section(section_name)
		libtype = self._section_libtype(section)

		search_buckets: dict[str, dict[str, object]] = {}

		for entry in entries or []:
			rating_key = str(entry.get("rating_key") or "").strip()
			if not rating_key:
				continue

			raw_titles = [
				str(item).strip()
				for item in (entry.get("raw_titles") or [])
				if str(item).strip()
			]

			if not raw_titles:
				title = str(entry.get("title") or "").strip()
				if title:
					raw_titles = [title]

			normalized_titles = {
				str(item).strip()
				for item in (entry.get("normalized_titles") or [])
				if str(item).strip()
			}

			if not normalized_titles:
				title = str(entry.get("title") or "").strip()
				if title:
					normalized_titles.add(_normalize_text(title))

			entry_year = entry.get("year")

			for raw_title in raw_titles:
				bucket = search_buckets.setdefault(
					raw_title,
					{
						"rating_keys": set(),
						"normalized_titles": set(),
						"years": set(),
					},
				)
				bucket["rating_keys"].add(rating_key)
				bucket["normalized_titles"].update(normalized_titles)
				bucket["years"].add(entry_year)

		results: list[PlexMatch] = []
		seen_keys: set[str] = set()

		for raw_title, bucket in sorted(search_buckets.items(), key=lambda item: len(item[0]), reverse=True):
			try:
				items = section.search(
					title=raw_title,
					libtype=libtype,
					maxresults=PLEX_TITLE_SEARCH_LIMIT,
				)
			except Exception:
				items = []

			for item in items:
				item_rating_key = str(getattr(item, "ratingKey", "") or "").strip()
				if not item_rating_key or item_rating_key in seen_keys:
					continue

				item_titles = self._item_titles(item)
				item_year = self._item_year(item)

				if item_rating_key not in bucket["rating_keys"]:
					if bucket["normalized_titles"] and not (item_titles & bucket["normalized_titles"]):
						continue

					if (
						item_year is not None
						and bucket["years"]
						and None not in bucket["years"]
						and item_year not in bucket["years"]
					):
						continue

				seen_keys.add(item_rating_key)
				results.append(
					PlexMatch(
						item=item,
						title=getattr(item, "title", "Unknown"),
						year=item_year,
						match_source="local_cache",
					)
				)

				if limit and len(results) >= limit:
					return results[:limit]

		return results

	def find_person_items(
		self,
		section_name: str,
		person_name: str,
		aliases: list[str] | None = None,
		limit: int | None = None,
	) -> list[PlexMatch]:
		section = self._get_section(section_name)
		libtype = self._section_libtype(section)

		candidate_names: list[str] = []
		seen_names: set[str] = set()

		for raw_name in [person_name] + (aliases or []):
			clean_name = (raw_name or "").strip()
			norm_name = _normalize_text(clean_name)
			if clean_name and norm_name and norm_name not in seen_names:
				seen_names.add(norm_name)
				candidate_names.append(clean_name)

		results: list[PlexMatch] = []
		seen_keys: set[str] = set()
		search_limit = limit or PLEX_PERSON_SEARCH_LIMIT

		for candidate_name in candidate_names:
			try:
				items = section.search(
					libtype=libtype,
					actor=candidate_name,
					maxresults=search_limit,
				)
			except Exception:
				items = []

			for item in items:
				rating_key = str(getattr(item, "ratingKey", ""))
				if not rating_key or rating_key in seen_keys:
					continue

				seen_keys.add(rating_key)
				results.append(
					PlexMatch(
						item=item,
						title=getattr(item, "title", "Unknown"),
						year=self._item_year(item),
						match_source="person_role",
					)
				)

				if limit and len(results) >= limit:
					return results[:limit]

		return results

	def find_person_items_via_scan(
		self,
		section_name: str,
		person_name: str,
		aliases: list[str] | None = None,
		limit: int | None = None,
	) -> list[PlexMatch]:
		section = self._get_section(section_name)

		candidate_names: set[str] = set()
		for raw_name in [person_name] + (aliases or []):
			normalized_name = _normalize_text(raw_name)
			if normalized_name:
				candidate_names.add(normalized_name)

		if not candidate_names:
			return []

		results: list[PlexMatch] = []
		seen_keys: set[str] = set()

		try:
			with self._temporary_request_timeout(PLEX_FULL_SCAN_REQUEST_TIMEOUT):
				items = section.all()
		except Exception:
			return []

		for item in items:
			item_people_names = self._item_person_names(item)
			if not item_people_names:
				continue

			if not (item_people_names & candidate_names):
				continue

			rating_key = str(getattr(item, "ratingKey", ""))
			if not rating_key or rating_key in seen_keys:
				continue

			seen_keys.add(rating_key)
			results.append(
				PlexMatch(
					item=item,
					title=getattr(item, "title", "Unknown"),
					year=self._item_year(item),
					match_source="person_role_scan",
				)
			)

			if limit and len(results) >= limit:
				return results[:limit]

		return results

	def resolve_credits_to_items(
		self,
		section_name: str,
		credits: list[dict],
		media_type: str | None = None,
		limit: int | None = None,
	) -> list[PlexMatch]:
		section = self._get_section(section_name)

		expected_external_ids = {
			'tmdb': set(),
			'imdb': set(),
			'tvdb': set(),
		}
		expected_titles_with_year: set[tuple[str, int | None]] = set()
		expected_titles_without_year: set[str] = set()

		for credit in credits:
			credit_media_type = credit.get('media_type')
			if media_type and credit_media_type != media_type:
				continue

			credit_external_ids = self._credit_external_ids(credit)
			for provider, values in credit_external_ids.items():
				expected_external_ids[provider].update(values)

			candidate_titles = media_candidate_titles(credit)
			if not candidate_titles:
				continue

			credit_year = media_year(credit)

			for candidate_title in candidate_titles:
				expected_titles_without_year.add(candidate_title)
				expected_titles_with_year.add((candidate_title, credit_year))

		results: list[PlexMatch] = []
		seen_keys: set[str] = set()

		try:
			with self._temporary_request_timeout(PLEX_FULL_SCAN_REQUEST_TIMEOUT):
				items = section.all()
		except Exception:
			return []

		for item in items:
			rating_key = str(getattr(item, 'ratingKey', ''))
			if not rating_key or rating_key in seen_keys:
				continue

			item_external_ids = self._item_external_ids(item)
			has_external_match = any(
				item_external_ids.get(provider, set()) & expected_external_ids.get(provider, set())
				for provider in ('tmdb', 'imdb', 'tvdb')
			)

			if has_external_match:
				seen_keys.add(rating_key)
				results.append(
					PlexMatch(
						item=item,
						title=getattr(item, 'title', 'Unknown'),
						year=self._item_year(item),
						match_source='external_id',
					)
				)
				if limit and len(results) >= limit:
					return results[:limit]
				continue

			item_titles = self._item_titles(item)
			item_year = self._item_year(item)

			if item_year is not None:
				has_title_match = any(
					(item_title, item_year) in expected_titles_with_year
					for item_title in item_titles
				)
			else:
				has_title_match = any(
					item_title in expected_titles_without_year
					for item_title in item_titles
				)

			if not has_title_match:
				continue

			seen_keys.add(rating_key)
			results.append(
				PlexMatch(
					item=item,
					title=getattr(item, 'title', 'Unknown'),
					year=item_year,
					match_source='title_fallback',
				)
			)

			if limit and len(results) >= limit:
				return results[:limit]

		return results

	def find_items_by_credit_titles(
		self,
		section_name: str,
		credits: list[dict],
		media_type: str | None = None,
		limit: int | None = None,
	) -> list[PlexMatch]:
		section = self._get_section(section_name)
		libtype = self._section_libtype(section)

		years_by_title: dict[str, set[int | None]] = {}
		raw_titles_by_normalized: dict[str, set[str]] = {}

		for credit in credits:
			credit_media_type = credit.get("media_type")
			if media_type and credit_media_type != media_type:
				continue

			raw_title = (credit.get("title") or credit.get("name") or "").strip()
			title = _normalize_text(raw_title)
			if not title:
				continue

			raw_date = credit.get("release_date") or credit.get("first_air_date") or ""
			year = int(raw_date[:4]) if raw_date[:4].isdigit() else None

			years_by_title.setdefault(title, set()).add(year)
			raw_titles_by_normalized.setdefault(title, set()).add(raw_title)

		if not years_by_title:
			return []

		results: list[PlexMatch] = []
		seen_keys: set[str] = set()
		per_title_limit = PLEX_TITLE_SEARCH_LIMIT

		for normalized_title, expected_years in years_by_title.items():
			candidate_raw_titles = sorted(
				raw_titles_by_normalized.get(normalized_title) or [],
				key=len,
				reverse=True,
			)

			for raw_title in candidate_raw_titles:
				try:
					items = section.search(
						title=raw_title,
						libtype=libtype,
						maxresults=per_title_limit,
					)
				except Exception:
					items = []

				for item in items:
					item_titles = self._item_titles(item)
					if normalized_title not in item_titles:
						continue

					item_year = self._item_year(item)
					if item_year is not None and None not in expected_years and item_year not in expected_years:
						continue

					rating_key = str(getattr(item, "ratingKey", ""))
					if not rating_key or rating_key in seen_keys:
						continue

					seen_keys.add(rating_key)
					results.append(
						PlexMatch(
							item=item,
							title=getattr(item, "title", "Unknown"),
							year=item_year,
							match_source="tmdb_title",
						)
					)

					if limit and len(results) >= limit:
						return results[:limit]

				if limit and len(results) >= limit:
					return results[:limit]

		return results

	def find_items_by_credit_titles_via_scan(
		self,
		section_name: str,
		credits: list[dict],
		media_type: str | None = None,
		limit: int | None = None,
	) -> list[PlexMatch]:
		section = self._get_section(section_name)

		expected_titles: dict[str, set[int | None]] = {}

		for credit in credits:
			credit_media_type = credit.get("media_type")
			if media_type and credit_media_type != media_type:
				continue

			raw_title = (credit.get("title") or credit.get("name") or "").strip()
			normalized_title = _normalize_text(raw_title)
			if not normalized_title:
				continue

			raw_date = credit.get("release_date") or credit.get("first_air_date") or ""
			year = int(raw_date[:4]) if raw_date[:4].isdigit() else None

			expected_titles.setdefault(normalized_title, set()).add(year)

		if not expected_titles:
			return []

		results: list[PlexMatch] = []
		seen_keys: set[str] = set()

		try:
			with self._temporary_request_timeout(PLEX_FULL_SCAN_REQUEST_TIMEOUT):
				items = section.all()
		except Exception:
			return []

		for item in items:
			item_titles = self._item_titles(item)
			if not item_titles:
				continue

			matched_title = None
			expected_years = None

			for item_title in item_titles:
				if item_title in expected_titles:
					matched_title = item_title
					expected_years = expected_titles[item_title]
					break

			if not matched_title:
				continue

			item_year = self._item_year(item)
			if (
				item_year is not None
				and expected_years is not None
				and None not in expected_years
				and item_year not in expected_years
			):
				continue

			rating_key = str(getattr(item, "ratingKey", ""))
			if not rating_key or rating_key in seen_keys:
				continue

			seen_keys.add(rating_key)
			results.append(
				PlexMatch(
					item=item,
					title=getattr(item, "title", "Unknown"),
					year=item_year,
					match_source="tmdb_title_scan",
				)
			)

			if limit and len(results) >= limit:
				return results[:limit]

		return results

	def upsert_collection(
		self,
		section_name: str,
		title: str,
		summary: str,
		items: Iterable[object],
		publish_on_home: bool = False,
		publish_on_friends_home: bool = False,
		poster_url: str | None = None,
	) -> tuple[str | None, int, str]:
		section = self._get_section(section_name)
		items = list(items)

		if not items:
			return None, 0, "No matching items found in Plex"

		existing = None
		for collection in section.collections():
			if collection.title == title:
				existing = collection
				break

		if existing:
			try:
				current_items = {item.ratingKey: item for item in existing.items()}
				desired_items = {item.ratingKey: item for item in items}

				to_add = [
					item for key, item in desired_items.items()
					if key not in current_items
				]
				to_remove = [
					item for key, item in current_items.items()
					if key not in desired_items
				]

				if to_add:
					existing.addItems(to_add)
				if to_remove:
					existing.removeItems(to_remove)

				try:
					existing.editSummary(summary)
					existing.editSortTitle(_default_collection_sort_title(title))

					if poster_url:
						existing.uploadPoster(url=poster_url)
				except Exception:
					pass

				try:
					existing.reload()
				except Exception:
					pass

				message = "Updated existing collection"
				publish_message = self._apply_collection_visibility(
					existing,
					publish_on_home=publish_on_home,
					publish_on_friends_home=publish_on_friends_home,
				)
				if publish_message:
					message = f"{message} ({publish_message})"

				return str(existing.ratingKey), len(items), message

			except Exception as exc:
				return None, 0, f"Collection update failed: {exc}"

		try:
			collection = section.createCollection(
				title=title,
				items=items,
				summary=summary,
			)

			try:
				collection.editSummary(summary)
				collection.editSortTitle(_default_collection_sort_title(title))

				if poster_url:
					collection.uploadPoster(url=poster_url)
			except Exception:
				pass

			try:
				collection.reload()
			except Exception:
				pass

			message = "Created collection"
			publish_message = self._apply_collection_visibility(
				collection,
				publish_on_home=publish_on_home,
				publish_on_friends_home=publish_on_friends_home,
			)
			if publish_message:
				message = f"{message} ({publish_message})"

			return str(collection.ratingKey), len(items), message

		except Exception as exc:
			return None, 0, f"Collection creation failed: {exc}"

	def _apply_collection_visibility(
		self,
		collection,
		*,
		publish_on_home: bool,
		publish_on_friends_home: bool,
	) -> str:
		if not publish_on_home and not publish_on_friends_home:
			return "visibility: disabled"

		last_error = None

		for attempt in range(2):
			try:
				if attempt:
					time.sleep(0.6)
					try:
						collection.reload()
					except Exception:
						pass

				managed_hub = collection.visibility()
				if managed_hub is None:
					last_error = "managed hub not found"
					continue

				managed_hub.updateVisibility(
					home=publish_on_home,
					shared=publish_on_friends_home,
				).reload()

				return (
					f"visibility: home={'on' if publish_on_home else 'off'}, "
					f"friends={'on' if publish_on_friends_home else 'off'}"
				)
			except Exception as exc:
				last_error = str(exc)

		return f"visibility update failed: {last_error}" if last_error else "visibility update failed"

	def delete_collection_by_key(
		self,
		section_name: str,
		collection_key: str | None,
		fallback_title: str | None = None,
	) -> str:
		section = self._get_section(section_name)
		collections = list(section.collections())

		target = None
		target_key = str(collection_key).strip() if collection_key else ""
		normalized_fallback_title = _normalize_text(fallback_title)

		if collection_key:
			for collection in collections:
				if str(getattr(collection, "ratingKey", "")) == str(collection_key):
					target = collection
					break

		if target is None and fallback_title:
			for collection in collections:
				if (collection.title or "").strip() == fallback_title.strip():
					target = collection
					break

		if target is None and normalized_fallback_title:
			for collection in collections:
				if _normalize_text(collection.title) == normalized_fallback_title:
					target = collection
					break

		if target is None:
			available_titles = sorted(
				{
					(collection.title or "").strip()
					for collection in collections
					if (collection.title or "").strip()
				}
			)
			return (
				"Collection not found in Plex "
				f"(section={section_name}, "
				f"key={collection_key or ''} | "
				f"title={fallback_title or ''}, "
				f"available={available_titles[:30]})"
			)

		target_title = (getattr(target, "title", "") or "").strip()

		try:
			target.delete()
		except Exception as exc:
			return f"Collection deletion failed: {exc}"

		remaining = list(section.collections())

		for collection in remaining:
			if target_key and str(getattr(collection, "ratingKey", "")) == target_key:
				return "Collection still present after deletion"

		if target_title:
			normalized_target_title = _normalize_text(target_title)
			for collection in remaining:
				if _normalize_text(collection.title) == normalized_target_title:
					return "Collection still present after deletion"

		return "Collection deleted"