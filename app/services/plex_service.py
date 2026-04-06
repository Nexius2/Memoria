from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import threading
import unicodedata
import time

import requests
from plexapi.server import PlexServer as PlexApiServer


PLEX_REQUEST_TIMEOUT = 20
PLEX_TITLE_SEARCH_LIMIT = 12
PLEX_PERSON_SEARCH_LIMIT = 100
PLEX_MIN_REQUEST_INTERVAL = 0.2


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

    def _section_libtype(self, section) -> str:
        return "movie" if getattr(section, "type", None) == "movie" else "show"

    def list_title_keys(self, section_name: str) -> tuple[set[tuple[str, int | None]], set[str]]:
        section = self._get_section(section_name)
        keys_with_year: set[tuple[str, int | None]] = set()
        keys_without_year: set[str] = set()

        for item in section.all():
            item_year = self._item_year(item)
            for title in self._item_titles(item):
                keys_without_year.add(title)
                keys_with_year.add((title, item_year))

        return keys_with_year, keys_without_year

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

                except Exception as exc:
                    return (
                        str(getattr(existing, "ratingKey", "")),
                        len(items),
                        f"Collection updated, but metadata update failed: {exc}",
                    )

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
                return (
                    str(getattr(existing, "ratingKey", "")),
                    len(items),
                    f"Collection update failed: {exc}",
                )

        try:
            collection = section.createCollection(
                title=title,
                items=items,
            )

            try:
                collection.editSummary(summary)
                collection.editSortTitle(_default_collection_sort_title(title))

                if poster_url:
                    collection.uploadPoster(url=poster_url)

            except Exception as exc:
                return (
                    str(getattr(collection, "ratingKey", "")),
                    len(items),
                    f"Collection created, but metadata update failed: {exc}",
                )

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
                    "visibility updated: "
                    f"home={'yes' if publish_on_home else 'no'}, "
                    f"friends_home={'yes' if publish_on_friends_home else 'no'}"
                )

            except Exception as exc:
                last_error = exc

        return f"visibility update failed: {last_error}"

    def delete_collection_by_key(
        self,
        section_name: str,
        collection_key: str | None,
        fallback_title: str | None = None,
    ) -> str:
        section = self._get_section(section_name)
        collections = list(section.collections())
        target = None

        normalized_fallback_title = _normalize_text(fallback_title)

        if collection_key:
            for collection in collections:
                if str(getattr(collection, "ratingKey", "")) == str(collection_key):
                    target = collection
                    break

        if not target and fallback_title:
            for collection in collections:
                if (collection.title or "").strip() == fallback_title.strip():
                    target = collection
                    break

        if not target and normalized_fallback_title:
            for collection in collections:
                if _normalize_text(collection.title) == normalized_fallback_title:
                    target = collection
                    break

        if not target:
            available_titles = ", ".join(
                sorted(
                    {
                        (collection.title or "").strip()
                        for collection in collections
                        if (collection.title or "").strip()
                    }
                )
            )
            return (
                "Collection not found | "
                f"section={section_name} | "
                f"key={collection_key or ''} | "
                f"title={fallback_title or ''} | "
                f"available={available_titles}"
            )

        target_title = target.title or fallback_title or ""
        target_key = str(getattr(target, "ratingKey", "") or "")

        target.delete()

        remaining = list(section.collections())

        for collection in remaining:
            if target_key and str(getattr(collection, "ratingKey", "")) == target_key:
                return (
                    "Collection delete verification failed | "
                    f"section={section_name} | key={target_key} | title={target_title}"
                )

        normalized_target_title = _normalize_text(target_title)
        if normalized_target_title:
            for collection in remaining:
                if _normalize_text(collection.title) == normalized_target_title:
                    return (
                        "Collection delete verification failed | "
                        f"section={section_name} | key={target_key} | title={target_title}"
                    )

        return (
            "Collection deleted | "
            f"section={section_name} | key={target_key} | title={target_title}"
        )