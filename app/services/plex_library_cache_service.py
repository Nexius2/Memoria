from __future__ import annotations

import json
from datetime import datetime, timedelta

from ..extensions import db
from ..models import LibraryTarget
from .plex_service import PlexService
from .media_identity_service import filter_credits_against_library_cache


def load_library_title_cache(
	target: LibraryTarget,
) -> tuple[
	set[tuple[str, int | None]],
	set[str],
	dict[str, set[str]],
]:
	try:
		payload = json.loads(target.plex_titles_cache_json or '{}')
	except Exception:
		payload = {}

	raw_keys_with_year = payload.get('keys_with_year') or []
	raw_keys_without_year = payload.get('keys_without_year') or []
	raw_external_ids = payload.get('external_ids') or {}

	keys_with_year: set[tuple[str, int | None]] = set()
	keys_without_year: set[str] = set()
	external_ids = {
		'tmdb': set(),
		'imdb': set(),
		'tvdb': set(),
	}

	for item in raw_keys_with_year:
		if not isinstance(item, list) or len(item) != 2:
			continue

		title = item[0]
		year = item[1]

		if not isinstance(title, str) or not title:
			continue

		if year is not None:
			try:
				year = int(year)
			except (TypeError, ValueError):
				year = None

		keys_with_year.add((title, year))

	for item in raw_keys_without_year:
		if isinstance(item, str) and item:
			keys_without_year.add(item)

	if isinstance(raw_external_ids, dict):
		for provider in ('tmdb', 'imdb', 'tvdb'):
			values = raw_external_ids.get(provider) or []
			if isinstance(values, list):
				for value in values:
					clean_value = str(value or '').strip()
					if clean_value:
						external_ids[provider].add(clean_value)

	return keys_with_year, keys_without_year, external_ids


def refresh_library_title_cache(
	target: LibraryTarget,
) -> tuple[
	set[tuple[str, int | None]],
	set[str],
	dict[str, set[str]],
]:
	plex = PlexService(
		target.plex_server.base_url,
		target.plex_server.token,
		target.plex_server.verify_ssl,
	)

	keys_with_year: set[tuple[str, int | None]] = set()
	keys_without_year: set[str] = set()
	external_ids = {
		'tmdb': set(),
		'imdb': set(),
		'tvdb': set(),
	}

	for item in plex.list_library_items_for_index(
		target.section_name,
		include_people=False,
	):
		year = item.get('year')

		for normalized_title in item.get('normalized_titles') or []:
			keys_without_year.add(normalized_title)
			keys_with_year.add((normalized_title, year))

		item_external_ids = item.get('external_ids') or {}
		for provider in ('tmdb', 'imdb', 'tvdb'):
			for value in item_external_ids.get(provider) or []:
				clean_value = str(value or '').strip()
				if clean_value:
					external_ids[provider].add(clean_value)

	sorted_keys_with_year = sorted(
		keys_with_year,
		key=lambda item: (item[0], item[1] is None, item[1] if item[1] is not None else 0),
	)

	payload = {
		'keys_with_year': [[title, year] for title, year in sorted_keys_with_year],
		'keys_without_year': sorted(keys_without_year),
		'external_ids': {
			'tmdb': sorted(external_ids.get('tmdb') or []),
			'imdb': sorted(external_ids.get('imdb') or []),
			'tvdb': sorted(external_ids.get('tvdb') or []),
		},
	}

	target.plex_titles_cache_json = json.dumps(payload, ensure_ascii=False)
	target.plex_titles_cached_at = datetime.utcnow()
	target.plex_titles_cache_status = 'ready'
	target.plex_titles_cache_error = None
	db.session.commit()

	return keys_with_year, keys_without_year, external_ids


def refresh_library_title_cache_safe(
	target: LibraryTarget,
) -> tuple[
	set[tuple[str, int | None]],
	set[str],
	dict[str, set[str]],
]:
	try:
		return refresh_library_title_cache(target)
	except Exception as exc:
		target.plex_titles_cache_status = 'error'
		target.plex_titles_cache_error = str(exc)
		target.plex_titles_cached_at = datetime.utcnow()
		db.session.commit()
		return load_library_title_cache(target)


def is_library_cache_due(target: LibraryTarget, refresh_hours: int = 12) -> bool:
	refresh_hours = max(int(refresh_hours or 24), 1)

	if not target.plex_titles_cached_at:
		return True

	cutoff = datetime.utcnow() - timedelta(hours=refresh_hours)
	return target.plex_titles_cached_at <= cutoff


def get_library_title_cache(
	target: LibraryTarget,
) -> tuple[
	set[tuple[str, int | None]],
	set[str],
	dict[str, set[str]],
]:
	if target.plex_titles_cache_status == 'ready' and target.plex_titles_cached_at:
		return load_library_title_cache(target)

	return refresh_library_title_cache_safe(target)





def filter_credits_with_library_cache(
	target: LibraryTarget,
	credits: list[dict],
	*,
	media_type: str | None = None,
) -> list[dict]:
	keys_with_year, keys_without_year, cache_external_ids = get_library_title_cache(target)
	return filter_credits_against_library_cache(
		credits,
		keys_with_year=keys_with_year,
		keys_without_year=keys_without_year,
		cache_external_ids=cache_external_ids,
		media_type=media_type,
	)