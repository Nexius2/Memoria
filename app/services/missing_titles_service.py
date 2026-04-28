from __future__ import annotations

import json
from datetime import datetime, timedelta

from ..extensions import db
from ..models import AppSettings, LibraryTarget, Person, PlexServer
from .tmdb_service import TmdbService
from .plex_library_cache_service import (
	load_library_title_cache,
	refresh_library_title_cache_safe,
)
from .media_identity_service import (
	credit_matches_external_ids,
	credit_matches_title_keys,
	enrich_credit_list_external_ids,
	item_external_ids,
	media_year,
	normalize_media_title,
	primary_media_title,
)





def _sort_key(item: dict):
	item_date = item.get('release_date') or item.get('first_air_date') or ''
	item_title = item.get('title') or item.get('name') or ''
	return (item_date or '0000-00-00', item_title.lower())


def _dedupe(items: list[dict]) -> list[dict]:
	seen = set()
	output = []

	for item in items:
		external_ids = item_external_ids(item)

		primary_external_key = None
		for provider in ('tmdb', 'imdb', 'tvdb'):
			values = sorted(external_ids.get(provider) or [])
			if values:
				primary_external_key = f'{provider}:{values[0]}'
				break

		if primary_external_key:
			key = ('external', primary_external_key)
		else:
			year_int = media_year(item)
			key = ('title', normalize_media_title(primary_media_title(item)), year_int, item.get('media_type'))

		if key in seen:
			continue

		seen.add(key)
		output.append({
			'id': item.get('id'),
			'media_type': item.get('media_type'),
			'title': item.get('title'),
			'name': item.get('name'),
			'original_title': item.get('original_title'),
			'original_name': item.get('original_name'),
			'release_date': item.get('release_date'),
			'first_air_date': item.get('first_air_date'),
			'imdb_id': item.get('imdb_id'),
			'tvdb_id': item.get('tvdb_id'),
		})

	output.sort(key=_sort_key, reverse=True)
	return output





def _compute_missing_titles(tmdb_credits: dict) -> tuple[list[dict], list[dict]]:
	plex_movie_keys: set[tuple[str, int | None]] = set()
	plex_movie_titles: set[str] = set()
	plex_show_keys: set[tuple[str, int | None]] = set()
	plex_show_titles: set[str] = set()
	plex_movie_external_ids = {
		'tmdb': set(),
		'imdb': set(),
		'tvdb': set(),
	}
	plex_show_external_ids = {
		'tmdb': set(),
		'imdb': set(),
		'tvdb': set(),
	}

	targets = (
		LibraryTarget.query
		.join(PlexServer, LibraryTarget.plex_server_id == PlexServer.id)
		.filter(
			LibraryTarget.enabled.is_(True),
			PlexServer.enabled.is_(True),
		)
		.all()
	)

	for target in targets:
		try:
			if target.plex_titles_cache_status == 'ready' and target.plex_titles_cached_at:
				keys_with_year, keys_without_year, external_ids = load_library_title_cache(target)
			else:
				keys_with_year, keys_without_year, external_ids = refresh_library_title_cache_safe(target)

			if target.media_type == 'movie':
				plex_movie_keys |= keys_with_year
				plex_movie_titles |= keys_without_year
				for provider in ('tmdb', 'imdb', 'tvdb'):
					plex_movie_external_ids[provider] |= external_ids.get(provider, set())
			else:
				plex_show_keys |= keys_with_year
				plex_show_titles |= keys_without_year
				for provider in ('tmdb', 'imdb', 'tvdb'):
					plex_show_external_ids[provider] |= external_ids.get(provider, set())

		except Exception:
			continue

	missing_movies = []
	missing_shows = []

	for item in (tmdb_credits.get('cast') or []) + (tmdb_credits.get('crew') or []):
		media_type = item.get('media_type')
		title = item.get('title') or item.get('name')

		if not title:
			continue

		if media_type == 'movie':
			has_external_match = credit_matches_external_ids(item, plex_movie_external_ids)
			has_title_match = credit_matches_title_keys(item, plex_movie_keys, plex_movie_titles)

			if not has_external_match and not has_title_match:
				missing_movies.append(item)

		elif media_type == 'tv':
			has_external_match = credit_matches_external_ids(item, plex_show_external_ids)
			has_title_match = credit_matches_title_keys(item, plex_show_keys, plex_show_titles)

			if not has_external_match and not has_title_match:
				missing_shows.append(item)

	return _dedupe(missing_movies), _dedupe(missing_shows)


def load_person_missing_titles(person: Person) -> tuple[list[dict], list[dict]]:
	try:
		movies = json.loads(person.missing_titles_movies_json or '[]')
	except Exception:
		movies = []

	try:
		shows = json.loads(person.missing_titles_shows_json or '[]')
	except Exception:
		shows = []

	if not isinstance(movies, list):
		movies = []

	if not isinstance(shows, list):
		shows = []

	return movies, shows


def refresh_person_missing_titles(
	person: Person,
	*,
	settings: AppSettings | None = None,
	tmdb_credits: dict | None = None,
) -> tuple[list[dict], list[dict]]:
	settings = settings or AppSettings.get_or_create()

	if not settings.tmdb_api_key:
		person.missing_titles_status = 'disabled'
		person.missing_titles_error = 'TMDb API key is not configured.'
		person.missing_titles_scanned_at = None
		db.session.commit()
		return load_person_missing_titles(person)

	try:
		tmdb = TmdbService(settings.tmdb_api_key)

		if tmdb_credits is None:
			if not person.tmdb_person_id:
				match = tmdb.search_person(
					person.name,
					death_date=person.death_date.isoformat() if person.death_date else None,
				)
				if not match or not match.get('id'):
					person.missing_titles_status = 'error'
					person.missing_titles_error = 'No TMDb match found.'
					person.missing_titles_scanned_at = datetime.utcnow()
					db.session.commit()
					return load_person_missing_titles(person)

				person.tmdb_person_id = match.get('id')

			tmdb_credits = tmdb.person_credits(person.tmdb_person_id)

		if tmdb_credits is not None:
			tmdb_credits = {
				'cast': enrich_credit_list_external_ids(tmdb_credits.get('cast') or [], tmdb=tmdb),
				'crew': enrich_credit_list_external_ids(tmdb_credits.get('crew') or [], tmdb=tmdb),
			}

		missing_movies, missing_shows = _compute_missing_titles(tmdb_credits)

		person.missing_titles_movies_json = json.dumps(missing_movies, ensure_ascii=False)
		person.missing_titles_shows_json = json.dumps(missing_shows, ensure_ascii=False)
		person.missing_titles_status = 'ready'
		person.missing_titles_error = None
		person.missing_titles_scanned_at = datetime.utcnow()

		db.session.commit()
		return missing_movies, missing_shows

	except Exception as exc:
		person.missing_titles_status = 'error'
		person.missing_titles_error = str(exc)
		person.missing_titles_scanned_at = datetime.utcnow()
		db.session.commit()
		return load_person_missing_titles(person)


def is_missing_titles_refresh_due(person: Person, refresh_hours: int) -> bool:
	refresh_hours = max(int(refresh_hours or 24), 1)

	if not person.missing_titles_scanned_at:
		return True

	cutoff = datetime.utcnow() - timedelta(hours=refresh_hours)
	return person.missing_titles_scanned_at <= cutoff