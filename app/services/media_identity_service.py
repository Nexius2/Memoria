from __future__ import annotations

from .tmdb_service import TmdbService


def normalize_media_title(value: str | None) -> str:
	clean = ''.join(ch.lower() if ch.isalnum() else ' ' for ch in (value or ''))
	return ' '.join(clean.split())


def media_year(item: dict) -> int | None:
	raw_year = (item.get('release_date') or item.get('first_air_date') or '')[:4]
	return int(raw_year) if raw_year.isdigit() else None


def media_candidate_titles(item: dict) -> set[str]:
	titles: set[str] = set()

	for key in ('title', 'name', 'original_title', 'original_name'):
		normalized = normalize_media_title(item.get(key))
		if normalized:
			titles.add(normalized)

	return titles


def primary_media_title(item: dict) -> str:
	for key in ('title', 'name', 'original_title', 'original_name'):
		value = (item.get(key) or '').strip()
		if value:
			return value
	return ''


def item_external_ids(item: dict) -> dict[str, set[str]]:
	external_ids = {
		'tmdb': set(),
		'imdb': set(),
		'tvdb': set(),
	}

	tmdb_id = item.get('id')
	if tmdb_id is not None:
		clean_tmdb_id = str(tmdb_id).strip()
		if clean_tmdb_id:
			external_ids['tmdb'].add(clean_tmdb_id)

	imdb_id = item.get('imdb_id')
	if imdb_id:
		external_ids['imdb'].add(str(imdb_id).strip())

	tvdb_id = item.get('tvdb_id')
	if tvdb_id is not None:
		clean_tvdb_id = str(tvdb_id).strip()
		if clean_tvdb_id:
			external_ids['tvdb'].add(clean_tvdb_id)

	return external_ids


def credit_matches_external_ids(
	credit: dict,
	cache_external_ids: dict[str, set[str]],
) -> bool:
	credit_external_ids = item_external_ids(credit)
	return any(
		credit_external_ids.get(provider, set()) & cache_external_ids.get(provider, set())
		for provider in ('tmdb', 'imdb', 'tvdb')
	)


def credit_matches_title_keys(
	credit: dict,
	keys_with_year: set[tuple[str, int | None]],
	keys_without_year: set[str],
) -> bool:
	candidate_titles = media_candidate_titles(credit)
	if not candidate_titles:
		return False

	year = media_year(credit)

	if year is not None:
		return any((title, year) in keys_with_year for title in candidate_titles)

	return any(title in keys_without_year for title in candidate_titles)


def enrich_credit_list_external_ids(
	credits: list[dict],
	*,
	tmdb: TmdbService | None = None,
) -> list[dict]:
	if not credits:
		return []

	if tmdb is None:
		return [dict(item or {}) for item in credits]

	enriched: list[dict] = []
	for credit in credits:
		current = dict(credit or {})
		if current.get('media_type') not in {'movie', 'tv'}:
			enriched.append(current)
			continue

		has_tmdb_id = current.get('id') is not None
		has_imdb_id = bool(str(current.get('imdb_id') or '').strip())
		has_tvdb_id = current.get('tvdb_id') not in (None, '')

		if has_tmdb_id and has_imdb_id and (has_tvdb_id or current.get('media_type') == 'movie'):
			enriched.append(current)
			continue

		enriched.append(tmdb.enrich_credit_external_ids(current))

	return enriched


def filter_credits_against_library_cache(
	credits: list[dict],
	*,
	keys_with_year: set[tuple[str, int | None]],
	keys_without_year: set[str],
	cache_external_ids: dict[str, set[str]],
	media_type: str | None = None,
) -> list[dict]:
	filtered: list[dict] = []
	seen_keys: set[tuple[str | None, int | None, str | None, str | None]] = set()

	for credit in credits:
		credit_media_type = credit.get('media_type')
		if media_type and credit_media_type != media_type:
			continue

		primary_title = normalize_media_title(primary_media_title(credit))
		if not primary_title:
			continue

		year = media_year(credit)
		has_external_match = credit_matches_external_ids(credit, cache_external_ids)
		has_title_match = credit_matches_title_keys(credit, keys_with_year, keys_without_year)

		if not has_external_match and not has_title_match:
			continue

		credit_external_ids = item_external_ids(credit)
		primary_external_key = None
		for provider in ('tmdb', 'imdb', 'tvdb'):
			values = sorted(credit_external_ids.get(provider) or [])
			if values:
				primary_external_key = f'{provider}:{values[0]}'
				break

		dedupe_key = (primary_title, year, credit_media_type, primary_external_key)
		if dedupe_key in seen_keys:
			continue

		seen_keys.add(dedupe_key)
		filtered.append(credit)

	return filtered