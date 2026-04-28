from __future__ import annotations

import json
import unicodedata
from datetime import datetime

from ..extensions import db
from ..models import LibraryTarget, PlexMediaIndex
from .plex_service import PlexService
from .media_identity_service import media_candidate_titles, media_year


INDEX_BATCH_SIZE = 500


def _normalize_text(value: str | None) -> str:
    if not value:
        return ''

    value = unicodedata.normalize('NFKD', value)
    value = ''.join(ch for ch in value if not unicodedata.combining(ch))
    value = ''.join(ch.lower() if ch.isalnum() else ' ' for ch in value)
    return ' '.join(value.split())


def _load_json_list(value: str | None) -> list[str]:
    try:
        data = json.loads(value or '[]')
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    output: list[str] = []
    seen: set[str] = set()

    for item in data:
        clean_item = str(item or '').strip()
        if not clean_item or clean_item in seen:
            continue
        seen.add(clean_item)
        output.append(clean_item)

    return output


def _flush_index_batch(batch: list[PlexMediaIndex]) -> int:
    if not batch:
        return 0

    db.session.bulk_save_objects(batch)
    count = len(batch)
    batch.clear()
    return count


def refresh_library_local_index(
    target: LibraryTarget,
    *,
    task_run_id: int | None = None,
    progress_callback=None,
) -> dict:
    plex = PlexService(
        target.plex_server.base_url,
        target.plex_server.token,
        target.plex_server.verify_ssl,
    )

    now = datetime.utcnow()

    target.plex_titles_cache_status = 'running'
    target.plex_titles_cache_error = None
    db.session.commit()

    PlexMediaIndex.query.filter_by(target_id=target.id).delete(synchronize_session=False)
    db.session.commit()

    keys_with_year: set[tuple[str, int | None]] = set()
    keys_without_year: set[str] = set()
    external_ids = {
        'tmdb': set(),
        'imdb': set(),
        'tvdb': set(),
    }

    batch: list[PlexMediaIndex] = []
    processed_items = 0

    for item in plex.list_library_items_for_index(
        target.section_name,
        include_people=False,
    ):
        processed_items += 1

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

        batch.append(
            PlexMediaIndex(
                target_id=target.id,
                plex_server_id=target.plex_server_id,
                rating_key=str(item.get('rating_key') or '').strip(),
                media_type=str(item.get('media_type') or '').strip() or target.media_type,
                title=str(item.get('title') or '').strip() or 'Unknown',
                original_title=(str(item.get('original_title') or '').strip() or None),
                year=year,
                tmdb_id=(str(item.get('tmdb_id') or '').strip() or None),
                imdb_id=(str(item.get('imdb_id') or '').strip() or None),
                tvdb_id=(str(item.get('tvdb_id') or '').strip() or None),
                raw_titles_json=json.dumps(item.get('raw_titles') or [], ensure_ascii=False),
                normalized_titles_json=json.dumps(item.get('normalized_titles') or [], ensure_ascii=False),
                normalized_people_json='[]',
                scanned_at=now,
            )
        )

        if len(batch) >= INDEX_BATCH_SIZE:
            _flush_index_batch(batch)
            db.session.commit()

            if progress_callback:
                progress_callback(
                    processed_items=processed_items,
                    success_items=processed_items,
                    error_items=0,
                    message=(
                        f'Plex cache refresh running... '
                        f'{processed_items} item(s) indexed on '
                        f'{target.plex_server.name} / {target.section_name}.'
                    ),
                )

    _flush_index_batch(batch)

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
    target.plex_titles_cached_at = now
    target.plex_titles_cache_status = 'ready'
    target.plex_titles_cache_error = None
    db.session.commit()

    return {
        'status': 'ready',
        'items_count': processed_items,
    }


def refresh_library_local_index_safe(
    target: LibraryTarget,
    *,
    task_run_id: int | None = None,
    progress_callback=None,
) -> dict:
    try:
        return refresh_library_local_index(
            target,
            task_run_id=task_run_id,
            progress_callback=progress_callback,
        )
    except Exception as exc:
        db.session.rollback()

        target = LibraryTarget.query.get(target.id)
        if target:
            target.plex_titles_cache_status = 'error'
            target.plex_titles_cache_error = str(exc)
            target.plex_titles_cached_at = datetime.utcnow()
            db.session.commit()

        return {
            'status': 'error',
            'items_count': 0,
            'error': str(exc),
        }


def _credit_external_ids(credit: dict) -> dict[str, set[str]]:
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
        clean_imdb_id = str(imdb_id).strip()
        if clean_imdb_id:
            external_ids['imdb'].add(clean_imdb_id)

    tvdb_id = credit.get('tvdb_id')
    if tvdb_id is not None:
        clean_tvdb_id = str(tvdb_id).strip()
        if clean_tvdb_id:
            external_ids['tvdb'].add(clean_tvdb_id)

    return external_ids


def find_local_matches_for_target(
    target: LibraryTarget,
    *,
    person_name: str,
    aliases: list[str] | None = None,
    tmdb_credits: dict | None = None,
    media_type: str | None = None,
) -> list[dict]:
    plex_media_type = 'movie' if target.media_type == 'movie' else 'show'
    credit_media_type = media_type or ('movie' if target.media_type == 'movie' else 'tv')

    rows = (
        PlexMediaIndex.query
        .filter_by(
            target_id=target.id,
            media_type=plex_media_type,
        )
        .all()
    )

    if not rows:
        return []

    candidate_names: set[str] = set()
    for raw_name in [person_name] + (aliases or []):
        normalized_name = _normalize_text(raw_name)
        if normalized_name:
            candidate_names.add(normalized_name)

    expected_external_ids = {
        'tmdb': set(),
        'imdb': set(),
        'tvdb': set(),
    }
    expected_titles_with_year: set[tuple[str, int | None]] = set()
    expected_titles_without_year: set[str] = set()

    credits = []
    if tmdb_credits:
        credits = (tmdb_credits.get('cast') or []) + (tmdb_credits.get('crew') or [])

    for credit in credits:
        if credit_media_type and credit.get('media_type') != credit_media_type:
            continue

        credit_external_ids = _credit_external_ids(credit)
        for provider, values in credit_external_ids.items():
            expected_external_ids[provider].update(values)

        credit_titles = media_candidate_titles(credit)
        credit_year = media_year(credit)

        for credit_title in credit_titles:
            expected_titles_without_year.add(credit_title)
            expected_titles_with_year.add((credit_title, credit_year))

    matches_by_rating_key: dict[str, dict] = {}

    for row in rows:
        row_titles = set(_load_json_list(row.normalized_titles_json))

        row_external_ids = {
            'tmdb': {row.tmdb_id} if row.tmdb_id else set(),
            'imdb': {row.imdb_id} if row.imdb_id else set(),
            'tvdb': {row.tvdb_id} if row.tvdb_id else set(),
        }

        score = 0
        reasons: list[str] = []

        if any(
            row_external_ids.get(provider, set()) & expected_external_ids.get(provider, set())
            for provider in ('tmdb', 'imdb', 'tvdb')
        ):
            score += 50
            reasons.append('external_cache')

        has_title_match = False

        if row.year is not None:
            has_title_match = any((item_title, row.year) in expected_titles_with_year for item_title in row_titles)

        if not has_title_match:
            has_title_match = any(item_title in expected_titles_without_year for item_title in row_titles)

        if has_title_match:
            score += 10
            reasons.append('title_cache')

        if score <= 0:
            continue

        existing = matches_by_rating_key.get(row.rating_key)
        if existing and int(existing.get('score') or 0) >= score:
            continue

        matches_by_rating_key[row.rating_key] = {
            'rating_key': row.rating_key,
            'title': row.title,
            'year': row.year,
            'raw_titles': _load_json_list(row.raw_titles_json),
            'normalized_titles': list(row_titles),
            'match_source': ','.join(reasons),
            'score': score,
        }

    return sorted(
        matches_by_rating_key.values(),
        key=lambda item: (
            int(item.get('score') or 0),
            str(item.get('title') or ''),
            int(item.get('year') or 0),
        ),
        reverse=True,
    )