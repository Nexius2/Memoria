from __future__ import annotations

import json
from datetime import datetime, timedelta

from ..extensions import db
from ..models import LibraryTarget
from .plex_service import PlexService


def load_library_title_cache(target: LibraryTarget) -> tuple[set[tuple[str, int | None]], set[str]]:
    try:
        payload = json.loads(target.plex_titles_cache_json or '{}')
    except Exception:
        payload = {}

    raw_keys_with_year = payload.get('keys_with_year') or []
    raw_keys_without_year = payload.get('keys_without_year') or []

    keys_with_year: set[tuple[str, int | None]] = set()
    keys_without_year: set[str] = set()

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

    return keys_with_year, keys_without_year


def refresh_library_title_cache(target: LibraryTarget) -> tuple[set[tuple[str, int | None]], set[str]]:
    plex = PlexService(
        target.plex_server.base_url,
        target.plex_server.token,
        target.plex_server.verify_ssl,
    )

    keys_with_year, keys_without_year = plex.list_title_keys(target.section_name)

    payload = {
        'keys_with_year': [[title, year] for title, year in sorted(keys_with_year)],
        'keys_without_year': sorted(keys_without_year),
    }

    target.plex_titles_cache_json = json.dumps(payload, ensure_ascii=False)
    target.plex_titles_cached_at = datetime.utcnow()
    target.plex_titles_cache_status = 'ready'
    target.plex_titles_cache_error = None
    db.session.commit()

    return keys_with_year, keys_without_year


def refresh_library_title_cache_safe(target: LibraryTarget) -> tuple[set[tuple[str, int | None]], set[str]]:
    try:
        return refresh_library_title_cache(target)
    except Exception as exc:
        target.plex_titles_cache_status = 'error'
        target.plex_titles_cache_error = str(exc)
        target.plex_titles_cached_at = datetime.utcnow()
        db.session.commit()
        return load_library_title_cache(target)


def is_library_cache_due(target: LibraryTarget, refresh_hours: int = 24) -> bool:
    refresh_hours = max(int(refresh_hours or 24), 1)

    if not target.plex_titles_cached_at:
        return True

    cutoff = datetime.utcnow() - timedelta(hours=refresh_hours)
    return target.plex_titles_cached_at <= cutoff


def get_library_title_cache(target: LibraryTarget) -> tuple[set[tuple[str, int | None]], set[str]]:
    if target.plex_titles_cache_status == 'ready' and target.plex_titles_cached_at:
        return load_library_title_cache(target)

    return refresh_library_title_cache_safe(target)


def filter_credits_with_library_cache(
    target: LibraryTarget,
    credits: list[dict],
    *,
    media_type: str | None = None,
) -> list[dict]:
    keys_with_year, keys_without_year = get_library_title_cache(target)
    filtered: list[dict] = []
    seen_keys: set[tuple[str, int | None, str | None]] = set()

    for credit in credits:
        credit_media_type = credit.get('media_type')
        if media_type and credit_media_type != media_type:
            continue

        raw_title = (credit.get('title') or credit.get('name') or '').strip()
        if not raw_title:
            continue

        normalized_title = ''.join(ch.lower() if ch.isalnum() else ' ' for ch in raw_title)
        normalized_title = ' '.join(normalized_title.split())
        if not normalized_title:
            continue

        raw_date = credit.get('release_date') or credit.get('first_air_date') or ''
        year = int(raw_date[:4]) if raw_date[:4].isdigit() else None

        has_match = (
            (normalized_title, year) in keys_with_year
            or normalized_title in keys_without_year
        )

        if not has_match:
            continue

        dedupe_key = (normalized_title, year, credit_media_type)
        if dedupe_key in seen_keys:
            continue

        seen_keys.add(dedupe_key)
        filtered.append(credit)

    return filtered