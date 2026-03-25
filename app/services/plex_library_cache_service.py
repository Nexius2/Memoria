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