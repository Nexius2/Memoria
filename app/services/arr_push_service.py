from __future__ import annotations

from datetime import date, datetime, timedelta
from threading import Lock

from sqlalchemy import or_

from ..extensions import db
from ..models import (
    AppLog,
    AppSettings,
    ArrActivity,
    ArrServer,
    LibraryTarget,
    Person,
    PlexServer,
    TributeEvent,
)
from .arr_service import ArrService
from .missing_titles_service import load_person_missing_titles


_AUTO_ARR_PUSH_LOCK = Lock()
_ERROR_RETRY_DELAY_HOURS = 6
_INVALID_RETRY_DELAY_HOURS = 24

def _log_app_event(
    level: str,
    message: str,
    *,
    details: str | None = None,
    related_type: str | None = None,
    related_id: int | None = None,
) -> None:
    try:
        entry = AppLog(
            level=level,
            source='arr',
            message=message,
            details=details,
            related_type=related_type,
            related_id=related_id,
        )
        db.session.add(entry)
        db.session.commit()
    except Exception:
        db.session.rollback()


def _log_arr_activity(
    *,
    person: Person,
    target: LibraryTarget,
    media_kind: str,
    external_id: int | None,
    title: str,
    year: int | None,
    result: dict,
) -> ArrActivity:
    activity = ArrActivity(
        person_id=person.id,
        arr_server_id=target.arr_server_id,
        library_target_id=target.id,
        media_kind=media_kind,
        external_id=external_id,
        tmdb_id=result.get('tmdb_id'),
        tvdb_id=result.get('tvdb_id'),
        title=title,
        year=year,
        status=result.get('status') or 'error',
        message=result.get('message'),
        request_payload=result.get('request_payload'),
        response_payload=result.get('response_payload'),
    )
    db.session.add(activity)
    db.session.commit()

    try:
        log_entry = AppLog(
            level='error' if activity.status == 'error' else ('warning' if activity.status in {'invalid', 'already_exists'} else 'info'),
            source='arr',
            message=f'Arr {activity.status}: {activity.title}',
            details=activity.message,
            related_type='arr_activity',
            related_id=activity.id,
        )
        db.session.add(log_entry)
        db.session.commit()
    except Exception:
        db.session.rollback()

    return activity


def _already_handled(
    *,
    person_id: int,
    target_id: int,
    media_kind: str,
    tmdb_id: int | None,
    tvdb_id: int | None,
    title: str,
    year: int | None,
) -> bool:
    query = ArrActivity.query.filter(
        ArrActivity.person_id == person_id,
        ArrActivity.library_target_id == target_id,
        ArrActivity.media_kind == media_kind,
    )

    external_filters = []
    if tmdb_id:
        external_filters.extend([
            ArrActivity.tmdb_id == tmdb_id,
            ArrActivity.external_id == tmdb_id,
        ])
    if tvdb_id:
        external_filters.extend([
            ArrActivity.tvdb_id == tvdb_id,
            ArrActivity.external_id == tvdb_id,
        ])

    if external_filters:
        query = query.filter(or_(*external_filters))
    else:
        query = query.filter(
            ArrActivity.title == title,
            ArrActivity.year == year,
        )

    last_activity = (
        query
        .order_by(ArrActivity.created_at.desc(), ArrActivity.id.desc())
        .first()
    )

    if not last_activity:
        return False

    if last_activity.status in {'created', 'already_exists'}:
        return True

    now = datetime.utcnow()

    if last_activity.status == 'error':
        if last_activity.created_at and last_activity.created_at >= now - timedelta(hours=_ERROR_RETRY_DELAY_HOURS):
            return True

    if last_activity.status == 'invalid':
        if last_activity.created_at and last_activity.created_at >= now - timedelta(hours=_INVALID_RETRY_DELAY_HOURS):
            return True

    return False


def _load_targets_for_media_kind(media_kind: str) -> list[LibraryTarget]:
    expected_arr_kind = 'radarr' if media_kind == 'movie' else 'sonarr'

    return (
        LibraryTarget.query
        .join(PlexServer, LibraryTarget.plex_server_id == PlexServer.id)
        .join(ArrServer, LibraryTarget.arr_server_id == ArrServer.id)
        .filter(
            LibraryTarget.enabled.is_(True),
            PlexServer.enabled.is_(True),
            LibraryTarget.arr_server_id.isnot(None),
            LibraryTarget.media_type == media_kind,
            ArrServer.kind == expected_arr_kind,
            ArrServer.enabled.is_(True),
        )
        .order_by(PlexServer.name.asc(), LibraryTarget.section_name.asc())
        .all()
    )

def push_missing_titles_for_active_person_events(
    person: Person,
    *,
    settings: AppSettings | None = None,
) -> dict:
    settings = settings or AppSettings.get_or_create()

    if not settings.auto_arr_enabled:
        return {
            'enabled': False,
            'processed_items': 0,
            'created_items': 0,
            'already_exists_items': 0,
            'invalid_items': 0,
            'error_items': 0,
            'skipped_items': 0,
            'message': 'Automatic Arr push is disabled.',
        }

    today = date.today()

    active_events = (
        TributeEvent.query
        .filter(
            TributeEvent.person_id == person.id,
            TributeEvent.status == 'active',
            TributeEvent.start_date <= today,
            TributeEvent.end_date >= today,
        )
        .all()
    )

    if not active_events:
        return {
            'enabled': True,
            'processed_items': 0,
            'created_items': 0,
            'already_exists_items': 0,
            'invalid_items': 0,
            'error_items': 0,
            'skipped_items': 0,
            'message': f'No active tribute event for "{person.name}".',
        }

    media_modes = {
        (event.media_mode or 'both').strip().lower()
        for event in active_events
    }

    if 'both' in media_modes or ('movie' in media_modes and 'show' in media_modes):
        merged_media_mode = 'both'
    elif 'show' in media_modes:
        merged_media_mode = 'show'
    else:
        merged_media_mode = 'movie'

    return push_missing_titles_for_person(
        person,
        media_mode=merged_media_mode,
        settings=settings,
    )

def push_missing_titles_for_person(
    person: Person,
    *,
    media_mode: str = 'both',
    settings: AppSettings | None = None,
) -> dict:
    settings = settings or AppSettings.get_or_create()

    if not settings.auto_arr_enabled:
        return {
            'enabled': False,
            'processed_items': 0,
            'created_items': 0,
            'already_exists_items': 0,
            'invalid_items': 0,
            'error_items': 0,
            'skipped_items': 0,
            'message': 'Automatic Arr push is disabled.',
        }

    missing_movies, missing_shows = load_person_missing_titles(person)

    work_items: list[tuple[str, dict]] = []

    if media_mode in {'both', 'movie'}:
        for item in missing_movies:
            work_items.append(('movie', item))

    if media_mode in {'both', 'show'}:
        for item in missing_shows:
            work_items.append(('show', item))

    processed_items = 0
    created_items = 0
    already_exists_items = 0
    invalid_items = 0
    error_items = 0
    skipped_items = 0

    targets_by_kind = {
        'movie': _load_targets_for_media_kind('movie'),
        'show': _load_targets_for_media_kind('show'),
    }

    for media_kind, item in work_items:
        targets = targets_by_kind.get(media_kind) or []
        if not targets:
            skipped_items += 1
            continue

        tmdb_id = item.get('id')
        tvdb_id = item.get('tvdb_id')
        title = (
            item.get('title')
            or item.get('name')
            or item.get('original_title')
            or item.get('original_name')
            or ''
        )
        raw_year = (item.get('release_date') or item.get('first_air_date') or '')[:4]
        year = int(raw_year) if raw_year.isdigit() else None

        if not title:
            skipped_items += 1
            continue

        for target in targets:
            if _already_handled(
                person_id=person.id,
                target_id=target.id,
                media_kind=media_kind,
                tmdb_id=tmdb_id,
                tvdb_id=tvdb_id,
                title=title,
                year=year,
            ):
                skipped_items += 1
                continue

            processed_items += 1
            service = ArrService(target.arr_server)

            if media_kind == 'movie':
                result = service.ensure_movie(
                    title=title,
                    tmdb_id=tmdb_id,
                    year=year,
                )
            else:
                result = service.ensure_series(
                    title=title,
                    tvdb_id=tvdb_id,
                    tmdb_id=tmdb_id,
                    year=year,
                )

            _log_arr_activity(
                person=person,
                target=target,
                media_kind=media_kind,
                external_id=tvdb_id or tmdb_id,
                title=title,
                year=year,
                result=result,
            )

            status = result.get('status')
            if status == 'created':
                created_items += 1
            elif status == 'already_exists':
                already_exists_items += 1
            elif status == 'invalid':
                invalid_items += 1
            elif status == 'error':
                error_items += 1

    message = (
        f'Arr push finished for "{person.name}": '
        f'{processed_items} processed, '
        f'{created_items} created, '
        f'{already_exists_items} already existed, '
        f'{invalid_items} invalid, '
        f'{error_items} error, '
        f'{skipped_items} skipped.'
    )

    if processed_items > 0 or skipped_items > 0:
        _log_app_event(
            'info' if error_items == 0 else 'warning',
            message,
            related_type='person',
            related_id=person.id,
        )

    return {
        'enabled': True,
        'processed_items': processed_items,
        'created_items': created_items,
        'already_exists_items': already_exists_items,
        'invalid_items': invalid_items,
        'error_items': error_items,
        'skipped_items': skipped_items,
        'message': message,
    }


def push_active_events_missing_to_arr(settings: AppSettings | None = None) -> dict:
    if not _AUTO_ARR_PUSH_LOCK.acquire(blocking=False):
        return {
            'total_items': 0,
            'processed_items': 0,
            'success_items': 0,
            'error_items': 0,
            'message': 'Automatic Arr push skipped because another run is already in progress.',
        }

    try:
        settings = settings or AppSettings.get_or_create()

        if not settings.auto_arr_enabled:
            return {
                'total_items': 0,
                'processed_items': 0,
                'success_items': 0,
                'error_items': 0,
                'message': 'Automatic Arr push is disabled.',
            }

        from datetime import date

        today = date.today()
        active_events = (
            db.session.query(Person,)
            .join(Person.events)
            .filter(
                Person.id == Person.id,
            )
        )

        from ..models import TributeEvent

        active_events = (
            TributeEvent.query
            .filter(
                TributeEvent.status == 'active',
                TributeEvent.start_date <= today,
                TributeEvent.end_date >= today,
            )
            .order_by(TributeEvent.id.desc())
            .all()
        )

        total_items = len(active_events)
        processed_items = 0
        success_items = 0
        error_items = 0

        for event in active_events:
            person = Person.query.get(event.person_id)
            if not person:
                continue

            result = push_missing_titles_for_person(
                person,
                media_mode=event.media_mode,
                settings=settings,
            )

            processed_items += 1

            if result['error_items'] > 0:
                error_items += 1
            else:
                success_items += 1

        message = (
            f'Automatic Arr push finished. '
            f'{processed_items} active events processed, '
            f'{success_items} success, {error_items} error.'
        )

        if processed_items > 0:
            _log_app_event(
                'info' if error_items == 0 else 'warning',
                message,
            )

        return {
            'total_items': total_items,
            'processed_items': processed_items,
            'success_items': success_items,
            'error_items': error_items,
            'message': message,
        }
    finally:
        _AUTO_ARR_PUSH_LOCK.release()