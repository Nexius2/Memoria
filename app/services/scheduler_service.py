from __future__ import annotations

from datetime import date, datetime, timedelta
from threading import Lock
from flask import current_app

from ..extensions import scheduler, db
from ..models import (
    AppSettings,
    Person,
    TributeEvent,
    PlexServer,
    LibraryTarget,
    DetectionRun,
    TaskRun,
    AppLog,
    ArrActivity,
    make_slug,
    create_or_retrigger_event,
)
from ..services.detection_service import DetectionService
from ..utils.person_duplicates import find_existing_person_duplicate
from ..services.collection_service import sync_event, expire_due_events, remove_event_collections
from ..services.missing_titles_service import refresh_person_missing_titles, is_missing_titles_refresh_due
from ..services.plex_library_cache_service import refresh_library_title_cache_safe, is_library_cache_due
from ..services.arr_push_service import (
    push_active_events_missing_to_arr,
    push_missing_titles_for_active_person_events,
)
from ..blueprints.servers import sync_server_libraries
_DETECTION_RUN_LOCK = Lock()
_SYNC_TASK_LOCK = Lock()
_EXPIRE_TASK_LOCK = Lock()
_CLEANUP_TASK_LOCK = Lock()

def log_app_event(
    level: str,
    source: str,
    message: str,
    *,
    details: str | None = None,
    related_type: str | None = None,
    related_id: int | None = None,
) -> None:
    try:
        entry = AppLog(
            level=level,
            source=source,
            message=message,
            details=details,
            related_type=related_type,
            related_id=related_id,
        )
        db.session.add(entry)
        db.session.commit()
    except Exception:
        db.session.rollback()

def register_jobs(app):
    scheduler.add_job(
        func=lambda: _run_in_app(app, discover_libraries_daily),
        trigger='interval',
        hours=24,
        id='discover_libraries_daily',
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: _run_in_app(app, refresh_plex_library_caches),
        trigger='interval',
        hours=24,
        id='refresh_plex_library_caches',
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: _run_in_app(app, enqueue_task_run, app, 'cleanup', 'auto'),
        trigger='interval',
        hours=24,
        id='cleanup_retention_daily',
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: _run_in_app(app, enqueue_auto_detection_run, app),
        trigger='interval',
        hours=6,
        id='auto_detection_run',
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: _run_in_app(app, sync_active_events),
        trigger='interval',
        hours=12,
        id='sync_active_events',
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: _run_in_app(app, expire_events),
        trigger='interval',
        hours=6,
        id='expire_events',
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: _run_in_app(app, refresh_missing_titles_cache),
        trigger='interval',
        hours=1,
        id='refresh_missing_titles_cache',
        replace_existing=True,
    )
    scheduler.add_job(
        func=lambda: _run_in_app(app, push_active_events_missing_to_arr),
        trigger='interval',
        hours=1,
        id='push_active_events_missing_to_arr',
        replace_existing=True,
    )

def _run_in_app(app, fn, *args, **kwargs):
    with app.app_context():
        fn(*args, **kwargs)

def refresh_plex_library_caches(task_run_id: int | None = None) -> dict:
    targets = (
        LibraryTarget.query
        .join(PlexServer, LibraryTarget.plex_server_id == PlexServer.id)
        .filter(
            LibraryTarget.enabled.is_(True),
            PlexServer.enabled.is_(True),
        )
        .all()
    )

    total_items = len(targets)
    processed_items = 0
    success_items = 0
    error_items = 0

    for target in targets:
        if not is_library_cache_due(target, 24):
            continue

        processed_items += 1
        refresh_library_title_cache_safe(target)

        db.session.expire_all()
        refreshed_target = LibraryTarget.query.get(target.id)

        if refreshed_target and refreshed_target.plex_titles_cache_status == 'ready':
            success_items += 1
        else:
            error_items += 1

    message = (
        f'Plex library cache refresh finished. '
        f'{processed_items} libraries refreshed, {success_items} success, {error_items} error.'
    )

    if processed_items > 0:
        log_app_event(
            'info' if error_items == 0 else 'warning',
            'scheduler',
            message,
        )

    return {
        'total_items': total_items,
        'processed_items': processed_items,
        'success_items': success_items,
        'error_items': error_items,
        'message': message,
    }

def refresh_missing_titles_cache(task_run_id: int | None = None) -> dict:
    settings = AppSettings.get_or_create()

    if not settings.auto_missing_titles_enabled:
        return {
            'total_items': 0,
            'processed_items': 0,
            'success_items': 0,
            'error_items': 0,
            'message': 'Automatic missing titles scan is disabled.',
        }

    if not settings.tmdb_api_key:
        return {
            'total_items': 0,
            'processed_items': 0,
            'success_items': 0,
            'error_items': 0,
            'message': 'TMDb API key is not configured.',
        }

    people = (
        Person.query
        .order_by(Person.updated_at.desc(), Person.id.desc())
        .all()
    )

    total_items = len(people)
    processed_items = 0
    success_items = 0
    error_items = 0
    arr_created_items = 0
    arr_error_items = 0

    for person in people:
        if not is_missing_titles_refresh_due(person, settings.missing_titles_refresh_hours):
            continue

        processed_items += 1
        refresh_person_missing_titles(person, settings=settings)

        db.session.expire_all()
        refreshed_person = Person.query.get(person.id)

        if refreshed_person and refreshed_person.missing_titles_status == 'ready':
            success_items += 1

            arr_result = push_missing_titles_for_active_person_events(
                refreshed_person,
                settings=settings,
            )
            arr_created_items += int(arr_result.get('created_items') or 0)
            arr_error_items += int(arr_result.get('error_items') or 0)
        else:
            error_items += 1

    message = (
        f'Automatic missing titles scan finished. '
        f'{processed_items} people refreshed, {success_items} success, {error_items} error. '
        f'Arr: {arr_created_items} created, {arr_error_items} error.'
    )

    if processed_items > 0:
        log_app_event(
            'info' if error_items == 0 and arr_error_items == 0 else 'warning',
            'scheduler',
            message,
        )

    return {
        'total_items': total_items,
        'processed_items': processed_items,
        'success_items': success_items,
        'error_items': error_items,
        'message': message,
    }

def recover_stale_detection_runs(max_age_minutes: int = 10) -> int:
    cutoff = datetime.utcnow() - timedelta(minutes=max_age_minutes)

    stale_runs = (
        DetectionRun.query
        .filter(DetectionRun.status.in_(['pending', 'running']))
        .all()
    )

    updated = 0

    for run in stale_runs:
        reference_time = run.started_at or run.created_at
        if not reference_time:
            continue

        if reference_time >= cutoff:
            continue

        previous_status = run.status
        run.status = 'error'
        run.finished_at = datetime.utcnow()

        if not run.error_message:
            if previous_status == 'pending':
                run.error_message = 'Detection job stayed pending too long and was marked as stale automatically.'
            else:
                run.error_message = 'Detection job stayed running too long and was marked as stale automatically.'

        updated += 1

    if updated:
        db.session.commit()
        log_app_event(
            'warning',
            'scheduler',
            f'{updated} stale detection job(s) recovered automatically.',
        )

    return updated

def recover_stale_task_runs(max_age_minutes: int = 60) -> int:
    cutoff = datetime.utcnow() - timedelta(minutes=max_age_minutes)

    stale_runs = (
        TaskRun.query
        .filter(TaskRun.status.in_(['pending', 'running']))
        .all()
    )

    updated = 0

    for run in stale_runs:
        reference_time = run.started_at or run.created_at
        if not reference_time:
            continue

        if reference_time >= cutoff:
            continue

        previous_status = run.status
        run.status = 'error'
        run.finished_at = datetime.utcnow()

        if not run.message:
            if previous_status == 'pending':
                run.message = f'{run.task_type} job stayed pending too long and was marked as stale automatically.'
            else:
                run.message = f'{run.task_type} job stayed running too long and was marked as stale automatically.'

        updated += 1

    if updated:
        db.session.commit()
        log_app_event(
            'warning',
            'scheduler',
            f'{updated} stale task job(s) recovered automatically.',
        )

    return updated


def _task_lock_for(task_type: str) -> Lock:
    if task_type == 'sync':
        return _SYNC_TASK_LOCK
    if task_type == 'expire':
        return _EXPIRE_TASK_LOCK
    if task_type == 'cleanup':
        return _CLEANUP_TASK_LOCK
    raise ValueError(f'Unsupported task type: {task_type}')


def _task_runner_for(task_type: str):
    if task_type == 'sync':
        return sync_active_events
    if task_type == 'expire':
        return expire_events
    if task_type == 'cleanup':
        return cleanup_history
    raise ValueError(f'Unsupported task type: {task_type}')


def _update_task_run_progress(
    task_run_id: int,
    *,
    total_items: int | None = None,
    processed_items: int | None = None,
    success_items: int | None = None,
    error_items: int | None = None,
    message: str | None = None,
) -> None:
    run = TaskRun.query.get(task_run_id)
    if not run:
        return

    if total_items is not None:
        run.total_items = int(total_items)
    if processed_items is not None:
        run.processed_items = int(processed_items)
    if success_items is not None:
        run.success_items = int(success_items)
    if error_items is not None:
        run.error_items = int(error_items)
    if message is not None:
        run.message = message

    db.session.commit()


def enqueue_task_run(app, task_type: str, requested_by: str = 'manual') -> tuple[TaskRun, bool]:
    lock = _task_lock_for(task_type)

    with lock:
        recover_stale_task_runs()

        existing = (
            TaskRun.query
            .filter(
                TaskRun.task_type == task_type,
                TaskRun.status.in_(['pending', 'running']),
            )
            .order_by(TaskRun.created_at.desc())
            .first()
        )
        if existing:
            log_app_event(
                'warning',
                task_type,
                f'{task_type.title()} job request ignored because another job is already running.',
                related_type='task_run',
                related_id=existing.id,
            )
            return existing, False

        run = TaskRun(
            task_type=task_type,
            status='pending',
            requested_by=requested_by,
        )
        db.session.add(run)
        db.session.commit()

        scheduler.add_job(
            func=lambda run_id=run.id: _run_in_app(app, execute_task_run, run_id),
            trigger='date',
            run_date=datetime.now(scheduler.timezone) + timedelta(seconds=1),
            id=f'task_run_{task_type}_{run.id}',
            replace_existing=True,
            misfire_grace_time=30,
        )

        log_app_event(
            'info',
            task_type,
            f'{task_type.title()} job queued.',
            related_type='task_run',
            related_id=run.id,
        )

        return run, True


def execute_task_run(run_id: int) -> None:
    run = TaskRun.query.get(run_id)
    if not run:
        return

    lock = _task_lock_for(run.task_type)
    acquired = lock.acquire(blocking=False)

    if not acquired:
        if run.status in ['pending', 'running']:
            run.status = 'error'
            run.finished_at = datetime.utcnow()
            run.message = f'Another {run.task_type} job is already running.'
            db.session.commit()
            log_app_event(
                'error',
                run.task_type,
                f'{run.task_type.title()} job failed to start because another job is already running.',
                related_type='task_run',
                related_id=run.id,
            )
        return

    try:
        run = TaskRun.query.get(run_id)
        if not run:
            return

        if run.status not in ['pending', 'running']:
            return

        run.status = 'running'
        run.started_at = datetime.utcnow()
        run.finished_at = None
        run.message = None
        run.total_items = 0
        run.processed_items = 0
        run.success_items = 0
        run.error_items = 0
        db.session.commit()

        log_app_event(
            'info',
            run.task_type,
            f'{run.task_type.title()} job started.',
            related_type='task_run',
            related_id=run.id,
        )

        try:
            runner = _task_runner_for(run.task_type)
            result = runner(task_run_id=run.id)

            run = TaskRun.query.get(run_id)
            if run:
                run.status = 'success'
                run.total_items = int(result.get('total_items') or 0)
                run.processed_items = int(result.get('processed_items') or 0)
                run.success_items = int(result.get('success_items') or 0)
                run.error_items = int(result.get('error_items') or 0)
                run.message = result.get('message')
                run.finished_at = datetime.utcnow()
                db.session.commit()

                log_app_event(
                    'info',
                    run.task_type,
                    f'{run.task_type.title()} job finished successfully.',
                    details=run.message,
                    related_type='task_run',
                    related_id=run.id,
                )

        except Exception as exc:
            db.session.rollback()

            run = TaskRun.query.get(run_id)
            if run:
                run.status = 'error'
                run.message = str(exc)
                run.finished_at = datetime.utcnow()
                db.session.commit()

                log_app_event(
                    'error',
                    run.task_type,
                    f'{run.task_type.title()} job failed.',
                    details=str(exc),
                    related_type='task_run',
                    related_id=run.id,
                )

    finally:
        lock.release()

def _enqueue_detection_run(
    app,
    *,
    requested_by: str,
    log_when_skipped: bool,
) -> tuple[DetectionRun | None, bool]:
    with _DETECTION_RUN_LOCK:
        recover_stale_detection_runs()

        existing = (
            DetectionRun.query
            .filter(DetectionRun.status.in_(['pending', 'running']))
            .order_by(DetectionRun.created_at.desc())
            .first()
        )
        if existing:
            if log_when_skipped:
                log_app_event(
                    'warning',
                    'detection',
                    'Detection job request ignored because another detection job is already running.',
                    related_type='detection_run',
                    related_id=existing.id,
                )
            return existing, False

        run = DetectionRun(
            status='pending',
            requested_by=requested_by,
        )
        db.session.add(run)
        db.session.commit()

        scheduler.add_job(
            func=lambda run_id=run.id: _run_in_app(app, execute_detection_run, run_id),
            trigger='date',
            run_date=datetime.now(scheduler.timezone) + timedelta(seconds=1),
            id=f'detection_run_{run.id}',
            replace_existing=True,
            misfire_grace_time=30,
        )

        log_app_event(
            'info',
            'detection',
            f'Detection job queued ({requested_by}).',
            related_type='detection_run',
            related_id=run.id,
        )

        return run, True


def enqueue_detection_run(app) -> tuple[DetectionRun | None, bool]:
    return _enqueue_detection_run(
        app,
        requested_by='manual',
        log_when_skipped=True,
    )


def enqueue_auto_detection_run(app) -> tuple[DetectionRun | None, bool]:
    settings = AppSettings.get_or_create()

    if not settings.auto_detection_enabled:
        return None, False

    return _enqueue_detection_run(
        app,
        requested_by='auto',
        log_when_skipped=False,
    )


def execute_detection_run(run_id: int) -> None:
    acquired = _DETECTION_RUN_LOCK.acquire(blocking=False)
    if not acquired:
        run = DetectionRun.query.get(run_id)
        if run and run.status in ['pending', 'running']:
            run.status = 'error'
            run.finished_at = datetime.utcnow()
            run.error_message = 'Another detection job is already running.'
            db.session.commit()
            log_app_event(
                'error',
                'detection',
                'Detection job failed to start because another detection job is already running.',
                related_type='detection_run',
                related_id=run.id,
            )
        return

    try:
        run = DetectionRun.query.get(run_id)
        if not run:
            return

        if run.status not in ['pending', 'running']:
            return

        run.status = 'running'
        run.started_at = datetime.utcnow()
        run.finished_at = None
        run.error_message = None
        db.session.commit()

        log_app_event(
            'info',
            'detection',
            'Detection job started.',
            related_type='detection_run',
            related_id=run.id,
        )

        try:
            sync_new_events = (run.requested_by == 'auto')
            result = auto_detect_and_sync(force=True, sync_new_events=sync_new_events)

            run.status = 'success'
            run.candidates_cached = int(result.get('cached') or 0)
            run.people_upserted = int(result.get('updated_people') or 0)
            run.events_created = int(result.get('created_events') or 0)
            run.finished_at = datetime.utcnow()
            db.session.commit()

            log_app_event(
                'info',
                'detection',
                'Detection job finished successfully.',
                details=(
                    f'{run.candidates_cached} cached, '
                    f'{run.people_upserted} people updated, '
                    f'{run.events_created} events created.'
                ),
                related_type='detection_run',
                related_id=run.id,
            )

        except Exception as exc:
            db.session.rollback()

            run = DetectionRun.query.get(run_id)
            if run:
                run.status = 'error'
                run.error_message = str(exc)
                run.finished_at = datetime.utcnow()
                db.session.commit()

                log_app_event(
                    'error',
                    'detection',
                    'Detection job failed.',
                    details=str(exc),
                    related_type='detection_run',
                    related_id=run.id,
                )

    finally:
        _DETECTION_RUN_LOCK.release()


def _upsert_person_from_row(row: dict) -> tuple[Person, bool]:
    slug = make_slug(row['name'])
    new_death_date = date.fromisoformat(row['death_date'])

    person = Person.query.filter_by(slug=slug).first()

    if not person:
        person, _ = find_existing_person_duplicate(
            slug=slug,
            name=row['name'],
            death_date=new_death_date,
            imdb_id=row.get('imdb_id'),
            wikidata_id=row.get('wikidata_id'),
        )

    if not person:
        person = Person(
            name=row['name'],
            slug=slug,
            death_date=new_death_date,
            country=row.get('country'),
            professions_csv=row.get('professions_csv'),
            source='web',
            source_url=row.get('source_url'),
            imdb_id=row.get('imdb_id'),
            wikidata_id=row.get('wikidata_id'),
        )
        db.session.add(person)
        db.session.flush()
        return person, True

    changed = False

    if person.name != row['name']:
        person.name = row['name']
        changed = True
    if person.death_date != new_death_date:
        person.death_date = new_death_date
        changed = True
    if person.country != row.get('country'):
        person.country = row.get('country')
        changed = True
    if person.professions_csv != row.get('professions_csv'):
        person.professions_csv = row.get('professions_csv')
        changed = True
    if person.source_url != row.get('source_url'):
        person.source_url = row.get('source_url')
        changed = True
    if person.imdb_id != row.get('imdb_id'):
        person.imdb_id = row.get('imdb_id')
        changed = True
    if person.wikidata_id != row.get('wikidata_id'):
        person.wikidata_id = row.get('wikidata_id')
        changed = True

    return person, changed

def _compute_selection_priority(row: dict, person: Person | None) -> int:
    score = int(row.get('popularity_score') or 0)

    if person:
        if person.manual_priority is not None:
            score = int(person.manual_priority)

        if person.is_pinned:
            score += 1_000_000

    return score


def _select_rows_for_auto_events(rows: list[dict], people_by_slug: dict[str, Person], max_people: int) -> list[dict]:
    eligible_rows = []

    for row in rows:
        person = people_by_slug.get(row['slug'])

        if person:
            if person.exclude_from_auto:
                continue
            if person.ignore_until and person.ignore_until >= date.today():
                continue

        row_copy = dict(row)
        row_copy['_selection_priority'] = _compute_selection_priority(row, person)
        eligible_rows.append(row_copy)

    eligible_rows.sort(
        key=lambda item: (
            item['_selection_priority'],
            item.get('death_date') or '',
            item.get('name') or '',
        ),
        reverse=True,
    )

    return eligible_rows[:max_people]

def _cancel_out_of_scope_web_events(selected_slugs: set[str]) -> None:
    active_web_events = (
        TributeEvent.query
        .join(Person)
        .filter(
            TributeEvent.status == 'active',
            TributeEvent.source == 'web',
        )
        .all()
    )

    for event in active_web_events:
        person = event.person

        if person.slug in selected_slugs:
            continue

        if person.is_pinned:
            continue

        if person.manual_priority is not None and person.manual_priority > 0:
            continue

        remove_event_collections(event)
        event.status = 'cancelled'
        event.note = (
            'Cancelled automatically because the person is no longer selected '
            'by the current auto-selection logic.'
        )


def auto_detect_and_sync(force: bool = False, sync_new_events: bool = True):
    settings = AppSettings.get_or_create()

    if not force and not settings.auto_detection_enabled:
        return {'cached': 0, 'created_events': 0, 'updated_people': 0}

    detector = DetectionService(settings)
    rows = detector.refresh_candidate_cache(limit=max(settings.max_people * 4, 12))

    updated_people = 0
    created_events = 0
    people_by_slug: dict[str, Person] = {}

    for row in rows:
        person, changed = _upsert_person_from_row(row)
        people_by_slug[row['slug']] = person
        if changed:
            updated_people += 1

    selected_rows = _select_rows_for_auto_events(
        rows=rows,
        people_by_slug=people_by_slug,
        max_people=settings.max_people,
    )
    selected_slugs = {row['slug'] for row in selected_rows}

    _cancel_out_of_scope_web_events(selected_slugs)

    active_events = (
        TributeEvent.query
        .join(Person)
        .filter(TributeEvent.status == 'active')
        .all()
    )

    active_events_by_person_id = {
        event.person_id: event
        for event in active_events
    }

    active_web_events_by_slug = {
        event.person.slug: event
        for event in active_events
        if event.source == 'web'
    }

    for row in selected_rows:
        person = people_by_slug[row['slug']]
        active_event = active_events_by_person_id.get(person.id)
        selection_priority = int(row.get('_selection_priority') or row.get('popularity_score') or 0)

        if active_event:
            active_event.priority = selection_priority
            continue

    available_web_slots = max(settings.max_people - len(active_web_events_by_slug), 0)

    for row in selected_rows:
        if available_web_slots <= 0:
            break

        person = people_by_slug[row['slug']]

        if person.id in active_events_by_person_id:
            continue

        selection_priority = int(row.get('_selection_priority') or row.get('popularity_score') or 0)

        event = create_or_retrigger_event(
            person,
            settings.default_media_mode,
            settings.display_days,
            source='web',
            priority=selection_priority,
        )
        db.session.flush()

        if sync_new_events:
            sync_event(event)

        active_events_by_person_id[person.id] = event
        active_web_events_by_slug[row['slug']] = event
        created_events += 1
        available_web_slots -= 1

    db.session.commit()

    return {
        'cached': len(rows),
        'created_events': created_events,
        'updated_people': updated_people,
    }


def sync_active_events(task_run_id: int | None = None):
    events = TributeEvent.query.filter_by(status='active').all()

    total_items = len(events)
    processed_items = 0
    success_items = 0
    error_items = 0

    if task_run_id:
        _update_task_run_progress(
            task_run_id,
            total_items=total_items,
            processed_items=0,
            success_items=0,
            error_items=0,
            message='Sync started...',
        )

    log_app_event(
        'info',
        'sync',
        f'Sync pass started for {total_items} active event(s).',
        related_type='task_run',
        related_id=task_run_id,
    )

    for event in events:
        try:
            if event.end_date >= date.today():
                sync_event(event)
                success_items += 1
        except Exception as exc:
            error_items += 1
            current_app.logger.exception(
                'Background sync failed for event %s: %s',
                event.id,
                exc,
            )
            log_app_event(
                'error',
                'sync',
                f'Event sync failed for {event.person.name if event.person else "unknown person"}.',
                details=str(exc),
                related_type='event',
                related_id=event.id,
            )
        finally:
            processed_items += 1

            if task_run_id:
                _update_task_run_progress(
                    task_run_id,
                    total_items=total_items,
                    processed_items=processed_items,
                    success_items=success_items,
                    error_items=error_items,
                    message=f'Sync running... {processed_items}/{total_items}',
                )

    log_app_event(
        'info',
        'sync',
        f'Sync pass finished: {success_items} success, {error_items} error(s).',
        related_type='task_run',
        related_id=task_run_id,
    )

    return {
        'total_items': total_items,
        'processed_items': processed_items,
        'success_items': success_items,
        'error_items': error_items,
        'message': f'{success_items} event(s) synced, {error_items} error(s).',
    }


def expire_events(task_run_id: int | None = None):
    total_items = (
        TributeEvent.query
        .filter(
            TributeEvent.status == 'active',
            TributeEvent.end_date < date.today(),
        )
        .count()
    )

    if task_run_id:
        _update_task_run_progress(
            task_run_id,
            total_items=total_items,
            processed_items=0,
            success_items=0,
            error_items=0,
            message='Expire check started...',
        )

    log_app_event(
        'info',
        'expire',
        f'Expire check started for {total_items} event(s).',
        related_type='task_run',
        related_id=task_run_id,
    )

    expired_count = expire_due_events()

    if task_run_id:
        _update_task_run_progress(
            task_run_id,
            total_items=total_items,
            processed_items=total_items,
            success_items=expired_count,
            error_items=0,
            message=f'{expired_count} event(s) expired.',
        )

    log_app_event(
        'info',
        'expire',
        f'Expire check finished: {expired_count} event(s) expired.',
        related_type='task_run',
        related_id=task_run_id,
    )

    return {
        'total_items': total_items,
        'processed_items': total_items,
        'success_items': expired_count,
        'error_items': 0,
        'message': f'{expired_count} event(s) expired.',
    }

def cleanup_history(task_run_id: int | None = None):
    settings = AppSettings.get_or_create()
    now = datetime.utcnow()

    log_cutoff = now - timedelta(days=max(int(settings.log_retention_days or 30), 1))
    job_cutoff = now - timedelta(days=max(int(settings.job_retention_days or 30), 1))
    arr_cutoff = now - timedelta(days=max(int(settings.arr_activity_retention_days or 90), 1))

    if task_run_id:
        _update_task_run_progress(
            task_run_id,
            total_items=0,
            processed_items=0,
            success_items=0,
            error_items=0,
            message='Retention cleanup started...',
        )

    log_app_event(
        'info',
        'cleanup',
        'Retention cleanup started.',
        related_type='task_run',
        related_id=task_run_id,
    )

    old_logs_query = AppLog.query.filter(AppLog.created_at < log_cutoff)

    old_detection_runs_query = (
        DetectionRun.query
        .filter(DetectionRun.created_at < job_cutoff)
        .filter(DetectionRun.status.notin_(['pending', 'running']))
    )

    old_task_runs_query = (
        TaskRun.query
        .filter(TaskRun.created_at < job_cutoff)
        .filter(TaskRun.status.notin_(['pending', 'running']))
    )

    old_arr_activity_query = ArrActivity.query.filter(ArrActivity.created_at < arr_cutoff)

    log_count = old_logs_query.count()
    detection_run_count = old_detection_runs_query.count()
    task_run_count = old_task_runs_query.count()
    arr_activity_count = old_arr_activity_query.count()

    total_items = log_count + detection_run_count + task_run_count + arr_activity_count

    old_logs_query.delete(synchronize_session=False)
    old_detection_runs_query.delete(synchronize_session=False)
    old_task_runs_query.delete(synchronize_session=False)
    old_arr_activity_query.delete(synchronize_session=False)

    db.session.commit()

    message = (
        f'{log_count} log(s) deleted · '
        f'{detection_run_count} detection run(s) deleted · '
        f'{task_run_count} task run(s) deleted · '
        f'{arr_activity_count} Arr activit(y/ies) deleted'
    )

    if task_run_id:
        _update_task_run_progress(
            task_run_id,
            total_items=total_items,
            processed_items=total_items,
            success_items=total_items,
            error_items=0,
            message=message,
        )

    log_app_event(
        'info',
        'cleanup',
        'Retention cleanup finished successfully.',
        details=message,
        related_type='task_run',
        related_id=task_run_id,
    )

    return {
        'total_items': total_items,
        'processed_items': total_items,
        'success_items': total_items,
        'error_items': 0,
        'message': message,
    }

def discover_libraries_daily():
    servers = PlexServer.query.filter_by(enabled=True).all()

    for server in servers:
        try:
            sync_server_libraries(server)
            db.session.commit()
        except Exception:
            db.session.rollback()