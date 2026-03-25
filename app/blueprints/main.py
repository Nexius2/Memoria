from collections import Counter
from datetime import date, timedelta

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app
from sqlalchemy.orm import joinedload
from ..utils.country_utils import normalize_country_label
from ..services.collection_service import sync_event
from ..utils.person_duplicates import find_existing_person_duplicate

from ..extensions import db
from ..models import (
    TributeEvent,
    AppSettings,
    DetectionCandidate,
    DetectionRun,
    TaskRun,
    AppLog,
    Person,
    create_or_retrigger_event,
)
from ..services.scheduler_service import (
    sync_active_events,
    expire_events,
    enqueue_detection_run,
    enqueue_task_run,
    recover_stale_detection_runs,
    recover_stale_task_runs,
)

bp = Blueprint('main', __name__)


def _upsert_person_from_candidate(candidate: DetectionCandidate) -> tuple[Person, bool]:
    person = Person.query.filter_by(slug=candidate.slug).first()

    if not person:
        person, _ = find_existing_person_duplicate(
            slug=candidate.slug,
            name=candidate.name,
            death_date=candidate.death_date,
            imdb_id=candidate.imdb_id,
            wikidata_id=candidate.wikidata_id,
        )

    if not person:
        person = Person(
            name=candidate.name,
            slug=candidate.slug,
            death_date=candidate.death_date,
            country=candidate.country,
            professions_csv=candidate.professions_csv,
            source='web',
            source_url=candidate.source_url,
            imdb_id=candidate.imdb_id,
            wikidata_id=candidate.wikidata_id,
        )
        db.session.add(person)
        db.session.flush()
        return person, True

    changed = False

    if person.name != candidate.name:
        person.name = candidate.name
        changed = True
    if person.death_date != candidate.death_date:
        person.death_date = candidate.death_date
        changed = True
    if person.country != candidate.country:
        person.country = candidate.country
        changed = True
    if person.professions_csv != candidate.professions_csv:
        person.professions_csv = candidate.professions_csv
        changed = True
    if person.source_url != candidate.source_url:
        person.source_url = candidate.source_url
        changed = True
    if person.imdb_id != candidate.imdb_id:
        person.imdb_id = candidate.imdb_id
        changed = True
    if person.wikidata_id != candidate.wikidata_id:
        person.wikidata_id = candidate.wikidata_id
        changed = True

    return person, changed

def _ensure_person_and_apply_selection(
    candidate: DetectionCandidate,
    *,
    is_pinned: bool | None = None,
    exclude_from_auto: bool | None = None,
    manual_priority: int | None = None,
    ignore_days: int | None = None,
) -> Person:
    person, _ = _upsert_person_from_candidate(candidate)

    if is_pinned is not None:
        person.is_pinned = is_pinned

    if exclude_from_auto is not None:
        person.exclude_from_auto = exclude_from_auto
        if exclude_from_auto:
            person.ignore_until = None

    if manual_priority is not None:
        person.manual_priority = manual_priority

    if ignore_days is not None:
        from datetime import date, timedelta
        person.ignore_until = date.today() + timedelta(days=max(ignore_days, 0))
        person.exclude_from_auto = False

    return person

def _build_candidate_rows(candidates: list[DetectionCandidate]) -> list[dict]:
    slugs = [candidate.slug for candidate in candidates]
    people_by_slug = {}
    active_events_by_person_id = {}

    if slugs:
        people = Person.query.filter(Person.slug.in_(slugs)).all()
        people_by_slug = {person.slug: person for person in people}

        person_ids = [person.id for person in people]
        if person_ids:
            active_events = (
                TributeEvent.query
                .filter(
                    TributeEvent.person_id.in_(person_ids),
                    TributeEvent.status == 'active',
                )
                .all()
            )
            active_events_by_person_id = {
                event.person_id: event for event in active_events
            }

    rows = []
    for candidate in candidates:
        person = people_by_slug.get(candidate.slug)
        active_event = active_events_by_person_id.get(person.id) if person else None
        rows.append({
            'candidate': candidate,
            'person': person,
            'active_event': active_event,
        })

    return rows

def _format_run_duration_seconds(started_at, finished_at) -> int | None:
    if not started_at:
        return None

    end_value = finished_at or started_at
    try:
        return max(int((end_value - started_at).total_seconds()), 0)
    except Exception:
        return None


def _build_job_history_rows(limit: int = 50) -> list[dict]:
    detection_runs = (
        DetectionRun.query
        .order_by(DetectionRun.created_at.desc(), DetectionRun.id.desc())
        .limit(limit)
        .all()
    )

    task_runs = (
        TaskRun.query
        .order_by(TaskRun.created_at.desc(), TaskRun.id.desc())
        .limit(limit)
        .all()
    )

    rows = []

    for run in detection_runs:
        rows.append({
            'id': run.id,
            'model': 'detection_run',
            'job_type': 'detection',
            'status': run.status,
            'requested_by': run.requested_by,
            'created_at': run.created_at,
            'started_at': run.started_at,
            'finished_at': run.finished_at,
            'duration_seconds': _format_run_duration_seconds(run.started_at, run.finished_at),
            'summary': (
                f'{run.candidates_cached} cached · '
                f'{run.people_upserted} people updated · '
                f'{run.events_created} events created'
            ) if run.status == 'success' else 'Detection run',
            'details': run.error_message if run.status == 'error' else None,
        })

    for run in task_runs:
        rows.append({
            'id': run.id,
            'model': 'task_run',
            'job_type': run.task_type,
            'status': run.status,
            'requested_by': run.requested_by,
            'created_at': run.created_at,
            'started_at': run.started_at,
            'finished_at': run.finished_at,
            'duration_seconds': _format_run_duration_seconds(run.started_at, run.finished_at),
            'summary': (
                f'{run.processed_items}/{run.total_items} processed · '
                f'{run.success_items} success · '
                f'{run.error_items} error'
            ) if run.status == 'success' else f'{run.task_type.title()} run',
            'details': run.message,
        })

    rows.sort(
        key=lambda row: (row['created_at'] or row['started_at'], row['id']),
        reverse=True,
    )

    return rows[:limit]

def _build_log_related_meta(row: AppLog) -> dict:
    related_type = (row.related_type or '').strip()
    related_id = row.related_id

    meta = {
        'related_label': None,
        'related_url': None,
    }

    if not related_type or not related_id:
        return meta

    if related_type == 'person':
        person = Person.query.get(related_id)
        if person:
            meta['related_label'] = f'Person · {person.name}'
            meta['related_url'] = url_for('people.detail', person_id=person.id)
            return meta

    if related_type == 'event':
        event = TributeEvent.query.get(related_id)
        if event and event.person:
            meta['related_label'] = f'Event · {event.person.name}'
            meta['related_url'] = url_for('people.detail', person_id=event.person.id)
            return meta

    if related_type == 'task_run':
        meta['related_label'] = f'Task run #{related_id}'
        meta['related_url'] = url_for('main.jobs')
        return meta

    if related_type == 'detection_run':
        meta['related_label'] = f'Detection run #{related_id}'
        meta['related_url'] = url_for('main.jobs')
        return meta

    if related_type == 'arr_activity':
        meta['related_label'] = f'Arr activity #{related_id}'
        meta['related_url'] = url_for('arr.index')
        return meta

    meta['related_label'] = f'{related_type} #{related_id}'
    return meta


def _build_app_logs_rows(log_rows: list[AppLog]) -> list[dict]:
    rows = []

    for row in log_rows:
        meta = _build_log_related_meta(row)
        rows.append({
            'id': row.id,
            'level': row.level,
            'source': row.source,
            'message': row.message,
            'details': row.details,
            'created_at': row.created_at,
            'related_type': row.related_type,
            'related_id': row.related_id,
            'related_label': meta['related_label'],
            'related_url': meta['related_url'],
        })

    return rows

def _normalize_dashboard_country(value: str | None) -> str | None:
    normalized = normalize_country_label(value)
    if not normalized or normalized in {'—', '-'}:
        return None
    return normalized


def _build_dashboard_overview() -> dict:
    all_events = (
        TributeEvent.query
        .options(
            joinedload(TributeEvent.person),
            joinedload(TributeEvent.publications),
        )
        .all()
    )

    active_events = [event for event in all_events if event.status == 'active']
    live_active_events = [event for event in active_events if event.is_active]
    expiring_soon_events = [event for event in live_active_events if event.days_remaining <= 7]

    country_counter = Counter()
    for event in live_active_events:
        if not event.person:
            continue
        normalized_country = _normalize_dashboard_country(event.person.country)
        if normalized_country:
            country_counter[normalized_country] += 1

    top_countries = [
        {'name': name, 'count': count}
        for name, count in country_counter.most_common(5)
    ]

    problematic_events = []
    stale_sync_cutoff = date.today() - timedelta(days=2)

    for event in active_events:
        reasons = []

        if not event.publications:
            reasons.append('No publication')

        if not event.last_synced_at:
            reasons.append('Never synced')
        elif event.last_synced_at.date() < stale_sync_cutoff:
            reasons.append('Sync older than 2 days')

        if reasons:
            problematic_events.append({
                'event': event,
                'reasons': reasons,
            })

    problematic_events.sort(
        key=lambda item: (
            item['event'].days_remaining if item['event'].status == 'active' else 999999,
            -(item['event'].priority or 0),
            (item['event'].person.name.lower() if item['event'].person and item['event'].person.name else ''),
        )
    )

    running_detection_jobs = (
        DetectionRun.query
        .filter(DetectionRun.status.in_(['pending', 'running']))
        .count()
    )
    running_task_jobs = (
        TaskRun.query
        .filter(TaskRun.status.in_(['pending', 'running']))
        .count()
    )

    status_counts = {
        'active': sum(1 for event in all_events if event.status == 'active'),
        'expired': sum(1 for event in all_events if event.status == 'expired'),
        'cancelled': sum(1 for event in all_events if event.status == 'cancelled'),
        'all': len(all_events),
    }

    return {
        'active_events_count': len(live_active_events),
        'expiring_soon_count': len(expiring_soon_events),
        'published_targets_count': sum(len(event.publications or []) for event in live_active_events),
        'cached_candidates_count': DetectionCandidate.query.count(),
        'people_count': Person.query.count(),
        'running_jobs_count': running_detection_jobs + running_task_jobs,
        'top_countries': top_countries,
        'status_counts': status_counts,
        'problematic_events': problematic_events[:6],
        'problematic_events_count': len(problematic_events),
    }

def _sort_dashboard_events(events: list[TributeEvent], sort_by: str) -> list[TributeEvent]:
    if sort_by == 'end_asc':
        return sorted(
            events,
            key=lambda event: (
                event.end_date or date.max,
                -(event.priority or 0),
                (event.person.name.lower() if event.person and event.person.name else ''),
            ),
        )

    if sort_by == 'death_desc':
        return sorted(
            events,
            key=lambda event: (
                event.person.death_date if event.person and event.person.death_date else date.min,
                event.priority or 0,
                (event.person.name.lower() if event.person and event.person.name else ''),
            ),
            reverse=True,
        )

    if sort_by == 'name_asc':
        return sorted(
            events,
            key=lambda event: (
                (event.person.name.lower() if event.person and event.person.name else ''),
                event.end_date or date.max,
            ),
        )

    if sort_by == 'publications_desc':
        return sorted(
            events,
            key=lambda event: (
                len(event.publications or []),
                event.priority or 0,
                (event.person.name.lower() if event.person and event.person.name else ''),
            ),
            reverse=True,
        )

    return sorted(
        events,
        key=lambda event: (
            event.priority or 0,
            event.start_date or date.min,
            (event.person.name.lower() if event.person and event.person.name else ''),
        ),
        reverse=True,
    )

@bp.route('/')
def dashboard():
    status = (request.args.get('status') or 'active').strip()
    view = (request.args.get('view') or 'all').strip()
    sort_by = (request.args.get('sort') or 'priority_desc').strip()

    query = (
        TributeEvent.query
        .options(
            joinedload(TributeEvent.person),
            joinedload(TributeEvent.publications),
        )
    )

    if status != 'all':
        query = query.filter_by(status=status)

    events = query.all()
    settings = AppSettings.get_or_create()

    recover_stale_detection_runs()
    recover_stale_task_runs()

    dashboard_overview = _build_dashboard_overview()

    if view == 'soon':
        events = [
            event for event in events
            if event.status == 'active' and event.days_remaining <= 7
        ]
    elif view == 'problematic':
        events = [
            event for event in events
            if event.status == 'active' and (
                not event.publications or not event.last_synced_at
            )
        ]
    elif view == 'published':
        events = [
            event for event in events
            if len(event.publications or []) > 0
        ]
    elif view == 'unpublished':
        events = [
            event for event in events
            if len(event.publications or []) == 0
        ]

    events = _sort_dashboard_events(events, sort_by)

    recent_candidates = (
        DetectionCandidate.query
        .order_by(
            DetectionCandidate.popularity_score.desc(),
            DetectionCandidate.death_date.desc(),
            DetectionCandidate.name.asc(),
        )
        .limit(12)
        .all()
    )

    candidate_rows = _build_candidate_rows(recent_candidates)

    if not candidate_rows:
        fallback_people = (
            Person.query
            .filter_by(source='web')
            .options(joinedload(Person.events))
            .order_by(Person.death_date.desc(), Person.created_at.desc())
            .limit(12)
            .all()
        )

        candidate_rows = [
            {
                'candidate': None,
                'person': person,
                'active_event': next(
                    (
                        event for event in person.events
                        if event.status == 'active' and event.is_active
                    ),
                    None,
                ),
            }
            for person in fallback_people
        ]

    latest_detection_run = (
        DetectionRun.query
        .order_by(DetectionRun.created_at.desc())
        .first()
    )

    latest_sync_run = (
        TaskRun.query
        .filter_by(task_type='sync')
        .order_by(TaskRun.created_at.desc())
        .first()
    )

    latest_expire_run = (
        TaskRun.query
        .filter_by(task_type='expire')
        .order_by(TaskRun.created_at.desc())
        .first()
    )

    return render_template(
        'dashboard.html',
        events=events,
        settings=settings,
        status=status,
        recent_candidates=recent_candidates,
        candidate_rows=candidate_rows,
        latest_detection_run=latest_detection_run,
        latest_sync_run=latest_sync_run,
        latest_expire_run=latest_expire_run,
        dashboard_overview=dashboard_overview,
        view=view,
        sort_by=sort_by,
    )

@bp.get('/jobs')
def jobs():
    recover_stale_detection_runs()
    recover_stale_task_runs()

    limit_raw = (request.args.get('limit') or '50').strip()
    allowed_limits = {25, 50, 100, 200}

    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 50

    if limit not in allowed_limits:
        limit = 50

    job_type = (request.args.get('job_type') or 'all').strip()
    status_filter = (request.args.get('status') or 'all').strip()
    date_from_raw = (request.args.get('date_from') or '').strip()
    date_to_raw = (request.args.get('date_to') or '').strip()

    rows = _build_job_history_rows(limit=200)

    if job_type != 'all':
        rows = [row for row in rows if row['job_type'] == job_type]

    if status_filter != 'all':
        rows = [row for row in rows if row['status'] == status_filter]

    if date_from_raw:
        try:
            date_from_value = date.fromisoformat(date_from_raw)
            rows = [
                row for row in rows
                if row['created_at'] and row['created_at'].date() >= date_from_value
            ]
        except ValueError:
            date_from_raw = ''

    if date_to_raw:
        try:
            date_to_value = date.fromisoformat(date_to_raw)
            rows = [
                row for row in rows
                if row['created_at'] and row['created_at'].date() <= date_to_value
            ]
        except ValueError:
            date_to_raw = ''

    rows = rows[:limit]

    return render_template(
        'jobs.html',
        job_rows=rows,
        limit=limit,
        job_type=job_type,
        status_filter=status_filter,
        date_from=date_from_raw,
        date_to=date_to_raw,
    )

@bp.get('/logs')
def logs():
    limit_raw = (request.args.get('limit') or '100').strip()
    allowed_limits = {25, 50, 100, 200}

    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 100

    if limit not in allowed_limits:
        limit = 100

    level = (request.args.get('level') or 'all').strip()
    source = (request.args.get('source') or 'all').strip()
    date_from_raw = (request.args.get('date_from') or '').strip()
    date_to_raw = (request.args.get('date_to') or '').strip()

    query = AppLog.query.order_by(AppLog.created_at.desc(), AppLog.id.desc())

    if level != 'all':
        query = query.filter(AppLog.level == level)

    if source != 'all':
        query = query.filter(AppLog.source == source)

    if date_from_raw:
        try:
            date_from_value = date.fromisoformat(date_from_raw)
            query = query.filter(AppLog.created_at >= date_from_value)
        except ValueError:
            date_from_raw = ''

    if date_to_raw:
        try:
            date_to_value = date.fromisoformat(date_to_raw) + timedelta(days=1)
            query = query.filter(AppLog.created_at < date_to_value)
        except ValueError:
            date_to_raw = ''

    raw_log_rows = query.limit(limit).all()
    log_rows = _build_app_logs_rows(raw_log_rows)

    return render_template(
        'logs.html',
        log_rows=log_rows,
        limit=limit,
        level=level,
        source=source,
        date_from=date_from_raw,
        date_to=date_to_raw,
    )

@bp.post('/actions/run-detection')
def run_detection():
    run, created = enqueue_detection_run(current_app._get_current_object())

    if created:
        flash(
            'Detection started in background. Refresh candidates will appear automatically when the job finishes.',
            'success',
        )
    else:
        flash('A detection job is already running.', 'warning')

    return redirect(url_for('main.dashboard'))


@bp.get('/actions/detection-status')
def detection_status():
    recover_stale_detection_runs()
    run = DetectionRun.query.order_by(DetectionRun.created_at.desc()).first()

    if not run:
        return jsonify({
            'status': 'idle',
            'message': 'No detection job yet.',
        })

    return jsonify({
        'status': run.status,
        'id': run.id,
        'candidates_cached': run.candidates_cached,
        'people_upserted': run.people_upserted,
        'events_created': run.events_created,
        'error_message': run.error_message,
        'started_at': run.started_at.isoformat() if run.started_at else None,
        'finished_at': run.finished_at.isoformat() if run.finished_at else None,
        'message': (
            f'{run.candidates_cached} cached, '
            f'{run.people_upserted} people updated, '
            f'{run.events_created} events created'
            if run.status == 'success'
            else run.error_message
            if run.status == 'error'
            else 'Detection is running in background...'
        ),
    })

@bp.get('/actions/background-jobs-status')
def background_jobs_status():
    recover_stale_detection_runs()
    recover_stale_task_runs()

    detection_run = DetectionRun.query.order_by(DetectionRun.created_at.desc()).first()
    sync_run = (
        TaskRun.query
        .filter_by(task_type='sync')
        .order_by(TaskRun.created_at.desc())
        .first()
    )
    expire_run = (
        TaskRun.query
        .filter_by(task_type='expire')
        .order_by(TaskRun.created_at.desc())
        .first()
    )

    def serialize_detection(run):
        if not run:
            return {
                'status': 'idle',
                'message': 'No detection job yet.',
            }

        if run.status == 'success':
            message = (
                f'{run.candidates_cached} cached, '
                f'{run.people_upserted} people updated, '
                f'{run.events_created} events created'
            )
        elif run.status == 'error':
            message = run.error_message or 'Unknown error'
        else:
            message = 'Detection is running in background...'

        return {
            'id': run.id,
            'status': run.status,
            'message': message,
        }

    def serialize_task(run, idle_message):
        if not run:
            return {
                'status': 'idle',
                'message': idle_message,
            }

        if run.status == 'success':
            message = run.message or f'{run.success_items} success, {run.error_items} error(s).'
        elif run.status == 'error':
            message = run.message or 'Unknown error'
        else:
            message = run.message or f'{run.task_type} is running in background...'

        return {
            'id': run.id,
            'status': run.status,
            'total_items': run.total_items,
            'processed_items': run.processed_items,
            'success_items': run.success_items,
            'error_items': run.error_items,
            'message': message,
        }

    return jsonify({
        'detection': serialize_detection(detection_run),
        'sync': serialize_task(sync_run, 'No sync job yet.'),
        'expire': serialize_task(expire_run, 'No expire job yet.'),
    })

@bp.post('/actions/candidate/<int:candidate_id>/ensure-person')
def ensure_candidate_person(candidate_id: int):
    candidate = DetectionCandidate.query.get_or_404(candidate_id)
    person, created_or_changed = _upsert_person_from_candidate(candidate)
    db.session.commit()

    if created_or_changed:
        flash(f'{candidate.name} is now available in People.', 'success')
    else:
        flash(f'{candidate.name} was already up to date.', 'success')

    return redirect(url_for('people.detail', person_id=person.id))


@bp.post('/actions/candidate/<int:candidate_id>/trigger-event')
def trigger_candidate_event(candidate_id: int):
    candidate = DetectionCandidate.query.get_or_404(candidate_id)
    settings = AppSettings.get_or_create()

    person, _ = _upsert_person_from_candidate(candidate)

    active_event = TributeEvent.query.filter_by(
        person_id=person.id,
        status='active',
    ).first()

    if active_event:
        sync_event(active_event)
        flash(f'An active event already exists for {person.name}. Collections were rebuilt.', 'success')
        return redirect(url_for('people.detail', person_id=person.id))

    event = create_or_retrigger_event(
        person,
        settings.default_media_mode,
        settings.display_days,
        source='manual',
        note='Triggered from dashboard candidate',
        priority=candidate.popularity_score,
    )
    db.session.commit()

    sync_event(event)

    flash(
        f'Tribute event created for {person.name}. Plex collections were synced immediately.',
        'success',
    )
    return redirect(url_for('people.detail', person_id=person.id))

@bp.post('/actions/candidate/<int:candidate_id>/pin')
def pin_candidate(candidate_id: int):
    candidate = DetectionCandidate.query.get_or_404(candidate_id)
    person = _ensure_person_and_apply_selection(candidate, is_pinned=True)
    db.session.commit()

    flash(f'{person.name} is now pinned for auto selection.', 'success')
    return redirect(url_for('main.dashboard'))


@bp.post('/actions/candidate/<int:candidate_id>/exclude')
def exclude_candidate(candidate_id: int):
    candidate = DetectionCandidate.query.get_or_404(candidate_id)
    person = _ensure_person_and_apply_selection(candidate, exclude_from_auto=True)
    db.session.commit()

    flash(f'{person.name} is now excluded from auto selection.', 'success')
    return redirect(url_for('main.dashboard'))


@bp.post('/actions/candidate/<int:candidate_id>/ignore-7d')
def ignore_candidate_7d(candidate_id: int):
    candidate = DetectionCandidate.query.get_or_404(candidate_id)
    person = _ensure_person_and_apply_selection(candidate, ignore_days=7)
    db.session.commit()

    flash(f'{person.name} will be ignored for 7 days.', 'success')
    return redirect(url_for('main.dashboard'))


@bp.post('/actions/candidate/<int:candidate_id>/promote')
def promote_candidate(candidate_id: int):
    candidate = DetectionCandidate.query.get_or_404(candidate_id)
    boosted_priority = int(candidate.popularity_score or 0) + 5000
    person = _ensure_person_and_apply_selection(candidate, manual_priority=boosted_priority)
    db.session.commit()

    flash(f'{person.name} received a manual priority boost.', 'success')
    return redirect(url_for('main.dashboard'))

@bp.post('/actions/run-sync')
def run_sync():
    run, created = enqueue_task_run(current_app._get_current_object(), 'sync')

    if created:
        flash('Sync started in background.', 'success')
    else:
        flash('A sync job is already running.', 'warning')

    return redirect(url_for('main.dashboard'))


@bp.post('/actions/run-expire')
def run_expire():
    run, created = enqueue_task_run(current_app._get_current_object(), 'expire')

    if created:
        flash('Expire check started in background.', 'success')
    else:
        flash('An expire job is already running.', 'warning')

    return redirect(url_for('main.dashboard'))