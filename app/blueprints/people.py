from datetime import date, timedelta, datetime
from collections import defaultdict
import re
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify
from sqlalchemy.orm import joinedload
from ..utils.country_utils import normalize_country_label

from ..extensions import db, scheduler
from ..models import (
    Person,
    TributeEvent,
    AppSettings,
    make_slug,
    create_or_retrigger_event,
    CollectionPublication,
    LibraryTarget,
    PlexServer,
    ArrServer,
    DetectionCandidate,
    ArrActivity,
    AppLog,
)
from ..services.collection_service import sync_event, remove_event_collections
from ..services.tmdb_service import TmdbService
from ..services.arr_service import ArrService
from ..services.plex_service import PlexService
from ..services.missing_titles_service import refresh_person_missing_titles, load_person_missing_titles
from ..services.arr_push_service import (
    push_missing_titles_for_person,
    push_missing_titles_for_active_person_events,
)
from ..utils.string_utils import normalize_name, similarity
from ..utils.person_duplicates import find_existing_person_duplicate

bp = Blueprint('people', __name__)

def _queue_background_job(job_id: str, fn):
    scheduler.add_job(
        func=fn,
        trigger='date',
        run_date=datetime.now(scheduler.timezone) + timedelta(seconds=1),
        id=job_id,
        replace_existing=False,
        misfire_grace_time=30,
    )


def _run_trigger_event_job(app, person_id: int, event_id: int) -> None:
    from ..services.scheduler_service import log_app_event

    with app.app_context():
        person = Person.query.get(person_id)
        event = TributeEvent.query.get(event_id)

        if not person or not event:
            return

        try:
            sync_event(event)

            settings = AppSettings.get_or_create()
            refresh_person_missing_titles(person, settings=settings)
            arr_result = push_missing_titles_for_person(
                person,
                media_mode=event.media_mode,
                settings=settings,
            )

            log_app_event(
                'info',
                'collections',
                (
                    f'Tribute event synced for "{person.name}". '
                    f'Arr: {arr_result["created_items"]} created, '
                    f'{arr_result["already_exists_items"]} already existed, '
                    f'{arr_result["error_items"]} error.'
                ),
                related_type='person',
                related_id=person.id,
            )
        except Exception as exc:
            db.session.rollback()
            log_app_event(
                'error',
                'collections',
                f'Tribute event sync failed for "{person.name}".',
                details=str(exc),
                related_type='person',
                related_id=person.id,
            )


def _run_rebuild_event_job(app, person_id: int, event_id: int) -> None:
    from ..services.scheduler_service import log_app_event

    with app.app_context():
        person = Person.query.get(person_id)
        event = TributeEvent.query.get(event_id)

        if not person or not event:
            return

        try:
            sync_event(event)

            settings = AppSettings.get_or_create()
            refresh_person_missing_titles(person, settings=settings)
            arr_result = push_missing_titles_for_person(
                person,
                media_mode=event.media_mode,
                settings=settings,
            )

            log_app_event(
                'info',
                'collections',
                (
                    f'Collection rebuild finished for "{person.name}". '
                    f'Arr: {arr_result["created_items"]} created, '
                    f'{arr_result["already_exists_items"]} already existed, '
                    f'{arr_result["error_items"]} error.'
                ),
                related_type='person',
                related_id=person.id,
            )
        except Exception as exc:
            db.session.rollback()
            log_app_event(
                'error',
                'collections',
                f'Collection rebuild failed for "{person.name}".',
                details=str(exc),
                related_type='person',
                related_id=person.id,
            )


def _run_stop_event_job(app, person_id: int, event_id: int) -> None:
    from ..services.scheduler_service import log_app_event

    with app.app_context():
        person = Person.query.get(person_id)
        event = TributeEvent.query.get(event_id)

        if not person or not event:
            return

        try:
            remove_event_collections(event)
            event.status = 'cancelled'
            db.session.commit()

            log_app_event(
                'info',
                'collections',
                f'Tribute event stopped and collections removed for "{person.name}".',
                related_type='person',
                related_id=person.id,
            )
        except Exception as exc:
            db.session.rollback()
            log_app_event(
                'error',
                'collections',
                f'Failed to stop tribute event for "{person.name}".',
                details=str(exc),
                related_type='person',
                related_id=person.id,
            )

_TMDb_KNOWN_DEPARTMENT_TO_PROFESSION = {
    'Acting': 'actor',
    'Directing': 'director',
    'Writing': 'writer',
    'Production': 'producer',
    'Editing': 'editor',
    'Sound': 'composer',
    'Camera': 'cinematographer',
    'Art': 'designer',
    'Costume & Make-Up': 'designer',
    'Visual Effects': 'vfx',
    'Creator': 'creator',
}

def _run_refresh_metadata_job(app, person_id: int) -> None:
    from ..services.scheduler_service import log_app_event

    with app.app_context():
        person = Person.query.get(person_id)
        if not person:
            return

        settings = AppSettings.get_or_create()

        if not settings.tmdb_api_key:
            log_app_event(
                'warning',
                'tmdb',
                f'TMDb refresh skipped for "{person.name}": API key is not configured.',
                related_type='person',
                related_id=person.id,
            )
            return

        try:
            enriched = _enrich_person_from_tmdb(person, settings)

            if not enriched:
                log_app_event(
                    'warning',
                    'tmdb',
                    f'No TMDb match found for "{person.name}".',
                    related_type='person',
                    related_id=person.id,
                )
                return

            db.session.commit()
            refresh_person_missing_titles(person, settings=settings)

            log_app_event(
                'info',
                'tmdb',
                f'Person metadata refreshed from TMDb for "{person.name}".',
                related_type='person',
                related_id=person.id,
            )
        except Exception as exc:
            db.session.rollback()
            log_app_event(
                'error',
                'tmdb',
                f'TMDb refresh failed for "{person.name}".',
                details=str(exc),
                related_type='person',
                related_id=person.id,
            )


def _run_refresh_missing_titles_job(app, person_id: int) -> None:
    from ..services.scheduler_service import log_app_event

    with app.app_context():
        person = Person.query.get(person_id)
        if not person:
            return

        settings = AppSettings.get_or_create()

        if not settings.tmdb_api_key:
            log_app_event(
                'warning',
                'tmdb',
                f'Missing titles refresh skipped for "{person.name}": TMDb API key is not configured.',
                related_type='person',
                related_id=person.id,
            )
            return

        try:
            refresh_person_missing_titles(person, settings=settings)

            db.session.expire_all()
            refreshed_person = Person.query.get(person_id)
            if not refreshed_person:
                return

            arr_result = push_missing_titles_for_active_person_events(
                refreshed_person,
                settings=settings,
            )

            log_app_event(
                'info' if arr_result.get('error_items', 0) == 0 else 'warning',
                'tmdb',
                (
                    f'Missing titles refreshed for "{refreshed_person.name}". '
                    f'Arr: {arr_result["created_items"]} created, '
                    f'{arr_result["already_exists_items"]} already existed, '
                    f'{arr_result["error_items"]} error, '
                    f'{arr_result["skipped_items"]} skipped.'
                ),
                related_type='person',
                related_id=refreshed_person.id,
            )
        except Exception as exc:
            db.session.rollback()
            log_app_event(
                'error',
                'tmdb',
                f'Missing titles refresh failed for "{person.name}".',
                details=str(exc),
                related_type='person',
                related_id=person.id,
            )

def _normalize_duplicate_key(value: str | None) -> str:
    if not value:
        return ''

    normalized = value.strip().lower()
    normalized = re.sub(r'[^a-z0-9]+', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized


def _build_duplicate_groups(people: list[Person]) -> list[dict]:
    by_slug = defaultdict(list)
    by_name_and_death = defaultdict(list)

    for person in people:
        if person.slug:
            by_slug[person.slug].append(person)

        normalized_name = _normalize_duplicate_key(person.name)
        if normalized_name and person.death_date:
            by_name_and_death[(normalized_name, person.death_date.isoformat())].append(person)

    raw_groups = []

    for slug, grouped_people in by_slug.items():
        if len(grouped_people) > 1:
            raw_groups.append({
                'reason': f'same slug: {slug}',
                'people': grouped_people,
            })

    for (normalized_name, death_date), grouped_people in by_name_and_death.items():
        if len(grouped_people) > 1:
            raw_groups.append({
                'reason': f'same normalized name + death date: {normalized_name} / {death_date}',
                'people': grouped_people,
            })

    deduped_groups = []
    seen_signatures = set()

    for group in raw_groups:
        signature = tuple(sorted(person.id for person in group['people']))
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        deduped_groups.append({
            'reason': group['reason'],
            'people': sorted(
                group['people'],
                key=lambda person: (
                    person.death_date or date.min,
                    person.created_at or datetime.min,
                    (person.name or '').lower(),
                ),
                reverse=True,
            ),
        })

    deduped_groups.sort(
        key=lambda group: (
            len(group['people']),
            group['reason'],
        ),
        reverse=True,
    )

    return deduped_groups

def _extract_professions_from_tmdb(details: dict) -> str | None:
    department = (details.get('known_for_department') or '').strip()
    if not department:
        return None

    profession = _TMDb_KNOWN_DEPARTMENT_TO_PROFESSION.get(department)
    return profession


def _enrich_person_from_tmdb(person: Person, settings: AppSettings) -> bool:
    if not settings.tmdb_api_key:
        return False

    tmdb = TmdbService(settings.tmdb_api_key)

    match = None
    if person.tmdb_person_id:
        try:
            details = tmdb.person_details(person.tmdb_person_id)
            match = {'id': person.tmdb_person_id, 'name': details.get('name')}
        except Exception:
            details = {}
    else:
        details = {}
        match = tmdb.search_person(
            person.name,
            death_date=person.death_date.isoformat() if person.death_date else None,
        )
        if not match:
            return False

        person.tmdb_person_id = match.get('id') or person.tmdb_person_id
        person.tmdb_manual_override = False

    if person.tmdb_person_id and not details:
        details = tmdb.person_details(person.tmdb_person_id)

    external_ids = {}
    if person.tmdb_person_id:
        try:
            external_ids = tmdb.person_external_ids(person.tmdb_person_id)
        except Exception:
            external_ids = {}

    tmdb_name = (details.get('name') or (match or {}).get('name') or '').strip()
    if tmdb_name:
        proposed_slug = make_slug(tmdb_name)
        existing_person = (
            Person.query
            .filter(Person.slug == proposed_slug, Person.id != person.id)
            .first()
        )

        person.name = tmdb_name

        if not existing_person:
            person.slug = proposed_slug

    if person.tmdb_person_id:
        person.source_url = f'https://www.themoviedb.org/person/{person.tmdb_person_id}'

    deathday = details.get('deathday')
    place_of_birth = (details.get('place_of_birth') or '').strip()

    if deathday:
        try:
            person.death_date = datetime.strptime(deathday, '%Y-%m-%d').date()
        except ValueError:
            pass

    normalized_country = normalize_country_label(place_of_birth)
    if normalized_country:
        person.country = normalized_country
    else:
        person.country = normalize_country_label(person.country)

    tmdb_profession = _extract_professions_from_tmdb(details)
    if tmdb_profession:
        person.professions_csv = tmdb_profession

    elif (match or {}).get('known_for_department') and not person.professions_csv:
        fallback_profession = _TMDb_KNOWN_DEPARTMENT_TO_PROFESSION.get((match or {}).get('known_for_department'))
        if fallback_profession:
            person.professions_csv = fallback_profession

    imdb_id = (external_ids.get('imdb_id') or '').strip()
    wikidata_id = (external_ids.get('wikidata_id') or '').strip()

    if imdb_id:
        person.imdb_id = imdb_id

    if wikidata_id:
        person.wikidata_id = wikidata_id

    return True

def _build_people_rows(people: list[Person]) -> list[dict]:
    candidate_rows = db.session.query(
        DetectionCandidate.slug,
        DetectionCandidate.popularity_score,
    ).all()

    candidate_priority_by_slug = {
        slug: int(popularity_score or 0)
        for slug, popularity_score in candidate_rows
    }

    candidate_slugs = set(candidate_priority_by_slug.keys())

    rows = []

    for person in people:
        active_event = next(
            (
                event for event in person.events
                if event.status == 'active' and event.is_active
            ),
            None,
        )

        if active_event:
            status = 'active_event'
            in_event_label = f'{active_event.start_date} → {active_event.end_date}'
            days_left = active_event.days_remaining
        elif person.slug in candidate_slugs:
            status = 'candidate'
            in_event_label = '—'
            days_left = None
        else:
            status = 'known_person'
            in_event_label = '—'
            days_left = None

        missing_movies, missing_shows = load_person_missing_titles(person)
        missing_movies_count = len(missing_movies)
        missing_shows_count = len(missing_shows)
        missing_total_count = missing_movies_count + missing_shows_count

        rows.append({
            'person': person,
            'active_event': active_event,
            'status': status,
            'is_candidate': person.slug in candidate_slugs,
            'is_pinned': person.is_pinned,
            'is_excluded': person.exclude_from_auto,
            'is_ignored': person.is_ignored_now,
            'is_forced': person.force_publish,
            'manual_priority': person.manual_priority,
            'candidate_priority': candidate_priority_by_slug.get(person.slug),
            'effective_priority': candidate_priority_by_slug.get(person.slug, int(person.web_priority or 0)),
            'in_event_label': in_event_label,
            'days_left': days_left,
            'missing_movies_count': missing_movies_count,
            'missing_shows_count': missing_shows_count,
            'missing_total_count': missing_total_count,
            'missing_titles_status': person.missing_titles_status,
            'missing_titles_scanned_at': person.missing_titles_scanned_at,
            'missing_titles_error': person.missing_titles_error,
        })

    return rows

@bp.route('/')
def index():
    settings = AppSettings.get_or_create()

    q = (request.args.get('q') or '').strip().lower()
    status_filter = (request.args.get('status') or 'all').strip()
    source_filter = (request.args.get('source') or 'all').strip()
    missing_filter = (request.args.get('missing') or 'all').strip()
    sort_by = (request.args.get('sort') or 'death_desc').strip()

    page_raw = (request.args.get('page') or '1').strip()
    per_page = 4

    try:
        page = int(page_raw)
    except ValueError:
        page = 1

    if page < 1:
        page = 1

    people = (
        Person.query
        .options(joinedload(Person.events))
        .all()
    )

    person_rows = _build_people_rows(people)
    duplicate_groups = _build_duplicate_groups(people)

    if q:
        filtered_rows = []
        for row in person_rows:
            person = row['person']
            haystack = ' '.join([
                person.name or '',
                person.country or '',
                person.source or '',
                person.professions_csv or '',
                person.notes or '',
                person.selection_note or '',
            ]).lower()

            if q in haystack:
                filtered_rows.append(row)

        person_rows = filtered_rows

    if status_filter != 'all':
        person_rows = [
            row for row in person_rows
            if row['status'] == status_filter
        ]

    if source_filter != 'all':
        person_rows = [
            row for row in person_rows
            if (row['person'].source or '') == source_filter
        ]

    if missing_filter == 'with_missing':
        person_rows = [
            row for row in person_rows
            if row['missing_total_count'] > 0
        ]
    elif missing_filter == 'scan_error':
        person_rows = [
            row for row in person_rows
            if row['missing_titles_status'] == 'error'
        ]
    elif missing_filter == 'pending':
        person_rows = [
            row for row in person_rows
            if row['missing_titles_status'] == 'pending'
        ]

    if sort_by == 'death_asc':
        person_rows.sort(
            key=lambda row: (
                row['person'].death_date or date.min,
                (row['person'].name or '').lower(),
            )
        )
    elif sort_by == 'created_desc':
        person_rows.sort(
            key=lambda row: (
                row['person'].created_at or date.min,
                (row['person'].name or '').lower(),
            ),
            reverse=True,
        )
    elif sort_by == 'created_asc':
        person_rows.sort(
            key=lambda row: (
                row['person'].created_at or date.min,
                (row['person'].name or '').lower(),
            )
        )
    elif sort_by == 'priority_desc':
        person_rows.sort(
            key=lambda row: (
                1 if row['person'].is_pinned else 0,
                row['person'].manual_priority if row['person'].manual_priority is not None else -1,
                row['person'].death_date or date.min,
                (row['person'].name or '').lower(),
            ),
            reverse=True,
        )
    elif sort_by == 'missing_desc':
        person_rows.sort(
            key=lambda row: (
                row['missing_total_count'],
                row['missing_movies_count'],
                row['missing_shows_count'],
                row['person'].death_date or date.min,
                (row['person'].name or '').lower(),
            ),
            reverse=True,
        )
    else:
        person_rows.sort(
            key=lambda row: (
                row['person'].death_date or date.min,
                row['person'].created_at or date.min,
                (row['person'].name or '').lower(),
            ),
            reverse=True,
        )

    total_people_count = len(person_rows)
    total_pages = max((total_people_count + per_page - 1) // per_page, 1)

    if page > total_pages:
        page = total_pages

    start_index = (page - 1) * per_page
    end_index = start_index + per_page

    paginated_rows = person_rows[start_index:end_index]

    page_rows_start = start_index + 1 if total_people_count > 0 else 0
    page_rows_end = min(end_index, total_people_count)

    return render_template(
        'people.html',
        person_rows=paginated_rows,
        settings=settings,
        q=q,
        status_filter=status_filter,
        source_filter=source_filter,
        missing_filter=missing_filter,
        sort_by=sort_by,
        duplicate_groups=duplicate_groups,
        page=page,
        per_page=per_page,
        total_people_count=total_people_count,
        total_pages=total_pages,
        page_rows_start=page_rows_start,
        page_rows_end=page_rows_end,
    )


def find_possible_duplicates(person: Person, threshold: float = 0.85):
    results = []

    base_name = normalize_name(person.name)
    if not base_name:
        return results

    all_people = Person.query.filter(Person.id != person.id).all()

    for other in all_people:
        other_name = normalize_name(other.name)
        if not other_name:
            continue

        score = 0.0
        reasons = []

        # Match fort par IDs externes
        if person.tmdb_person_id and other.tmdb_person_id and person.tmdb_person_id == other.tmdb_person_id:
            score = max(score, 1.0)
            reasons.append('same TMDb ID')

        if person.imdb_id and other.imdb_id and person.imdb_id == other.imdb_id:
            score = max(score, 1.0)
            reasons.append('same IMDb ID')

        if person.wikidata_id and other.wikidata_id and person.wikidata_id == other.wikidata_id:
            score = max(score, 1.0)
            reasons.append('same Wikidata ID')

        # Similarité de nom
        name_score = similarity(base_name, other_name)
        score = max(score, name_score)

        if name_score >= 0.99:
            reasons.append('same normalized name')
        elif name_score >= threshold:
            reasons.append(f'name similarity {round(name_score * 100)}%')

        # Bonus si même date de décès
        if person.death_date and other.death_date and person.death_date == other.death_date:
            score = min(score + 0.05, 1.0)
            reasons.append('same death date')

        # Bonus léger si même pays
        if person.country and other.country and person.country == other.country:
            score = min(score + 0.02, 1.0)
            reasons.append('same country')

        if score >= threshold:
            results.append({
                'person': other,
                'score': round(score, 2),
                'reason': ', '.join(dict.fromkeys(reasons)) if reasons else 'possible duplicate',
                'suggest_merge': score >= 0.95,
            })

    results.sort(
        key=lambda item: (
            item['suggest_merge'],
            item['score'],
            (item['person'].created_at or datetime.min),
        ),
        reverse=True,
    )
    return results

@bp.post('/create')
def create_person():
    settings = AppSettings.get_or_create()

    name = request.form['name'].strip()
    slug = make_slug(name)
    death_date = date.fromisoformat(request.form['death_date'])

    duplicate, duplicate_reason = find_existing_person_duplicate(
        slug=slug,
        name=name,
        death_date=death_date,
    )
    if duplicate:
        flash(f'Person already exists ({duplicate_reason}).', 'warning')
        return redirect(url_for('people.detail', person_id=duplicate.id))

    person = Person(
        name=name,
        slug=slug,
        death_date=death_date,
        country=normalize_country_label(request.form.get('country', '').strip() or None),
        professions_csv=request.form.get('professions_csv', '').strip() or None,
        source='manual',
        notes=request.form.get('notes', '').strip() or None,
    )
    db.session.add(person)
    db.session.flush()

    enriched = False
    try:
        enriched = _enrich_person_from_tmdb(person, settings)
    except Exception as exc:
        flash(f'Person added, but TMDb enrichment failed: {exc}', 'warning')

    duplicate, duplicate_reason = find_existing_person_duplicate(
        person_id=person.id,
        slug=person.slug,
        name=person.name,
        death_date=person.death_date,
        tmdb_person_id=person.tmdb_person_id,
        imdb_id=person.imdb_id,
        wikidata_id=person.wikidata_id,
    )
    if duplicate:
        duplicate_id = duplicate.id
        duplicate_name = duplicate.name
        db.session.rollback()
        flash(f'Duplicate prevented: matched existing person "{duplicate_name}" ({duplicate_reason}).', 'warning')
        return redirect(url_for('people.detail', person_id=duplicate_id))

    db.session.commit()

    if enriched:
        flash('Person added and enriched from TMDb.', 'success')
    else:
        flash('Person added.', 'success')

    return redirect(url_for('people.detail', person_id=person.id))

def _merge_people(source: Person, target: Person) -> None:
    if source.id == target.id:
        raise ValueError('Source and target must be different people.')

    if not target.country and source.country:
        target.country = source.country

    if not target.professions_csv and source.professions_csv:
        target.professions_csv = source.professions_csv

    if not target.source_url and source.source_url:
        target.source_url = source.source_url

    if not target.tmdb_person_id and source.tmdb_person_id:
        target.tmdb_person_id = source.tmdb_person_id

    if not target.imdb_id and source.imdb_id:
        target.imdb_id = source.imdb_id

    if not target.wikidata_id and source.wikidata_id:
        target.wikidata_id = source.wikidata_id

    if not target.notes and source.notes:
        target.notes = source.notes
    elif target.notes and source.notes and source.notes.strip() not in target.notes:
        target.notes = f'{target.notes}\n\n---\nMerged notes:\n{source.notes}'

    if not target.selection_note and source.selection_note:
        target.selection_note = source.selection_note
    elif target.selection_note and source.selection_note and source.selection_note.strip() not in target.selection_note:
        target.selection_note = f'{target.selection_note}\n\n---\nMerged selection note:\n{source.selection_note}'

    if target.manual_priority is None and source.manual_priority is not None:
        target.manual_priority = source.manual_priority
    elif target.manual_priority is not None and source.manual_priority is not None:
        target.manual_priority = max(target.manual_priority, source.manual_priority)

    target.is_pinned = target.is_pinned or source.is_pinned
    target.exclude_from_auto = target.exclude_from_auto or source.exclude_from_auto
    target.force_publish = target.force_publish or source.force_publish

    if target.ignore_until and source.ignore_until:
        target.ignore_until = max(target.ignore_until, source.ignore_until)
    elif not target.ignore_until and source.ignore_until:
        target.ignore_until = source.ignore_until

    if not target.death_date and source.death_date:
        target.death_date = source.death_date

    for event in source.events:
        event.person = target

    for activity in source.arr_activities:
        activity.person = target

    DetectionCandidate.query.filter_by(slug=source.slug).delete(synchronize_session=False)

    db.session.delete(source)

@bp.post('/<int:person_id>/refresh-metadata')
def refresh_metadata(person_id: int):
    from ..services.scheduler_service import log_app_event

    person = Person.query.get_or_404(person_id)
    settings = AppSettings.get_or_create()

    if not settings.tmdb_api_key:
        flash('TMDb API key is not configured.', 'warning')
        return redirect(url_for('people.detail', person_id=person.id))

    app_obj = current_app._get_current_object()
    job_id = f'refresh_metadata_{person.id}_{int(datetime.utcnow().timestamp())}'

    _queue_background_job(
        job_id,
        lambda app=app_obj, pid=person.id: _run_refresh_metadata_job(app, pid),
    )

    log_app_event(
        'info',
        'tmdb',
        f'TMDb refresh queued for "{person.name}".',
        related_type='person',
        related_id=person.id,
    )

    flash(
        f'TMDb refresh started in background for "{person.name}". Refresh the page later to see the updated metadata.',
        'success',
    )
    return redirect(url_for('people.detail', person_id=person.id))

@bp.get('/<int:person_id>/tmdb-candidates')
def tmdb_candidates(person_id: int):
    person = Person.query.get_or_404(person_id)
    settings = AppSettings.get_or_create()

    if not settings.tmdb_api_key:
        return jsonify({
            'ok': False,
            'message': 'TMDb API key is not configured.',
            'candidates': [],
        }), 400

    try:
        candidates = _get_tmdb_candidates_for_person(person, settings)
    except Exception as exc:
        return jsonify({
            'ok': False,
            'message': f'TMDb candidate lookup failed: {exc}',
            'candidates': [],
        }), 502

    return jsonify({
        'ok': True,
        'person': {
            'id': person.id,
            'name': person.name,
            'tmdb_person_id': person.tmdb_person_id,
            'tmdb_manual_override': person.tmdb_manual_override,
        },
        'candidates': [_serialize_tmdb_candidate(candidate) for candidate in candidates],
    })


@bp.post('/<int:person_id>/link-tmdb')
@bp.post('/<int:person_id>/select-tmdb-match')
def link_tmdb(person_id: int):
    person = Person.query.get_or_404(person_id)
    settings = AppSettings.get_or_create()

    if not settings.tmdb_api_key:
        message = 'TMDb API key is not configured.'
        if _wants_json_response():
            return jsonify({'ok': False, 'message': message}), 400
        flash(message, 'warning')
        return redirect(url_for('people.detail', person_id=person.id))

    tmdb_person_id_raw = (request.form.get('tmdb_person_id') or '').strip()

    try:
        tmdb_person_id = int(tmdb_person_id_raw)
    except (TypeError, ValueError):
        message = 'Invalid TMDb person ID.'
        if _wants_json_response():
            return jsonify({'ok': False, 'message': message}), 400
        flash(message, 'danger')
        return redirect(url_for('people.detail', person_id=person.id))

    existing_person, duplicate_reason = find_existing_person_duplicate(
        person_id=person.id,
        tmdb_person_id=tmdb_person_id,
    )
    if existing_person:
        message = (
            f'Cannot link this TMDb person: it is already used by "{existing_person.name}" ({duplicate_reason}).'
        )
        if _wants_json_response():
            return jsonify({
                'ok': False,
                'message': message,
                'existing_person': {
                    'id': existing_person.id,
                    'name': existing_person.name,
                },
            }), 409
        flash(message, 'warning')
        return redirect(url_for('people.detail', person_id=existing_person.id))

    try:
        person.tmdb_person_id = tmdb_person_id
        person.tmdb_manual_override = True

        enriched = _enrich_person_from_tmdb(person, settings)
        if not enriched:
            db.session.rollback()
            message = 'Selected TMDb person could not be loaded.'
            if _wants_json_response():
                return jsonify({'ok': False, 'message': message}), 404
            flash(message, 'danger')
            return redirect(url_for('people.detail', person_id=person.id))

        existing_person, duplicate_reason = find_existing_person_duplicate(
            person_id=person.id,
            slug=person.slug,
            name=person.name,
            death_date=person.death_date,
            tmdb_person_id=person.tmdb_person_id,
            imdb_id=person.imdb_id,
            wikidata_id=person.wikidata_id,
        )
        if existing_person:
            existing_person_id = existing_person.id
            existing_person_name = existing_person.name
            db.session.rollback()
            message = (
                f'Duplicate prevented: matched existing person "{existing_person_name}" ({duplicate_reason}).'
            )
            if _wants_json_response():
                return jsonify({
                    'ok': False,
                    'message': message,
                    'existing_person': {
                        'id': existing_person_id,
                        'name': existing_person_name,
                    },
                }), 409
            flash(message, 'warning')
            return redirect(url_for('people.detail', person_id=existing_person_id))

        db.session.commit()
        refresh_person_missing_titles(person, settings=settings)

        success_message = f'TMDb link updated manually for "{person.name}".'
        if _wants_json_response():
            return jsonify({
                'ok': True,
                'message': success_message,
                'person': {
                    'id': person.id,
                    'name': person.name,
                    'tmdb_person_id': person.tmdb_person_id,
                    'tmdb_manual_override': person.tmdb_manual_override,
                    'source_url': person.source_url,
                },
            })

        flash(success_message, 'success')
    except Exception as exc:
        db.session.rollback()
        error_message = f'TMDb manual link failed: {exc}'
        if _wants_json_response():
            return jsonify({'ok': False, 'message': error_message}), 400
        flash(error_message, 'danger')

    return redirect(url_for('people.detail', person_id=person.id))


@bp.post('/<int:person_id>/rematch-tmdb')
def rematch_tmdb(person_id: int):
    person = Person.query.get_or_404(person_id)
    settings = AppSettings.get_or_create()

    person.tmdb_person_id = None
    person.tmdb_manual_override = False

    if (person.source_url or '').startswith('https://www.themoviedb.org/person/'):
        person.source_url = None

    db.session.commit()

    candidates = []
    if settings.tmdb_api_key:
        try:
            candidates = _get_tmdb_candidates_for_person(person, settings)
        except Exception:
            candidates = []

    success_message = (
        f'TMDb link cleared for "{person.name}". Review the suggested candidates below and pick the correct one.'
    )

    if _wants_json_response():
        return jsonify({
            'ok': True,
            'message': success_message,
            'person': {
                'id': person.id,
                'name': person.name,
                'tmdb_person_id': person.tmdb_person_id,
                'tmdb_manual_override': person.tmdb_manual_override,
            },
            'candidates': [_serialize_tmdb_candidate(candidate) for candidate in candidates],
        })

    flash(success_message, 'success')
    return redirect(url_for('people.detail', person_id=person.id))

@bp.post('/<int:person_id>/refresh-missing-titles')
def refresh_missing_titles(person_id: int):
    from ..services.scheduler_service import log_app_event

    person = Person.query.get_or_404(person_id)
    settings = AppSettings.get_or_create()

    if not settings.tmdb_api_key:
        flash('TMDb API key is not configured.', 'warning')
        return redirect(url_for('people.detail', person_id=person.id))

    app_obj = current_app._get_current_object()
    job_id = f'refresh_missing_titles_{person.id}_{int(datetime.utcnow().timestamp())}'

    _queue_background_job(
        job_id,
        lambda app=app_obj, pid=person.id: _run_refresh_missing_titles_job(app, pid),
    )

    log_app_event(
        'info',
        'tmdb',
        f'Missing titles refresh queued for "{person.name}".',
        related_type='person',
        related_id=person.id,
    )

    flash(
        f'Missing titles refresh started in background for "{person.name}". Refresh the page later to see the updated result.',
        'success',
    )
    return redirect(url_for('people.detail', person_id=person.id))

@bp.post('/<int:person_id>/merge-into')
def merge_into(person_id: int):
    source = (
        Person.query
        .options(
            joinedload(Person.events).joinedload(TributeEvent.publications).joinedload(CollectionPublication.target),
            joinedload(Person.arr_activities),
        )
        .get_or_404(person_id)
    )

    target_id_raw = (request.form.get('target_person_id') or '').strip()
    if not target_id_raw:
        flash('Target person ID is required.', 'warning')
        return redirect(url_for('people.detail', person_id=source.id))

    try:
        target_id = int(target_id_raw)
    except ValueError:
        flash('Target person ID must be a number.', 'danger')
        return redirect(url_for('people.detail', person_id=source.id))

    if target_id == source.id:
        flash('You cannot merge a person into itself.', 'warning')
        return redirect(url_for('people.detail', person_id=source.id))

    target = (
        Person.query
        .options(
            joinedload(Person.events),
            joinedload(Person.arr_activities),
        )
        .get(target_id)
    )

    if not target:
        flash('Target person not found.', 'warning')
        return redirect(url_for('people.detail', person_id=source.id))

    source_name = source.name
    target_name = target.name
    target_person_id = target.id

    try:
        _merge_people(source, target)
        db.session.commit()
        flash(f'{source_name} merged into {target_name}.', 'success')
        return redirect(url_for('people.detail', person_id=target_person_id))

    except Exception as exc:
        db.session.rollback()
        flash(f'Failed to merge people: {exc}', 'danger')
        return redirect(url_for('people.detail', person_id=source.id))

@bp.post('/bulk-action')
def bulk_action():
    person_ids_raw = request.form.getlist('person_ids')
    action = (request.form.get('bulk_action') or '').strip()

    redirect_kwargs = {
        'q': (request.form.get('q') or '').strip(),
        'status': (request.form.get('status') or 'all').strip(),
        'source': (request.form.get('source') or 'all').strip(),
        'sort': (request.form.get('sort') or 'death_desc').strip(),
    }

    if not person_ids_raw:
        flash('Select at least one person.', 'warning')
        return redirect(url_for('people.index', **redirect_kwargs))

    try:
        person_ids = [int(value) for value in person_ids_raw]
    except ValueError:
        flash('Invalid person selection.', 'danger')
        return redirect(url_for('people.index', **redirect_kwargs))

    persons = (
        Person.query
        .options(
            joinedload(Person.events).joinedload(TributeEvent.publications).joinedload(CollectionPublication.target),
            joinedload(Person.arr_activities),
        )
        .filter(Person.id.in_(person_ids))
        .all()
    )

    if not persons:
        flash('No matching people found.', 'warning')
        return redirect(url_for('people.index', **redirect_kwargs))

    today = date.today()
    count = 0

    try:
        if action == 'pin':
            for person in persons:
                if not person.is_pinned:
                    person.is_pinned = True
                    count += 1
            db.session.commit()
            flash(f'{count} person(s) pinned.', 'success')

        elif action == 'unpin':
            for person in persons:
                if person.is_pinned:
                    person.is_pinned = False
                    count += 1
            db.session.commit()
            flash(f'{count} person(s) unpinned.', 'success')

        elif action == 'exclude':
            for person in persons:
                changed = False
                if not person.exclude_from_auto:
                    person.exclude_from_auto = True
                    changed = True
                if person.ignore_until is not None:
                    person.ignore_until = None
                    changed = True
                if changed:
                    count += 1
            db.session.commit()
            flash(f'{count} person(s) excluded.', 'success')

        elif action == 'unexclude':
            for person in persons:
                if person.exclude_from_auto:
                    person.exclude_from_auto = False
                    count += 1
            db.session.commit()
            flash(f'{count} person(s) unexcluded.', 'success')

        elif action == 'ignore_30':
            for person in persons:
                if not person.exclude_from_auto:
                    person.ignore_until = today + timedelta(days=30)
                    count += 1
            db.session.commit()
            flash(f'{count} person(s) ignored for 30 days.', 'success')

        elif action == 'clear_ignore':
            for person in persons:
                if person.ignore_until is not None:
                    person.ignore_until = None
                    count += 1
            db.session.commit()
            flash(f'{count} ignore flag(s) cleared.', 'success')

        elif action == 'delete':
            for person in persons:
                for event in person.events:
                    if event.publications:
                        remove_event_collections(event)

                DetectionCandidate.query.filter_by(slug=person.slug).delete(synchronize_session=False)
                db.session.delete(person)
                count += 1

            db.session.commit()
            flash(f'{count} person(s) deleted.', 'success')

        else:
            flash('Unknown bulk action.', 'danger')

    except Exception as exc:
        db.session.rollback()
        flash(f'Bulk action failed: {exc}', 'danger')

    return redirect(url_for('people.index', **redirect_kwargs))

def _serialize_tmdb_candidate(candidate: dict) -> dict:
    return {
        'id': candidate.get('id'),
        'name': candidate.get('name'),
        'match_score': candidate.get('match_score'),
        'match_popularity': candidate.get('match_popularity'),
        'deathday': candidate.get('deathday'),
        'known_for_department': candidate.get('known_for_department'),
        'also_known_as': candidate.get('also_known_as') or [],
        'profile_image_url': candidate.get('profile_image_url'),
        'source_url': (
            f"https://www.themoviedb.org/person/{candidate.get('id')}"
            if candidate.get('id') else None
        ),
    }


def _wants_json_response() -> bool:
    if request.args.get('format') == 'json':
        return True

    accept = (request.headers.get('Accept') or '').lower()
    requested_with = (request.headers.get('X-Requested-With') or '').lower()
    return 'application/json' in accept or requested_with == 'xmlhttprequest'


def _get_tmdb_candidates_for_person(person: Person, settings: AppSettings, *, limit: int = 5) -> list[dict]:
    if not settings.tmdb_api_key:
        return []

    tmdb = TmdbService(settings.tmdb_api_key)
    return tmdb.search_person_candidates(
        person.name,
        death_date=person.death_date.isoformat() if person.death_date else None,
        limit=limit,
    )


def _load_tmdb_context(person: Person, settings: AppSettings):
    tmdb_match = None
    tmdb_credits = {'cast': [], 'crew': []}

    if not settings.tmdb_api_key:
        return tmdb_match, tmdb_credits

    tmdb = TmdbService(settings.tmdb_api_key)
    tmdb_person_id = person.tmdb_person_id

    if not tmdb_person_id:
        tmdb_match = tmdb.search_person(
            person.name,
            death_date=person.death_date.isoformat() if person.death_date else None,
        )
        tmdb_person_id = (tmdb_match or {}).get('id')

    if tmdb_person_id:
        tmdb_credits = tmdb.person_credits(tmdb_person_id)

    return tmdb_match, tmdb_credits


def _load_tmdb_person_photo(person: Person, settings: AppSettings, tmdb_match: dict | None = None) -> str | None:
    if not settings.tmdb_api_key:
        return None

    try:
        tmdb = TmdbService(settings.tmdb_api_key)
        tmdb_person_id = person.tmdb_person_id or (tmdb_match or {}).get('id')

        if not tmdb_person_id:
            return None

        return tmdb.person_profile_image_url(tmdb_person_id)

    except Exception:
        return None

@bp.route('/<int:person_id>')
def detail(person_id: int):
    person = (
        Person.query
        .options(
            joinedload(Person.events).joinedload(TributeEvent.publications).joinedload(CollectionPublication.target),
            joinedload(Person.arr_activities).joinedload(ArrActivity.arr_server),
            joinedload(Person.arr_activities).joinedload(ArrActivity.library_target),
        )
        .get_or_404(person_id)
    )
    settings = AppSettings.get_or_create()

    tmdb_match = None
    tmdb_candidates = []
    tmdb_credits = {'cast': [], 'crew': []}
    person_photo_url = None
    missing_movies, missing_shows = load_person_missing_titles(person)
    force_refresh_missing = (request.args.get('scan_missing') or '') == '1'

    library_targets = (
        LibraryTarget.query
        .join(PlexServer, LibraryTarget.plex_server_id == PlexServer.id)
        .outerjoin(ArrServer, LibraryTarget.arr_server_id == ArrServer.id)
        .filter(
            LibraryTarget.enabled.is_(True),
            PlexServer.enabled.is_(True),
        )
        .order_by(PlexServer.name.asc(), LibraryTarget.section_name.asc())
        .all()
    )

    if settings.tmdb_api_key:
        try:
            tmdb_match, tmdb_credits = _load_tmdb_context(person, settings)
            person_photo_url = _load_tmdb_person_photo(person, settings, tmdb_match=tmdb_match)

            if not person.tmdb_person_id:
                tmdb_candidates = _get_tmdb_candidates_for_person(person, settings)

            if force_refresh_missing:
                missing_movies, missing_shows = refresh_person_missing_titles(
                    person,
                    settings=settings,
                    tmdb_credits=tmdb_credits,
                )
            else:
                missing_movies, missing_shows = load_person_missing_titles(person)

        except Exception as exc:
            flash(f'TMDB lookup failed: {exc}', 'warning')
            
    duplicates = find_possible_duplicates(person)

    return render_template(
        'person_detail.html',
        person=person,
        settings=settings,
        tmdb_match=tmdb_match,
        tmdb_candidates=tmdb_candidates,
        tmdb_credits=tmdb_credits,
        person_photo_url=person_photo_url,
        missing_movies=missing_movies,
        missing_shows=missing_shows,
        library_targets=library_targets,
        duplicates=duplicates,
    )

@bp.post('/<int:person_id>/selection-settings')
def selection_settings(person_id: int):
    person = Person.query.get_or_404(person_id)

    manual_priority_raw = (request.form.get('manual_priority') or '').strip()
    ignore_days_raw = (request.form.get('ignore_days') or '').strip()

    person.manual_priority = int(manual_priority_raw) if manual_priority_raw else None
    person.is_pinned = request.form.get('is_pinned') == '1'
    person.exclude_from_auto = request.form.get('exclude_from_auto') == '1'
    person.force_publish = request.form.get('force_publish') == '1'
    person.selection_note = request.form.get('selection_note', '').strip() or None

    if person.exclude_from_auto:
        person.ignore_until = None
    else:
        if ignore_days_raw:
            person.ignore_until = date.today() + timedelta(days=max(int(ignore_days_raw), 0))
        elif request.form.get('clear_ignore') == '1':
            person.ignore_until = None

    db.session.commit()
    flash('Selection settings updated.', 'success')
    return redirect(url_for('people.detail', person_id=person.id))

@bp.post('/<int:person_id>/trigger')
def trigger(person_id: int):
    from ..services.scheduler_service import log_app_event

    person = Person.query.get_or_404(person_id)
    settings = AppSettings.get_or_create()

    display_days = int(request.form.get('display_days') or settings.display_days)
    media_mode = request.form.get('media_mode') or settings.default_media_mode

    event = create_or_retrigger_event(
        person,
        media_mode,
        display_days,
        source='manual',
        note=request.form.get('note'),
    )
    db.session.commit()

    app_obj = current_app._get_current_object()
    job_id = f'trigger_event_sync_{event.id}_{int(datetime.utcnow().timestamp())}'

    _queue_background_job(
        job_id,
        lambda app=app_obj, pid=person.id, eid=event.id: _run_trigger_event_job(app, pid, eid),
    )

    log_app_event(
        'info',
        'collections',
        f'Tribute event queued for "{person.name}".',
        related_type='person',
        related_id=person.id,
    )

    flash(
        f'Tribute event queued in background for "{person.name}". Reload in a moment to check Plex publications.',
        'success',
    )
    return redirect(url_for('people.detail', person_id=person.id))


@bp.post('/event/<int:event_id>/rebuild')
def rebuild_event(event_id: int):
    from ..services.scheduler_service import log_app_event

    event = TributeEvent.query.get_or_404(event_id)
    person_id = event.person_id
    person_name = event.person.name

    app_obj = current_app._get_current_object()
    job_id = f'rebuild_event_{event.id}_{int(datetime.utcnow().timestamp())}'

    _queue_background_job(
        job_id,
        lambda app=app_obj, pid=person_id, eid=event.id: _run_rebuild_event_job(app, pid, eid),
    )

    log_app_event(
        'info',
        'collections',
        f'Collection rebuild queued for "{person_name}".',
        related_type='person',
        related_id=person_id,
    )

    flash(
        f'Collection rebuild started in background for "{person_name}". Reload in a moment to check publications.',
        'success',
    )
    return redirect(url_for('people.detail', person_id=person_id))


@bp.post('/event/<int:event_id>/delete')
def delete_event(event_id: int):
    from ..services.scheduler_service import log_app_event

    event = TributeEvent.query.get_or_404(event_id)
    person_id = event.person_id
    person_name = event.person.name

    app_obj = current_app._get_current_object()
    job_id = f'stop_event_{event.id}_{int(datetime.utcnow().timestamp())}'

    _queue_background_job(
        job_id,
        lambda app=app_obj, pid=person_id, eid=event.id: _run_stop_event_job(app, pid, eid),
    )

    log_app_event(
        'info',
        'collections',
        f'Tribute stop queued for "{person_name}".',
        related_type='person',
        related_id=person_id,
    )

    flash(
        f'Event stop started in background for "{person_name}". Reload in a moment to check collection removal.',
        'success',
    )
    return redirect(url_for('people.detail', person_id=person_id))

@bp.post('/<int:person_id>/delete')
def delete_person(person_id: int):
    person = (
        Person.query
        .options(
            joinedload(Person.events).joinedload(TributeEvent.publications).joinedload(CollectionPublication.target),
            joinedload(Person.arr_activities),
        )
        .get_or_404(person_id)
    )

    person_name = person.name
    person_slug = person.slug

    try:
        # Nettoyer d'abord les collections Plex encore publiées
        for event in person.events:
            if event.publications:
                remove_event_collections(event)

        # Supprimer les candidats liés pour éviter une réapparition immédiate
        DetectionCandidate.query.filter_by(slug=person_slug).delete(synchronize_session=False)

        # Supprimer la personne (les events / publications / arr_activities tombent via cascade ORM)
        db.session.delete(person)
        db.session.commit()

        flash(f'{person_name} deleted.', 'success')
        return redirect(url_for('people.index'))

    except Exception as exc:
        db.session.rollback()
        flash(f'Failed to delete person: {exc}', 'danger')
        return redirect(url_for('people.detail', person_id=person_id))

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

@bp.post('/<int:person_id>/add-missing')
def add_missing(person_id: int):
    person = Person.query.get_or_404(person_id)
    target_id = int(request.form['target_id'])
    media_kind = request.form['media_kind']
    external_id = int(request.form['external_id']) if request.form.get('external_id') else None
    title = request.form['title']
    year = int(request.form['year']) if request.form.get('year') else None

    target = LibraryTarget.query.get_or_404(target_id)
    if not target.arr_server:
        result = {
            'status': 'invalid',
            'message': 'No Arr server linked to this library target.',
            'item': None,
        }
        _log_arr_activity(
            person=person,
            target=target,
            media_kind=media_kind,
            external_id=external_id,
            title=title,
            year=year,
            result=result,
        )
        flash(result['message'], 'warning')
        return redirect(url_for('people.detail', person_id=person.id))

    service = ArrService(target.arr_server)

    if media_kind == 'movie':
        result = service.ensure_movie(
            title=title,
            tmdb_id=external_id,
            year=year,
        )
    else:
        result = service.ensure_series(
            title=title,
            tvdb_id=None,
            tmdb_id=external_id,
            year=year,
        )

    _log_arr_activity(
        person=person,
        target=target,
        media_kind=media_kind,
        external_id=external_id,
        title=title,
        year=year,
        result=result,
    )

    if result['status'] == 'created':
        flash(result['message'], 'success')
    elif result['status'] == 'already_exists':
        flash(result['message'], 'warning')
    elif result['status'] == 'invalid':
        flash(result['message'], 'warning')
    else:
        flash(f'Failed to send to Arr: {result["message"]}', 'danger')

    return redirect(url_for('people.detail', person_id=person.id))





def _normalize(title: str, year: int | None):
    clean = ''.join(ch.lower() if ch.isalnum() else ' ' for ch in (title or ''))
    return (' '.join(clean.split()), year)
