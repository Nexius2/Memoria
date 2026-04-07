import json
from datetime import datetime, timedelta

from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy import or_
from sqlalchemy.orm import joinedload

from ..extensions import db
from ..models import ArrServer, ArrActivity, LibraryTarget, Person, PlexServer, AppLog
from ..services.arr_service import ArrService
from ..services.missing_titles_service import load_person_missing_titles

bp = Blueprint('arr', __name__)

RECENT_ARR_ACTIVITY_LIMIT = 10
MISSING_TITLES_PAGE_LIMIT = 25
BULK_ARR_ERROR_RETRY_DELAY_HOURS = 6
BULK_ARR_INVALID_RETRY_DELAY_HOURS = 24


def _parse_int_field(name: str, default: int | None = None) -> int | None:
    raw = (request.form.get(name) or '').strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f'Invalid value for {name}: {raw}')


def _build_temp_arr_from_form() -> ArrServer:
    return ArrServer(
        name=(request.form.get('name') or 'Temporary').strip() or 'Temporary',
        kind=(request.form.get('kind') or 'radarr').strip(),
        base_url=(request.form.get('base_url') or '').strip(),
        api_key=(request.form.get('api_key') or '').strip(),
        root_folder=(request.form.get('root_folder') or '').strip(),
        quality_profile_id=_parse_int_field('quality_profile_id'),
        language_profile_id=_parse_int_field('language_profile_id'),
        search_on_add=request.form.get('search_on_add') == 'on',
        enabled=request.form.get('enabled') == 'on',
    )


def _base_arr_activity_query():
    return (
        ArrActivity.query
        .options(
            joinedload(ArrActivity.person),
            joinedload(ArrActivity.arr_server),
            joinedload(ArrActivity.library_target),
        )
        .order_by(ArrActivity.created_at.desc(), ArrActivity.id.desc())
    )


def _load_arr_activities(limit: int = RECENT_ARR_ACTIVITY_LIMIT, offset: int = 0):
    return _base_arr_activity_query().offset(offset).limit(limit).all()

def _normalize_arr_activity_title(value: str) -> str:
    return ' '.join(
        ''.join(ch.lower() if ch.isalnum() else ' ' for ch in (value or '')).split()
    )


def _load_latest_arr_activity_map() -> dict[tuple, ArrActivity]:
    activities = (
        ArrActivity.query
        .options(joinedload(ArrActivity.arr_server))
        .order_by(ArrActivity.created_at.desc(), ArrActivity.id.desc())
        .all()
    )

    latest_map: dict[tuple, ArrActivity] = {}

    for activity in activities:
        external_key = (
            activity.person_id,
            activity.media_kind,
            'external',
            activity.external_id,
        )
        title_key = (
            activity.person_id,
            activity.media_kind,
            'title',
            _normalize_arr_activity_title(activity.title),
            activity.year,
        )

        if activity.external_id is not None and external_key not in latest_map:
            latest_map[external_key] = activity

        if title_key not in latest_map:
            latest_map[title_key] = activity

    return latest_map

def _parse_page_arg(arg_name: str = 'page', default: int = 1) -> int:
    raw = (request.args.get(arg_name) or str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(value, 1)


def _load_arr_targets_for_missing_page(media_kind: str) -> list[LibraryTarget]:
    expected_arr_kind = 'radarr' if media_kind == 'movie' else 'sonarr'

    return (
        LibraryTarget.query
        .join(PlexServer, LibraryTarget.plex_server_id == PlexServer.id)
        .join(ArrServer, LibraryTarget.arr_server_id == ArrServer.id)
        .options(
            joinedload(LibraryTarget.plex_server),
            joinedload(LibraryTarget.arr_server),
        )
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


def _build_missing_title_rows() -> tuple[list[dict], list[LibraryTarget], list[LibraryTarget]]:
    movie_targets = _load_arr_targets_for_missing_page('movie')
    show_targets = _load_arr_targets_for_missing_page('show')

    movie_ready = bool(movie_targets)
    show_ready = bool(show_targets)

    latest_activity_map = _load_latest_arr_activity_map()

    people = Person.query.order_by(Person.name.asc()).all()

    rows: list[dict] = []

    for person in people:
        missing_movies, missing_shows = load_person_missing_titles(person)

        for item in missing_movies:
            item_date = item.get('release_date') or item.get('first_air_date') or ''
            item_year_raw = item_date[:4]
            item_year = int(item_year_raw) if item_year_raw.isdigit() else None
            item_external_id = item.get('id')
            item_title = item.get('title') or item.get('name') or 'Unknown title'

            activity = latest_activity_map.get(
                (person.id, 'movie', 'external', item_external_id)
            ) if item_external_id is not None else None

            if activity is None:
                activity = latest_activity_map.get(
                    (
                        person.id,
                        'movie',
                        'title',
                        _normalize_arr_activity_title(item_title),
                        item_year,
                    )
                )

            rows.append({
                'person_id': person.id,
                'person_name': person.name,
                'country': person.country or '',
                'source': person.source or '',
                'missing_titles_status': person.missing_titles_status or 'pending',
                'media_kind': 'movie',
                'external_id': item_external_id,
                'title': item_title,
                'year': item_year,
                'item_date': item_date,
                'arr_ready': movie_ready,
                'targets': movie_targets,
                'last_arr_activity': activity,
                'last_arr_status': activity.status if activity else None,
                'last_arr_message': activity.message if activity else None,
                'last_arr_server_name': activity.arr_server.name if activity and activity.arr_server else None,
                'last_arr_created_at': activity.created_at if activity else None,
            })

        for item in missing_shows:
            item_date = item.get('first_air_date') or item.get('release_date') or ''
            item_year_raw = item_date[:4]
            item_year = int(item_year_raw) if item_year_raw.isdigit() else None
            item_external_id = item.get('id')
            item_title = item.get('name') or item.get('title') or 'Unknown title'

            activity = latest_activity_map.get(
                (person.id, 'show', 'external', item_external_id)
            ) if item_external_id is not None else None

            if activity is None:
                activity = latest_activity_map.get(
                    (
                        person.id,
                        'show',
                        'title',
                        _normalize_arr_activity_title(item_title),
                        item_year,
                    )
                )

            rows.append({
                'person_id': person.id,
                'person_name': person.name,
                'country': person.country or '',
                'source': person.source or '',
                'missing_titles_status': person.missing_titles_status or 'pending',
                'media_kind': 'show',
                'external_id': item_external_id,
                'title': item_title,
                'year': item_year,
                'item_date': item_date,
                'arr_ready': show_ready,
                'targets': show_targets,
                'last_arr_activity': activity,
                'last_arr_status': activity.status if activity else None,
                'last_arr_message': activity.message if activity else None,
                'last_arr_server_name': activity.arr_server.name if activity and activity.arr_server else None,
                'last_arr_created_at': activity.created_at if activity else None,
            })

    rows.sort(
        key=lambda row: (
            row.get('item_date') or '0000-00-00',
            row.get('title') or '',
            row.get('person_name') or '',
        ),
        reverse=True,
    )

    return rows, movie_targets, show_targets

def _redirect_to_missing_titles_from_form():
    return redirect(url_for(
        'arr.missing_titles',
        page=(request.form.get('return_page') or '1').strip() or '1',
        search=(request.form.get('return_search') or '').strip(),
        media_kind=(request.form.get('return_media_kind') or 'all').strip() or 'all',
        arr_ready=(request.form.get('return_arr_ready') or 'all').strip() or 'all',
        source=(request.form.get('return_source') or 'all').strip() or 'all',
        country=(request.form.get('return_country') or 'all').strip() or 'all',
    ))


def _log_arr_activity_for_missing_titles(
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


def _already_handled_for_bulk_send(
    *,
    person_id: int,
    target_id: int,
    media_kind: str,
    external_id: int | None,
    title: str,
    year: int | None,
) -> bool:
    query = ArrActivity.query.filter(
        ArrActivity.person_id == person_id,
        ArrActivity.library_target_id == target_id,
        ArrActivity.media_kind == media_kind,
    )

    if external_id is not None:
        query = query.filter(
            or_(
                ArrActivity.tmdb_id == external_id,
                ArrActivity.tvdb_id == external_id,
                ArrActivity.external_id == external_id,
            )
        )
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
        if last_activity.created_at and last_activity.created_at >= now - timedelta(hours=BULK_ARR_ERROR_RETRY_DELAY_HOURS):
            return True

    if last_activity.status == 'invalid':
        if last_activity.created_at and last_activity.created_at >= now - timedelta(hours=BULK_ARR_INVALID_RETRY_DELAY_HOURS):
            return True

    return False


def _send_missing_title_to_target(
    *,
    person: Person,
    target: LibraryTarget,
    media_kind: str,
    external_id: int | None,
    title: str,
    year: int | None,
) -> dict:
    if _already_handled_for_bulk_send(
        person_id=person.id,
        target_id=target.id,
        media_kind=media_kind,
        external_id=external_id,
        title=title,
        year=year,
    ):
        return {
            'status': 'skipped',
            'message': f'Skipped "{title}" because it was already handled recently for this target.',
        }

    if not target.arr_server:
        result = {
            'status': 'invalid',
            'message': 'No Arr server linked to this library target.',
            'item': None,
        }
        _log_arr_activity_for_missing_titles(
            person=person,
            target=target,
            media_kind=media_kind,
            external_id=external_id,
            title=title,
            year=year,
            result=result,
        )
        return result

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

    _log_arr_activity_for_missing_titles(
        person=person,
        target=target,
        media_kind=media_kind,
        external_id=external_id,
        title=title,
        year=year,
        result=result,
    )

    return result

def _init_arr_profile_metadata(arr_servers: list[ArrServer]) -> None:
    for arr in arr_servers:
        arr.quality_profile_name = None
        arr.language_profile_name = None
        arr.available_quality_profiles = []
        arr.available_language_profiles = []
        arr.quality_profile_missing = False
        arr.language_profile_missing = False

def _attach_arr_profile_metadata(arr_servers: list[ArrServer]) -> None:
    _init_arr_profile_metadata(arr_servers)

    for arr in arr_servers:
        if not arr.enabled:
            continue

        try:
            service = ArrService(arr)

            quality_profiles = service.quality_profiles() or []
            language_profiles = service.language_profiles() or []

            quality_map = {
                item.get('id'): item.get('name')
                for item in quality_profiles
                if item.get('id') is not None
            }
            language_map = {
                item.get('id'): item.get('name')
                for item in language_profiles
                if item.get('id') is not None
            }

            arr.quality_profile_name = quality_map.get(arr.quality_profile_id)
            arr.language_profile_name = language_map.get(arr.language_profile_id)

            arr.quality_profile_missing = (
                arr.quality_profile_id is not None
                and arr.quality_profile_name is None
            )
            arr.language_profile_missing = (
                arr.kind == 'sonarr'
                and arr.language_profile_id is not None
                and arr.language_profile_name is None
            )

            arr.available_quality_profiles = list(quality_profiles)
            if arr.quality_profile_missing:
                arr.available_quality_profiles.insert(0, {
                    'id': arr.quality_profile_id,
                    'name': f'⚠ Missing profile (ID {arr.quality_profile_id})',
                })

            if arr.kind == 'sonarr':
                arr.available_language_profiles = list(language_profiles)
                if arr.language_profile_missing:
                    arr.available_language_profiles.insert(0, {
                        'id': arr.language_profile_id,
                        'name': f'⚠ Missing profile (ID {arr.language_profile_id})',
                    })
            else:
                arr.available_language_profiles = []

        except Exception:
            arr.available_quality_profiles = []
            arr.available_language_profiles = []
            arr.quality_profile_name = None
            arr.language_profile_name = None
            arr.quality_profile_missing = False
            arr.language_profile_missing = False

@bp.route('/')
def index():
    arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()
    _init_arr_profile_metadata(arr_servers)

    status_filter = (request.args.get('status') or 'all').strip()
    server_filter = (request.args.get('server') or 'all').strip()
    media_kind_filter = (request.args.get('media_kind') or 'all').strip()

    limit = RECENT_ARR_ACTIVITY_LIMIT

    page_raw = (request.args.get('page') or '1').strip()
    try:
        page = int(page_raw)
    except ValueError:
        page = 1
    if page < 1:
        page = 1

    offset = (page - 1) * limit

    activity_query = _base_arr_activity_query()

    if status_filter != 'all':
        activity_query = activity_query.filter(ArrActivity.status == status_filter)

    if server_filter != 'all':
        try:
            server_id = int(server_filter)
            activity_query = activity_query.filter(ArrActivity.arr_server_id == server_id)
        except ValueError:
            server_filter = 'all'

    if media_kind_filter != 'all':
        activity_query = activity_query.filter(ArrActivity.media_kind == media_kind_filter)

    total_arr_activities = activity_query.count()
    arr_activities = activity_query.offset(offset).limit(limit).all()
    has_previous_page = page > 1
    has_next_page = (offset + len(arr_activities)) < total_arr_activities

    return render_template(
        'arr.html',
        arr_servers=arr_servers,
        arr_form_data=None,
        arr_discovery=None,
        arr_activities=arr_activities,
        status_filter=status_filter,
        server_filter=server_filter,
        media_kind_filter=media_kind_filter,
        limit=limit,
        page=page,
        total_arr_activities=total_arr_activities,
        has_previous_page=has_previous_page,
        has_next_page=has_next_page,
        recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
    )

@bp.route('/missing-titles')
def missing_titles():
    search = (request.args.get('search') or '').strip()
    media_kind_filter = (request.args.get('media_kind') or 'all').strip()
    arr_ready_filter = (request.args.get('arr_ready') or 'all').strip()
    source_filter = (request.args.get('source') or 'all').strip()
    country_filter = (request.args.get('country') or 'all').strip()

    page = _parse_page_arg('page', 1)
    limit = MISSING_TITLES_PAGE_LIMIT
    offset = (page - 1) * limit

    rows, movie_targets, show_targets = _build_missing_title_rows()

    available_sources = sorted({row['source'] for row in rows if row['source']})
    available_countries = sorted({row['country'] for row in rows if row['country']})

    filtered_rows = rows

    if search:
        search_lower = search.lower()
        filtered_rows = [
            row for row in filtered_rows
            if (
                search_lower in (row['title'] or '').lower()
                or search_lower in (row['person_name'] or '').lower()
                or search_lower in (row['country'] or '').lower()
                or search_lower in (row['source'] or '').lower()
            )
        ]

    if media_kind_filter != 'all':
        filtered_rows = [
            row for row in filtered_rows
            if row['media_kind'] == media_kind_filter
        ]

    if arr_ready_filter == 'ready':
        filtered_rows = [row for row in filtered_rows if row['arr_ready']]
    elif arr_ready_filter == 'not_ready':
        filtered_rows = [row for row in filtered_rows if not row['arr_ready']]

    if source_filter != 'all':
        filtered_rows = [
            row for row in filtered_rows
            if row['source'] == source_filter
        ]

    if country_filter != 'all':
        filtered_rows = [
            row for row in filtered_rows
            if row['country'] == country_filter
        ]

    total_missing_titles = len(filtered_rows)
    paginated_rows = filtered_rows[offset:offset + limit]

    has_previous_page = page > 1
    has_next_page = (offset + len(paginated_rows)) < total_missing_titles

    return render_template(
        'missing_titles.html',
        rows=paginated_rows,
        total_missing_titles=total_missing_titles,
        page=page,
        limit=limit,
        has_previous_page=has_previous_page,
        has_next_page=has_next_page,
        search=search,
        media_kind_filter=media_kind_filter,
        arr_ready_filter=arr_ready_filter,
        source_filter=source_filter,
        country_filter=country_filter,
        available_sources=available_sources,
        available_countries=available_countries,
        movie_targets=movie_targets,
        show_targets=show_targets,
    )

@bp.post('/missing-titles/bulk-send')
def bulk_send_missing_titles():
    target_id_raw = (request.form.get('target_id') or '').strip()
    selected_items_raw = request.form.getlist('selected_items')

    if not target_id_raw:
        flash('Please select an Arr target for bulk send.', 'warning')
        return _redirect_to_missing_titles_from_form()

    if not selected_items_raw:
        flash('Please select at least one missing title.', 'warning')
        return _redirect_to_missing_titles_from_form()

    try:
        target_id = int(target_id_raw)
    except ValueError:
        flash('Invalid Arr target selected.', 'danger')
        return _redirect_to_missing_titles_from_form()

    target = LibraryTarget.query.get_or_404(target_id)

    processed_items = 0
    created_items = 0
    already_exists_items = 0
    invalid_items = 0
    error_items = 0
    skipped_items = 0
    incompatible_items = 0

    for raw_item in selected_items_raw:
        try:
            payload = json.loads(raw_item)
        except Exception:
            skipped_items += 1
            continue

        person_id = payload.get('person_id')
        media_kind = (payload.get('media_kind') or '').strip()
        title = (payload.get('title') or '').strip()

        if not person_id or not media_kind or not title:
            skipped_items += 1
            continue

        if media_kind != target.media_type:
            incompatible_items += 1
            continue

        person = Person.query.get(person_id)
        if not person:
            skipped_items += 1
            continue

        external_id_raw = payload.get('external_id')
        year_raw = payload.get('year')

        try:
            external_id = int(external_id_raw) if external_id_raw not in (None, '', 'null') else None
        except (TypeError, ValueError):
            external_id = None

        try:
            year = int(year_raw) if year_raw not in (None, '', 'null') else None
        except (TypeError, ValueError):
            year = None

        result = _send_missing_title_to_target(
            person=person,
            target=target,
            media_kind=media_kind,
            external_id=external_id,
            title=title,
            year=year,
        )

        status = result.get('status')

        if status == 'skipped':
            skipped_items += 1
            continue

        processed_items += 1

        if status == 'created':
            created_items += 1
        elif status == 'already_exists':
            already_exists_items += 1
        elif status == 'invalid':
            invalid_items += 1
        elif status == 'error':
            error_items += 1

    summary = (
        f'Bulk Arr send finished: '
        f'{processed_items} processed, '
        f'{created_items} created, '
        f'{already_exists_items} already existed, '
        f'{invalid_items} invalid, '
        f'{error_items} error, '
        f'{skipped_items} skipped'
    )

    if incompatible_items:
        summary += f', {incompatible_items} incompatible with selected target type'

    summary += '.'

    if error_items > 0:
        flash(summary, 'danger')
    elif invalid_items > 0 or already_exists_items > 0 or incompatible_items > 0:
        flash(summary, 'warning')
    else:
        flash(summary, 'success')

    return _redirect_to_missing_titles_from_form()

@bp.post('/test-fill')
def test_fill():
    arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()
    _init_arr_profile_metadata(arr_servers)

    form_data = {
        'name': request.form.get('name', '').strip(),
        'kind': request.form.get('kind', 'radarr'),
        'base_url': request.form.get('base_url', '').strip(),
        'api_key': request.form.get('api_key', '').strip(),
        'root_folder': request.form.get('root_folder', '').strip(),
        'quality_profile_id': request.form.get('quality_profile_id', '').strip(),
        'language_profile_id': request.form.get('language_profile_id', '').strip(),
        'search_on_add': request.form.get('search_on_add') == 'on',
        'enabled': request.form.get('enabled') == 'on',
    }

    status_filter = 'all'
    server_filter = 'all'
    media_kind_filter = 'all'
    limit = RECENT_ARR_ACTIVITY_LIMIT

    arr_activities = _load_arr_activities(limit)

    try:
        temp_arr = _build_temp_arr_from_form()
        discovery = ArrService(temp_arr).test_and_discover()

        if discovery['ok']:
            flash(discovery['message'], 'success')
        else:
            flash(f'Arr test failed: {discovery["message"]}', 'danger')

        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=form_data,
            arr_discovery=discovery,
            arr_activities=arr_activities,
            status_filter=status_filter,
            server_filter=server_filter,
            media_kind_filter=media_kind_filter,
            limit=limit,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
        )
    except Exception as exc:
        flash(f'Arr test failed: {exc}', 'danger')
        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=form_data,
            arr_discovery=None,
            arr_activities=arr_activities,
            status_filter=status_filter,
            server_filter=server_filter,
            media_kind_filter=media_kind_filter,
            limit=limit,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
        )


@bp.post('/create')
def create_arr():
    arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()
    _init_arr_profile_metadata(arr_servers)

    form_data = {
        'name': request.form.get('name', '').strip(),
        'kind': request.form.get('kind', 'radarr'),
        'base_url': request.form.get('base_url', '').strip(),
        'api_key': request.form.get('api_key', '').strip(),
        'root_folder': request.form.get('root_folder', '').strip(),
        'quality_profile_id': request.form.get('quality_profile_id', '').strip(),
        'language_profile_id': request.form.get('language_profile_id', '').strip(),
        'search_on_add': request.form.get('search_on_add') == 'on',
        'enabled': request.form.get('enabled') == 'on',
    }

    arr_activities = _load_arr_activities(RECENT_ARR_ACTIVITY_LIMIT)

    try:
        arr = _build_temp_arr_from_form()
    except Exception as exc:
        flash(f'Invalid Arr form: {exc}', 'danger')
        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=form_data,
            arr_discovery=None,
            arr_activities=arr_activities,
            status_filter='all',
            server_filter='all',
            media_kind_filter='all',
            limit=RECENT_ARR_ACTIVITY_LIMIT,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
        )

    discovery = ArrService(arr).test_and_discover()

    if not discovery.get('ok'):
        flash(f'Arr server not added: {discovery.get("message")}', 'danger')
        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=form_data,
            arr_discovery=discovery,
            arr_activities=arr_activities,
            status_filter='all',
            server_filter='all',
            media_kind_filter='all',
            limit=RECENT_ARR_ACTIVITY_LIMIT,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
        )

    root_folders = discovery.get('root_folders') or []
    quality_profiles = discovery.get('quality_profiles') or []
    language_profiles = discovery.get('language_profiles') or []

    selected_root = (request.form.get('root_folder') or '').strip()
    if not selected_root and root_folders:
        selected_root = (root_folders[0].get('path') or '').strip()

    valid_root_paths = {
        (item.get('path') or '').strip()
        for item in root_folders
        if (item.get('path') or '').strip()
    }
    if not selected_root or selected_root not in valid_root_paths:
        flash('Arr server not added: invalid or missing root folder.', 'danger')
        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=form_data,
            arr_discovery=discovery,
            arr_activities=arr_activities,
            status_filter='all',
            server_filter='all',
            media_kind_filter='all',
            limit=RECENT_ARR_ACTIVITY_LIMIT,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
        )

    selected_quality_id = _parse_int_field('quality_profile_id')
    if selected_quality_id is None and quality_profiles:
        selected_quality_id = quality_profiles[0].get('id')

    valid_quality_ids = {
        item.get('id')
        for item in quality_profiles
        if item.get('id') is not None
    }
    if selected_quality_id is None or selected_quality_id not in valid_quality_ids:
        flash('Arr server not added: invalid or missing quality profile.', 'danger')
        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=form_data,
            arr_discovery=discovery,
            arr_activities=arr_activities,
            status_filter='all',
            server_filter='all',
            media_kind_filter='all',
            limit=RECENT_ARR_ACTIVITY_LIMIT,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
        )

    selected_language_id = _parse_int_field('language_profile_id')
    if arr.kind == 'sonarr' and selected_language_id is not None:
        valid_language_ids = {
            item.get('id')
            for item in language_profiles
            if item.get('id') is not None
        }
        if selected_language_id not in valid_language_ids:
            flash('Arr server not added: invalid language profile.', 'danger')
            return render_template(
                'arr.html',
                arr_servers=arr_servers,
                arr_form_data=form_data,
                arr_discovery=discovery,
                arr_activities=arr_activities,
                status_filter='all',
                server_filter='all',
                media_kind_filter='all',
                limit=RECENT_ARR_ACTIVITY_LIMIT,
                page=1,
                total_arr_activities=len(arr_activities),
                has_previous_page=False,
                has_next_page=False,
                recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
            )

    arr.root_folder = selected_root
    arr.quality_profile_id = selected_quality_id
    arr.language_profile_id = selected_language_id if arr.kind == 'sonarr' else None

    db.session.add(arr)
    db.session.commit()
    flash('Arr server added.', 'success')
    return redirect(url_for('arr.index'))

@bp.post('/<int:arr_id>/update')
def update_arr(arr_id: int):
    arr = ArrServer.query.get_or_404(arr_id)

    new_name = (request.form.get('name') or '').strip()
    new_kind = (request.form.get('kind') or 'radarr').strip()
    new_base_url = (request.form.get('base_url') or '').strip()
    new_api_key = (request.form.get('api_key') or '').strip()
    new_root_folder = (request.form.get('root_folder') or '').strip()
    new_quality_profile_id = _parse_int_field('quality_profile_id')
    new_language_profile_id = _parse_int_field('language_profile_id') if new_kind == 'sonarr' else None
    new_search_on_add = request.form.get('search_on_add') == 'on'
    new_enabled = request.form.get('enabled') == 'on'

    temp_arr = ArrServer(
        id=arr.id,
        name=new_name or arr.name,
        kind=new_kind,
        base_url=new_base_url,
        api_key=new_api_key,
        root_folder=new_root_folder,
        quality_profile_id=new_quality_profile_id,
        language_profile_id=new_language_profile_id,
        search_on_add=new_search_on_add,
        enabled=new_enabled,
    )

    discovery = ArrService(temp_arr).test_and_discover()

    if not discovery.get('ok'):
        arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()
        _init_arr_profile_metadata(arr_servers)

        target_arr = next((item for item in arr_servers if item.id == arr.id), None)
        if target_arr is not None:
            _attach_arr_profile_metadata([target_arr])

        arr_activities = _load_arr_activities(RECENT_ARR_ACTIVITY_LIMIT)

        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=None,
            arr_discovery=None,
            arr_activities=arr_activities,
            status_filter='all',
            server_filter='all',
            media_kind_filter='all',
            limit=RECENT_ARR_ACTIVITY_LIMIT,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
            arr_open_edit_id=arr.id,
            arr_modal_error_title='Unable to validate Arr server',
            arr_modal_error_message=(
                f'Memoria could not reload the configuration from {temp_arr.kind.title()}. '
                f'Reason: {discovery.get("message")}'
            ),
        )

    root_folders = discovery.get('root_folders') or []
    quality_profiles = discovery.get('quality_profiles') or []
    language_profiles = discovery.get('language_profiles') or []

    valid_root_paths = {
        (item.get('path') or '').strip()
        for item in root_folders
        if (item.get('path') or '').strip()
    }

    valid_quality_ids = {
        item.get('id')
        for item in quality_profiles
        if item.get('id') is not None
    }

    valid_language_ids = {
        item.get('id')
        for item in language_profiles
        if item.get('id') is not None
    }

    error_message = None

    if not new_root_folder or new_root_folder not in valid_root_paths:
        error_message = (
            'The selected root folder no longer exists in Arr. '
            'The page has been reloaded with the latest values from Arr.'
        )
    elif new_quality_profile_id is None or new_quality_profile_id not in valid_quality_ids:
        error_message = (
            'The selected quality profile no longer exists in Arr. '
            'The page has been reloaded with the latest profiles from Arr. '
            'Please choose a valid profile and save again.'
        )
    elif new_kind == 'sonarr' and new_language_profile_id is not None and new_language_profile_id not in valid_language_ids:
        error_message = (
            'The selected language profile no longer exists in Arr. '
            'The page has been reloaded with the latest profiles from Arr. '
            'Please choose a valid profile and save again.'
        )

    if error_message:
        arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()
        _init_arr_profile_metadata(arr_servers)

        target_arr = next((item for item in arr_servers if item.id == arr.id), None)
        if target_arr is not None:
            _attach_arr_profile_metadata([target_arr])

        arr_activities = _load_arr_activities(RECENT_ARR_ACTIVITY_LIMIT)

        return render_template(
            'arr.html',
            arr_servers=arr_servers,
            arr_form_data=None,
            arr_discovery=None,
            arr_activities=arr_activities,
            status_filter='all',
            server_filter='all',
            media_kind_filter='all',
            limit=RECENT_ARR_ACTIVITY_LIMIT,
            page=1,
            total_arr_activities=len(arr_activities),
            has_previous_page=False,
            has_next_page=False,
            recent_arr_activity_limit=RECENT_ARR_ACTIVITY_LIMIT,
            arr_open_edit_id=arr.id,
            arr_modal_error_title='Arr profile changed',
            arr_modal_error_message=error_message,
        )

    arr.name = new_name
    arr.kind = new_kind
    arr.base_url = new_base_url
    arr.api_key = new_api_key
    arr.root_folder = new_root_folder
    arr.quality_profile_id = new_quality_profile_id
    arr.language_profile_id = new_language_profile_id if new_kind == 'sonarr' else None
    arr.search_on_add = new_search_on_add
    arr.enabled = new_enabled

    db.session.commit()
    flash('Arr server updated.', 'success')
    return redirect(url_for('arr.index'))

@bp.post('/<int:arr_id>/delete')
def delete_arr(arr_id: int):
    arr = ArrServer.query.get_or_404(arr_id)
    db.session.delete(arr)
    db.session.commit()
    flash('Arr server deleted.', 'success')
    return redirect(url_for('arr.index'))