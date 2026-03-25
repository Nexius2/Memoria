from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy.orm import joinedload

from ..extensions import db
from ..models import ArrServer, ArrActivity
from ..services.arr_service import ArrService

bp = Blueprint('arr', __name__)


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


def _load_arr_activities(limit: int = 50):
    return (
        ArrActivity.query
        .options(
            joinedload(ArrActivity.person),
            joinedload(ArrActivity.arr_server),
            joinedload(ArrActivity.library_target),
        )
        .order_by(ArrActivity.created_at.desc(), ArrActivity.id.desc())
        .limit(limit)
        .all()
    )


@bp.route('/')
def index():
    arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()

    status_filter = (request.args.get('status') or 'all').strip()
    server_filter = (request.args.get('server') or 'all').strip()
    media_kind_filter = (request.args.get('media_kind') or 'all').strip()

    limit_raw = (request.args.get('limit') or '50').strip()
    allowed_limits = {25, 50, 100, 200}
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 50
    if limit not in allowed_limits:
        limit = 50

    activity_query = (
        ArrActivity.query
        .options(
            joinedload(ArrActivity.person),
            joinedload(ArrActivity.arr_server),
            joinedload(ArrActivity.library_target),
        )
        .order_by(ArrActivity.created_at.desc(), ArrActivity.id.desc())
    )

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

    arr_activities = activity_query.limit(limit).all()

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
    )


@bp.post('/test-fill')
def test_fill():
    arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()

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
    limit = 50

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
        )


@bp.post('/create')
def create_arr():
    arr_servers = ArrServer.query.order_by(ArrServer.name.asc()).all()

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

    arr_activities = _load_arr_activities(50)

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
            limit=50,
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
            limit=50,
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
            limit=50,
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
            limit=50,
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
                limit=50,
            )

    arr.root_folder = selected_root
    arr.quality_profile_id = selected_quality_id
    arr.language_profile_id = selected_language_id if arr.kind == 'sonarr' else None

    db.session.add(arr)
    db.session.commit()
    flash('Arr server added.', 'success')
    return redirect(url_for('arr.index'))


@bp.post('/<int:arr_id>/delete')
def delete_arr(arr_id: int):
    arr = ArrServer.query.get_or_404(arr_id)
    db.session.delete(arr)
    db.session.commit()
    flash('Arr server deleted.', 'success')
    return redirect(url_for('arr.index'))