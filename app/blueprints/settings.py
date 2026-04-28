from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file

from ..extensions import db
from ..models import AppSettings
from ..utils.country_utils import normalize_countries_csv
from ..services.backup_service import (
    list_database_backups,
    create_database_backup,
    restore_database_backup,
    get_backup_directory,
    get_database_path,
    get_latest_backup_info,
)

bp = Blueprint('settings', __name__)


def _parse_positive_int(field_name: str, label: str, minimum: int = 1) -> int:
    raw_value = (request.form.get(field_name) or '').strip()

    if not raw_value:
        raise ValueError(f'{label} is required.')

    try:
        value = int(raw_value)
    except ValueError:
        raise ValueError(f'{label} must be a valid integer.')

    if value < minimum:
        raise ValueError(f'{label} must be greater than or equal to {minimum}.')

    return value


@bp.route('/')
def index():
    settings = AppSettings.get_or_create()
    backups = list_database_backups()
    latest_backup = get_latest_backup_info()

    return render_template(
        'settings.html',
        settings=settings,
        backups=backups,
        latest_backup=latest_backup,
        backup_directory=str(get_backup_directory()),
        database_path=str(get_database_path()),
    )


@bp.post('/save')
def save():
    settings = AppSettings.get_or_create()
    is_autosave = request.form.get('_autosave') == '1'

    try:
        collection_name_template = (request.form.get('collection_name_template') or '').strip()
        if not collection_name_template:
            raise ValueError('Collection name template is required.')

        collection_summary_template = (request.form.get('collection_summary_template') or '').strip()
        if not collection_summary_template:
            raise ValueError('Collection summary template is required.')

        default_media_mode = (request.form.get('default_media_mode') or '').strip()
        if default_media_mode not in {'both', 'movie', 'show'}:
            raise ValueError('Default media mode is invalid.')

        ui_language = (request.form.get('ui_language') or 'auto').strip().lower()
        if ui_language not in {'auto', 'en', 'fr'}:
            raise ValueError('Interface language is invalid.')


        settings.ui_language = ui_language
        settings.auto_detection_enabled = request.form.get('auto_detection_enabled') == 'on'
        settings.detection_window_days = _parse_positive_int(
            'detection_window_days',
            'Detection window days',
        )
        settings.display_days = _parse_positive_int(
            'display_days',
            'Display days',
        )
        settings.max_people = _parse_positive_int(
            'max_people',
            'Max simultaneous people',
        )
        settings.min_people_priority_display = _parse_positive_int(
            'min_people_priority_display',
            'Minimum people priority level',
            minimum=0,
        )
        settings.log_retention_days = _parse_positive_int(
            'log_retention_days',
            'Log retention days',
        )
        settings.job_retention_days = _parse_positive_int(
            'job_retention_days',
            'Job retention days',
        )
        settings.arr_activity_retention_days = _parse_positive_int(
            'arr_activity_retention_days',
            'Arr activity retention days',
        )
        settings.auto_backup_enabled = request.form.get('auto_backup_enabled') == 'on'
        settings.backup_interval_hours = _parse_positive_int(
            'backup_interval_hours',
            'Backup interval hours',
        )
        settings.backup_retention_count = _parse_positive_int(
            'backup_retention_count',
            'Backup retention count',
        )
        settings.countries_csv = normalize_countries_csv(request.form.get('countries_csv') or '')
        settings.professions_csv = (request.form.get('professions_csv') or '').strip()
        settings.publish_on_home = request.form.get('publish_on_home') == 'on'
        settings.publish_on_friends_home = request.form.get('publish_on_friends_home') == 'on'
        settings.collection_name_template = collection_name_template
        settings.collection_summary_template = collection_summary_template
        settings.default_media_mode = default_media_mode
        settings.deduplicate_people = request.form.get('deduplicate_people') == 'on'
        settings.tmdb_api_key = (request.form.get('tmdb_api_key') or '').strip() or None
        settings.auto_missing_titles_enabled = request.form.get('auto_missing_titles_enabled') == 'on'
        settings.missing_titles_refresh_hours = _parse_positive_int(
            'missing_titles_refresh_hours',
            'Missing titles refresh hours',
        )
        settings.auto_arr_enabled = request.form.get('auto_arr_enabled') == 'on'

        db.session.commit()

    except ValueError as exc:
        if is_autosave:
            return jsonify({'ok': False, 'message': str(exc)}), 400

        flash(str(exc), 'danger')
        return redirect(url_for('settings.index'))

    except Exception:
        db.session.rollback()

        if is_autosave:
            return jsonify({'ok': False, 'message': 'Unexpected error while saving settings.'}), 500

        flash('Unexpected error while saving settings.', 'danger')
        return redirect(url_for('settings.index'))

    if is_autosave:
        return jsonify({'ok': True, 'message': 'Saved'})

    flash('Settings saved.', 'success')
    return redirect(url_for('settings.index'))

@bp.post('/backup-now')
def backup_now():
    try:
        backup_path = create_database_backup(trigger='manual')
        flash(f'Database backup created: {backup_path.name}', 'success')
    except Exception as exc:
        flash(f'Backup failed: {exc}', 'danger')

    return redirect(url_for('settings.index'))


@bp.get('/backups/<path:filename>')
def download_backup(filename: str):
    backup_dir = get_backup_directory()
    backup_path = (backup_dir / filename).resolve()

    if backup_path.parent != backup_dir.resolve() or not backup_path.exists():
        flash('Backup file not found.', 'danger')
        return redirect(url_for('settings.index'))

    return send_file(
        backup_path,
        as_attachment=True,
        download_name=backup_path.name,
    )

@bp.post('/backups/<path:filename>/restore')
def restore_backup(filename: str):
    try:
        result = restore_database_backup(filename, create_safety_backup=True)

        safety_backup_path = result.get('safety_backup_path')
        restored_from = result.get('restored_from')

        if safety_backup_path is not None:
            flash(
                f'Database restored from {restored_from.name}. Safety backup created first: {safety_backup_path.name}.',
                'success',
            )
        else:
            flash(
                f'Database restored from {restored_from.name}.',
                'success',
            )

    except Exception as exc:
        flash(f'Restore failed: {exc}', 'danger')

    return redirect(url_for('settings.index'))