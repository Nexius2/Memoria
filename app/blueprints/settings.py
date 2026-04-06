from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify

from ..extensions import db
from ..models import AppSettings
from ..utils.country_utils import normalize_countries_csv

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
    return render_template('settings.html', settings=settings)


@bp.post('/save')
def save():
    settings = AppSettings.get_or_create()
    is_autosave = request.form.get('_autosave') == '1'

    try:
        app_name = (request.form.get('app_name') or '').strip()
        if not app_name:
            raise ValueError('App name is required.')

        collection_name_template = (request.form.get('collection_name_template') or '').strip()
        if not collection_name_template:
            raise ValueError('Collection name template is required.')

        collection_summary_template = (request.form.get('collection_summary_template') or '').strip()
        if not collection_summary_template:
            raise ValueError('Collection summary template is required.')

        default_media_mode = (request.form.get('default_media_mode') or '').strip()
        if default_media_mode not in {'both', 'movie', 'show'}:
            raise ValueError('Default media mode is invalid.')

        settings.app_name = app_name
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