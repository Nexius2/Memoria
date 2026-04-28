import os
from datetime import timezone
from pathlib import Path

from flask import Flask
from sqlalchemy import inspect, text

from .extensions import db, scheduler
from .models import AppSettings
from .utils.i18n import translate, get_current_language, get_available_languages

def _read_info_version() -> str | None:
    info_path = Path(__file__).resolve().parent.parent / 'INFO'

    if not info_path.exists():
        return None

    try:
        for raw_line in info_path.read_text(encoding='utf-8').splitlines():
            line = raw_line.strip()
            if not line or '=' not in line:
                continue

            key, value = line.split('=', 1)
            if key.strip() == 'VERSION':
                return value.strip() or None
    except Exception:
        return None

    return None

def _ensure_runtime_schema():
    inspector = inspect(db.engine)
    table_names = set(inspector.get_table_names())

    if 'app_settings' not in table_names:
        return

    existing_columns = {
        column['name']
        for column in inspector.get_columns('app_settings')
    }

    alter_statements: list[str] = []

    if 'ui_language' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN ui_language VARCHAR(10) NOT NULL DEFAULT 'auto'"
        )

    if 'log_retention_days' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN log_retention_days INTEGER NOT NULL DEFAULT 30"
        )

    if 'job_retention_days' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN job_retention_days INTEGER NOT NULL DEFAULT 30"
        )

    if 'arr_activity_retention_days' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN arr_activity_retention_days INTEGER NOT NULL DEFAULT 90"
        )

    if 'auto_missing_titles_enabled' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN auto_missing_titles_enabled BOOLEAN NOT NULL DEFAULT 1"
        )

    if 'missing_titles_refresh_hours' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN missing_titles_refresh_hours INTEGER NOT NULL DEFAULT 24"
        )

    if 'auto_arr_enabled' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN auto_arr_enabled BOOLEAN NOT NULL DEFAULT 1"
        )

    if 'min_people_priority_display' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN min_people_priority_display INTEGER NOT NULL DEFAULT 25"
        )

    if 'auto_backup_enabled' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN auto_backup_enabled BOOLEAN NOT NULL DEFAULT 1"
        )

    if 'backup_interval_hours' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN backup_interval_hours INTEGER NOT NULL DEFAULT 24"
        )

    if 'backup_retention_count' not in existing_columns:
        alter_statements.append(
            "ALTER TABLE app_settings ADD COLUMN backup_retention_count INTEGER NOT NULL DEFAULT 14"
        )

    if 'person' in table_names:
        person_columns = {
            column['name']
            for column in inspector.get_columns('person')
        }

        if 'tmdb_manual_override' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN tmdb_manual_override BOOLEAN NOT NULL DEFAULT 0"
            )

        if 'missing_titles_movies_json' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN missing_titles_movies_json TEXT NOT NULL DEFAULT '[]'"
            )

        if 'missing_titles_shows_json' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN missing_titles_shows_json TEXT NOT NULL DEFAULT '[]'"
            )

        if 'missing_titles_status' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN missing_titles_status VARCHAR(20) NOT NULL DEFAULT 'pending'"
            )

        if 'missing_titles_error' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN missing_titles_error TEXT"
            )

        if 'missing_titles_scanned_at' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN missing_titles_scanned_at DATETIME"
            )

        if 'web_priority' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN web_priority INTEGER NOT NULL DEFAULT 0"
            )

        if 'force_publish' not in person_columns:
            alter_statements.append(
                "ALTER TABLE person ADD COLUMN force_publish BOOLEAN NOT NULL DEFAULT 0"
            )

    if 'library_target' in table_names:
        library_target_columns = {
            column['name']
            for column in inspector.get_columns('library_target')
        }

        if 'plex_titles_cache_json' not in library_target_columns:
            alter_statements.append(
                """ALTER TABLE library_target ADD COLUMN plex_titles_cache_json TEXT NOT NULL DEFAULT '{"keys_with_year":[],"keys_without_year":[]}'"""
            )

        if 'plex_titles_cached_at' not in library_target_columns:
            alter_statements.append(
                "ALTER TABLE library_target ADD COLUMN plex_titles_cached_at DATETIME"
            )

        if 'plex_titles_cache_status' not in library_target_columns:
            alter_statements.append(
                "ALTER TABLE library_target ADD COLUMN plex_titles_cache_status VARCHAR(20) NOT NULL DEFAULT 'pending'"
            )

        if 'plex_titles_cache_error' not in library_target_columns:
            alter_statements.append(
                "ALTER TABLE library_target ADD COLUMN plex_titles_cache_error TEXT"
            )

    if 'task_run' in table_names:
        task_run_columns = {
            column['name']
            for column in inspector.get_columns('task_run')
        }

        if 'plex_server_id' not in task_run_columns:
            alter_statements.append(
                "ALTER TABLE task_run ADD COLUMN plex_server_id INTEGER"
            )

    if not alter_statements:
        return

    with db.engine.begin() as connection:
        for statement in alter_statements:
            connection.execute(text(statement))


def create_app():
    app = Flask(__name__, instance_relative_config=True)

    instance_path = Path(app.instance_path)
    instance_path.mkdir(parents=True, exist_ok=True)

    db_path = os.getenv('DATABASE_PATH', instance_path / 'memoria.db')

    app.config.update(
        SECRET_KEY=os.getenv('SECRET_KEY', 'dev-secret-change-me'),
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{db_path}",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SCHEDULER_ENABLED=os.getenv('SCHEDULER_ENABLED', '1') == '1',
    )

    db.init_app(app)

    from .blueprints.main import bp as main_bp
    from .blueprints.servers import bp as servers_bp
    from .blueprints.settings import bp as settings_bp
    from .blueprints.people import bp as people_bp
    from .blueprints.arr import bp as arr_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(servers_bp, url_prefix='/servers')
    app.register_blueprint(settings_bp, url_prefix='/settings')
    app.register_blueprint(people_bp, url_prefix='/people')
    app.register_blueprint(arr_bp, url_prefix='/arr')

    def datetime_utc_iso(value):
        if not value:
            return ''

        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)

        return value.isoformat().replace('+00:00', 'Z')

    app.jinja_env.filters['datetime_utc_iso'] = datetime_utc_iso

    @app.context_processor
    def inject_globals():
        from .models import AppSettings, ArrServer, LibraryTarget
        return {
            'settings': AppSettings.get_or_create(),
            'arr_servers': ArrServer.query.order_by(ArrServer.name.asc()).all(),
            'library_targets': LibraryTarget.query.order_by(LibraryTarget.section_name.asc()).all(),
            'app_version': _read_info_version(),
            't': translate,
            'current_language': get_current_language(),
            'available_languages': get_available_languages(),
        }

    with app.app_context():
        db.create_all()
        _ensure_runtime_schema()
        AppSettings.get_or_create()

        if app.config['SCHEDULER_ENABLED'] and not scheduler.running:
            from .services.scheduler_service import register_jobs, schedule_startup_catchup
            register_jobs(app)
            scheduler.start()
            schedule_startup_catchup(app)

    return app