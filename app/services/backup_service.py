from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from flask import current_app

from ..extensions import db
from ..models import AppLog


def _log_backup_event(level: str, message: str, *, details: str | None = None) -> None:
    try:
        entry = AppLog(
            level=level,
            source='backup',
            message=message,
            details=details,
            related_type='backup',
            related_id=None,
        )
        db.session.add(entry)
        db.session.commit()
    except Exception:
        db.session.rollback()


def get_database_path() -> Path:
    uri = current_app.config.get('SQLALCHEMY_DATABASE_URI') or ''
    prefix = 'sqlite:///'

    if not uri.startswith(prefix):
        raise RuntimeError('Only sqlite databases are supported by the backup system.')

    raw_path = uri[len(prefix):]
    return Path(raw_path).resolve()


def get_backup_directory() -> Path:
    backup_dir = Path(current_app.instance_path) / 'backups'
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir


def get_backup_path(filename: str) -> Path:
    backup_dir = get_backup_directory().resolve()
    backup_path = (backup_dir / filename).resolve()

    if backup_path.parent != backup_dir:
        raise FileNotFoundError('Backup file not found.')

    if not backup_path.exists():
        raise FileNotFoundError('Backup file not found.')

    return backup_path


def list_database_backups() -> list[dict]:
    backup_dir = get_backup_directory()
    rows = []

    for path in sorted(backup_dir.glob('memoria_backup_*.sqlite3'), reverse=True):
        stat = path.stat()
        created_at = datetime.fromtimestamp(stat.st_mtime)

        rows.append({
            'filename': path.name,
            'path': path,
            'size_bytes': stat.st_size,
            'created_at': created_at,
        })

    return rows


def _build_backup_filename() -> str:
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    return f'memoria_backup_{timestamp}.sqlite3'


def create_database_backup(*, trigger: str = 'manual') -> Path:
    source_path = get_database_path()

    if not source_path.exists():
        raise FileNotFoundError(f'Database file not found: {source_path}')

    backup_dir = get_backup_directory()
    backup_path = backup_dir / _build_backup_filename()

    source_connection = None
    target_connection = None

    try:
        source_connection = sqlite3.connect(str(source_path))
        target_connection = sqlite3.connect(str(backup_path))
        source_connection.backup(target_connection)
        target_connection.commit()

        _log_backup_event(
            'info',
            f'Database backup created ({trigger}).',
            details=str(backup_path),
        )

        return backup_path

    except Exception as exc:
        _log_backup_event(
            'error',
            f'Database backup failed ({trigger}).',
            details=str(exc),
        )
        raise

    finally:
        if target_connection is not None:
            target_connection.close()

        if source_connection is not None:
            source_connection.close()


def restore_database_backup(filename: str, *, create_safety_backup: bool = True) -> dict:
    source_path = get_database_path()
    backup_path = get_backup_path(filename)

    if not source_path.exists():
        raise FileNotFoundError(f'Database file not found: {source_path}')

    safety_backup_path = None
    restore_tmp_path = source_path.with_name(f'{source_path.stem}.restore_tmp{source_path.suffix}')

    backup_connection = None
    restore_connection = None

    try:
        if create_safety_backup:
            safety_backup_path = create_database_backup(trigger='pre_restore')

        db.session.remove()
        db.engine.dispose()

        if restore_tmp_path.exists():
            restore_tmp_path.unlink()

        backup_connection = sqlite3.connect(str(backup_path))
        restore_connection = sqlite3.connect(str(restore_tmp_path))

        backup_connection.backup(restore_connection)
        restore_connection.commit()

        restore_connection.close()
        restore_connection = None
        backup_connection.close()
        backup_connection = None

        os.replace(str(restore_tmp_path), str(source_path))

        db.session.remove()
        db.engine.dispose()

        details_lines = [
            f'restored_from={backup_path}',
        ]

        if safety_backup_path is not None:
            details_lines.append(f'safety_backup={safety_backup_path}')

        _log_backup_event(
            'warning',
            'Database restored from backup.',
            details='\n'.join(details_lines),
        )

        return {
            'restored_from': backup_path,
            'safety_backup_path': safety_backup_path,
        }

    except Exception as exc:
        if restore_tmp_path.exists():
            try:
                restore_tmp_path.unlink()
            except Exception:
                pass

        _log_backup_event(
            'error',
            'Database restore failed.',
            details=f'backup={backup_path}\nerror={exc}',
        )
        raise

    finally:
        if restore_connection is not None:
            restore_connection.close()

        if backup_connection is not None:
            backup_connection.close()


def prune_old_backups(retention_count: int) -> int:
    backups = list_database_backups()
    safe_retention_count = max(int(retention_count or 0), 1)

    deleted_count = 0

    for row in backups[safe_retention_count:]:
        try:
            row['path'].unlink(missing_ok=True)
            deleted_count += 1
        except Exception:
            continue

    if deleted_count:
        _log_backup_event(
            'info',
            'Old database backups pruned.',
            details=f'deleted_count={deleted_count}',
        )

    return deleted_count


def get_latest_backup_info() -> dict | None:
    backups = list_database_backups()
    return backups[0] if backups else None


def is_auto_backup_due(interval_hours: int) -> bool:
    latest_backup = get_latest_backup_info()

    if latest_backup is None:
        return True

    safe_interval_hours = max(int(interval_hours or 0), 1)
    cutoff = datetime.utcnow() - timedelta(hours=safe_interval_hours)

    return latest_backup['created_at'] <= cutoff


def run_automatic_backup(*, interval_hours: int, retention_count: int) -> dict:
    created = False
    backup_path = None

    if is_auto_backup_due(interval_hours):
        backup_path = create_database_backup(trigger='auto')
        created = True

    deleted_count = prune_old_backups(retention_count)

    return {
        'created': created,
        'backup_path': str(backup_path) if backup_path else None,
        'deleted_count': deleted_count,
    }