from datetime import datetime, timedelta

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app

from ..extensions import db, scheduler
from ..models import PlexServer, LibraryTarget
from ..services.plex_service import PlexService
from ..services.plex_library_cache_service import refresh_library_title_cache_safe

bp = Blueprint('servers', __name__)


def sync_server_libraries(server: PlexServer) -> tuple[int, int]:
    """
    Découvre les bibliothèques Plex pour un serveur donné.
    - ajoute les nouvelles bibliothèques manquantes
    - ne coche rien par défaut
    - met à jour le media_type si la bibliothèque existe déjà
    Retourne: (created_count, updated_count)
    """
    plex = PlexService(server.base_url, server.token, server.verify_ssl)

    existing = {
        (x.section_name, x.media_type): x
        for x in server.libraries
    }

    created = 0
    updated = 0

    for section in plex.list_library_sections():
        key = (section['title'], section['type'])
        target = existing.get(key)

        if target is None:
            db.session.add(
                LibraryTarget(
                    plex_server=server,
                    section_name=section['title'],
                    media_type=section['type'],
                    enabled=False,
                    publish_on_home=False,
                    publish_on_friends_home=False,
                )
            )
            created += 1
        else:
            # sécurité si un jour le type remonte différemment
            if target.media_type != section['type']:
                target.media_type = section['type']
                updated += 1

    return created, updated

def _run_refresh_server_cache_job(app, server_id: int) -> None:
    from ..services.scheduler_service import log_app_event

    with app.app_context():
        server = PlexServer.query.get(server_id)
        if not server:
            return

        libraries = [
            library
            for library in server.libraries
            if library.enabled
        ]

        if not libraries:
            log_app_event(
                'warning',
                'plex_cache',
                f'Plex cache refresh skipped for server "{server.name}": no enabled library.',
                related_type='server',
                related_id=server.id,
            )
            return

        success_count = 0
        error_count = 0

        for library in libraries:
            refresh_library_title_cache_safe(library)
            db.session.expire_all()

            refreshed_library = LibraryTarget.query.get(library.id)
            if refreshed_library and refreshed_library.plex_titles_cache_status == 'ready':
                success_count += 1
            else:
                error_count += 1

        log_app_event(
            'info' if error_count == 0 else 'warning',
            'plex_cache',
            (
                f'Plex cache refresh finished for server "{server.name}". '
                f'{success_count} success, {error_count} error.'
            ),
            related_type='server',
            related_id=server.id,
        )

@bp.route('/')
def index():
    servers = PlexServer.query.order_by(PlexServer.name.asc()).all()
    return render_template('servers.html', servers=servers)


@bp.post('/create')
def create_server():
    server = PlexServer(
        name=request.form['name'].strip(),
        base_url=request.form['base_url'].strip(),
        token=request.form['token'].strip(),
        verify_ssl=request.form.get('verify_ssl') == 'on',
        enabled=request.form.get('enabled') == 'on',
    )
    db.session.add(server)
    db.session.commit()

    try:
        created, updated = sync_server_libraries(server)
        db.session.commit()
        flash(
            f'Plex server added. {created} libraries discovered automatically'
            + (f', {updated} updated.' if updated else '.'),
            'success',
        )
    except Exception as exc:
        db.session.rollback()
        flash(
            f'Plex server added, but automatic library discovery failed: {exc}',
            'warning',
        )

    return redirect(url_for('servers.index'))

@bp.post('/<int:server_id>/update')
def update_server(server_id: int):
    server = PlexServer.query.get_or_404(server_id)

    server.name = request.form['name'].strip()
    server.base_url = request.form['base_url'].strip()
    server.token = request.form['token'].strip()
    server.verify_ssl = request.form.get('verify_ssl') == 'on'
    server.enabled = request.form.get('enabled') == 'on'

    db.session.commit()
    flash('Plex server updated.', 'success')
    return redirect(url_for('servers.index'))

@bp.post('/<int:server_id>/delete')
def delete_server(server_id: int):
    server = PlexServer.query.get_or_404(server_id)
    db.session.delete(server)
    db.session.commit()
    flash('Plex server deleted.', 'success')
    return redirect(url_for('servers.index'))


@bp.post('/<int:server_id>/discover')
def discover_libraries(server_id: int):
    server = PlexServer.query.get_or_404(server_id)

    try:
        created, updated = sync_server_libraries(server)
        db.session.commit()
        flash(
            f'{created} libraries discovered'
            + (f', {updated} updated.' if updated else '.'),
            'success',
        )
    except Exception as exc:
        db.session.rollback()
        flash(f'Library discovery failed: {exc}', 'danger')

    return redirect(url_for('servers.index'))

@bp.post('/<int:server_id>/refresh-cache')
def refresh_server_cache(server_id: int):
    from ..services.scheduler_service import log_app_event

    server = PlexServer.query.get_or_404(server_id)

    libraries = [
        library
        for library in server.libraries
        if library.enabled
    ]

    if not libraries:
        flash('No enabled library to refresh for this server.', 'warning')
        return redirect(url_for('servers.index'))

    app_obj = current_app._get_current_object()
    job_id = f'refresh_server_cache_{server.id}_{int(datetime.utcnow().timestamp())}'

    scheduler.add_job(
        func=lambda app=app_obj, sid=server.id: _run_refresh_server_cache_job(app, sid),
        trigger='date',
        run_date=datetime.now(scheduler.timezone) + timedelta(seconds=1),
        id=job_id,
        replace_existing=False,
        misfire_grace_time=30,
    )

    log_app_event(
        'info',
        'plex_cache',
        f'Plex cache refresh queued for server "{server.name}".',
        related_type='server',
        related_id=server.id,
    )

    flash(
        f'Plex cache refresh started in background for "{server.name}". Reload the page in a moment to see the updated status.',
        'success',
    )

    return redirect(url_for('servers.index'))

@bp.post('/library/<int:target_id>/toggle')
def toggle_library(target_id: int):
    target = LibraryTarget.query.get_or_404(target_id)

    target.enabled = request.form.get('enabled') == 'on'
    target.publish_on_home = request.form.get('publish_on_home') == 'on'
    target.publish_on_friends_home = request.form.get('publish_on_friends_home') == 'on'

    arr_server_id = request.form.get('arr_server_id')
    target.arr_server_id = int(arr_server_id) if arr_server_id else None

    db.session.commit()

    if request.form.get('_autosave') == '1':
        return ('', 204)

    flash('Library settings updated.', 'success')
    return redirect(url_for('servers.index'))