from __future__ import annotations

from datetime import datetime, date
from flask import current_app

from ..extensions import db
from ..models import AppSettings, TributeEvent, CollectionPublication, LibraryTarget, PlexServer
from .plex_service import PlexService
from .tmdb_service import TmdbService
from .plex_library_cache_service import filter_credits_with_library_cache


def render_template_text(template: str, *, person, event, target=None) -> str:
    values = {
        'person_name': person.name,
        'death_date': person.death_date.isoformat(),
        'country': person.country or '',
        'days_remaining': event.days_remaining,
        'library_name': target.section_name if target else '',
        'server_name': target.plex_server.name if target else '',
        'profession': ', '.join(person.professions()),
    }
    try:
        return template.format(**values)
    except Exception:
        return template


def _load_tmdb_credits(person, settings: AppSettings) -> dict | None:
    if not settings.tmdb_api_key:
        return None

    try:
        tmdb = TmdbService(settings.tmdb_api_key)

        if not person.tmdb_person_id:
            match = tmdb.search_person(person.name)
            if not match:
                return None

            person.tmdb_person_id = match.get('id')
            db.session.flush()

        return tmdb.person_credits(person.tmdb_person_id)

    except Exception:
        current_app.logger.exception("TMDb lookup failed for person %s", person.name)
        return None

def _load_tmdb_person_poster_url(person, settings: AppSettings) -> str | None:
    if not settings.tmdb_api_key:
        return None

    try:
        tmdb = TmdbService(settings.tmdb_api_key)

        if not person.tmdb_person_id:
            match = tmdb.search_person(
                person.name,
                death_date=person.death_date.isoformat() if person.death_date else None,
            )
            if not match:
                return None

            person.tmdb_person_id = match.get('id')
            db.session.flush()

        return tmdb.person_profile_image_url(person.tmdb_person_id)

    except Exception:
        current_app.logger.exception("TMDb poster lookup failed for person %s", person.name)
        return None

def _find_matches_for_target(plex: PlexService, target, person, tmdb_credits: dict | None):
    if tmdb_credits:
        media_type = 'movie' if target.media_type == 'movie' else 'tv'
        credits = (tmdb_credits.get('cast') or []) + (tmdb_credits.get('crew') or [])

        cached_credits = filter_credits_with_library_cache(
            target,
            credits,
            media_type=media_type,
        )

        if cached_credits:
            title_matches = plex.find_items_by_credit_titles(
                target.section_name,
                cached_credits,
                media_type=media_type,
            )
            if title_matches:
                return title_matches

        title_matches = plex.find_items_by_credit_titles(
            target.section_name,
            credits,
            media_type=media_type,
        )
        if title_matches:
            return title_matches

    return plex.find_person_items(target.section_name, person.name)


def sync_event(event: TributeEvent) -> None:
    settings = AppSettings.get_or_create()
    person = event.person
    tmdb_credits = _load_tmdb_credits(person, settings)
    tmdb_person_poster_url = _load_tmdb_person_poster_url(person, settings)

    targets = (
        db.session.query(LibraryTarget)
        .join(PlexServer, LibraryTarget.plex_server_id == PlexServer.id)
        .filter(
            LibraryTarget.enabled.is_(True),
            PlexServer.enabled.is_(True),
        )
        .order_by(PlexServer.name.asc(), LibraryTarget.section_name.asc())
        .all()
    )

    for target in targets:
        if event.media_mode != 'both' and target.media_type != event.media_mode:
            continue

        try:
            plex = PlexService(
                target.plex_server.base_url,
                target.plex_server.token,
                target.plex_server.verify_ssl,
            )

            matches = _find_matches_for_target(
                plex=plex,
                target=target,
                person=person,
                tmdb_credits=tmdb_credits,
            )

            title = render_template_text(
                settings.collection_name_template,
                person=person,
                event=event,
                target=target,
            )
            summary = render_template_text(
                settings.collection_summary_template,
                person=person,
                event=event,
                target=target,
            )

            publish_on_home = bool(settings.publish_on_home and target.publish_on_home)
            publish_on_friends_home = bool(
                settings.publish_on_friends_home and target.publish_on_friends_home
            )

            publication = CollectionPublication.query.filter_by(
                event_id=event.id,
                target_id=target.id,
            ).first()

            if not publication:
                publication = CollectionPublication(
                    event=event,
                    target=target,
                    collection_title=title,
                )
                db.session.add(publication)

            key, media_count, message = plex.upsert_collection(
                target.section_name,
                title,
                summary,
                [m.item for m in matches],
                publish_on_home=publish_on_home,
                publish_on_friends_home=publish_on_friends_home,
                poster_url=tmdb_person_poster_url,
            )

            publication.plex_collection_key = key
            publication.collection_title = title
            publication.media_count = media_count
            publication.status = 'synced' if media_count else 'missing'
            publication.last_message = message
            publication.last_synced_at = datetime.utcnow()

        except Exception as exc:
            current_app.logger.exception(
                'Failed to sync event %s on target %s',
                event.id,
                target.id,
            )

            publication = CollectionPublication.query.filter_by(
                event_id=event.id,
                target_id=target.id,
            ).first()

            if not publication:
                publication = CollectionPublication(
                    event=event,
                    target=target,
                    collection_title='Unknown',
                )
                db.session.add(publication)

            publication.status = 'error'
            publication.last_message = str(exc)
            publication.last_synced_at = datetime.utcnow()

    event.last_synced_at = datetime.utcnow()
    db.session.commit()


def expire_due_events() -> dict:
    events = (
        TributeEvent.query
        .filter(
            TributeEvent.status == 'active',
            TributeEvent.end_date < date.today(),
        )
        .all()
    )

    processed_items = 0
    success_items = 0
    error_items = 0

    for event in events:
        processed_items += 1

        removal_result = remove_event_collections(event)

        if removal_result['all_removed']:
            event.status = 'expired'
            success_items += 1
        else:
            error_items += 1
            current_app.logger.warning(
                'Event %s was not marked as expired because some collections could not be removed.',
                event.id,
            )

    db.session.commit()

    return {
        'processed_items': processed_items,
        'success_items': success_items,
        'error_items': error_items,
        'message': (
            f'{success_items} event(s) expired successfully, '
            f'{error_items} event(s) kept active for retry.'
        ),
    }


def remove_event_collections(event: TributeEvent) -> dict:
    total_items = len(event.publications)
    removed_items = 0
    error_items = 0

    for publication in event.publications:
        if publication.status == 'removed':
            removed_items += 1
            continue

        try:
            target = publication.target
            plex = PlexService(
                target.plex_server.base_url,
                target.plex_server.token,
                target.plex_server.verify_ssl,
            )
            message = plex.delete_collection_by_key(
                target.section_name,
                publication.plex_collection_key,
                publication.collection_title,
            )

            if message == 'Collection not found':
                publication.status = 'removed'
                publication.last_message = 'Collection already absent on Plex.'
                publication.last_synced_at = datetime.utcnow()
                removed_items += 1
                continue

            publication.status = 'removed'
            publication.last_message = message
            publication.last_synced_at = datetime.utcnow()
            removed_items += 1

        except Exception as exc:
            publication.status = 'error'
            publication.last_message = f'Delete failed: {exc}'
            publication.last_synced_at = datetime.utcnow()
            error_items += 1

    db.session.flush()

    return {
        'total_items': total_items,
        'removed_items': removed_items,
        'error_items': error_items,
        'all_removed': error_items == 0,
    }