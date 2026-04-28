from __future__ import annotations

from datetime import datetime, date
from flask import current_app

from ..extensions import db
from ..models import AppSettings, TributeEvent, CollectionPublication, LibraryTarget, PlexServer
from .plex_service import PlexService
from .tmdb_service import TmdbService
from .plex_library_cache_service import filter_credits_with_library_cache
from .plex_local_index_service import find_local_matches_for_target
from .media_identity_service import enrich_credit_list_external_ids

def _log_app_event(
    level: str,
    source: str,
    message: str,
    *,
    details: str | None = None,
    related_type: str | None = None,
    related_id: int | None = None,
) -> None:
    try:
        from .scheduler_service import log_app_event

        log_app_event(
            level,
            source,
            message,
            details=details,
            related_type=related_type,
            related_id=related_id,
        )
    except Exception:
        current_app.logger.exception(
            'Failed to write app log entry from collection_service.'
        )


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
            match = tmdb.search_person(
                person.name,
                death_date=person.death_date.isoformat() if person.death_date else None,
            )
            if not match:
                return None

            person.tmdb_person_id = match.get('id')
            db.session.flush()

        tmdb_credits = tmdb.person_credits(person.tmdb_person_id)
        return {
            'cast': enrich_credit_list_external_ids(tmdb_credits.get('cast') or [], tmdb=tmdb),
            'crew': enrich_credit_list_external_ids(tmdb_credits.get('crew') or [], tmdb=tmdb),
        }

    except Exception:
        current_app.logger.exception("TMDb lookup failed for person %s", person.name)
        return None

def _load_tmdb_person_aliases(person, settings: AppSettings) -> list[str]:
    if not settings.tmdb_api_key:
        return []

    try:
        tmdb = TmdbService(settings.tmdb_api_key)

        if not person.tmdb_person_id:
            match = tmdb.search_person(
                person.name,
                death_date=person.death_date.isoformat() if person.death_date else None,
            )
            if not match:
                return []

            person.tmdb_person_id = match.get('id')
            db.session.flush()

        details = tmdb.person_details(person.tmdb_person_id)
        aliases = details.get('also_known_as') or []

        output: list[str] = []
        seen: set[str] = set()
        for raw_alias in aliases:
            clean_alias = (raw_alias or '').strip()
            normalized_alias = clean_alias.casefold()
            if not clean_alias or normalized_alias in seen:
                continue
            seen.add(normalized_alias)
            output.append(clean_alias)

        return output

    except Exception:
        current_app.logger.exception("TMDb alias lookup failed for person %s", person.name)
        return []

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

def _find_matches_for_target(plex: PlexService, target, person, tmdb_credits: dict | None, person_aliases: list[str] | None = None):
    matches_by_rating_key: dict[str, object] = {}
    media_type = 'movie' if target.media_type == 'movie' else 'tv'

    def add_matches(matches):
        for match in matches or []:
            rating_key = str(getattr(match.item, 'ratingKey', '')).strip()
            if not rating_key:
                continue
            if rating_key in matches_by_rating_key:
                continue
            matches_by_rating_key[rating_key] = match

    local_entries = find_local_matches_for_target(
        target,
        person_name=person.name,
        aliases=person_aliases,
        tmdb_credits=tmdb_credits,
        media_type=media_type,
    )

    if local_entries:
        local_matches = plex.resolve_local_cache_entries_to_items(
            target.section_name,
            local_entries,
            media_type=media_type,
        )
        add_matches(local_matches)

    if matches_by_rating_key:
        return list(matches_by_rating_key.values())

    plex_matches = plex.find_person_items(
        target.section_name,
        person.name,
        aliases=person_aliases,
    )
    add_matches(plex_matches)



    if tmdb_credits:
        credits = (tmdb_credits.get('cast') or []) + (tmdb_credits.get('crew') or [])

        try:
            credits = sorted(
                credits,
                key=lambda x: x.get('popularity') or 0,
                reverse=True
            )
        except Exception:
            current_app.logger.exception("Failed to sort credits by popularity")

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
            add_matches(title_matches)



    return list(matches_by_rating_key.values())


def sync_event(event: TributeEvent) -> None:
    settings = AppSettings.get_or_create()
    person = event.person
    tmdb_credits = _load_tmdb_credits(person, settings)
    tmdb_person_poster_url = _load_tmdb_person_poster_url(person, settings)
    person_aliases = _load_tmdb_person_aliases(person, settings)

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

    plex_by_server_id: dict[int, PlexService] = {}

    for target in targets:
        if event.media_mode != 'both' and target.media_type != event.media_mode:
            continue

        try:
            _log_app_event(
                'info',
                'sync',
                (
                    f'Sync target start for {person.name} '
                    f'on server "{target.plex_server.name}" / library "{target.section_name}".'
                ),
                related_type='event',
                related_id=event.id,
            )

            plex = plex_by_server_id.get(target.plex_server_id)
            if plex is None:
                plex = PlexService(
                    target.plex_server.base_url,
                    target.plex_server.token,
                    target.plex_server.verify_ssl,
                )
                plex_by_server_id[target.plex_server_id] = plex

            matches = _find_matches_for_target(
                plex=plex,
                target=target,
                person=person,
                tmdb_credits=tmdb_credits,
                person_aliases=person_aliases,
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

            _log_app_event(
                'info',
                'sync',
                (
                    f'Sync target done for {person.name} '
                    f'on server "{target.plex_server.name}" / library "{target.section_name}" '
                    f'with {media_count} item(s).'
                ),
                details=message,
                related_type='event',
                related_id=event.id,
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


def expire_due_events(task_run_id: int | None = None) -> dict:
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
        person_name = event.person.name if event.person else f'person#{event.person_id}'

        _log_app_event(
            'info',
            'expire',
            f'Expiring event for {person_name}.',
            related_type='event',
            related_id=event.id,
        )

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
            _log_app_event(
                'info',
                'expire',
                (
                    f'Removing collection "{publication.collection_title}" '
                    f'for {event.person.name if event.person else f"person#{event.person_id}"} '
                    f'on server "{target.plex_server.name}" / library "{target.section_name}".'
                ),
                related_type='event',
                related_id=event.id,
            )
            message = plex.delete_collection_by_key(
                target.section_name,
                publication.plex_collection_key,
                publication.collection_title,
            )

            if message.startswith('Collection deleted'):
                publication.status = 'removed'
                publication.last_message = message
                publication.last_synced_at = datetime.utcnow()
                removed_items += 1

                _log_app_event(
                    'info',
                    'expire',
                    (
                        f'Collection removed for '
                        f'{event.person.name if event.person else f"person#{event.person_id}"} '
                        f'on server "{target.plex_server.name}" / library "{target.section_name}".'
                    ),
                    details=message,
                    related_type='event',
                    related_id=event.id,
                )
                continue

            if message.startswith('Collection not found'):
                publication.status = 'removed'
                publication.last_message = message
                publication.last_synced_at = datetime.utcnow()
                removed_items += 1

                _log_app_event(
                    'warning',
                    'expire',
                    (
                        f'Collection could not be found for '
                        f'{event.person.name if event.person else f"person#{event.person_id}"} '
                        f'on server "{target.plex_server.name}" / library "{target.section_name}".'
                    ),
                    details=message,
                    related_type='event',
                    related_id=event.id,
                )
                continue

            publication.status = 'error'
            publication.last_message = message
            publication.last_synced_at = datetime.utcnow()
            error_items += 1

            _log_app_event(
                'error',
                'expire',
                (
                    f'Collection removal verification failed for '
                    f'{event.person.name if event.person else f"person#{event.person_id}"} '
                    f'on server "{target.plex_server.name}" / library "{target.section_name}".'
                ),
                details=message,
                related_type='event',
                related_id=event.id,
            )

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