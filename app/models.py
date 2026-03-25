from __future__ import annotations
from datetime import datetime, date, timedelta
from .extensions import db
from .utils.country_utils import normalize_country_label, normalize_country_key


class TimestampMixin:
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AppSettings(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    app_name = db.Column(db.String(120), default='Memoria')
    auto_detection_enabled = db.Column(db.Boolean, default=False, nullable=False)
    detection_window_days = db.Column(db.Integer, default=2, nullable=False)
    display_days = db.Column(db.Integer, default=7, nullable=False)
    max_people = db.Column(db.Integer, default=2, nullable=False)
    countries_csv = db.Column(db.Text, default='France,United States,United Kingdom', nullable=False)
    professions_csv = db.Column(db.Text, default='actor,actress,director', nullable=False)
    publish_on_home = db.Column(db.Boolean, default=True, nullable=False)
    publish_on_friends_home = db.Column(db.Boolean, default=True, nullable=False)
    collection_name_template = db.Column(db.String(255), default='In memory of {person_name}', nullable=False)
    collection_summary_template = db.Column(
        db.Text,
        default='Tribute collection for {person_name}, who passed away on {death_date}.',
        nullable=False,
    )
    default_media_mode = db.Column(db.String(20), default='both', nullable=False)
    deduplicate_people = db.Column(db.Boolean, default=True, nullable=False)
    tmdb_api_key = db.Column(db.String(255), nullable=True)
    auto_missing_titles_enabled = db.Column(db.Boolean, default=True, nullable=False)
    missing_titles_refresh_hours = db.Column(db.Integer, default=24, nullable=False)
    auto_arr_enabled = db.Column(db.Boolean, default=True, nullable=False)

    log_retention_days = db.Column(db.Integer, default=30, nullable=False)
    job_retention_days = db.Column(db.Integer, default=30, nullable=False)
    arr_activity_retention_days = db.Column(db.Integer, default=90, nullable=False)

    web_source = db.Column(db.String(30), default='wikidata', nullable=False)

    @classmethod
    def get_or_create(cls) -> 'AppSettings':
        settings = cls.query.first()
        if settings:
            return settings
        settings = cls()
        db.session.add(settings)
        db.session.commit()
        return settings

    def countries(self) -> list[str]:
        values = []
        seen = set()

        for raw in (self.countries_csv or '').split(','):
            label = normalize_country_label(raw)
            if not label:
                continue

            key = normalize_country_key(label)
            if not key or key in seen:
                continue

            seen.add(key)
            values.append(label)

        return values

    def professions(self) -> list[str]:
        return [x.strip().lower() for x in (self.professions_csv or '').split(',') if x.strip()]


class PlexServer(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True)
    base_url = db.Column(db.String(255), nullable=False)
    token = db.Column(db.String(255), nullable=False)
    verify_ssl = db.Column(db.Boolean, default=True, nullable=False)
    enabled = db.Column(db.Boolean, default=True, nullable=False)

    libraries = db.relationship('LibraryTarget', back_populates='plex_server', cascade='all, delete-orphan')


class ArrServer(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True)
    kind = db.Column(db.String(20), nullable=False)  # radarr / sonarr
    base_url = db.Column(db.String(255), nullable=False)
    api_key = db.Column(db.String(255), nullable=False)
    root_folder = db.Column(db.String(255), nullable=False)
    quality_profile_id = db.Column(db.Integer, nullable=False)
    language_profile_id = db.Column(db.Integer, nullable=True)
    search_on_add = db.Column(db.Boolean, default=True, nullable=False)
    enabled = db.Column(db.Boolean, default=True, nullable=False)

    mappings = db.relationship('LibraryTarget', back_populates='arr_server')


class LibraryTarget(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    plex_server_id = db.Column(db.Integer, db.ForeignKey('plex_server.id'), nullable=False)
    arr_server_id = db.Column(db.Integer, db.ForeignKey('arr_server.id'), nullable=True)
    section_name = db.Column(db.String(120), nullable=False)
    media_type = db.Column(db.String(20), nullable=False)  # movie / show
    enabled = db.Column(db.Boolean, default=False, nullable=False)
    publish_on_home = db.Column(db.Boolean, default=False, nullable=False)
    publish_on_friends_home = db.Column(db.Boolean, default=False, nullable=False)

    plex_titles_cache_json = db.Column(db.Text, default='{"keys_with_year":[],"keys_without_year":[]}', nullable=False)
    plex_titles_cached_at = db.Column(db.DateTime, nullable=True)
    plex_titles_cache_status = db.Column(db.String(20), default='pending', nullable=False)  # pending / ready / error
    plex_titles_cache_error = db.Column(db.Text, nullable=True)

    plex_server = db.relationship('PlexServer', back_populates='libraries')
    arr_server = db.relationship('ArrServer', back_populates='mappings')
    publications = db.relationship('CollectionPublication', back_populates='target', cascade='all, delete-orphan')


class Person(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), nullable=False, unique=True)
    death_date = db.Column(db.Date, nullable=False)
    country = db.Column(db.String(120), nullable=True)
    professions_csv = db.Column(db.Text, nullable=True)
    source = db.Column(db.String(30), nullable=False, default='manual')
    source_url = db.Column(db.String(500), nullable=True)
    tmdb_person_id = db.Column(db.Integer, nullable=True)
    tmdb_manual_override = db.Column(db.Boolean, default=False, nullable=False)
    imdb_id = db.Column(db.String(32), nullable=True)
    wikidata_id = db.Column(db.String(32), nullable=True)
    notes = db.Column(db.Text, nullable=True)

    manual_priority = db.Column(db.Integer, nullable=True)
    is_pinned = db.Column(db.Boolean, default=False, nullable=False)
    exclude_from_auto = db.Column(db.Boolean, default=False, nullable=False)
    ignore_until = db.Column(db.Date, nullable=True)
    selection_note = db.Column(db.Text, nullable=True)

    missing_titles_movies_json = db.Column(db.Text, default='[]', nullable=False)
    missing_titles_shows_json = db.Column(db.Text, default='[]', nullable=False)
    missing_titles_status = db.Column(db.String(20), default='pending', nullable=False)  # pending / ready / error / disabled
    missing_titles_error = db.Column(db.Text, nullable=True)
    missing_titles_scanned_at = db.Column(db.DateTime, nullable=True)

    events = db.relationship('TributeEvent', back_populates='person', cascade='all, delete-orphan')
    arr_activities = db.relationship('ArrActivity', back_populates='person', cascade='all, delete-orphan')

    def professions(self) -> list[str]:
        return [x.strip() for x in (self.professions_csv or '').split(',') if x.strip()]

    @property
    def is_ignored_now(self) -> bool:
        return bool(self.ignore_until and self.ignore_until >= date.today())

class DetectionCandidate(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), nullable=False)
    death_date = db.Column(db.Date, nullable=False)
    country = db.Column(db.String(120), nullable=True)
    source_url = db.Column(db.String(500), nullable=True)
    imdb_id = db.Column(db.String(32), nullable=True)
    wikidata_id = db.Column(db.String(32), nullable=True)
    popularity_score = db.Column(db.Integer, default=0, nullable=False)
    professions_csv = db.Column(db.Text, nullable=True)

class DetectionRun(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending/running/success/error
    requested_by = db.Column(db.String(20), default='manual', nullable=False)
    candidates_cached = db.Column(db.Integer, default=0, nullable=False)
    people_upserted = db.Column(db.Integer, default=0, nullable=False)
    events_created = db.Column(db.Integer, default=0, nullable=False)
    error_message = db.Column(db.Text, nullable=True)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)

class TaskRun(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    task_type = db.Column(db.String(30), nullable=False)  # sync / expire
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending/running/success/error
    requested_by = db.Column(db.String(20), default='manual', nullable=False)

    total_items = db.Column(db.Integer, default=0, nullable=False)
    processed_items = db.Column(db.Integer, default=0, nullable=False)
    success_items = db.Column(db.Integer, default=0, nullable=False)
    error_items = db.Column(db.Integer, default=0, nullable=False)

    message = db.Column(db.Text, nullable=True)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)

class AppLog(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)

    level = db.Column(db.String(20), default='info', nullable=False)  # info / warning / error
    source = db.Column(db.String(50), nullable=False)  # detection / sync / expire / scheduler / arr / app
    message = db.Column(db.Text, nullable=False)
    details = db.Column(db.Text, nullable=True)

    related_type = db.Column(db.String(30), nullable=True)  # detection_run / task_run / event / person / arr_activity / server
    related_id = db.Column(db.Integer, nullable=True)

class TributeEvent(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    person_id = db.Column(db.Integer, db.ForeignKey('person.id'), nullable=False)
    status = db.Column(db.String(20), default='active', nullable=False)  # active/expired/cancelled
    source = db.Column(db.String(30), default='manual', nullable=False)
    media_mode = db.Column(db.String(20), default='both', nullable=False)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=False)
    priority = db.Column(db.Integer, default=100, nullable=False)
    note = db.Column(db.Text, nullable=True)
    last_synced_at = db.Column(db.DateTime, nullable=True)
    remove_after_expiry = db.Column(db.Boolean, default=True, nullable=False)

    person = db.relationship('Person', back_populates='events')
    publications = db.relationship('CollectionPublication', back_populates='event', cascade='all, delete-orphan')

    @property
    def is_active(self) -> bool:
        today = date.today()
        return self.status == 'active' and self.start_date <= today <= self.end_date

    @property
    def days_remaining(self) -> int:
        return max((self.end_date - date.today()).days, 0)


class CollectionPublication(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)
    event_id = db.Column(db.Integer, db.ForeignKey('tribute_event.id'), nullable=False)
    target_id = db.Column(db.Integer, db.ForeignKey('library_target.id'), nullable=False)
    plex_collection_key = db.Column(db.String(120), nullable=True)
    collection_title = db.Column(db.String(255), nullable=False)
    media_count = db.Column(db.Integer, default=0, nullable=False)
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending/synced/missing/removed/error
    last_message = db.Column(db.Text, nullable=True)
    last_synced_at = db.Column(db.DateTime, nullable=True)

    event = db.relationship('TributeEvent', back_populates='publications')
    target = db.relationship('LibraryTarget', back_populates='publications')

class ArrActivity(db.Model, TimestampMixin):
    id = db.Column(db.Integer, primary_key=True)

    person_id = db.Column(db.Integer, db.ForeignKey('person.id'), nullable=False)
    arr_server_id = db.Column(db.Integer, db.ForeignKey('arr_server.id'), nullable=True)
    library_target_id = db.Column(db.Integer, db.ForeignKey('library_target.id'), nullable=True)

    media_kind = db.Column(db.String(20), nullable=False)  # movie / show

    # Ancien identifiant générique conservé pour compatibilité
    external_id = db.Column(db.Integer, nullable=True)

    # Nouveaux identifiants explicites
    tmdb_id = db.Column(db.Integer, nullable=True)
    tvdb_id = db.Column(db.Integer, nullable=True)

    title = db.Column(db.String(255), nullable=False)
    year = db.Column(db.Integer, nullable=True)

    status = db.Column(db.String(30), nullable=False, default='pending')  # created / already_exists / invalid / error
    message = db.Column(db.Text, nullable=True)

    # Debug utile
    request_payload = db.Column(db.Text, nullable=True)
    response_payload = db.Column(db.Text, nullable=True)

    person = db.relationship('Person', back_populates='arr_activities')
    arr_server = db.relationship('ArrServer')
    library_target = db.relationship('LibraryTarget')

def make_slug(value: str) -> str:
    return '-'.join(''.join(c.lower() if c.isalnum() else ' ' for c in value).split())


def create_or_retrigger_event(
    person: Person,
    media_mode: str,
    display_days: int,
    source: str = 'manual',
    note: str | None = None,
    priority: int | None = None,
) -> TributeEvent:
    start = date.today()
    end = start + timedelta(days=max(display_days - 1, 0))

    active_events = (
        TributeEvent.query
        .filter(
            TributeEvent.person_id == person.id,
            TributeEvent.status == 'active',
        )
        .order_by(TributeEvent.start_date.desc(), TributeEvent.id.desc())
        .all()
    )

    if active_events:
        primary_event = active_events[0]

        primary_event.media_mode = media_mode
        primary_event.start_date = start
        primary_event.end_date = end
        primary_event.source = source
        primary_event.status = 'active'
        primary_event.remove_after_expiry = True

        if note is not None:
            primary_event.note = note

        if priority is not None:
            primary_event.priority = priority

        duplicate_events = active_events[1:]
        for duplicate_event in duplicate_events:
            duplicate_event.status = 'cancelled'

            duplicate_note = (duplicate_event.note or '').strip()
            duplicate_suffix = (
                'Cancelled automatically because another active event for this person '
                'was retriggered and kept as the primary event.'
            )

            if duplicate_note:
                if duplicate_suffix not in duplicate_note:
                    duplicate_event.note = f'{duplicate_note}\n\n{duplicate_suffix}'
            else:
                duplicate_event.note = duplicate_suffix

        return primary_event

    event = TributeEvent(
        person=person,
        media_mode=media_mode,
        start_date=start,
        end_date=end,
        source=source,
        note=note,
        status='active',
        remove_after_expiry=True,
        priority=priority if priority is not None else 100,
    )
    db.session.add(event)
    return event
