from __future__ import annotations

from datetime import date

from ..models import Person
from .string_utils import normalize_name


def find_existing_person_duplicate(
    *,
    person_id: int | None = None,
    slug: str | None = None,
    name: str | None = None,
    death_date: date | None = None,
    tmdb_person_id: int | None = None,
    imdb_id: str | None = None,
    wikidata_id: str | None = None,
) -> tuple[Person | None, str | None]:
    query = Person.query

    if person_id is not None:
        query = query.filter(Person.id != person_id)

    candidates = query.all()

    normalized_name = normalize_name(name or '')
    imdb_id = (imdb_id or '').strip() or None
    wikidata_id = (wikidata_id or '').strip() or None
    slug = (slug or '').strip() or None

    for candidate in candidates:
        if tmdb_person_id and candidate.tmdb_person_id and candidate.tmdb_person_id == tmdb_person_id:
            return candidate, 'same TMDb ID'

    for candidate in candidates:
        if imdb_id and candidate.imdb_id and candidate.imdb_id == imdb_id:
            return candidate, 'same IMDb ID'

    for candidate in candidates:
        if wikidata_id and candidate.wikidata_id and candidate.wikidata_id == wikidata_id:
            return candidate, 'same Wikidata ID'

    for candidate in candidates:
        if slug and candidate.slug == slug:
            return candidate, 'same slug'

    if normalized_name and death_date:
        for candidate in candidates:
            candidate_normalized_name = normalize_name(candidate.name or '')
            if candidate_normalized_name == normalized_name and candidate.death_date == death_date:
                return candidate, 'same normalized name + death date'

    return None, None