from __future__ import annotations

from datetime import date, timedelta
from urllib.parse import unquote, urlparse
import requests

from ..extensions import db
from ..models import AppSettings, DetectionCandidate, make_slug
from ..utils.country_utils import normalize_country_label, normalize_country_key

class DetectionService:
    WIKIDATA_ENDPOINT = 'https://query.wikidata.org/sparql'

    def __init__(self, settings: AppSettings):
        self.settings = settings

    def recent_deaths(self, limit: int | None = None) -> list[dict]:
        start_date = date.today() - timedelta(days=max(self.settings.detection_window_days - 1, 0))
        countries_filter = self._country_filter(self.settings.countries())
        professions_filter = self._profession_filter(self.settings.professions())
        effective_limit = limit or max(self.settings.max_people * 4, 12)

        query = f'''
        PREFIX bd: <http://www.bigdata.com/rdf#>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX p: <http://www.wikidata.org/prop/>
        PREFIX ps: <http://www.wikidata.org/prop/statement/>
        PREFIX schema: <http://schema.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?person ?personLabel ?dateOfDeath ?countryLabel ?imdb ?article ?sitelinks WHERE {{
          ?person wdt:P31 wd:Q5 ;
                  wdt:P570 ?dateOfDeath .

          FILTER(?dateOfDeath >= "{start_date.isoformat()}T00:00:00Z"^^xsd:dateTime)

          OPTIONAL {{ ?person wdt:P27 ?country . }}
          OPTIONAL {{ ?person wdt:P345 ?imdb . }}
          OPTIONAL {{
            ?article schema:about ?person .
            FILTER(CONTAINS(STR(?article), "wikipedia.org"))
          }}
          OPTIONAL {{ ?person wikibase:sitelinks ?sitelinks . }}

          {countries_filter}
          {professions_filter}

          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        ORDER BY DESC(?dateOfDeath)
        LIMIT {effective_limit}
        '''

        headers = {
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'Memoria/1.0',
        }

        response = requests.get(
            self.WIKIDATA_ENDPOINT,
            params={'format': 'json', 'query': query},
            headers=headers,
            timeout=20,
        )
        response.raise_for_status()

        bindings = response.json().get('results', {}).get('bindings', [])
        rows = []
        seen = set()

        for row in bindings:
            person_uri = row.get('person', {}).get('value', '')
            wikidata_id = person_uri.rsplit('/', 1)[-1] if person_uri else None

            raw_name = row.get('personLabel', {}).get('value')
            source_url = row.get('article', {}).get('value')
            name = self._clean_name(raw_name, source_url)

            if not name:
                continue

            dedupe_key = (wikidata_id or '', name.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            death_date = row.get('dateOfDeath', {}).get('value', '')[:10]
            popularity_score_raw = row.get('sitelinks', {}).get('value')
            popularity_score = int(popularity_score_raw) if str(popularity_score_raw).isdigit() else 0

            rows.append({
                'name': name,
                'slug': make_slug(name),
                'death_date': death_date,
                'country': row.get('countryLabel', {}).get('value'),
                'wikidata_id': wikidata_id,
                'imdb_id': row.get('imdb', {}).get('value'),
                'source_url': source_url,
                'popularity_score': popularity_score,
            })

        rows = self._apply_python_filters(rows)

        rows.sort(
            key=lambda x: (
                x.get('popularity_score', 0),
                x.get('death_date', ''),
                x.get('name', '').lower(),
            ),
            reverse=True,
        )
        return rows

    def refresh_candidate_cache(self, limit: int | None = None) -> list[dict]:
        rows = self.recent_deaths(limit=limit)

        DetectionCandidate.query.delete()

        for row in rows:
            db.session.add(
                DetectionCandidate(
                    name=row['name'],
                    slug=row['slug'],
                    death_date=date.fromisoformat(row['death_date']),
                    country=row.get('country'),
                    source_url=row.get('source_url'),
                    imdb_id=row.get('imdb_id'),
                    wikidata_id=row.get('wikidata_id'),
                    popularity_score=row.get('popularity_score', 0),
                )
            )

        db.session.flush()
        return rows

    def _apply_python_filters(self, rows: list[dict]) -> list[dict]:
        allowed_countries = {
            normalize_country_key(country)
            for country in self.settings.countries()
            if normalize_country_key(country)
        }

        if not allowed_countries:
            return rows

        filtered = []
        for row in rows:
            normalized_label = normalize_country_label(row.get('country'))
            country_key = normalize_country_key(normalized_label)

            if not country_key:
                continue

            if country_key not in allowed_countries:
                continue

            row['country'] = normalized_label
            filtered.append(row)

        return filtered


    def _clean_name(self, raw_name: str | None, source_url: str | None) -> str | None:
        if raw_name and not self._looks_like_qid(raw_name):
            return raw_name.strip()

        fallback = self._name_from_source_url(source_url)
        if fallback and not self._looks_like_qid(fallback):
            return fallback

        return None

    def _looks_like_qid(self, value: str | None) -> bool:
        if not value:
            return False
        value = value.strip()
        return len(value) > 1 and value[0] == 'Q' and value[1:].isdigit()

    def _name_from_source_url(self, source_url: str | None) -> str | None:
        if not source_url:
            return None

        try:
            parsed = urlparse(source_url)
            last_part = parsed.path.rsplit('/', 1)[-1]
            if not last_part:
                return None

            cleaned = unquote(last_part).replace('_', ' ').strip()
            return cleaned or None

        except Exception:
            return None

    def _country_filter(self, countries: list[str]) -> str:
        if not countries:
            return ''

        values = ', '.join(f'"{country.lower()}"' for country in countries)
        return f'''
        FILTER(
          !BOUND(?countryLabel) ||
          LCASE(STR(?countryLabel)) IN ({values})
        )
        '''

    def _profession_filter(self, professions: list[str]) -> str:
        if not professions:
            return ''

        profession_filters = []
        for profession in professions:
            profession_filters.append(
                f'EXISTS {{ ?person p:P106/ps:P106 ?occupation . ?occupation rdfs:label "{profession}"@en. }}'
            )

        return f'FILTER({" || ".join(profession_filters)})'