from __future__ import annotations

import json
import re

import requests
from requests import HTTPError, RequestException

from ..extensions import db
from ..models import ArrServer, AppLog


class ArrService:
    def __init__(self, arr_server: ArrServer):
        self.arr_server = arr_server
        self.base_url = arr_server.base_url.rstrip('/')
        self.headers = {'X-Api-Key': arr_server.api_key}

    def _log(
        self,
        level: str,
        message: str,
        *,
        details: str | None = None,
        related_type: str | None = 'arr_server',
        related_id: int | None = None,
    ) -> None:
        try:
            entry = AppLog(
                level=level,
                source='arr',
                message=message,
                details=details,
                related_type=related_type,
                related_id=related_id if related_id is not None else self.arr_server.id,
            )
            db.session.add(entry)
            db.session.commit()
        except Exception:
            db.session.rollback()

    def _safe_json_dumps(self, value) -> str | None:
        if value is None:
            return None
        try:
            return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
        except Exception:
            return str(value)

    def _normalize_title(self, value: str | None) -> str:
        if not value:
            return ''
        normalized = value.casefold().strip()
        normalized = re.sub(r'[^a-z0-9]+', ' ', normalized)
        return ' '.join(normalized.split())

    def _pick_title_match(
        self,
        items: list[dict],
        *,
        title: str | None = None,
        year: int | None = None,
    ) -> dict | None:
        normalized_title = self._normalize_title(title)

        if not normalized_title:
            return None

        exact_year_matches: list[dict] = []
        loose_matches: list[dict] = []

        for item in items:
            item_title = self._normalize_title(item.get('title'))
            if item_title != normalized_title:
                continue

            item_year = item.get('year')

            if year is not None and item_year is not None:
                if item_year == year:
                    exact_year_matches.append(item)
                continue

            loose_matches.append(item)

        if exact_year_matches:
            return exact_year_matches[0]

        if len(loose_matches) == 1:
            return loose_matches[0]

        return None

    def _get(self, path: str, params: dict | None = None) -> list | dict:
        url = f'{self.base_url}{path}'
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            self._log(
                'error',
                f'GET {path} failed on {self.arr_server.name}.',
                details=self._build_error_details(exc, method='GET', path=path, payload=params),
            )
            raise

    def _post(self, path: str, payload: dict) -> dict:
        url = f'{self.base_url}{path}'
        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            self._log(
                'info',
                f'POST {path} succeeded on {self.arr_server.name}.',
                details=self._safe_json_dumps({
                    'request': payload,
                    'response': data,
                }),
            )
            return data
        except Exception as exc:
            self._log(
                'error',
                f'POST {path} failed on {self.arr_server.name}.',
                details=self._build_error_details(exc, method='POST', path=path, payload=payload),
            )
            raise

    def _build_error_details(
        self,
        exc: Exception,
        *,
        method: str,
        path: str,
        payload: dict | None = None,
    ) -> str:
        details = {
            'server': self.arr_server.name,
            'kind': self.arr_server.kind,
            'method': method,
            'path': path,
            'base_url': self.base_url,
            'payload': payload,
            'error': self._extract_error_message(exc),
        }

        if isinstance(exc, HTTPError) and exc.response is not None:
            details['status_code'] = exc.response.status_code
            details['response_text'] = (exc.response.text or '').strip()[:5000]

            try:
                details['response_json'] = exc.response.json()
            except Exception:
                pass
        elif isinstance(exc, RequestException):
            details['request_exception'] = exc.__class__.__name__

        return self._safe_json_dumps(details) or str(exc)

    def system_status(self) -> dict:
        return self._get('/api/v3/system/status')

    def root_folders(self) -> list[dict]:
        data = self._get('/api/v3/rootfolder')
        return data if isinstance(data, list) else []

    def quality_profiles(self) -> list[dict]:
        data = self._get('/api/v3/qualityprofile')
        return data if isinstance(data, list) else []

    def language_profiles(self) -> list[dict]:
        if self.arr_server.kind != 'sonarr':
            return []
        data = self._get('/api/v3/languageprofile')
        return data if isinstance(data, list) else []

    def test_and_discover(self) -> dict:
        try:
            status = self.system_status()
            roots = self.root_folders()
            quality_profiles = self.quality_profiles()
            language_profiles = self.language_profiles()

            checks = [
                f'System: {status.get("appName") or self.arr_server.kind.title()}',
                f'Root folders found: {len(roots)}',
                f'Quality profiles found: {len(quality_profiles)}',
            ]

            if self.arr_server.kind == 'sonarr':
                checks.append(f'Language profiles found: {len(language_profiles)}')

            if not roots:
                checks.append('Warning: no root folder returned by Arr.')
            if not quality_profiles:
                checks.append('Warning: no quality profile returned by Arr.')

            message = 'Connection successful.'
            if not roots or not quality_profiles:
                message = 'Connection OK, but discovery is incomplete.'

            self._log(
                'info',
                f'Test & Fill succeeded on {self.arr_server.name}.',
                details=self._safe_json_dumps({
                    'system': status,
                    'root_folder_count': len(roots),
                    'quality_profile_count': len(quality_profiles),
                    'language_profile_count': len(language_profiles),
                    'checks': checks,
                }),
            )

            return {
                'ok': True,
                'message': message,
                'system': status,
                'root_folders': roots,
                'quality_profiles': quality_profiles,
                'language_profiles': language_profiles,
                'checks': checks,
            }
        except Exception as exc:
            error_message = self._extract_error_message(exc)
            return {
                'ok': False,
                'message': error_message,
                'system': None,
                'root_folders': [],
                'quality_profiles': [],
                'language_profiles': [],
                'checks': [
                    f'Connection failed: {error_message}',
                ],
            }

    def _extract_error_message(self, exc: Exception) -> str:
        if isinstance(exc, HTTPError) and exc.response is not None:
            status_code = exc.response.status_code

            try:
                data = exc.response.json()
                if isinstance(data, dict):
                    message = data.get('message') or data.get('errorMessage')
                    if message:
                        return f'HTTP {status_code}: {message}'
            except Exception:
                pass

            text = (exc.response.text or '').strip()
            if text:
                return f'HTTP {status_code}: {text}'

            return f'HTTP {status_code} error'

        if isinstance(exc, RequestException):
            return f'{exc.__class__.__name__}: {exc}'

        return str(exc)

    def list_movies(self) -> list[dict]:
        data = self._get('/api/v3/movie')
        return data if isinstance(data, list) else []

    def list_series(self) -> list[dict]:
        data = self._get('/api/v3/series')
        return data if isinstance(data, list) else []

    def lookup_series(self, *, tvdb_id: int | None = None, tmdb_id: int | None = None, term: str | None = None) -> list[dict]:
        params = {}
        if tvdb_id:
            params['term'] = f'tvdb:{tvdb_id}'
        elif tmdb_id:
            params['term'] = f'tmdb:{tmdb_id}'
        elif term:
            params['term'] = term
        else:
            return []

        data = self._get('/api/v3/series/lookup', params=params)
        return data if isinstance(data, list) else []


    def _pick_lookup_series(
        self,
        results: list[dict],
        *,
        tvdb_id: int | None = None,
        tmdb_id: int | None = None,
        title: str | None = None,
        year: int | None = None,
    ) -> dict | None:
        for item in results:
            if tvdb_id and item.get('tvdbId') == tvdb_id:
                return item
            if tmdb_id and item.get('tmdbId') == tmdb_id:
                return item

        return self._pick_title_match(
            results,
            title=title,
            year=year,
        )


    def build_series_payload_from_lookup(
        self,
        lookup_item: dict,
    ) -> dict:
        payload = dict(lookup_item)

        payload['qualityProfileId'] = self.arr_server.quality_profile_id
        payload['rootFolderPath'] = self.arr_server.root_folder
        payload['monitored'] = True
        payload['addOptions'] = {
            'searchForMissingEpisodes': self.arr_server.search_on_add,
        }

        if self.arr_server.language_profile_id is not None:
            payload['languageProfileId'] = self.arr_server.language_profile_id
        else:
            payload.pop('languageProfileId', None)

        return payload

    def find_existing_movie(
        self,
        *,
        tmdb_id: int | None = None,
        title: str | None = None,
        year: int | None = None,
    ) -> dict | None:
        movies = self.list_movies()

        for item in movies:
            if tmdb_id and item.get('tmdbId') == tmdb_id:
                return item

        return self._pick_title_match(
            movies,
            title=title,
            year=year,
        )

    def find_existing_series(
        self,
        *,
        tvdb_id: int | None = None,
        tmdb_id: int | None = None,
        title: str | None = None,
        year: int | None = None,
    ) -> dict | None:
        series = self.list_series()

        for item in series:
            if tvdb_id and item.get('tvdbId') == tvdb_id:
                return item

            if tmdb_id and item.get('tmdbId') == tmdb_id:
                return item

        return self._pick_title_match(
            series,
            title=title,
            year=year,
        )

    def build_movie_payload(self, title: str, tmdb_id: int, year: int | None = None) -> dict:
        payload = {
            'title': title,
            'qualityProfileId': self.arr_server.quality_profile_id,
            'rootFolderPath': self.arr_server.root_folder,
            'tmdbId': tmdb_id,
            'monitored': True,
            'addOptions': {'searchForMovie': self.arr_server.search_on_add},
        }

        if isinstance(year, int):
            payload['year'] = year

        return payload

    def build_series_payload(
        self,
        title: str,
        tvdb_id: int | None,
        tmdb_id: int | None = None,
        year: int | None = None,
    ) -> dict:
        payload = {
            'title': title,
            'qualityProfileId': self.arr_server.quality_profile_id,
            'languageProfileId': self.arr_server.language_profile_id,
            'rootFolderPath': self.arr_server.root_folder,
            'monitored': True,
            'addOptions': {'searchForMissingEpisodes': self.arr_server.search_on_add},
        }

        if isinstance(year, int):
            payload['year'] = year

        if tvdb_id:
            payload['tvdbId'] = tvdb_id

        if tmdb_id:
            payload['tmdbId'] = tmdb_id

        return payload

    def add_movie(self, title: str, tmdb_id: int, year: int | None = None) -> dict:
        payload = self.build_movie_payload(title=title, tmdb_id=tmdb_id, year=year)
        return self._post('/api/v3/movie', payload)

    def add_series(
        self,
        title: str,
        tvdb_id: int | None,
        tmdb_id: int | None = None,
        year: int | None = None,
    ) -> dict:
        payload = self.build_series_payload(title=title, tvdb_id=tvdb_id, tmdb_id=tmdb_id, year=year)
        return self._post('/api/v3/series', payload)

    def ensure_movie(self, *, title: str, tmdb_id: int | None, year: int | None = None) -> dict:
        if not tmdb_id:
            return {
                'status': 'invalid',
                'message': 'Missing TMDb ID for movie.',
                'item': None,
                'tmdb_id': None,
                'tvdb_id': None,
                'request_payload': None,
                'response_payload': None,
            }

        existing = self.find_existing_movie(
            tmdb_id=tmdb_id,
            title=title,
            year=year,
        )
        if existing:
            return {
                'status': 'already_exists',
                'message': f'"{existing.get("title") or title}" already exists in {self.arr_server.name}.',
                'item': existing,
                'tmdb_id': tmdb_id,
                'tvdb_id': None,
                'request_payload': None,
                'response_payload': self._safe_json_dumps(existing),
            }

        payload = self.build_movie_payload(title=title, tmdb_id=tmdb_id, year=year)

        try:
            created = self._post('/api/v3/movie', payload)
            return {
                'status': 'created',
                'message': f'"{created.get("title") or title}" added to {self.arr_server.name}.',
                'item': created,
                'tmdb_id': tmdb_id,
                'tvdb_id': None,
                'request_payload': self._safe_json_dumps(payload),
                'response_payload': self._safe_json_dumps(created),
            }
        except Exception as exc:
            return {
                'status': 'error',
                'message': self._extract_error_message(exc),
                'item': None,
                'tmdb_id': tmdb_id,
                'tvdb_id': None,
                'request_payload': self._safe_json_dumps(payload),
                'response_payload': self._build_error_details(exc, method='POST', path='/api/v3/movie', payload=payload),
            }

    def ensure_series(
        self,
        *,
        title: str,
        tvdb_id: int | None = None,
        tmdb_id: int | None = None,
        year: int | None = None,
    ) -> dict:
        if not tvdb_id and not tmdb_id:
            return {
                'status': 'invalid',
                'message': 'Missing TVDb ID / TMDb ID for series.',
                'item': None,
                'tmdb_id': None,
                'tvdb_id': None,
                'request_payload': None,
                'response_payload': None,
            }

        existing = self.find_existing_series(
            tvdb_id=tvdb_id,
            tmdb_id=tmdb_id,
            title=title,
            year=year,
        )
        if existing:
            return {
                'status': 'already_exists',
                'message': f'"{existing.get("title") or title}" already exists in {self.arr_server.name}.',
                'item': existing,
                'tmdb_id': tmdb_id,
                'tvdb_id': tvdb_id,
                'request_payload': None,
                'response_payload': self._safe_json_dumps(existing),
            }

        lookup_results: list[dict] = []
        lookup_error_details: str | None = None

        try:
            if tvdb_id:
                lookup_results = self.lookup_series(tvdb_id=tvdb_id)
            elif tmdb_id:
                lookup_results = self.lookup_series(tmdb_id=tmdb_id)

            if not lookup_results and title:
                lookup_results = self.lookup_series(term=title)
        except Exception as exc:
            lookup_error_details = self._build_error_details(
                exc,
                method='GET',
                path='/api/v3/series/lookup',
                payload={
                    'tvdb_id': tvdb_id,
                    'tmdb_id': tmdb_id,
                    'title': title,
                    'year': year,
                },
            )

        lookup_item = self._pick_lookup_series(
            lookup_results,
            tvdb_id=tvdb_id,
            tmdb_id=tmdb_id,
            title=title,
            year=year,
        )

        if not lookup_item:
            return {
                'status': 'invalid',
                'message': f'Unable to resolve "{title}" from Sonarr lookup.',
                'item': None,
                'tmdb_id': tmdb_id,
                'tvdb_id': tvdb_id,
                'request_payload': self._safe_json_dumps({
                    'tvdb_id': tvdb_id,
                    'tmdb_id': tmdb_id,
                    'title': title,
                    'year': year,
                }),
                'response_payload': lookup_error_details or self._safe_json_dumps(lookup_results),
            }

        resolved_tvdb_id = lookup_item.get('tvdbId') or tvdb_id
        resolved_tmdb_id = lookup_item.get('tmdbId') or tmdb_id

        existing = self.find_existing_series(
            tvdb_id=resolved_tvdb_id,
            tmdb_id=resolved_tmdb_id,
            title=lookup_item.get('title') or title,
            year=lookup_item.get('year') or year,
        )
        if existing:
            return {
                'status': 'already_exists',
                'message': f'"{existing.get("title") or title}" already exists in {self.arr_server.name}.',
                'item': existing,
                'tmdb_id': resolved_tmdb_id,
                'tvdb_id': resolved_tvdb_id,
                'request_payload': None,
                'response_payload': self._safe_json_dumps(existing),
            }

        payload = self.build_series_payload_from_lookup(lookup_item)

        try:
            created = self._post('/api/v3/series', payload)
            return {
                'status': 'created',
                'message': f'"{created.get("title") or title}" added to {self.arr_server.name}.',
                'item': created,
                'tmdb_id': resolved_tmdb_id,
                'tvdb_id': resolved_tvdb_id,
                'request_payload': self._safe_json_dumps(payload),
                'response_payload': self._safe_json_dumps(created),
            }
        except Exception as exc:
            return {
                'status': 'error',
                'message': self._extract_error_message(exc),
                'item': None,
                'tmdb_id': resolved_tmdb_id,
                'tvdb_id': resolved_tvdb_id,
                'request_payload': self._safe_json_dumps(payload),
                'response_payload': self._build_error_details(exc, method='POST', path='/api/v3/series', payload=payload),
            }