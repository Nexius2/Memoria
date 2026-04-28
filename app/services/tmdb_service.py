from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher

import requests


class TmdbService:
    BASE = 'https://api.themoviedb.org/3'

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._external_ids_cache: dict[tuple[str, int], dict] = {}

    def _get(self, path: str, params: dict | None = None):
        params = params or {}
        params['api_key'] = self.api_key
        response = requests.get(f'{self.BASE}{path}', params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def _normalize_name(self, value: str | None) -> str:
        if not value:
            return ''

        normalized = unicodedata.normalize('NFKD', value)
        normalized = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = normalized.casefold().strip()
        normalized = re.sub(r'[^a-z0-9]+', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized

    def _tokenize_name(self, value: str | None) -> list[str]:
        normalized = self._normalize_name(value)
        if not normalized:
            return []
        return [token for token in normalized.split(' ') if token]

    def _strip_name_particles(self, tokens: list[str]) -> list[str]:
        particles = {
            'de', 'del', 'della', 'di', 'du', 'des',
            'la', 'le', 'les',
            'van', 'von', 'der', 'den',
            'da', 'dos', 'das',
            'el', 'al', 'bin', 'ibn',
        }
        return [token for token in tokens if token not in particles]

    def _build_name_match_variants(self, value: str | None) -> list[str]:
        variants: list[str] = []

        def add(raw: str | None):
            normalized = self._normalize_name(raw)
            if normalized and normalized not in variants:
                variants.append(normalized)

        original = (value or '').strip()
        tokens = self._tokenize_name(original)
        stripped_tokens = self._strip_name_particles(tokens)

        add(original)

        if tokens:
            add(' '.join(tokens))
            add(''.join(tokens))

        if len(tokens) >= 2:
            add(f'{tokens[0]} {tokens[-1]}')
            add(f'{tokens[-1]} {tokens[0]}')

        if len(tokens) >= 3:
            add(f'{tokens[0]} {" ".join(tokens[1:])}')
            add(f'{" ".join(tokens[:-1])} {tokens[-1]}')

        if stripped_tokens and stripped_tokens != tokens:
            add(' '.join(stripped_tokens))
            add(''.join(stripped_tokens))

            if len(stripped_tokens) >= 2:
                add(f'{stripped_tokens[0]} {stripped_tokens[-1]}')
                add(f'{stripped_tokens[-1]} {stripped_tokens[0]}')

        return variants

    def _similarity_ratio(self, left: str, right: str) -> float:
        if not left or not right:
            return 0.0
        return SequenceMatcher(None, left, right).ratio()

    def _extract_year(self, value: str | None) -> int | None:
        if not value:
            return None

        try:
            return datetime.strptime(value[:10], '%Y-%m-%d').year
        except Exception:
            return None

    def _extract_date(self, value: str | None) -> str | None:
        if not value:
            return None

        value = value.strip()
        if not value:
            return None

        try:
            return datetime.strptime(value[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
        except Exception:
            return None

    def _build_search_variants(self, name: str) -> list[str]:
        variants: list[str] = []

        def add(value: str | None):
            value = re.sub(r'\s+', ' ', (value or '').strip())
            if value and value not in variants:
                variants.append(value)

        original = (name or '').strip()
        normalized = self._normalize_name(original)
        tokens = self._tokenize_name(original)
        stripped_tokens = self._strip_name_particles(tokens)

        add(original)
        add(re.sub(r'\([^)]*\)', ' ', original))
        add(re.sub(r'\[[^\]]*\]', ' ', original))
        add(re.sub(r'["“”\'`]+', ' ', original))
        add(re.sub(r'[^A-Za-z0-9À-ÿ ]+', ' ', original))
        add(normalized)

        for variant in self._build_name_match_variants(original):
            add(variant)

        if len(tokens) >= 2:
            add(' '.join(reversed(tokens)))
            add(f'{tokens[0]} {tokens[-1]}')
            add(f'{tokens[-1]} {tokens[0]}')

            if len(tokens) > 2:
                add(' '.join(tokens[:2]))
                add(' '.join(tokens[-2:]))

        if stripped_tokens and stripped_tokens != tokens:
            add(' '.join(stripped_tokens))

            if len(stripped_tokens) >= 2:
                add(f'{stripped_tokens[0]} {stripped_tokens[-1]}')
                add(f'{stripped_tokens[-1]} {stripped_tokens[0]}')

        return variants[:12]

    def _score_name_against_candidate_name(self, query_name: str, candidate_name: str) -> int:
        query_variants = self._build_name_match_variants(query_name)
        candidate_variants = self._build_name_match_variants(candidate_name)

        if not query_variants or not candidate_variants:
            return -10_000

        best_score = -10_000

        for normalized_query in query_variants:
            for normalized_candidate in candidate_variants:
                score = 0

                if normalized_candidate == normalized_query:
                    score += 260
                elif normalized_candidate.startswith(normalized_query):
                    score += 160
                elif normalized_query in normalized_candidate:
                    score += 130
                elif normalized_candidate in normalized_query:
                    score += 90

                query_tokens = set(self._tokenize_name(normalized_query))
                candidate_tokens = set(self._tokenize_name(normalized_candidate))

                if query_tokens and candidate_tokens:
                    common_tokens = query_tokens & candidate_tokens
                    score += len(common_tokens) * 22

                    if query_tokens == candidate_tokens:
                        score += 80
                    elif common_tokens:
                        coverage = len(common_tokens) / max(len(query_tokens), 1)
                        score += int(coverage * 55)

                        extra_tokens = candidate_tokens - query_tokens
                        if extra_tokens:
                            score -= min(len(extra_tokens) * 8, 24)
                    else:
                        score -= 40



                    query_token_list = self._tokenize_name(normalized_query)
                    candidate_token_list = self._tokenize_name(normalized_candidate)

                    if len(query_token_list) >= 2 and len(candidate_token_list) >= 2:
                        if query_token_list[-1] == candidate_token_list[-1]:
                            score += 45

                        if query_token_list[0] == candidate_token_list[0]:
                            score += 18

                        if (
                            query_token_list[0][:1] == candidate_token_list[0][:1]
                            and query_token_list[-1] == candidate_token_list[-1]
                        ):
                            score += 12

                        if (
                            query_token_list[0] == candidate_token_list[-1]
                            and query_token_list[-1] == candidate_token_list[0]
                        ):
                            score += 10

                similarity = self._similarity_ratio(normalized_query, normalized_candidate)
                score += int(similarity * 120)

                if similarity < 0.45:
                    score -= 80
                elif similarity < 0.60:
                    score -= 30

                if score > best_score:
                    best_score = score

        return best_score

    def _score_person_match(
        self,
        query_name: str,
        candidate: dict,
        *,
        death_date: str | None = None,
        details: dict | None = None,
    ) -> tuple[int, float]:
        score = 0
        details = details or {}

        candidate_name = candidate.get('name') or details.get('name') or ''
        score += self._score_name_against_candidate_name(query_name, candidate_name)

        best_alias_score = 0
        aliases = candidate.get('also_known_as') or details.get('also_known_as') or []
        for alias in aliases:
            alias_score = self._score_name_against_candidate_name(query_name, alias)
            if alias_score > best_alias_score:
                best_alias_score = alias_score

        if best_alias_score > 0:
            score += int(best_alias_score * 0.75)

        known_for_department = (
            details.get('known_for_department')
            or candidate.get('known_for_department')
            or ''
        ).strip()

        if known_for_department == 'Acting':
            score += 35
        elif known_for_department in {'Directing', 'Writing', 'Production', 'Creator'}:
            score += 10
        elif known_for_department:
            score -= 15

        popularity = float(candidate.get('popularity') or details.get('popularity') or 0.0)

        if popularity > 20:
            score += 25
        elif popularity > 5:
            score += 10
        elif popularity < 1:
            score -= 30

        expected_death_date = self._extract_date(death_date)
        candidate_death_date = self._extract_date(details.get('deathday'))

        if expected_death_date:
            if candidate_death_date:
                if candidate_death_date == expected_death_date:
                    score += 260
                else:
                    expected_death_year = self._extract_year(expected_death_date)
                    candidate_death_year = self._extract_year(candidate_death_date)

                    if (
                        expected_death_year is not None
                        and candidate_death_year is not None
                        and candidate_death_year == expected_death_year
                    ):
                        score += 120
                    else:
                        score -= 260
            else:
                score -= 40

        known_for_items = candidate.get('known_for') or details.get('known_for') or []
        query_tokens = set(self._tokenize_name(query_name))

        if query_tokens and isinstance(known_for_items, list):
            for item in known_for_items[:3]:
                title = (
                    item.get('title')
                    or item.get('name')
                    or ''
                )
                title_tokens = set(self._tokenize_name(title))

                if title_tokens and (query_tokens & title_tokens):
                    score += 15
                    break

        return (score, popularity)

    def _collect_search_results(self, name: str) -> list[dict]:
        search_variants = self._build_search_variants(name)
        aggregated_results: dict[int, dict] = {}

        for variant in search_variants:
            try:
                data = self._get('/search/person', {'query': variant, 'include_adult': 'false'})
            except Exception:
                continue

            for result in data.get('results') or []:
                candidate_id = result.get('id')
                if not candidate_id:
                    continue

                existing = aggregated_results.get(candidate_id)
                if existing is None:
                    aggregated_results[candidate_id] = result
                    continue

                existing_popularity = float(existing.get('popularity') or 0.0)
                new_popularity = float(result.get('popularity') or 0.0)

                if new_popularity > existing_popularity:
                    aggregated_results[candidate_id] = result

        return list(aggregated_results.values())

    def search_person_candidates(
        self,
        name: str,
        *,
        death_date: str | None = None,
        limit: int = 5,
    ) -> list[dict]:
        results = self._collect_search_results(name)
        if not results:
            return []

        initial_ranked = sorted(
            results,
            key=lambda candidate: self._score_person_match(name, candidate, death_date=death_date),
            reverse=True,
        )

        ranked_candidates: list[dict] = []

        for candidate in initial_ranked[: max(limit, 8)]:
            details = {}
            candidate_id = candidate.get('id')

            if candidate_id:
                try:
                    details = self.person_details(candidate_id)
                except Exception:
                    details = {}

            score_tuple = self._score_person_match(
                name,
                candidate,
                death_date=death_date,
                details=details,
            )

            profile_path = (details.get('profile_path') or candidate.get('profile_path') or '').strip()

            ranked_candidate = dict(candidate)
            ranked_candidate['match_score'] = score_tuple[0]
            ranked_candidate['match_popularity'] = score_tuple[1]
            ranked_candidate['deathday'] = self._extract_date(details.get('deathday'))
            ranked_candidate['known_for_department'] = (
                details.get('known_for_department')
                or candidate.get('known_for_department')
                or ''
            )
            ranked_candidate['also_known_as'] = details.get('also_known_as') or []
            ranked_candidate['profile_image_url'] = (
                f'https://image.tmdb.org/t/p/w185{profile_path}'
                if profile_path else None
            )

            ranked_candidates.append(ranked_candidate)

        ranked_candidates.sort(
            key=lambda item: (
                item.get('match_score') or -10_000,
                item.get('match_popularity') or 0.0,
            ),
            reverse=True,
        )

        return ranked_candidates[: max(limit, 1)]

    def search_person(self, name: str, *, death_date: str | None = None) -> dict | None:
        reranked = self.search_person_candidates(name, death_date=death_date, limit=8)
        if not reranked:
            return None

        best_candidate = reranked[0]
        best_score_value = int(best_candidate.get('match_score') or -10_000)

        second_score_value = None
        if len(reranked) > 1:
            second_score_value = int(reranked[1].get('match_score') or -10_000)

        if best_score_value < 55:
            return None

        if (
            second_score_value is not None
            and not death_date
            and best_score_value < 140
            and (best_score_value - second_score_value) < 8
        ):
            return None

        return best_candidate

    def person_details(self, person_id: int) -> dict:
        return self._get(f'/person/{person_id}')

    def person_profile_image_url(self, person_id: int, size: str = 'w780') -> str | None:
        details = self.person_details(person_id)
        profile_path = (details.get('profile_path') or '').strip()
        if not profile_path:
            return None
        return f'https://image.tmdb.org/t/p/{size}{profile_path}'

    def person_external_ids(self, person_id: int) -> dict:
        return self._get(f'/person/{person_id}/external_ids')

    def media_external_ids(self, media_type: str, media_id: int) -> dict:
        media_type = 'tv' if media_type == 'tv' else 'movie'
        media_id = int(media_id)
        cache_key = (media_type, media_id)

        if cache_key in self._external_ids_cache:
            return self._external_ids_cache[cache_key]

        data = self._get(f'/{media_type}/{media_id}/external_ids')
        self._external_ids_cache[cache_key] = data or {}
        return self._external_ids_cache[cache_key]

    def enrich_credit_external_ids(self, credit: dict) -> dict:
        enriched = dict(credit or {})
        media_type = enriched.get('media_type')
        media_id = enriched.get('id')

        if media_type not in {'movie', 'tv'} or media_id is None:
            return enriched

        try:
            external_ids = self.media_external_ids(media_type, int(media_id))
        except Exception:
            return enriched

        imdb_id = str(external_ids.get('imdb_id') or '').strip()
        if imdb_id and not enriched.get('imdb_id'):
            enriched['imdb_id'] = imdb_id

        tvdb_id = external_ids.get('tvdb_id')
        if tvdb_id not in (None, '') and not enriched.get('tvdb_id'):
            enriched['tvdb_id'] = tvdb_id

        return enriched

    def person_credits(self, person_id: int) -> dict:
        return self._get(f'/person/{person_id}/combined_credits')