from __future__ import annotations

import re
import unicodedata

import pycountry


_SPECIAL_CASES = {
    'england': 'United Kingdom',
    'scotland': 'United Kingdom',
    'wales': 'United Kingdom',
    'northern ireland': 'United Kingdom',
    'great britain': 'United Kingdom',
    'britain': 'United Kingdom',
    'uk': 'United Kingdom',
    'u k': 'United Kingdom',
    'u.k.': 'United Kingdom',
    'gb': 'United Kingdom',
    'g b': 'United Kingdom',
    'uae': 'United Arab Emirates',
    'u a e': 'United Arab Emirates',
    'usa': 'United States',
    'u s a': 'United States',
    'u.s.a.': 'United States',
    'us': 'United States',
    'u s': 'United States',
    'u.s.': 'United States',
    'united states of america': 'United States',
}


def _ascii_fold(value: str) -> str:
    return (
        unicodedata.normalize('NFKD', value)
        .encode('ascii', 'ignore')
        .decode('ascii')
    )


def normalize_country_key(value: str | None) -> str:
    if not value:
        return ''

    normalized = _ascii_fold(value).casefold().strip()
    normalized = re.sub(r'[^a-z0-9]+', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized


def normalize_country_label(value: str | None) -> str | None:
    if not value:
        return None

    raw = value.strip()
    if not raw:
        return None

    direct_key = normalize_country_key(raw)
    if not direct_key:
        return None

    special = _SPECIAL_CASES.get(direct_key)
    if special:
        return special

    try:
        return pycountry.countries.lookup(raw).name
    except LookupError:
        pass

    parts = [part.strip() for part in raw.split(',') if part.strip()]
    for part in reversed(parts):
        part_key = normalize_country_key(part)
        special = _SPECIAL_CASES.get(part_key)
        if special:
            return special

        try:
            return pycountry.countries.lookup(part).name
        except LookupError:
            continue

    return raw


def normalize_countries_csv(value: str | None) -> str:
    if not value:
        return ''

    normalized_items = []
    seen = set()

    for raw_part in value.split(','):
        label = normalize_country_label(raw_part)
        if not label:
            continue

        key = normalize_country_key(label)
        if not key or key in seen:
            continue

        seen.add(key)
        normalized_items.append(label)

    return ','.join(normalized_items)