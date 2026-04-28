from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from flask import has_request_context, request

SUPPORTED_LANGUAGES = {
    'en': 'English',
    'fr': 'Français',
}


def get_available_languages() -> dict[str, str]:
    return SUPPORTED_LANGUAGES.copy()


@lru_cache(maxsize=8)
def _load_language_catalog(language: str) -> dict:
    lang_dir = Path(__file__).resolve().parent.parent / 'lang'
    lang_path = lang_dir / f'{language}.json'

    if not lang_path.exists():
        return {}

    try:
        return json.loads(lang_path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _resolve_key(catalog: dict, key: str):
    current = catalog

    for part in key.split('.'):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]

    return current


def get_current_language() -> str:
    from ..models import AppSettings

    settings = AppSettings.get_or_create()
    configured_language = (settings.ui_language or 'auto').strip().lower()

    if configured_language in SUPPORTED_LANGUAGES:
        return configured_language

    if has_request_context():
        best_match = request.accept_languages.best_match(list(SUPPORTED_LANGUAGES.keys()))
        if best_match in SUPPORTED_LANGUAGES:
            return best_match

    return 'en'


def translate(key: str, default: str | None = None, **kwargs) -> str:
    language = get_current_language()
    catalog = _load_language_catalog(language)

    value = _resolve_key(catalog, key)

    if not isinstance(value, str):
        fallback_catalog = _load_language_catalog('en')
        value = _resolve_key(fallback_catalog, key)

    if not isinstance(value, str):
        value = default if default is not None else key

    if kwargs:
        try:
            return value.format(**kwargs)
        except Exception:
            return value

    return value