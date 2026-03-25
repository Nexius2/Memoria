from difflib import SequenceMatcher
import re


def normalize_name(value: str | None) -> str:
    if not value:
        return ''
    value = value.casefold().strip()
    value = re.sub(r'[^a-z0-9]+', ' ', value)
    return ' '.join(value.split())


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()