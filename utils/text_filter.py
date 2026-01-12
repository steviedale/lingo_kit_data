"""Utility functions for detecting inappropriate language in sentences."""

from __future__ import annotations

import re
from typing import Dict, Iterable, Optional, Pattern, Tuple

Category = str
_CategoryPatterns = Dict[Category, Iterable[Pattern[str]]]


def _compile_patterns(raw_patterns: Dict[Category, Iterable[str]]) -> _CategoryPatterns:
    """Compile the provided regex patterns with case-insensitive matching."""

    compiled: Dict[Category, Iterable[Pattern[str]]] = {}
    for category, patterns in raw_patterns.items():
        compiled[category] = tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)
    return compiled


_RAW_PATTERNS: Dict[Category, Iterable[str]] = {
    "swearing": (
        r"\bfuck(?:ing|er|ed|s)?\b",
        r"\bshit(?:ty|s|head)?\b",
        r"\bdamn\b",
        r"\bdick\b",
        r"\basshole\b",
        r"\bbitch(?:es)?\b",
        r"\bcrap\b",
    ),
    "sexual content": (
        r"\bsex(?:ual|y|ing)?\b",
        r"\bpenis\b",
        r"\bvagina\b",
        r"\bclitoris\b",
        r"\bsemen\b",
        r"\borgasm\b",
        r"\banal\b",
        r"\bbreasts?\b",
        r"\bbdsm\b",
    ),
    "hate speech": (
        r"\bslut\b",
        r"\bwhore\b",
        r"\bretard(?:ed)?\b",
        r"\bfagg?ot\b",
        r"\bnigg(?:a|er)\b",
        r"\bchink\b",
        r"\bspic\b",
        r"\bkike\b",
        r"\bwetback\b",
    ),
    "violence": (
        r"\bkill(?:ing|ed|s)?\b",
        r"\bmurder(?:ing|ed|s)?\b",
        r"\bshoot(?:ing|s)?\b",
        r"\bstab(?:bing|s)?\b",
        r"\bslaughter\b",
    ),
    "drugs": (
        r"\bheroin\b",
        r"\bcocaine\b",
        r"\bmeth(?:amphetamine)?\b",
        r"\bmarijuana\b",
        r"\bweed\b",
        r"\bcrack\b",
        r"\bopioid\b",
    ),
    "self-harm": (
        r"\bsuicide\b",
        r"\bself-?harm\b",
        r"\bkill myself\b",
        r"\bcutting\b",
    ),
}

_CATEGORY_PATTERNS = _compile_patterns(_RAW_PATTERNS)


def classify_sentence(sentence: str) -> Tuple[bool, Optional[Category]]:
    """Classify a sentence as appropriate or inappropriate.

    Args:
        sentence: Sentence to classify.

    Returns:
        Tuple of (is_appropriate, category). ``is_appropriate`` is ``True`` when no
        inappropriate language is detected. When inappropriate content is found,
        ``is_appropriate`` is ``False`` and ``category`` contains the category name
        that triggered the detection.
    """

    if not sentence:
        return True, None

    for category, patterns in _CATEGORY_PATTERNS.items():
        for pattern in patterns:
            if pattern.search(sentence):
                return False, category

    return True, None


__all__ = ["classify_sentence"]
