import unicodedata
from typing import Optional, Tuple

def _normalize(s: str) -> str:
    """Lowercase, strip accents/spaces for fair comparisons."""
    if s is None:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s

def _similarity(a: str, b: str) -> float:
    """
    Return similarity in [0,1].
    Tries rapidfuzz if available, else falls back to difflib.
    """
    a, b = _normalize(a), _normalize(b)
    try:
        from rapidfuzz.fuzz import ratio  # optional, but faster/better
        return ratio(a, b) / 100.0
    except Exception:
        from difflib import SequenceMatcher
        return SequenceMatcher(None, a, b).ratio()

def find_translation(
    df,
    query: str,
    min_similarity: float = 0.6,
    query_column: str = "term_italian",
    translation_column: str = "translation_english",
) -> Optional[Tuple[str, str | list[str]]]:
    """
    Given a DataFrame with columns 'term_italian' and 'translation_english',
    Return (best_italian_term, translation_english_or_list) using fuzzy matching.
    - Exact match (case/accents-insensitive) is preferred.
    - Otherwise consider all candidates and compute similarity.
    - If multiple candidates share the top similarity >= min_similarity, return a list
      of their English definitions as the second tuple item. The first item is still a
      single best Italian term chosen by tie-breakers:
        1) terms that start with the query (normalized),
        2) then shorter Italian terms (fewer chars),
        3) then the first occurrence.
      If there is no tie, the second item is a single string.
    Returns None if nothing clears the threshold.
    """
    required = {query_column, translation_column}
    if not required.issubset(df.columns):
        raise ValueError(f"DataFrame must have '{query_column}' and '{translation_column}' columns")

    if df.empty:
        return None

    q_norm = _normalize(query)

    # 1) Exact match preference
    exact_mask = df[query_column].astype(str).apply(lambda s: _normalize(s) == q_norm)
    if exact_mask.any():
        matches = df[exact_mask]
        it_value = str(matches.iloc[0][query_column])
        # Collect all exact-match translations (deduplicated, order preserved)
        seen = set()
        translations: list[str] = []
        for en in matches[translation_column].astype(str).tolist():
            if en not in seen:
                seen.add(en)
                translations.append(en)
        if len(translations) == 1:
            return it_value, translations[0]
        return it_value, translations

    # 2) Fuzzy match across all candidates
    candidates = []
    for idx, row in df.iterrows():
        it = str(row[query_column])
        en = str(row[translation_column])
        score = _similarity(query, it)
        starts = _normalize(it).startswith(q_norm)
        candidates.append((score, starts, len(it), idx, it, en))

    # Determine the best score among candidates
    if not candidates:
        return None

    best_score = max(candidates, key=lambda t: t[0])[0]
    if best_score < min_similarity:
        return None

    # Consider all ties at the best score (within tiny epsilon for float safety)
    eps = 1e-12
    tied = [c for c in candidates if abs(c[0] - best_score) <= eps]

    # Choose a single best Italian term using tie-breakers
    tied.sort(key=lambda t: (-int(t[1]), t[2], t[3]))  # startswith desc, length asc, index asc
    best_it = tied[0][4]

    # Collect tied English definitions (preserve order, de-duplicate)
    seen = set()
    tied_definitions: list[str] = []
    for c in tied:
        en = c[5]
        if en not in seen:
            seen.add(en)
            tied_definitions.append(en)

    if len(tied_definitions) == 1:
        return best_it, [tied_definitions[0]]
    return best_it, tied_definitions
