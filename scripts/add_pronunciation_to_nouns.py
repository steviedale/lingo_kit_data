"""Populate noun CSVs with a learner-friendly pronunciation column.

The pronunciation that we generate here is intentionally simplified. The
goal is not to provide a perfect IPA transcription but rather a consistent
English-friendly respelling that highlights syllable boundaries and the
stressed syllable. The implementation follows a handful of Italian
phonotactic rules so that the output is usable for learners."""

from __future__ import annotations

import csv
import unicodedata
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[1]
NOUN_DIR = ROOT / "dataframes" / "dataframes_by_pos" / "noun"


def strip_accents(text: str) -> str:
    """Return *text* with accented characters replaced by their base letter."""

    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def is_vowel(ch: str) -> bool:
    """Return True when *ch* is a vowel (including accented vowels)."""

    if not ch:
        return False
    base = strip_accents(ch.lower())
    return base in {"a", "e", "i", "o", "u"}


ALLOWED_ONSETS = {
    "br",
    "bl",
    "cr",
    "cl",
    "dr",
    "fr",
    "fl",
    "gr",
    "gl",
    "gn",
    "pr",
    "pl",
    "tr",
    "vr",
}


def determine_coda_length(cluster: str, is_final: bool) -> int:
    """Return the number of consonants that should stay with the current syllable."""

    if cluster:
        cluster_clean = strip_accents(cluster.lower())
    else:
        cluster_clean = ""

    if not cluster:
        return 0
    if is_final:
        return len(cluster)
    if len(cluster) == 1:
        return 0

    first = cluster_clean[0]
    second = cluster_clean[1] if len(cluster_clean) > 1 else ""

    if first == "s":
        # s + consonant clusters keep the s with the previous syllable
        return 1

    if len(cluster_clean) == 2:
        if cluster_clean in ALLOWED_ONSETS:
            return 0
        if second in {"l", "r"}:
            return 0
        return 1

    # For longer clusters keep everything except the last two consonants.
    return len(cluster) - 2


def syllabify(word: str) -> List[str]:
    """Split *word* into syllables using simplified Italian heuristics."""

    word = unicodedata.normalize("NFC", word)
    syllables: List[str] = []
    idx = 0
    length = len(word)

    while idx < length:
        start = idx

        # onset
        while idx < length and not is_vowel(word[idx]):
            idx += 1

        # nucleus (at least one vowel)
        while idx < length and is_vowel(word[idx]):
            idx += 1

        cluster_start = idx
        while idx < length and not is_vowel(word[idx]):
            idx += 1

        cluster = word[cluster_start:idx]
        keep = determine_coda_length(cluster, idx == length)
        syllables.append(word[start:cluster_start + keep])

        idx = cluster_start + keep

    return [s for s in syllables if s]


def vowel_sound(vowel: str, next_char: str) -> str:
    """Return an English-friendly representation for *vowel* (+ optional *next_char*)."""

    vowel = strip_accents(vowel.lower())
    next_char = strip_accents(next_char.lower()) if next_char else ""

    if vowel == "i" and next_char in {"a", "e", "o", "u"}:
        mapping = {"a": "yah", "e": "yeh", "o": "yoh", "u": "yoo"}
        return mapping[next_char]
    if vowel == "u" and next_char in {"a", "e", "o"}:
        mapping = {"a": "wah", "e": "weh", "o": "woh"}
        return mapping[next_char]
    if vowel == "e" and next_char == "i":
        return "ay"
    if vowel == "a" and next_char == "i":
        return "eye"
    if vowel == "o" and next_char == "i":
        return "oy"
    if vowel == "u" and next_char == "i":
        return "wee"

    base = {
        "a": "ah",
        "e": "eh",
        "i": "ee",
        "o": "oh",
        "u": "oo",
    }
    return base.get(vowel, vowel)


def syllable_to_pronunciation(syllable: str) -> str:
    """Convert a syllable to a simplified pronunciation string."""

    plain = strip_accents(syllable.lower())
    result: List[str] = []
    i = 0
    length = len(plain)

    while i < length:
        remaining = plain[i:]

        if remaining.startswith("sch"):
            result.append("sk")
            i += 3
            continue
        if remaining.startswith("sci") and i + 3 < length and plain[i + 3] in "aou":
            result.append("sh")
            i += 3
            continue
        if remaining.startswith("sc") and i + 2 <= length and (i + 2 == length or plain[i + 2] in "ei"):
            result.append("sh")
            i += 2
            continue
        if remaining.startswith("sc"):
            result.append("sk")
            i += 2
            continue
        if remaining.startswith("gli"):
            result.append("ly")
            i += 3
            continue
        if remaining.startswith("gn"):
            result.append("ny")
            i += 2
            continue
        if remaining.startswith("ch"):
            result.append("k")
            i += 2
            continue
        if remaining.startswith("gh"):
            result.append("g")
            i += 2
            continue
        if remaining.startswith("qu"):
            result.append("kw")
            i += 2
            continue
        if remaining.startswith("gu"):
            if i + 2 < length and plain[i + 2] in "ei":
                result.append("g")
            else:
                result.append("gw")
            i += 2
            continue
        if plain[i] == "c":
            if i + 1 < length and plain[i + 1] == "i":
                result.append("ch")
                i += 2
                continue
            if i + 1 < length and plain[i + 1] in "ei":
                result.append("ch")
                i += 1
                continue
            result.append("k")
            i += 1
            continue
        if plain[i] == "g":
            if i + 1 < length and plain[i + 1] == "i":
                result.append("j")
                i += 2
                continue
            if i + 1 < length and plain[i + 1] in "ei":
                result.append("j")
                i += 1
                continue
            result.append("g")
            i += 1
            continue
        if plain[i] == "z":
            if i + 1 < length and plain[i + 1] == "z":
                result.append("dz")
                i += 2
            else:
                result.append("dz")
                i += 1
            continue
        if plain[i] == "h":
            i += 1
            continue
        if plain[i] in "aeiou":
            next_char = plain[i + 1] if i + 1 < length and plain[i + 1] in "aeiou" else ""
            result.append(vowel_sound(plain[i], next_char))
            i += 1
            if next_char:
                i += 1
            continue

        result.append(plain[i])
        i += 1

    return "".join(result)


def stress_index(syllables: List[str]) -> int:
    """Return the index of the stressed syllable in *syllables*."""

    for idx, syllable in enumerate(syllables):
        for ch in syllable:
            if ch in "àèéìòóù" or unicodedata.combining(ch) != 0:
                return idx

    if not syllables:
        return 0

    if strip_accents(syllables[-1][-1]).lower() in {"a", "e", "i", "o", "u"}:
        return max(len(syllables) - 2, 0)

    return len(syllables) - 1


def pronounce_word(word: str) -> str:
    word = unicodedata.normalize("NFC", word)
    word = word.replace("'", "")
    syllables = syllabify(word)
    if not syllables:
        return word

    accent = stress_index(syllables)

    rendered = []
    for idx, syllable in enumerate(syllables):
        pron = syllable_to_pronunciation(syllable)
        rendered.append(pron.upper() if idx == accent else pron)

    return "-".join(rendered)


def pronounce_term(term: str) -> str:
    parts: List[str] = []
    token = ""
    for ch in term:
        if ch.isalpha() or ch in "àèéìòóù'’":
            token += ch.replace("’", "'")
        else:
            if token:
                parts.append(pronounce_word(token))
                token = ""
            if not ch.isspace():
                parts.append(ch)
    if token:
        parts.append(pronounce_word(token))

    output = ""
    for piece in parts:
        if not output:
            output = piece
            continue
        if piece in {",", ".", ";", ":", "!", "?", "%"}:
            output = output.rstrip() + piece
        else:
            output += " " + piece

    return output


def add_pronunciations_to_csv(path: Path) -> None:
    rows: List[dict] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        if "pronunciation" not in fieldnames:
            fieldnames.append("pronunciation")
        for row in reader:
            term = row.get("term_italian", "")
            row["pronunciation"] = pronounce_term(term)
            rows.append(row)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    for csv_path in sorted(NOUN_DIR.glob("*.csv")):
        add_pronunciations_to_csv(csv_path)


if __name__ == "__main__":
    main()
