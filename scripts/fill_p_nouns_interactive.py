#!/usr/bin/env python3
"""Interactive helper to fill P-noun translations with manual review."""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Dict, List

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.add_pronunciation_column import pronounce_word
from scripts.fill_p_noun_translations import (
    DATA_DIR,
    derive_base_gloss,
    format_translation,
    parse_feats,
)


def iter_sentences(row: Dict[str, str]) -> List[str]:
    sentences = []
    for idx in range(1, 4):
        it = (row.get(f"sentence_{idx}_it") or "").strip()
        en = (row.get(f"sentence_{idx}_en") or "").strip()
        if it or en:
            sentences.append(f"IT{idx}: {it}\nEN{idx}: {en}".strip())
    return sentences


def prompt(text: str, suggestion: str, sentences: List[str]) -> str:
    print("\n===", text, "===")
    if sentences:
        print("\n".join(sentences))
    prompt_text = f"translation [{suggestion}]: " if suggestion else "translation: "
    answer = input(prompt_text).strip()
    return answer or suggestion


def process_file(path: Path) -> None:
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    if not rows:
        return
    base_gloss = derive_base_gloss(rows)
    has_masc = any('Gender=Masc' in (row.get('feats') or '') for row in rows)
    has_fem = any('Gender=Fem' in (row.get('feats') or '') for row in rows)
    gender_notes = has_masc and has_fem
    has_singular = any('Number=Plur' not in (row.get('feats') or '') for row in rows)
    for row in rows:
        feats = parse_feats(row.get('feats', ''))
        suggestion = format_translation(base_gloss, feats, gender_notes, has_singular)
        if (row.get('translation_en') or '').strip():
            continue
        sentences = iter_sentences(row)
        translation = prompt(row.get('text', ''), suggestion, sentences)
        row['translation_en'] = translation
        row['pronunciation'] = pronounce_word(row.get('text', ''))
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    for entry in sorted(DATA_DIR.iterdir()):
        if entry.suffix != '.tsv' or not entry.name.lower().startswith('p'):
            continue
        process_file(entry)


if __name__ == '__main__':
    main()
