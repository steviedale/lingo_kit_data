#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word


def iter_sentences(row):
    for idx in range(1, 4):
        it = (row.get(f"sentence_{idx}_it") or "").strip()
        en = (row.get(f"sentence_{idx}_en") or "").strip()
        if it or en:
            yield idx, it, en


def prompt(row, path):
    print("\n---")
    print(f"File: {path}")
    print(f"Text: {row.get('text', '')}")
    lemma = (row.get('lemma') or '').strip()
    feats = (row.get('feats') or '').strip()
    if lemma:
        print(f"Lemma: {lemma}")
    if feats:
        print(f"Features: {feats}")
    for idx, it, en in iter_sentences(row):
        if it:
            print(f"IT{idx}: {it}")
        if en:
            print(f"EN{idx}: {en}")
    while True:
        translation = input("translation> ").strip()
        if translation:
            return translation


def main():
    root = Path('vocabulary/dataframes/NOUN')
    files = sorted(root.glob('g*.tsv'))
    for path in files:
        with path.open(encoding='utf-8') as handle:
            reader = csv.DictReader(handle, delimiter='\t')
            rows = list(reader)
            fieldnames = reader.fieldnames or []
        if not fieldnames:
            continue
        changed = False
        for row in rows:
            translation = (row.get('translation_en') or '').strip()
            pronunciation = (row.get('pronunciation') or '').strip()
            if translation and pronunciation:
                continue
            translation = prompt(row, path)
            row['translation_en'] = translation
            row['pronunciation'] = pronounce_word(row.get('text', ''))
            changed = True
        if changed:
            with path.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
                writer.writeheader()
                writer.writerows(rows)


if __name__ == '__main__':
    main()
