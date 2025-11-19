#!/usr/bin/env python3
"""Interactive helper to build m_noun_translations.json."""
import json
from pathlib import Path

import pandas as pd

DATA_DIR = Path('vocabulary/dataframes/NOUN')
OUTPUT = Path('m_noun_translations.json')


def load_existing():
    if OUTPUT.exists():
        with OUTPUT.open() as f:
            return json.load(f)
    return {}


def rows_needing_translations():
    rows = {}
    for path in sorted(DATA_DIR.glob('m*.tsv')):
        df = pd.read_csv(path, sep='\t')
        for _, row in df.iterrows():
            translation = '' if pd.isna(row.get('translation_en')) else str(row.get('translation_en')).strip()
            pron = '' if pd.isna(row.get('pronunciation')) else str(row.get('pronunciation')).strip()
            if translation and pron:
                continue
            text = row['text']
            if text not in rows:
                sentences = []
                for i in range(1, 4):
                    it = row.get(f'sentence_{i}_it', '')
                    en = row.get(f'sentence_{i}_en', '')
                    if isinstance(it, float):
                        it = ''
                    if isinstance(en, float):
                        en = ''
                    sentences.append((it, en))
                rows[text] = {
                    'path': str(path),
                    'lemma': row.get('lemma', ''),
                    'feats': row.get('feats', ''),
                    'sentences': sentences,
                }
    return rows


def main():
    existing = load_existing()
    rows = rows_needing_translations()
    items = sorted(rows.items())
    for text, info in items:
        if text in existing and existing[text]:
            continue
        print('\n' + '=' * 40)
        print(f"Text: {text}")
        print(f"Path: {info['path']}")
        lemma = info.get('lemma', '')
        if lemma:
            print(f"Lemma: {lemma}")
        feats = info.get('feats', '')
        if isinstance(feats, str) and feats:
            print(f"Features: {feats}")
        for idx, (it, en) in enumerate(info['sentences'], 1):
            if not it and not en:
                continue
            print(f"Sentence {idx} IT: {it}")
            print(f"Sentence {idx} EN: {en}")
        while True:
            translation = input('Translation: ').strip()
            if translation:
                break
            print('Translation cannot be empty.')
        existing[text] = translation
        with OUTPUT.open('w') as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f"Saved translation for {text}.")
    print('All translations collected.')


if __name__ == '__main__':
    main()
