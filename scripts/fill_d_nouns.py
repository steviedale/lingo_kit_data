#!/usr/bin/env python3
"""Fill translation_en and pronunciation for D nouns."""
from pathlib import Path

import pandas as pd

DATA_FILE = Path('d_translations_data.tsv')


def load_data():
    df = pd.read_csv(DATA_FILE, sep='\t')
    data = {}
    for path, group in df.groupby('path'):
        data[path] = {
            row['text']: (row['pronunciation'], row['translation_en'])
            for _, row in group.iterrows()
        }
    return data


def main():
    data = load_data()
    for path, entries in data.items():
        df = pd.read_csv(path, sep='\t')
        for text, (pron, translation) in entries.items():
            mask = df['text'] == text
            if not mask.any():
                raise SystemExit(f"Missing text {text} in {path}")
            df.loc[mask, 'pronunciation'] = pron
            df.loc[mask, 'translation_en'] = translation
        if df['pronunciation'].isna().any() or (df['pronunciation'].astype(str).str.strip() == '').any():
            raise SystemExit(f"Pronunciation still missing in {path}")
        if df['translation_en'].isna().any() or (df['translation_en'].astype(str).str.strip() == '').any():
            raise SystemExit(f"Translation still missing in {path}")
        df.to_csv(path, sep='\t', index=False)


if __name__ == '__main__':
    main()
