#!/usr/bin/env python3
"""Fill translation_en and pronunciation for NOUN TSVs starting with M."""
from pathlib import Path
import json

import pandas as pd

from utils.add_pronunciation_column import pronounce_word

DATA_DIR = Path('vocabulary/dataframes/NOUN')
DATA_FILE = Path('m_noun_translations.json')

with DATA_FILE.open() as f:
    TEXT_TRANSLATIONS = json.load(f)


def fill_file(path: Path) -> None:
    df = pd.read_csv(path, sep='\t')
    updated = False
    for idx, row in df.iterrows():
        text = row['text']
        translation = (row.get('translation_en') or '')
        translation = translation.strip() if isinstance(translation, str) else ''
        if not translation:
            if text not in TEXT_TRANSLATIONS:
                raise KeyError(f"Missing translation for {text} in {path}")
            df.at[idx, 'translation_en'] = TEXT_TRANSLATIONS[text]
            updated = True
        pron = (row.get('pronunciation') or '')
        pron = pron.strip() if isinstance(pron, str) else ''
        if not pron:
            df.at[idx, 'pronunciation'] = pronounce_word(text)
            updated = True
    if df['translation_en'].isna().any() or (df['translation_en'].astype(str).str.strip() == '').any():
        raise ValueError(f"Translation still missing in {path}")
    if df['pronunciation'].isna().any() or (df['pronunciation'].astype(str).str.strip() == '').any():
        raise ValueError(f"Pronunciation still missing in {path}")
    if updated:
        df.to_csv(path, sep='\t', index=False)


def main():
    for path in sorted(DATA_DIR.glob('m*.tsv')):
        fill_file(path)


if __name__ == '__main__':
    main()
