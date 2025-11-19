import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word

DATA_FILE = Path('scripts/data/ci_co_translations.tsv')
TRANSLATIONS = {}
if DATA_FILE.exists():
    with DATA_FILE.open() as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                word, gloss = line.split('|', 1)
            except ValueError:
                raise SystemExit(f"Bad translation line: {line!r}")
            TRANSLATIONS[word.strip()] = gloss.strip()
else:
    raise SystemExit(f"Missing data file: {DATA_FILE}")

ROOT = Path('vocabulary/dataframes/NOUN')
FILES = sorted([p for p in ROOT.glob('*.tsv') if 'ci' <= p.stem.lower() < 'cp'])

missing_words = set()

for path in FILES:
    with path.open(newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        rows = list(reader)
        fieldnames = reader.fieldnames
    changed = False
    for row in rows:
        word = row['text']
        info = TRANSLATIONS.get(word)
        if info:
            if not row['translation_en'].strip():
                row['translation_en'] = info
                changed = True
        else:
            if not row['translation_en'].strip():
                missing_words.add(word)
        if not row['pronunciation'].strip():
            row['pronunciation'] = pronounce_word(word)
            changed = True
    if changed:
        with path.open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)

if missing_words:
    raise SystemExit(f"Missing translations for {len(missing_words)} words: {sorted(missing_words)}")
