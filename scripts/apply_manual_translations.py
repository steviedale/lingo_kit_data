from pathlib import Path
import csv

BASE = Path('vocabulary/n_grams/trigrams')
MANUAL = Path('notes/manual_translations')

CHUNKS = {
    'chunk_048.tsv': 'chunk_048.trans',
    'chunk_049.tsv': 'chunk_049.trans',
    'chunk_050.tsv': 'chunk_050.trans',
    'chunk_051.tsv': 'chunk_051.trans',
    'chunk_052.tsv': 'chunk_052.trans',
}

def load_translations(name):
    path = MANUAL / name
    mapping = {}
    with path.open() as f:
        for line in f:
            line = line.rstrip('\n')
            if not line:
                continue
            idx = int(line[:3])
            text = line[3:].strip()
            if not text:
                raise SystemExit(f'Missing translation text for index {idx} in {name}')
            mapping[idx] = text
    return mapping

def apply_translations(tsv_name, translations):
    path = BASE / tsv_name
    rows = []
    with path.open() as f:
        reader = csv.reader(f, delimiter='\t')
        rows = list(reader)
    header = rows[0]
    changed = 0
    for idx, row in enumerate(rows[1:], 1):
        if row[-1].strip():
            continue
        try:
            row[-1] = translations[idx]
        except KeyError as exc:
            raise SystemExit(f'Missing translation for row {idx} in {tsv_name}') from exc
        changed += 1
    if changed != len(translations):
        raise SystemExit(
            f'Translation count mismatch for {tsv_name}: '
            f'used {changed} but expected {len(translations)}'
        )
    with path.open('w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerows(rows)

def main():
    for tsv_name, trans_name in CHUNKS.items():
        translations = load_translations(trans_name)
        apply_translations(tsv_name, translations)

if __name__ == '__main__':
    main()
