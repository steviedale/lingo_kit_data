from pathlib import Path
import csv

def fill_file(path, translations):
    rows=[]
    with path.open() as f:
        reader=csv.reader(f, delimiter='\t')
        rows=list(reader)
    idx=0
    for row in rows[1:]:
        if not row[-1].strip():
            try:
                row[-1]=translations[idx]
            except IndexError as exc:
                raise SystemExit(f"Missing translations for {path}") from exc
            idx+=1
    if idx!=len(translations):
        raise SystemExit(f"Unused translations for {path}: {len(translations)-idx}")
    with path.open('w') as f:
        writer=csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerows(rows)

def read_translations(path):
    with path.open() as f:
        lines=[line.rstrip('\n') for line in f]
    return [line for line in lines if line.strip()]

def main():
    base=Path('vocabulary/n_grams/trigrams')
    data={
        'chunk_006.tsv': Path('chunk_006.trans'),
        'chunk_007.tsv': Path('chunk_007.trans'),
        'chunk_008.tsv': Path('chunk_008.trans'),
    }
    for filename, source in data.items():
        translations=read_translations(source)
        fill_file(base/filename, translations)

if __name__=='__main__':
    main()
