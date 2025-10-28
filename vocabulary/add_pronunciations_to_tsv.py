from __future__ import annotations

import csv
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word


DATAFRAMES_DIR = Path(__file__).resolve().parent / "dataframes"


def update_file(path: Path) -> None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if not fieldnames:
        return

    if "pronunciation" not in fieldnames:
        try:
            insert_index = fieldnames.index("text") + 1
        except ValueError:
            insert_index = len(fieldnames)
        fieldnames = fieldnames[:insert_index] + ["pronunciation"] + fieldnames[insert_index:]

    for row in rows:
        text_value = row.get("text", "") or ""
        row["pronunciation"] = pronounce_word(text_value)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    for path in sorted(DATAFRAMES_DIR.rglob("*.tsv")):
        update_file(path)


if __name__ == "__main__":
    main()
