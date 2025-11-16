#!/usr/bin/env python3
"""Verify that all VERB TSV files have translation and pronunciation entries."""
from __future__ import annotations

import csv
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
VERB_DIR = REPO_ROOT / "vocabulary" / "dataframes" / "VERB"


def find_missing_entries() -> list[tuple[pathlib.Path, dict[str, str]]]:
    missing: list[tuple[pathlib.Path, dict[str, str]]] = []
    for path in sorted(VERB_DIR.glob("*.tsv")):
        with path.open(encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                translation = row.get("translation_en", "").strip()
                pronunciation = row.get("pronunciation", "").strip()
                if not translation or not pronunciation:
                    missing.append((path, row))
                    break
    return missing


def main() -> int:
    missing = find_missing_entries()
    if missing:
        print("Found TSV files with missing entries:\n")
        for path, row in missing:
            print(f"- {path.relative_to(REPO_ROOT)}: text='{row.get('text', '')}' translation='{row.get('translation_en', '')}' pronunciation='{row.get('pronunciation', '')}'")
        return 1
    print("All verb TSV files have translation and pronunciation entries.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
