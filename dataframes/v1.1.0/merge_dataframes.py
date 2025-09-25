#!/usr/bin/env python3
import argparse
import csv
import sys
from pathlib import Path

# ---- Unified schema (fixed column order) ----
UNIFIED_HEADER = [
    "term_italian","is_base","base_lemma_italian","part_of_speech","translation_english",
    "topics","subtype","person_number","gender","plurality","mood","tense",
    "is_comparative","is_superlative","is_compound",
    "article_type","article_italian","added_particle_italian","preposition",
    "notes","example_sentence_italian","example_sentence_english"
]

def normalize_bool(val, default="false"):
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in {"true","t","yes","y","1"}: return "true"
    if s in {"false","f","no","n","0"}: return "false"
    return default

def norm_na(val):
    if val is None: return "n/a"
    s = str(val).strip()
    return s if s != "" else "n/a"

def pick(d, key, default=""):
    # Get and strip without forcing n/a (some fields want empty allowed)
    v = d.get(key, "")
    return v.strip() if isinstance(v, str) else ("" if v is None else str(v))

def detect_pos(header_lower_set):
    # Minimal header fingerprints for each legacy template
    H = header_lower_set
    def has(*cols): return all(c in H for c in cols)

    if has("mood","tense","is_compound","base_term_italian"): return "verb"
    if has("pronoun_kind","person_number","base_word_italian"): return "pronoun"
    if has("preposition_type","base_word_italian") and not has("article_type"): return "preposition"
    if has("article_type","base_word_italian"): return "article"
    if has("determiner_type","base_word_italian"): return "determiner"
    if has("conjunction_type","base_word_italian") or has("conjunction_type","base_word_italian","example_sentence_italian"): return "conjunction"
    if has("is_comparative","is_superlative","base_term_italian"): return "adverb"
    if has("gender","plurality","base_term_italian") and has("article_type","article_italian","added_particle_italian","preposition"):
        return "noun"
    if has("gender","plurality","base_term_italian") and not has("article_type"):
        return "adjective"
    return None

def base_from_any(row):
    # Prefer base_term_italian then base_word_italian then base_lemma_italian if present
    for k in ("base_term_italian","base_word_italian","base_lemma_italian","base_term","base"):
        if k in row and row[k].strip():
            return row[k].strip()
    # As a last resort fallback to term_italian
    return row.get("term_italian","").strip()

def map_common(row, part_of_speech):
    # Defaults for all unified fields
    out = {k: "n/a" for k in UNIFIED_HEADER}
    out["term_italian"] = pick(row, "term_italian") or pick(row, "term")
    out["is_base"] = normalize_bool(row.get("is_base"))
    out["base_lemma_italian"] = base_from_any(row)
    out["part_of_speech"] = part_of_speech
    out["translation_english"] = pick(row, "translation_english")
    out["topics"] = pick(row, "topics")
    out["notes"] = pick(row, "notes")
    out["example_sentence_italian"] = pick(row, "example_sentence_italian")
    out["example_sentence_english"] = pick(row, "example_sentence_english")

    # Common morpho
    out["person_number"] = norm_na(row.get("person_number"))
    out["gender"] = norm_na(row.get("gender"))
    out["plurality"] = norm_na(row.get("plurality"))
    out["mood"] = "n/a"
    out["tense"] = "n/a"
    out["is_comparative"] = "false"
    out["is_superlative"] = "false"
    out["is_compound"] = "false"
    out["article_type"] = "n/a"
    out["article_italian"] = "n/a"
    out["added_particle_italian"] = "n/a"
    out["preposition"] = "n/a"
    out["subtype"] = "n/a"
    return out

def map_row(row, pos):
    # Normalize keys to lower for robust access while preserving values
    row = { (k.strip().lower() if isinstance(k,str) else k): (v if v is not None else "") for k,v in row.items() }

    if pos == "verb":
        out = map_common(row, "verb")
        out["mood"] = norm_na(row.get("mood"))
        out["tense"] = norm_na(row.get("tense"))
        out["is_compound"] = normalize_bool(row.get("is_compound"), default="false")
        return out

    if pos == "pronoun":
        out = map_common(row, "pronoun")
        out["subtype"] = norm_na(row.get("pronoun_kind"))
        return out

    if pos == "preposition":
        out = map_common(row, "preposition")
        out["subtype"] = norm_na(row.get("preposition_type"))
        return out

    if pos == "article":
        out = map_common(row, "article")
        out["subtype"] = norm_na(row.get("article_type"))
        out["article_type"] = norm_na(row.get("article_type"))
        out["article_italian"] = norm_na(row.get("article_italian"))
        out["added_particle_italian"] = norm_na(row.get("added_particle_italian"))
        out["preposition"] = norm_na(row.get("preposition"))
        return out

    if pos == "determiner":
        out = map_common(row, "determiner")
        out["subtype"] = norm_na(row.get("determiner_type"))
        return out

    if pos == "conjunction":
        out = map_common(row, "conjunction")
        out["subtype"] = norm_na(row.get("conjunction_type"))
        return out

    if pos == "adverb":
        out = map_common(row, "adverb")
        out["is_comparative"] = normalize_bool(row.get("is_comparative"), default="false")
        out["is_superlative"] = normalize_bool(row.get("is_superlative"), default="false")
        return out

    if pos == "noun":
        out = map_common(row, "noun")
        out["article_type"] = norm_na(row.get("article_type"))
        out["article_italian"] = norm_na(row.get("article_italian"))
        out["added_particle_italian"] = norm_na(row.get("added_particle_italian"))
        out["preposition"] = norm_na(row.get("preposition"))
        # subtype not used for nouns → keep n/a
        return out

    if pos == "adjective":
        out = map_common(row, "adjective")
        return out

    # Unknown: pass through minimally with guessed POS
    return map_common(row, norm_na("unknown"))

def read_csv_with_header(path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        dialect = None
        try:
            dialect = sniffer.sniff(sample)
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        headers = [h.strip().lower() for h in reader.fieldnames] if reader.fieldnames else []
        rows = [r for r in reader]
    return headers, rows

def main():
    ap = argparse.ArgumentParser(description="Merge LingoKit CSVs into a unified format.")
    ap.add_argument("input", nargs="+", help="CSV files or directories to scan")
    ap.add_argument("-o","--output", required=True, help="Output CSV path")
    args = ap.parse_args()

    files = []
    for item in args.input:
        p = Path(item)
        if p.is_dir():
            files.extend(sorted(p.rglob("*.csv")))
        elif p.is_file() and p.suffix.lower()==".csv":
            files.append(p)
        else:
            print(f"Skipping non-CSV path: {p}", file=sys.stderr)

    if not files:
        print("No CSV files found.", file=sys.stderr)
        sys.exit(1)

    merged = []
    unknown_files = []

    for path in files:
        headers, rows = read_csv_with_header(path)
        header_set = set(headers)
        pos = detect_pos(header_set)
        if pos is None:
            unknown_files.append(path)
            # Attempt soft guess: if it has 'conjection' typo schema
            if "conjection_type" in header_set:
                pos = "conjunction"
                # Fix key name in rows
                new_rows = []
                for r in rows:
                    nr = {k.lower(): v for k,v in r.items()}
                    if "conjection_type" in nr and "conjunction_type" not in nr:
                        nr["conjunction_type"] = nr.pop("conjection_type")
                    new_rows.append(nr)
                rows = new_rows
            else:
                # Skip truly unknown file
                continue

        for r in rows:
            merged.append(map_row(r, pos))

    # Write unified CSV
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=UNIFIED_HEADER, extrasaction="ignore")
        writer.writeheader()
        for row in merged:
            # Ensure required boolean defaults
            row["is_base"] = normalize_bool(row.get("is_base"))
            row["is_comparative"] = normalize_bool(row.get("is_comparative"))
            row["is_superlative"] = normalize_bool(row.get("is_superlative"))
            row["is_compound"] = normalize_bool(row.get("is_compound"))
            # Fill any missing fields with n/a to be safe
            for k in UNIFIED_HEADER:
                if row.get(k, "") == "":
                    row[k] = "n/a"
            writer.writerow(row)

    if unknown_files:
        print("Finished with some files skipped (unrecognized schema):", file=sys.stderr)
        for p in unknown_files:
            print(f"  - {p}", file=sys.stderr)
    else:
        print("Finished successfully.")

if __name__ == "__main__":
    main()
