#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Mine Italian MWE candidates from raw text using spaCy.
- Extract lemma n-grams (2..N) with stopword-aware filtering
- Extract dependency-based patterns:
    * VERB + (ADP) + NOUN   (verb-preposition(-noun) frames)
    * VERB + NOUN           (light-verb constructions)
    * ADJ  + NOUN           (collocations)
    * ADP + NOUN + ADP      (bundles like "al posto di")
- Score with freq, PMI, t-score, and log-likelihood (G^2)
- Export CSV with canonical (lemma) form, surface examples, and pattern type.

Usage:
  python extract_mwe_it.py --input data/*.txt --out mwe.csv --min-freq 15 --max-ngram 4 --top 5000
"""

import argparse
import glob
import math
import os
import re
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import Iterable, List, Tuple, Dict, Optional

import pandas as pd
import spacy
from spacy.tokens import Doc
from tqdm import tqdm

# ---------- Helpers ----------

ITALIAN_QUOTES_RE = re.compile(r"[«»“”„]")

def normalize_text(s: str) -> str:
    s = ITALIAN_QUOTES_RE.sub('"', s)
    return s.replace("\u00A0", " ").strip()

def read_texts(paths: List[str]) -> Iterable[str]:
    files = []
    for p in paths:
        files.extend(glob.glob(p))
    for fp in files:
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = normalize_text(line)
                if line:
                    yield line

def is_good_token(tok) -> bool:
    # Keep words; drop punctuation/space
    return not (tok.is_space or tok.is_punct)

def is_det(tok) -> bool:
    return tok.pos_ == "DET"

def keep_in_ngram(tok) -> bool:
    # Keep content + some function words (ADP, PART, PRON clitics), drop DET
    if not is_good_token(tok):
        return False
    if is_det(tok):
        return False
    return True

def lemma_seq(toks: List) -> Tuple[str, ...]:
    return tuple(t.lemma_.lower() for t in toks)

def surface_seq(toks: List) -> str:
    return " ".join(t.text for t in toks)


# ---------- Stats ----------

def pmi(count_xy, count_x, count_y, N):
    # PMI in bits
    return math.log2((count_xy * N) / max(1, count_x * count_y))

def t_score(count_xy, count_x, count_y, N):
    expected = (count_x * count_y) / max(1.0, N)
    return (count_xy - expected) / math.sqrt(max(1.0, count_xy))

def g2_log_likelihood(count_xy, count_x, count_y, N):
    # Dunning's G^2 for bigrams
    k11 = count_xy
    k12 = count_x - count_xy
    k21 = count_y - count_xy
    k22 = N - (k11 + k12 + k21)
    def safe_log(x):
        return math.log(x) if x > 0 else 0.0
    def term(k, n, p):
        return 0 if k == 0 else 2 * k * (safe_log(k) - safe_log(n) - safe_log(p))
    row1 = k11 + k12
    row2 = k21 + k22
    col1 = k11 + k21
    col2 = k12 + k22
    total = row1 + row2
    # Expected probs:
    p1 = col1 / total if total else 0.0
    p2 = col2 / total if total else 0.0
    return term(k11, total, p1) + term(k12, total, p2) + term(k21, total, p1) + term(k22, total, p2)


# ---------- Data containers ----------

@dataclass
class Candidate:
    key: Tuple[str, ...]                # canonical lemma tuple
    typ: str                            # "ngram","verb_prep_noun","verb_prep","verb_noun","adj_noun","prep_bundle"
    count: int = 0
    left_count: int = 0                 # for bigram stats (w1)
    right_count: int = 0                # (w2)
    examples: deque = field(default_factory=lambda: deque(maxlen=5))
    # scores
    pmi: Optional[float] = None
    tscore: Optional[float] = None
    g2: Optional[float] = None
    avg_len: float = 0.0                # avg surface len (tokens)

    def add(self, surface: str, left=None, right=None):
        self.count += 1
        if surface:
            self.examples.append(surface)
        if left is not None:
            self.left_count += left
        if right is not None:
            self.right_count += right


# ---------- Extractors ----------

class Extractor:
    def __init__(self, nlp, max_ngram=4):
        self.nlp = nlp
        self.max_ngram = max_ngram
        self.unigram_counts = Counter()         # lemma counts
        self.candidates: Dict[Tuple[str, ...], Candidate] = {}
        self.total_tokens = 0

    def _add_candidate(self, lemmas: Tuple[str, ...], typ: str, surface: str):
        cand = self.candidates.get(lemmas)
        if cand is None:
            cand = Candidate(key=lemmas, typ=typ)
            self.candidates[lemmas] = cand
        cand.add(surface)

    def process_doc(self, doc: Doc):
        # Count unigrams (lemmas)
        for tok in doc:
            if is_good_token(tok):
                self.unigram_counts[tok.lemma_.lower()] += 1
                self.total_tokens += 1

        # N-grams by lemmas (2..max_n)
        toks = [t for t in doc if keep_in_ngram(t)]
        L = len(toks)
        for n in range(2, self.max_ngram + 1):
            for i in range(L - n + 1):
                window = toks[i:i+n]
                # Skip windows that are all stopwords (except allow ADP bundles like "al posto di")
                if all(w.is_stop and w.pos_ != "ADP" for w in window):
                    continue
                lemmas = lemma_seq(window)
                surf = surface_seq(window)
                self._add_candidate(lemmas, "ngram", surf)

        # Dependency patterns
        for v in doc:
            if v.pos_ != "VERB":
                continue

            # VERB + NOUN (direct object or close tie) → light verb
            for ch in v.children:
                if ch.pos_ == "NOUN" and ch.dep_ in {"obj", "iobj"}:
                    lem = (v.lemma_.lower(), ch.lemma_.lower())
                    span = doc[min(v.i, ch.i):max(v.i, ch.i)+1]
                    self._add_candidate(lem, "verb_noun", span.text)

            # VERB … NOUN with ADP case (prepositional object): VERB+ADP+NOUN and VERB+ADP
            # Pattern: noun n where n.dep in {"obl","obj","iobj"} AND has child adp with dep="case"
            for n in v.subtree:
                if getattr(n, "pos_", None) == "NOUN" and n.dep_ in {"obl", "obj", "iobj"} and n.head == v:
                    adps = [c for c in n.children if c.pos_ == "ADP" and c.dep_ == "case"]
                    if adps:
                        adp = adps[0]
                        lem_vpn = (v.lemma_.lower(), adp.lemma_.lower(), n.lemma_.lower())
                        self._add_candidate(lem_vpn, "verb_prep_noun", doc[v.left_edge.i:n.right_edge.i+1].text)
                        # Also record the frame without noun: VERB+ADP
                        lem_vp = (v.lemma_.lower(), adp.lemma_.lower())
                        self._add_candidate(lem_vp, "verb_prep", f"{v.text} {adp.text}")

        # ADJ + NOUN (amod)
        for n in doc:
            if n.pos_ == "NOUN":
                for ch in n.children:
                    if ch.pos_ == "ADJ" and ch.dep_ == "amod":
                        lem = (ch.lemma_.lower(), n.lemma_.lower())
                        span = doc[min(ch.i, n.i):max(ch.i, n.i)+1]
                        self._add_candidate(lem, "adj_noun", span.text)

        # ADP + NOUN + ADP bundles like "al posto di"
        for i, t in enumerate(doc):
            if t.pos_ == "ADP":
                # look ahead for NOUN then ADP within small window
                for j in range(i+1, min(i+6, len(doc))):
                    if doc[j].pos_ == "NOUN":
                        for k in range(j+1, min(j+6, len(doc))):
                            if doc[k].pos_ == "ADP":
                                lem = (t.lemma_.lower(), doc[j].lemma_.lower(), doc[k].lemma_.lower())
                                span = doc[i:k+1]
                                self._add_candidate(lem, "prep_bundle", span.text)
                                break
                        break

    def finalize_scores(self, min_freq=10):
        N = max(1, self.total_tokens)
        # Precompute left/right counts for bigrams (needed for PMI, t-score, G^2)
        for key, cand in self.candidates.items():
            if cand.count < min_freq:
                continue
            # unigram marginals (for first/last only; generalization: product of all)
            if len(key) == 2:
                cand.left_count = self.unigram_counts.get(key[0], 1)
                cand.right_count = self.unigram_counts.get(key[1], 1)
                cand.pmi = pmi(cand.count, cand.left_count, cand.right_count, N)
                cand.tscore = t_score(cand.count, cand.left_count, cand.right_count, N)
                cand.g2 = g2_log_likelihood(cand.count, cand.left_count, cand.right_count, N)
            else:
                # for n>2 we'll leave bigram-only stats empty; PMI could be approximated but is noisier
                cand.pmi = None
                cand.tscore = None
                cand.g2 = None
            # avg surface token length (rough “cognitive load” proxy)
            if cand.examples:
                lens = [len(e.split()) for e in cand.examples]
                cand.avg_len = sum(lens) / len(lens)


# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Extract Italian MWE candidates with spaCy.")
    ap.add_argument("--input", nargs="+", required=True, help="Input glob(s): .txt files with raw sentences.")
    ap.add_argument("--out", required=True, help="Output CSV path.")
    ap.add_argument("--model", default="it_core_news_lg", help="spaCy model (default: it_core_news_lg).")
    ap.add_argument("--max-ngram", type=int, default=4, help="Maximum n-gram length (lemmas).")
    ap.add_argument("--min-freq", type=int, default=10, help="Minimum candidate frequency to keep.")
    ap.add_argument("--top", type=int, default=10000, help="Export top-K by ranked score.")
    args = ap.parse_args()

    print(f"Loading spaCy model: {args.model}")
    nlp = spacy.load(args.model, disable=["ner"])
    nlp.max_length = 4_000_000

    extractor = Extractor(nlp, max_ngram=args.max_ngram)

    texts = read_texts(args.input)
    # Use pipe() for speed; keep sentence boundaries
    for doc in tqdm(nlp.pipe(texts, batch_size=500, n_process=1), desc="Processing"):
        extractor.process_doc(doc)

    extractor.finalize_scores(min_freq=args.min_freq)

    # Rank: combine frequency + association & pattern priors
    rows = []
    for key, cand in extractor.candidates.items():
        if cand.count < args.min_freq:
            continue
        # Basic rank score: freq weight + association where available
        assoc = 0.0
        if cand.g2 is not None:
            assoc = cand.g2
        elif cand.tscore is not None:
            assoc = cand.tscore
        freq_weight = math.log1p(cand.count)
        # light priors by pattern type (idiom-y patterns upweighted a bit)
        typ_prior = {
            "verb_prep_noun": 1.3,
            "verb_prep": 1.2,
            "verb_noun": 1.15,
            "adj_noun": 1.0,
            "prep_bundle": 1.25,
            "ngram": 1.0,
        }.get(cand.typ, 1.0)
        score = typ_prior * (freq_weight + 0.015 * assoc)

        rows.append({
            "canonical_lemmas": " ".join(key),
            "type": cand.typ,
            "count": cand.count,
            "unigram_left": cand.left_count or None,
            "unigram_right": cand.right_count or None,
            "pmi": cand.pmi,
            "t_score": cand.tscore,
            "g2": cand.g2,
            "avg_len": cand.avg_len,
            "examples": " || ".join(list(cand.examples)),
            "rank_score": score
        })

    if not rows:
        print("No candidates met the frequency threshold. Try lowering --min-freq.")
        return

    df = pd.DataFrame(rows)
    df.sort_values(["rank_score", "count"], ascending=[False, False], inplace=True)
    df = df.head(args.top).reset_index(drop=True)

    # Light cleanups/columns for downstream LLM labeling
    df.insert(0, "canonical_form", df["canonical_lemmas"])  # you can later normalize surface
    df["teaching_tier_guess"] = df["type"].map({
        "verb_prep_noun": 2,
        "verb_prep": 2,
        "verb_noun": 2,
        "adj_noun": 1,
        "prep_bundle": 2,
        "ngram": 2
    }).fillna(2)

    out_path = os.path.abspath(args.out)
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df):,} candidates → {out_path}")

if __name__ == "__main__":
    main()
