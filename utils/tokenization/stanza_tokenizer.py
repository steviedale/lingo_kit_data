import json
import uuid
from pathlib import Path

import pandas as pd
import stanza

# Run once (no-op if already present)
stanza.download("it")

REMAPPING_PATH = Path(__file__).resolve().parent / "remapping" / "MAP.json"


def _load_token_remap(map_path: Path = REMAPPING_PATH):
    """
    Load the token-specific remapping rules from MAP.json so new rules are
    automatically picked up without code changes.
    """
    try:
        with map_path.open("r", encoding="utf-8") as fh:
            raw_rules = json.load(fh)
    except FileNotFoundError:
        return {}

    remap = {}
    for entry in raw_rules:
        before = entry.get("before") or {}
        after = entry.get("after") or {}
        before_text = before.get("text")
        before_lemma = before.get("lemma")
        before_pos = before.get("pos")
        if not before_text or not before_lemma or not before_pos:
            continue

        key = (before_text.lower(), before_lemma.lower(), before_pos)
        remap[key] = {
            "text": (after.get("text") or before_text).lower(),
            "lemma": (after.get("lemma") or before_lemma).lower(),
            "pos": after.get("pos") or before_pos,
        }
    return remap


def get_token_hash(term, lemma, pos):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{term}-{lemma}-{pos}"))

# Group by lemma so cliticized forms land in the base verb bucket
def get_group_hash(lemma, pos):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{lemma}-{pos}"))

def _merge_feats(words):
    parts = []
    for w in words:
        if w.feats:
            parts.extend(w.feats.split("|"))
    seen = set()
    dedup = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            dedup.append(p)
    return "|".join(dedup) if dedup else None

def _join_or_none(seq, sep="+"):
    seq = [s for s in seq if s is not None]
    return sep.join(seq) if seq else None

def _length(w):
    return len(w.text or "")

# POS priority for choosing the representative piece (content > function)
REP_PRIORITY = [
    "VERB", "NOUN", "ADJ", "ADV", "ADP", "AUX", "PROPN",
    "DET", "PRON", "PART", "NUM", "SCONJ", "CCONJ",
    "INTJ", "SYM", "PUNCT", "X"
]

def _pos_rank(upos: str) -> int:
    return REP_PRIORITY.index(upos) if upos in REP_PRIORITY else len(REP_PRIORITY)

def _is_clitic(w) -> bool:
    # Italian enclitics usually flagged with Clitic=Yes
    return bool(w.feats and "Clitic=Yes" in w.feats)

def _pick_representative(words):
    """
    Prefer non-clitic content items over clitics; then POS priority; then longest.
    Avoids picking PRON chunks like 'glielo' over the verb stem 'dar'.
    """
    non_clitics = [w for w in words if not _is_clitic(w) and w.upos not in {"PRON", "PART", "DET"}]
    candidates = non_clitics if non_clitics else words

    best_rank = min(_pos_rank(w.upos) for w in candidates)
    same_rank = [w for w in candidates if _pos_rank(w.upos) == best_rank]
    return max(same_rank, key=_length)

def _dedup_pairs(a_list, b_list):
    """Deduplicate (a, b) pairs in order, used for parts_lemma/parts_pos cleanup."""
    seen = set()
    A, B = [], []
    for a, b in zip(a_list, b_list):
        key = (a, b)
        if key in seen:
            continue
        seen.add(key)
        A.append(a)
        B.append(b)
    return A, B

class StanzaTokenizer:
    def __init__(self):
        # You can add 'mwt' if you like: processors="tokenize,mwt,pos,lemma,depparse"
        self.nlp = stanza.Pipeline("it", processors="tokenize,pos,lemma,depparse")
        self.token_remap = _load_token_remap()

    def _apply_remapping(self, token_dict):
        """
        Apply POS-wide and token-specific remapping rules, then refresh hashes.
        """
        remapped = dict(token_dict)
        original_text = remapped["text"]
        original_lemma = remapped["lemma"]
        original_pos = remapped["pos"]

        key = (original_text.lower(), original_lemma.lower(), original_pos)
        if key in self.token_remap:
            rule = self.token_remap[key]
            remapped["text"] = rule.get("text", remapped["text"]).lower()
            remapped["lemma"] = rule.get("lemma", remapped["lemma"]).lower()
            remapped["pos"] = rule.get("pos", remapped["pos"])

        # Global rule: treat AUX as VERB
        if remapped["pos"] == "AUX":
            remapped["pos"] = "VERB"

        # Keep helper fields aligned when they mirror the main values
        if remapped.get("parts_lemma") == original_lemma:
            remapped["parts_lemma"] = remapped["lemma"]
        if remapped.get("parts_pos") == original_pos:
            remapped["parts_pos"] = remapped["pos"]

        remapped["token_hash"] = get_token_hash(remapped["text"], remapped["lemma"], remapped["pos"])
        remapped["group_hash"] = get_group_hash(remapped["lemma"], remapped["pos"])
        return remapped

    def _emit_single_word(self, w):
        text = w.text.lower()
        lemma = (w.lemma or w.text).lower()
        pos = w.upos
        token_data = {
            "text": text,
            "lemma": lemma,               # single (dominant) lemma
            "pos": pos,                   # single (dominant) POS
            "xpos": w.xpos,
            "head": w.head,
            "deprel": w.deprel,
            "feats": w.feats,
            "parts_lemma": lemma,         # for reference/debug (optional)
            "parts_pos": pos,
            "vector": None,
            "token_hash": get_token_hash(text, lemma, pos),
            "group_hash": get_group_hash(lemma, pos),
        }
        return self._apply_remapping(token_data)

    def _emit_combined_token(self, token):
        """
        Combine multiple word-pieces under one surface token row.
        Works for apostrophe tokens (e.g., "l'", "dell'") and cliticized verbs (e.g., "aiutarvi").
        """
        surface = token.text.lower()
        words = token.words

        # For reference/debug
        parts_lemma_list = [(w.lemma or w.text).lower() for w in words]
        parts_pos_list   = [w.upos for w in words]

        # Deduplicate identical (lemma, POS) pairs (e.g., "l'" -> ('il','DET') once)
        parts_lemma_list, parts_pos_list = _dedup_pairs(parts_lemma_list, parts_pos_list)

        # Representative decides lemma / POS / head / deprel / xpos
        rep = _pick_representative(words)
        lemma = (rep.lemma or rep.text).lower()
        pos = rep.upos

        feats = _merge_feats(words)  # or use rep.feats if you prefer single-source feats

        token_data = {
            "text": surface,
            "lemma": lemma,               # single dominant lemma (e.g., 'aiutare', 'di')
            "pos": pos,                   # single dominant POS (e.g., 'VERB', 'ADP')
            "xpos": rep.xpos,
            "head": rep.head,
            "deprel": rep.deprel,
            "feats": feats,
            "parts_lemma": _join_or_none(parts_lemma_list, "+"),
            "parts_pos": _join_or_none(parts_pos_list, "+"),
            "vector": None,
            "token_hash": get_token_hash(surface, lemma, pos),
            "group_hash": get_group_hash(lemma, pos),
        }
        return self._apply_remapping(token_data)

    def tokenize(self, text):
        doc = self.nlp(text)
        rows = []
        for sent in doc.sentences:
            for token in sent.tokens:
                if len(token.words) == 1:
                    rows.append(self._emit_single_word(token.words[0]))
                else:
                    rows.append(self._emit_combined_token(token))
        return rows

    def tokenize_to_df(self, text):
        tokens = self.tokenize(text)
        if not tokens:
            return pd.DataFrame(columns=[
                "text","lemma","pos","xpos","head","deprel","feats",
                "parts_lemma","parts_pos","vector","token_hash","group_hash"
            ])
        data = {key: [] for key in tokens[0].keys()}
        for tok in tokens:
            for k, v in tok.items():
                data[k].append(v)
        return pd.DataFrame(data)
