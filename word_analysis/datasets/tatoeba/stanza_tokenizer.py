### METHOD 1 ###
# # !pip install stanza
# import stanza
# import pandas as pd
# import uuid


# stanza.download("it")  # once


# def get_token_hash(term, lemma, pos):

#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{term}-{lemma}-{pos}"))

# def get_group_hash(lemma, pos):
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{lemma}-{pos}"))


# class StanzaTokenizer:
#     def __init__(self):
#         self.nlp = stanza.Pipeline("it", processors="tokenize,pos,lemma,depparse")

#     def tokenize(self, text):
#         doc = self.nlp(text)
#         tokens = []
#         for sentence in doc.sentences:
#             for token in sentence.tokens:
#                 for word in token.words:
#                     text = word.text.lower()
#                     lemma = word.lemma.lower()
#                     tokens.append({
#                         "text": text,
#                         "lemma": lemma,
#                         "pos": word.upos,
#                         "xpos": word.xpos,
#                         "head": word.head,
#                         "deprel": word.deprel,
#                         "feats": word.feats,
#                         "vector": None,  # Stanza does not provide word vectors
#                         "token_hash": get_token_hash(text, lemma, word.upos),
#                         "group_hash": get_group_hash(lemma, word.upos),
#                     })
#         return tokens
    
#     def tokenize_to_df(self, text):
#         tokens = self.tokenize(text)
#         data = {
#             key: [] for key in tokens[0].keys()
#         }
#         for token in tokens:
#             for key, value in token.items():
#                 data[key].append(value)
#         df = pd.DataFrame(data)
#         return df


### METHOD 2 ###
# # !pip install stanza
# import stanza
# import pandas as pd
# import uuid

# stanza.download("it")  # run once

# def get_token_hash(term, lemma, pos):
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{term}-{lemma}-{pos}"))

# def get_group_hash(lemma, pos):
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{lemma}-{pos}"))

# def _merge_feats(words):
#     parts = []
#     for w in words:
#         if w.feats:
#             parts.extend(w.feats.split("|"))
#     # keep order but remove duplicates
#     seen = set()
#     dedup = []
#     for p in parts:
#         if p not in seen:
#             seen.add(p)
#             dedup.append(p)
#     return "|".join(dedup) if dedup else None

# def _join_or_none(seq, sep="+"):
#     seq = [s for s in seq if s is not None]
#     return sep.join(seq) if seq else None

# class StanzaTokenizer:
#     def __init__(self):
#         # You can add 'mwt' if you wish, but not required here
#         self.nlp = stanza.Pipeline("it", processors="tokenize,pos,lemma,depparse")

#     def _emit_single_word(self, w):
#         text = w.text.lower()
#         lemma = (w.lemma or w.text).lower()
#         pos = w.upos
#         return {
#             "text": text,
#             "lemma": lemma,
#             "pos": pos,
#             "xpos": w.xpos,
#             "head": w.head,
#             "deprel": w.deprel,
#             "feats": w.feats,
#             "vector": None,
#             "token_hash": get_token_hash(text, lemma, pos),
#             "group_hash": get_group_hash(lemma, pos),
#         }

#     def _emit_combined_token(self, token):
#         """
#         Combine multiple 'words' under one surface token row.
#         - If token.text contains an apostrophe, we still combine (e.g., "dell'")
#         - For head/deprel, prefer the VERB's head/deprel if present; else take the first word's.
#         """
#         surface = token.text.lower()

#         words = token.words
#         lemmas = [(w.lemma or w.text).lower() for w in words]
#         poses = [w.upos for w in words]
#         xposes = [w.xpos for w in words]
#         feats = _merge_feats(words)

#         # Pick a representative head/deprel: favor VERB, else first
#         rep = None
#         for w in words:
#             if w.upos == "VERB":
#                 rep = w
#                 break
#         if rep is None:
#             rep = words[0]

#         # Build combined fields
#         lemma = _join_or_none(lemmas, "+")
#         pos = _join_or_none(poses, "+")
#         xpos = _join_or_none(xposes, "+")

#         return {
#             "text": surface,
#             "lemma": lemma,
#             "pos": pos,
#             "xpos": xpos,
#             "head": rep.head,
#             "deprel": rep.deprel,
#             "feats": feats,
#             "vector": None,
#             "token_hash": get_token_hash(surface, lemma, pos),
#             "group_hash": get_group_hash(lemma, pos),
#         }

#     def tokenize(self, text):
#         doc = self.nlp(text)
#         rows = []
#         for sent in doc.sentences:
#             for token in sent.tokens:
#                 # If token maps to exactly one word, just emit that word.
#                 if len(token.words) == 1:
#                     rows.append(self._emit_single_word(token.words[0]))
#                     continue

#                 # Multiple words under one surface token:
#                 # 1) If the surface contains an apostrophe (e.g., "dell'"),
#                 #    keep the surface token combined.
#                 # 2) If no apostrophe (e.g., "aiutarvi"), also keep combined.
#                 # In both cases we combine lemmas/POS/feats like "VERB+PRON".
#                 rows.append(self._emit_combined_token(token))
#         return rows

#     def tokenize_to_df(self, text):
#         tokens = self.tokenize(text)
#         # Handle empty input gracefully
#         if not tokens:
#             return pd.DataFrame(columns=[
#                 "text","lemma","pos","xpos","head","deprel","feats",
#                 "vector","token_hash","group_hash"
#             ])
#         data = {key: [] for key in tokens[0].keys()}
#         for tok in tokens:
#             for k, v in tok.items():
#                 data[k].append(v)
#         return pd.DataFrame(data)

### METHOD 3 ###
# # !pip install stanza
# import stanza
# import pandas as pd
# import uuid

# # Run once (no-op if already present)
# stanza.download("it")

# def get_token_hash(term, lemma, pos):
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{term}-{lemma}-{pos}"))

# # Group by lemma so cliticized forms land in the base verb bucket
# def get_group_hash(lemma):
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{lemma}"))

# def _merge_feats(words):
#     parts = []
#     for w in words:
#         if w.feats:
#             parts.extend(w.feats.split("|"))
#     seen = set()
#     dedup = []
#     for p in parts:
#         if p not in seen:
#             seen.add(p)
#             dedup.append(p)
#     return "|".join(dedup) if dedup else None

# def _join_or_none(seq, sep="+"):
#     seq = [s for s in seq if s is not None]
#     return sep.join(seq) if seq else None

# def _length(w):
#     return len(w.text or "")

# # POS priority for choosing the representative piece (content > function)
# REP_PRIORITY = [
#     "VERB", "NOUN", "ADJ", "ADV", "ADP", "AUX", "PROPN",
#     "DET", "PRON", "PART", "NUM", "SCONJ", "CCONJ",
#     "INTJ", "SYM", "PUNCT", "X"
# ]

# def _pos_rank(upos: str) -> int:
#     return REP_PRIORITY.index(upos) if upos in REP_PRIORITY else len(REP_PRIORITY)

# def _is_clitic(w) -> bool:
#     # Italian enclitics usually flagged with Clitic=Yes
#     return bool(w.feats and "Clitic=Yes" in w.feats)

# def _pick_representative(words):
#     """
#     Prefer non-clitic content items over clitics; then POS priority; then longest.
#     Avoids picking PRON chunks like 'glielo' over the verb stem 'dar'.
#     """
#     non_clitics = [w for w in words if not _is_clitic(w) and w.upos not in {"PRON", "PART", "DET"}]
#     candidates = non_clitics if non_clitics else words

#     best_rank = min(_pos_rank(w.upos) for w in candidates)
#     same_rank = [w for w in candidates if _pos_rank(w.upos) == best_rank]
#     return max(same_rank, key=_length)

# class StanzaTokenizer:
#     def __init__(self):
#         # You can add 'mwt' if you like: processors="tokenize,mwt,pos,lemma,depparse"
#         self.nlp = stanza.Pipeline("it", processors="tokenize,pos,lemma,depparse")

#     def _emit_single_word(self, w):
#         text = w.text.lower()
#         lemma = (w.lemma or w.text).lower()
#         pos = w.upos
#         return {
#             "text": text,
#             "lemma": lemma,               # single (dominant) lemma
#             "pos": pos,                   # single (dominant) POS
#             "xpos": w.xpos,
#             "head": w.head,
#             "deprel": w.deprel,
#             "feats": w.feats,
#             "parts_lemma": lemma,         # for reference/debug (optional)
#             "parts_pos": pos,
#             "vector": None,
#             "token_hash": get_token_hash(text, lemma, pos),
#             "group_hash": get_group_hash(lemma),
#         }

#     def _emit_combined_token(self, token):
#         """
#         Combine multiple word-pieces under one surface token row.
#         Works for apostrophe tokens (e.g., "dell'") and cliticized verbs (e.g., "aiutarvi").
#         """
#         surface = token.text.lower()
#         words = token.words

#         # For reference/debug
#         parts_lemma_list = [(w.lemma or w.text).lower() for w in words]
#         parts_pos_list = [w.upos for w in words]

#         # Representative decides lemma / POS / head / deprel / xpos
#         rep = _pick_representative(words)
#         lemma = (rep.lemma or rep.text).lower()
#         pos = rep.upos

#         # Features: merged across parts (switch to rep.feats if preferred)
#         feats = _merge_feats(words)

#         return {
#             "text": surface,
#             "lemma": lemma,               # single (dominant) lemma (e.g., 'aiutare')
#             "pos": pos,                   # single (dominant) POS (e.g., 'VERB')
#             "xpos": rep.xpos,
#             "head": rep.head,
#             "deprel": rep.deprel,
#             "feats": feats,
#             "parts_lemma": _join_or_none(parts_lemma_list, "+"),
#             "parts_pos": _join_or_none(parts_pos_list, "+"),
#             "vector": None,
#             "token_hash": get_token_hash(surface, lemma, pos),
#             "group_hash": get_group_hash(lemma),
#         }

#     def tokenize(self, text):
#         doc = self.nlp(text)
#         rows = []
#         for sent in doc.sentences:
#             for token in sent.tokens:
#                 if len(token.words) == 1:
#                     rows.append(self._emit_single_word(token.words[0]))
#                 else:
#                     rows.append(self._emit_combined_token(token))
#         return rows

#     def tokenize_to_df(self, text):
#         tokens = self.tokenize(text)
#         if not tokens:
#             return pd.DataFrame(columns=[
#                 "text","lemma","pos","xpos","head","deprel","feats",
#                 "parts_lemma","parts_pos","vector","token_hash","group_hash"
#             ])
#         data = {key: [] for key in tokens[0].keys()}
#         for tok in tokens:
#             for k, v in tok.items():
#                 data[k].append(v)
#         return pd.DataFrame(data)

### METHOD 4 ###
# !pip install stanza
import stanza
import pandas as pd
import uuid

# Run once (no-op if already present)
stanza.download("it")

def get_token_hash(term, lemma, pos):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{term}-{lemma}-{pos}"))

# Group by lemma so cliticized forms land in the base verb bucket
def get_group_hash(lemma):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{lemma}"))

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

    def _emit_single_word(self, w):
        text = w.text.lower()
        lemma = (w.lemma or w.text).lower()
        pos = w.upos
        return {
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
            "group_hash": get_group_hash(lemma),
        }

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

        return {
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
            "group_hash": get_group_hash(lemma),
        }

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
