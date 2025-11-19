#!/usr/bin/env python3
"""Fill translation_en and pronunciation for P-noun TSVs."""
from __future__ import annotations

import csv
import re
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Sequence
import sys
import unicodedata

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word

DATA_DIR = REPO_ROOT / "vocabulary" / "dataframes" / "NOUN"
SENTENCE_KEYS = ["sentence_1_en", "sentence_2_en", "sentence_3_en"]
WORD_PATTERN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ']+")
STOPWORDS = {
    'the', 'a', 'an', 'to', 'of', 'and', 'in', 'on', 'at', 'is', 'are', 'was', 'were', 'be',
    'being', 'been', 'has', 'have', 'had', 'do', 'did', 'does', 'done', 'this', 'that',
    'these', 'those', 'he', 'she', 'they', 'we', 'i', 'you', 'it', 'me', 'my', 'your',
    'yours', 'his', 'her', 'hers', 'their', 'theirs', 'our', 'ours', 'its', 'so', 'not',
    'no', 'as', 'for', 'with', 'from', 'into', 'onto', 'up', 'down', 'out', 'over', 'under',
    'off', 'by', 'about', 'than', 'then', 'there', 'here', 'who', 'whom', 'whose', 'what',
    'which', 'where', 'why', 'how', 'or', 'but', 'because', 'while', 'when', 'before',
    'after', 'during', 'even', 'just', 'only', 'if', 'very', 'really', 'more', 'most',
    'some', 'any', 'all', 'each', 'either', 'neither', 'both', 'few', 'little', 'lot',
    'lots', 'many', 'much', 'can', 'could', 'would', 'should', 'will', 'shall', 'may',
    'might', 'must', 'let', 'lets', 'say', 'says', 'said', 'go', 'goes', 'went', 'gone',
    'come', 'comes', 'came', 'get', 'gets', 'got', 'make', 'makes', 'made', 'take', 'takes',
    'took', 'see', 'sees', 'saw', 'seen', 'tell', 'tells', 'told', 'ask', 'asks', 'asked',
    'think', 'thinks', 'thought', 'know', 'knows', 'knew', 'known', 'want', 'wants',
    'wanted', 'need', 'needs', 'needed', 'feel', 'feels', 'felt', 'give', 'gives', 'gave',
    'find', 'finds', 'found', 'put', 'puts', 'keep', 'keeps', 'kept', 'like', 'likes',
    'liked', 'love', 'loves', 'loved', 'work', 'works', 'worked', 'look', 'looks', 'looked',
    'talk', 'talks', 'talked', 'trying', 'try', 'tries', 'tried', 'open', 'opens', 'opened',
    'close', 'closes', 'closed', 'eat', 'eats', 'ate', 'eaten', 'drink', 'drinks', 'drank',
    'drunk', 'drive', 'drives', 'drove', 'driven', 'live', 'lives', 'lived', 'stay', 'stays',
    'stayed', 'sleep', 'sleeps', 'slept', 'run', 'runs', 'ran', 'walk', 'walks', 'walked',
    'sit', 'sits', 'sat', 'stand', 'stands', 'stood', 'write', 'writes', 'wrote', 'written',
    'read', 'reads', 'reading', 'buy', 'buys', 'bought', 'sell', 'sells', 'sold', 'pay',
    'pays', 'paid', 'give', 'giving', 'having', 'getting', 'going', 'coming', 'doing',
    'making', 'taking', 'giving', 'seeing', 'saying', 'mr', 'mrs', 'ms', 'dr', 'tom', 'mary',
    'john', 'anna', 'anne', 'amy', 'mia', 'gianni', 'gianna', 'giovanni', 'marco', 'maria',
    'mario', 'marie', 'marry', 'matt', 'matthew', 'peter', 'paul', 'pablo', 'pedro', 'carlos',
    'carl', 'carlotta', 'sofia', 'samantha', 'sue', 'susan', 'suzie', 'suzanne', 'linda',
    'lucia', 'luigi', 'luca', 'lucio', 'lily', 'liam', 'kate', 'katie', 'karen', 'harry',
    'larry', 'gary', 'george', 'jane', 'janet', 'jill', 'jack', 'jim', 'james', 'robert',
    'roberto', 'richard', 'dick', 'dave', 'david', 'dan', 'daniel', 'charlie', 'charles',
    'steve', 'steven', 'stephen', 'sarah', 'sara', 'ali', 'omar', 'ahmad', 'ahmed', 'abdul',
    'yumi', 'hiroshi', 'ken', 'kenji', 'takeshi', 'taro', 'hana', 'emily', 'emma', 'olivia',
    'lucy', 'henry', 'louis', 'frank', 'fred', 'cash', 'fruit', 'meat', 'pasta'
}
VERB_LIKE = {'is', 'are', 'was', 'were', 'be', 'being', 'been', 'am', 'do', 'does', 'did'}
INVARIANT_NOUNS = {'fish', 'sheep', 'deer', 'moose', 'salmon', 'swine', 'aircraft', 'series', 'species'}
IRREGULAR_PLURALS = {
    'man': 'men',
    'woman': 'women',
    'child': 'children',
    'person': 'people',
    'mouse': 'mice',
    'goose': 'geese',
    'tooth': 'teeth',
    'foot': 'feet',
    'cactus': 'cacti',
    'focus': 'foci',
    'fungus': 'fungi',
    'nucleus': 'nuclei',
    'syllabus': 'syllabi',
    'analysis': 'analyses',
    'diagnosis': 'diagnoses',
    'oasis': 'oases',
    'thesis': 'theses',
    'crisis': 'crises',
    'phenomenon': 'phenomena',
    'criterion': 'criteria'
}


def strip_accents(text: str) -> str:
    return ''.join(ch for ch in unicodedata.normalize('NFD', text) if unicodedata.category(ch) != 'Mn')


def normalize_token(token: str) -> str:
    if not token:
        return ''
    cleaned = strip_accents(token.lower())
    return re.sub(r"[^a-z]", '', cleaned)


def prefix_overlap(a: str, b: str) -> int:
    if not a or not b:
        return 0
    length = min(len(a), len(b))
    count = 0
    for idx in range(length):
        if a[idx] != b[idx]:
            break
        count += 1
    return count


def tokenize_words(sentence: str) -> List[str]:
    if not sentence:
        return []
    return WORD_PATTERN.findall(sentence.lower())


def tokenize_sentence(sentence: str) -> List[str]:
    cleaned = sentence.replace('’', "'")
    tokens = [normalize_token(tok) for tok in WORD_PATTERN.findall(cleaned.lower())]
    return [tok for tok in tokens if tok]


def gather_tokens(sentences: Iterable[str]) -> List[str]:
    filtered: List[str] = []
    raw: List[str] = []
    for sent in sentences:
        if not sent:
            continue
        tokens = [normalize_token(tok) for tok in tokenize_words(sent)]
        tokens = [tok for tok in tokens if tok]
        raw.extend(tokens)
        filtered.extend(tok for tok in tokens if tok not in STOPWORDS)
    return filtered or raw


def proximity_tokens(text_value: str, sentence_it: str, sentence_en: str, window: int = 1) -> List[str]:
    target_norm = normalize_token(text_value)
    if not target_norm or not sentence_it or not sentence_en:
        return []
    it_tokens = [normalize_token(tok) for tok in tokenize_words(sentence_it)]
    it_tokens = [tok for tok in it_tokens if tok]
    if not it_tokens:
        return []
    try:
        pos = it_tokens.index(target_norm)
    except ValueError:
        return []
    ratio = (pos + 0.5) / len(it_tokens)
    en_tokens = [normalize_token(tok) for tok in tokenize_words(sentence_en)]
    en_tokens = [tok for tok in en_tokens if tok]
    if not en_tokens:
        return []
    approx = int(round(ratio * len(en_tokens) - 0.5))
    approx = max(0, min(len(en_tokens) - 1, approx))
    collected: List[str] = []
    for offset in range(window + 1):
        candidates = [approx] if offset == 0 else [approx - offset, approx + offset]
        for idx in candidates:
            if idx < 0 or idx >= len(en_tokens):
                continue
            token = en_tokens[idx]
            if not token:
                continue
            collected.append(token)
    return collected


def apply_modifier(base: str, sequences: Sequence[Sequence[str]]) -> str:
    if ' ' in base.strip():
        return base.strip()
    base_norm = normalize_token(base)
    if not base_norm:
        return base
    prev_counter = Counter()
    next_counter = Counter()
    occurrences = 0
    for tokens in sequences:
        for idx, token in enumerate(tokens):
            if token != base_norm:
                continue
            occurrences += 1
            if idx > 0:
                prev_counter[tokens[idx - 1]] += 1
            if idx + 1 < len(tokens):
                next_counter[tokens[idx + 1]] += 1
    if occurrences == 0:
        return base_norm
    threshold = max(2, occurrences - 1)
    for word, freq in prev_counter.most_common():
        if not word or word in STOPWORDS or word in VERB_LIKE:
            continue
        if freq >= threshold:
            return f"{word} {base_norm}"
    for word, freq in next_counter.most_common():
        if not word or word in STOPWORDS or word in VERB_LIKE:
            continue
        if freq >= threshold:
            return f"{base_norm} {word}"
    return base_norm


def best_phrase(tokens: Sequence[str]) -> str:
    usable = [tok for tok in tokens if tok]
    if not usable:
        return ''

    counts = Counter(usable)
    base, base_count = counts.most_common(1)[0]
    threshold = max(2, base_count - 1)

    prev_counter = Counter()
    next_counter = Counter()
    for idx, token in enumerate(usable):
        if token != base:
            continue
        if idx > 0:
            prev_counter[usable[idx - 1]] += 1
        if idx + 1 < len(usable):
            next_counter[usable[idx + 1]] += 1

    def choose_modifier(counter: Counter, position: str) -> str:
        for word, freq in counter.most_common():
            if word in STOPWORDS or word in VERB_LIKE:
                continue
            if freq >= threshold:
                return f"{word} {base}" if position == 'before' else f"{base} {word}"
        return ''

    phrase = choose_modifier(prev_counter, 'before')
    if phrase:
        return phrase
    phrase = choose_modifier(next_counter, 'after')
    if phrase:
        return phrase
    return base


def parse_feats(feats: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not feats:
        return result
    for chunk in feats.split('|'):
        if '=' not in chunk:
            continue
        key, value = chunk.split('=', 1)
        result[key] = value
    return result


def pluralize_word(word: str) -> str:
    base = word.strip()
    if not base:
        return base
    lower = base.lower()
    if lower in INVARIANT_NOUNS:
        return base
    irregular = IRREGULAR_PLURALS.get(lower)
    if irregular:
        return irregular
    if lower.endswith(('s', 'x', 'z', 'ch', 'sh')):
        return base + 'es'
    if lower.endswith('y') and len(lower) > 1 and lower[-2] not in 'aeiou':
        return base[:-1] + 'ies'
    if lower.endswith('f'):
        return base[:-1] + 'ves'
    if lower.endswith('fe'):
        return base[:-2] + 'ves'
    if lower.endswith('o') and len(lower) > 1 and lower[-2] not in 'aeiou':
        return base + 'es'
    return base + 's'


def pluralize_gloss(gloss: str) -> str:
    senses = [sense.strip() for sense in gloss.split(';') if sense.strip()]
    if not senses:
        return gloss
    plural_senses = []
    for sense in senses:
        parts = sense.split()
        if not parts:
            plural_senses.append(sense)
            continue
        parts[-1] = pluralize_word(parts[-1])
        plural_senses.append(' '.join(parts))
    return '; '.join(plural_senses)


def format_translation(base: str, feats: Dict[str, str], gender_notes: bool, allow_plural: bool) -> str:
    gloss = base
    number = feats.get('Number')
    if allow_plural and number == 'Plur':
        gloss = pluralize_gloss(gloss)
    if gender_notes:
        gender = feats.get('Gender')
        if gender == 'Masc':
            gloss = f"{gloss} (masculine)"
        elif gender == 'Fem':
            gloss = f"{gloss} (feminine)"
    return gloss


def derive_base_gloss(rows: List[Dict[str, str]]) -> str:
    priority_rows: List[Dict[str, str]] = []
    for row in rows:
        feats = parse_feats(row.get('feats', ''))
        if feats.get('Number') == 'Plur':
            continue
        priority_rows.append(row)
    source_rows = priority_rows or rows
    english_sentences: List[str] = []
    english_sequences: List[List[str]] = []
    proximity_counter: Counter[str] = Counter()
    for row in source_rows:
        text_value = row.get('text', '') or ''
        for idx in range(1, 4):
            sentence_it = row.get(f'sentence_{idx}_it', '') or ''
            sentence_en = row.get(f'sentence_{idx}_en', '') or ''
            if not sentence_en:
                continue
            english_sentences.append(sentence_en)
            tokens = [normalize_token(tok) for tok in tokenize_words(sentence_en)]
            tokens = [tok for tok in tokens if tok]
            if tokens:
                english_sequences.append(tokens)
            for token in proximity_tokens(text_value, sentence_it, sentence_en):
                if token and token not in STOPWORDS:
                    proximity_counter[token] += 1
    token_list = gather_tokens(english_sentences)
    global_counter = Counter(token_list)
    base = ''
    italian_base = normalize_token(rows[0].get('lemma') or rows[0].get('text') or '')
    combined_keys = sorted(set(global_counter) | set(proximity_counter))
    best_score = -1
    best_proximity = -1
    best_global = -1
    best_similarity = -1
    for word in combined_keys:
        if not word or word in STOPWORDS:
            continue
        score = global_counter.get(word, 0) + proximity_counter.get(word, 0)
        prox = proximity_counter.get(word, 0)
        glob = global_counter.get(word, 0)
        similarity = prefix_overlap(italian_base, word)
        if (score > best_score or
                (score == best_score and prox > best_proximity) or
                (score == best_score and prox == best_proximity and glob > best_global) or
                (score == best_score and prox == best_proximity and glob == best_global and
                 ((similarity >= 2 or best_similarity >= 2) and similarity > best_similarity))):
            base = word
            best_score = score
            best_proximity = prox
            best_global = glob
            best_similarity = similarity
    if not base:
        base = best_phrase(token_list).strip()
    if not base:
        base = (rows[0].get('lemma') or rows[0].get('text') or '').lower()
    elif english_sequences:
        base = apply_modifier(base, english_sequences)
    return base


def main() -> None:
    for entry in sorted(DATA_DIR.iterdir()):
        if entry.suffix != '.tsv' or not entry.name.lower().startswith('p'):
            continue
        with entry.open('r', encoding='utf-8', newline='') as handle:
            reader = csv.DictReader(handle, delimiter='\t')
            rows = list(reader)
        if not rows:
            continue
        base_gloss = derive_base_gloss(rows)
        print(f"{entry.name}\t{rows[0].get('lemma', '')}\t{base_gloss}")


if __name__ == '__main__':
    main()
