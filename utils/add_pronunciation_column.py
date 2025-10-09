import csv
import re
import unicodedata
from pathlib import Path

VOWELS_WITH_ACCENTS = 'aeiouàèéìòù'

NUCLEUS_MAP = {
    'ia': 'yah',
    'ie': 'yeh',
    'io': 'yoh',
    'iu': 'yoo',
    'ua': 'wah',
    'uo': 'woh',
    'ui': 'wee',
    'ue': 'weh',
    'ai': 'eye',
    'ei': 'ay',
    'oi': 'oy',
    'au': 'ow',
    'eu': 'eh-oo',
}

CONSONANT_MAP = {
    'b': 'b',
    'd': 'd',
    'f': 'f',
    'l': 'l',
    'm': 'm',
    'n': 'n',
    'p': 'p',
    'r': 'r',
    's': 's',
    't': 't',
    'v': 'v',
    'z': 'ts',
    'x': 'ks',
    'j': 'y',
    'k': 'k',
    'w': 'w',
    'y': 'y',
    'c': 'k',
    'g': 'g',
    'h': '',
}

ALLOWED_ONSETS = {
    'b','c','d','f','g','h','l','m','n','p','q','r','s','t','v','z',
    'br','cr','dr','fr','gr','pr','tr','vr','pl','cl','fl','gl','bl','gn','qu','gu','ch','gh','sc','sf','sl','sm','sn','sp','sr','st','sv','scr','spl','spr','str','scl','sfr','sgl','sgr','sdr','sbr','sgn','squ','sch','sci','sfo','svi','sfl','sgu','sdr','pr','dr','gr','cr','fr','br','pl','cl','fl','gl','bl','ps'
}


def strip_accents(text: str) -> str:
    return ''.join(ch for ch in unicodedata.normalize('NFD', text) if unicodedata.category(ch) != 'Mn')


def split_cluster(cluster: str) -> int:
    if not cluster:
        return 0
    cl = strip_accents(cluster.lower())
    length = len(cl)
    for k in range(length, 0, -1):
        suffix = cl[length - k:]
        if suffix in ALLOWED_ONSETS:
            return length - k
    return length - 1


def first_vowel_index(s: str) -> int:
    for idx, ch in enumerate(s):
        if strip_accents(ch.lower()) in VOWELS_WITH_ACCENTS:
            return idx
    return -1


def split_syllable_components(s: str):
    idx = first_vowel_index(s)
    if idx == -1:
        return s, '', ''
    onset = s[:idx]
    j = idx
    while j < len(s) and strip_accents(s[j].lower()) in VOWELS_WITH_ACCENTS:
        j += 1
    nucleus = s[idx:j]
    coda = s[j:]
    return onset, nucleus, coda


def map_onset(onset: str, nucleus: str) -> str:
    onset_norm = strip_accents(onset.lower())
    nucleus_norm = strip_accents(nucleus.lower()) if nucleus else ''
    result = ''
    i = 0
    while i < len(onset_norm):
        chunk3 = onset_norm[i:i + 3]
        chunk2 = onset_norm[i:i + 2]
        if chunk3 == 'gli' and nucleus_norm.startswith(('a', 'e', 'o', 'u')):
            result += 'ly'
            i += 3
            continue
        if chunk2 == 'gl' and nucleus_norm.startswith('i'):
            result += 'ly'
            i += 2
            continue
        if chunk2 == 'gn':
            result += 'ny'
            i += 2
            continue
        if chunk2 == 'sc':
            if nucleus_norm.startswith(('e', 'i')):
                result += 'sh'
            else:
                result += 'sk'
            i += 2
            continue
        if chunk2 == 'ch':
            result += 'k'
            i += 2
            continue
        if chunk2 == 'gh':
            result += 'g'
            i += 2
            continue
        if chunk2 == 'qu':
            result += 'kw'
            i += 2
            continue
        if chunk2 == 'gu' and nucleus_norm.startswith(('a', 'e', 'i', 'o')):
            result += 'gw'
            i += 2
            continue
        ch = onset_norm[i]
        if ch == 'c':
            if nucleus_norm.startswith(('e', 'i')):
                result += 'ch'
            else:
                result += 'k'
            i += 1
            continue
        if ch == 'g':
            if nucleus_norm.startswith(('e', 'i')):
                result += 'j'
            else:
                result += 'g'
            i += 1
            continue
        result += CONSONANT_MAP.get(ch, ch)
        i += 1
    return result


def map_vowel(nucleus: str) -> str:
    if not nucleus:
        return ''
    nucleus_norm = strip_accents(nucleus.lower())
    if nucleus_norm in NUCLEUS_MAP:
        return NUCLEUS_MAP[nucleus_norm]
    for length in range(len(nucleus_norm), 1, -1):
        prefix = nucleus_norm[:length]
        if prefix in NUCLEUS_MAP:
            rest = nucleus[length:]
            return NUCLEUS_MAP[prefix] + map_vowel(rest)
    result = ''
    for ch in nucleus:
        base = strip_accents(ch.lower())
        if base == 'a':
            result += 'ah'
        elif base == 'e':
            result += 'eh'
        elif base == 'i':
            result += 'ee'
        elif base == 'o':
            result += 'oh'
        elif base == 'u':
            result += 'oo'
        elif base == 'y':
            result += 'ee'
        else:
            result += base
    return result


def map_coda(coda: str) -> str:
    coda_norm = strip_accents(coda.lower())
    result = ''
    i = 0
    while i < len(coda_norm):
        chunk2 = coda_norm[i:i + 2]
        if chunk2 == 'gn':
            result += 'ny'
            i += 2
            continue
        if chunk2 == 'gl':
            result += 'l'
            i += 2
            continue
        if chunk2 == 'sc':
            result += 'sk'
            i += 2
            continue
        ch = coda_norm[i]
        if ch == 'c':
            result += 'k'
        elif ch == 'g':
            result += 'g'
        elif ch == 'z':
            result += 'ts'
        elif ch == 'h':
            pass
        else:
            result += CONSONANT_MAP.get(ch, ch)
        i += 1
    return result


def convert_syllable(syllable: str) -> str:
    onset, nucleus, coda = split_syllable_components(syllable)
    onset_sound = map_onset(onset, nucleus)
    nucleus_sound = map_vowel(nucleus)
    coda_sound = map_coda(coda)
    combined = onset_sound + nucleus_sound + coda_sound
    combined = re.sub(r'([yw])\\1+', r'\\1', combined)
    return combined


def syllabify(word: str):
    lower = strip_accents(word.lower())
    matches = list(re.finditer(f'[{VOWELS_WITH_ACCENTS}]+', lower))
    if not matches:
        return [word]
    prev_end = 0
    syllables = []
    for idx, match in enumerate(matches):
        start, end = match.span()
        cluster = word[prev_end:start]
        if idx == 0:
            onset = cluster
        else:
            split = split_cluster(cluster)
            if split > 0:
                syllables[-1] += cluster[:split]
            onset = cluster[split:]
        syllable = onset + word[start:end]
        syllables.append(syllable)
        prev_end = end
    trailing = word[prev_end:]
    if trailing:
        syllables[-1] += trailing
    return syllables


def stress_index(word: str, syllables) -> int:
    for idx, syll in enumerate(syllables):
        if any(ch in 'àèéìòù' for ch in syll.lower()):
            return idx
    if len(syllables) == 1:
        return 0
    return len(syllables) - 2


def pronounce_word(word: str) -> str:
    tokens = re.split(r'([ \t]+|-)', word)
    pieces = []
    for token in tokens:
        if not token:
            continue
        if token.isspace() or token == '-':
            pieces.append(token)
            continue
        cleaned = token.replace("'", '')
        if not cleaned:
            continue
        syllables = syllabify(cleaned)
        stress = stress_index(cleaned, syllables)
        pron_parts = []
        for idx, syll in enumerate(syllables):
            pron = convert_syllable(syll)
            pron = pron.upper() if idx == stress else pron.lower()
            pron_parts.append(pron)
        pieces.append('-'.join(pron_parts))
    return ''.join(pieces)


def add_pronunciations(directory: Path) -> None:
    for path in sorted(directory.glob('*.csv')):
        with path.open(newline='') as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            fieldnames = reader.fieldnames or []
        if not rows or 'term_italian' not in rows[0]:
            continue
        if 'pronunciation' not in fieldnames:
            fieldnames = fieldnames + ['pronunciation']
        for row in rows:
            term = row.get('term_italian', '') or ''
            row['pronunciation'] = pronounce_word(term)
        with path.open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)


def main() -> None:
    target_dir = Path('dataframes/dataframes_by_pos/verb')
    add_pronunciations(target_dir)


if __name__ == '__main__':
    main()
