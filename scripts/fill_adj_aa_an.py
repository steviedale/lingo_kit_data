from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word

DATA_DIR = Path('vocabulary/dataframes/ADJ')
TARGET_PREFIXES = tuple('a' + chr(code) for code in range(ord('a'), ord('n') + 1))
SKIP_FILES = {
    'alto.tsv',
    'altro.tsv',
    'americano.tsv',
    'anziano.tsv',
}

LEMMA_TRANSLATIONS = {
    'abbacchiato': 'dejected; downcast',
    'abbandonare': 'abandoned; deserted',
    'abbandonato': 'abandoned; neglected',
    'abbasido': 'abbasid',
    'abbassare': 'lowered; pulled down',
    'abbattere': 'felled; knocked down',
    'abbondante': 'abundant; plentiful',
    'abbozzato': 'sketched; rough; unfinished',
    'abbronzato': 'tanned; sunburned',
    'abile': 'able; skilled',
    'abissale': 'abyssal; deep-sea',
    'abitare': 'inhabited; lived-in',
    'abitativo': 'residential; housing',
    'abituale': 'habitual; usual',
    'abitudinario': 'creature of habit; routine-bound',
    'abominevole': 'abominable; detestable',
    'aborigeno': 'aboriginal; indigenous',
    'abusivo': 'illegal; unauthorized',
    'accademico': 'academic',
    'accadere': 'happened; occurred',
    'accanita': 'fierce; relentless',
    'accanito': 'fierce; relentless',
    'accendere': 'lit; switched on; bright',
    'acceso': 'lit; switched on; bright',
    'accessibile': 'accessible',
    'accessorio': 'accessory; incidental',
    'accettabile': 'acceptable; reasonable',
    'accettare': 'accepted; welcome',
    'accetto': 'welcome; acceptable',
    'accidentato': 'bumpy; uneven',
    'accogliente': 'cozy; welcoming',
    'accorto': 'shrewd; prudent',
    'accostatosi': 'having approached; drawing near',
    'acculturato': 'cultured; well-educated',
    'accurato': 'accurate; thorough',
    'acid': 'acid; psychedelic',
    'acido': 'acidic; sour',
    'acquatico': 'aquatic; water-based',
    'acustico': 'acoustic; sound-related',
    'acuto': 'sharp; acute',
    'adattabile': 'adaptable; flexible',
    'adatto': 'suitable; fitting',
    'addolorato': 'sorrowful; grief-stricken',
    'addormentato': 'asleep; drowsy',
    'adeguato': 'adequate; appropriate',
    'adesivo': 'adhesive; sticky',
    'adiacente': 'adjacent; next-door',
    'adolescente': 'adolescent; teenage',
    'adorabile': 'adorable; lovely',
    'adottato': 'adopted',
    'adulto': 'adult; grown-up',
    'aereo': 'aerial; air',
    'aeroportuale': 'airport; aviation',
    'affabile': 'affable; friendly',
    'affamato': 'hungry; starving',
    'affascinante': 'fascinating; charming',
    'affascinare': 'fascinated; captivated',
    'affaticato': 'tired; weary',
    'affermativo': 'affirmative; positive',
    'affettato': 'affected; mannered',
    'affettivo': 'affective; emotional',
    'affettuoso': 'affectionate; loving',
    'affezionato': 'fond; attached',
    'affidabile': 'reliable; trustworthy',
    'affilato': 'sharp; sharpened',
    'affine': 'related; akin',
    'affittare': 'rented',
    'affollato': 'crowded',
    'affordabile': 'affordable',
    'afoso': 'sultry; muggy',
    'africano': 'african',
    'afroamericano': 'african american',
    'aggiornato': 'updated; up to date',
    'aggressivo': 'aggressive',
    'agile': 'agile; nimble',
    'agitato': 'agitated; restless',
    'agnostico': 'agnostic',
    'agricolo': 'agricultural',
    'agrio': 'sour; tart',
    'alacre': 'brisk; diligent',
    'alato': 'winged',
    'albanese': 'albanian',
    'alberghiero': 'hotel; hospitality',
    'alcolico': 'alcoholic; boozy',
    'alcolizzato': 'alcoholic; addicted to alcohol',
    'alfabetico': 'alphabetic; alphabetical',
    'algebrico': 'algebraic',
    'algerina': 'algerian',
    'alieno': 'alien; foreign',
    'alimentare': 'food; dietary',
    'allacciare': 'fastened; buckled',
    'allarmato': 'alarmed; worried',
    'allegorico': 'allegorical',
    'allegro': 'cheerful; lively',
    'allergice': 'allergic',
    'allergico': 'allergic',
    'allettare': 'tempting; enticing',
    'allucinante': 'hallucinatory; mind-blowing',
    'alpino': 'alpine',
    'alternativo': 'alternative',
    'altezzoso': 'haughty; snobbish',
    'alticcio': 'tipsy; slightly drunk',
    'altisonanti': 'high-sounding; grandiose',
    'altruistico': 'altruistic; selfless',
    'altrà': 'other; different',
    'amabile': 'amiable; sweet',
    'amare': 'beloved; loved',
    'amario': 'bitter',
    'amaro': 'bitter',
    'amato': 'beloved; loved',
    'amazzonico': 'amazonian',
    'ambidestro': 'ambidextrous',
    'ambientale': 'environmental',
    'ambiguo': 'ambiguous; equivocal',
    'ambizioso': 'ambitious',
    'ambulante': 'itinerant; travelling',
    'americana': 'american',
    'amichevole': 'friendly',
    'amico': 'friend',
    'ammalare': 'sick; ill',
    'ammalato': 'sick; ill',
    'amministrativo': 'administrative',
    'ammirabile': 'admirable; praiseworthy',
    'ammirevole': 'admirable; commendable',
    'ammuffito': 'moldy; musty',
    'ammutolito': 'speechless; struck dumb',
    'amorale': 'amoral',
    'amoroso': 'loving; amorous',
    'ampio': 'wide; spacious',
    'anacronistico': 'anachronistic',
    'analcolico': 'non-alcoholic',
    'analfabeta': 'illiterate',
    'analgesico': 'analgesic; pain-relieving',
    'analogo': 'analogous; similar',
    'anatomico': 'anatomical',
    'ancestrale': 'ancestral; age-old',
    'anchilosato': 'stiff; immobilized',
    'andaluso': 'andalusian',
    'androfobico': 'afraid of men; androphobic',
    'anemico': 'anemic',
    'anglosassone': 'anglo-saxon',
    'angusto': 'narrow; cramped',
    'animale': 'animal; animal-like',
    'animato': 'animated; lively',
    'animoso': 'spirited; bold',
    'annegare': 'drowned',
    'annoiato': 'bored',
    'annuale': 'annual; yearly',
    'annuo': 'yearly; per year',
    'anomalo': 'anomalous; abnormal',
    'anonimo': 'anonymous',
    'ansioso': 'anxious; eager',
    'antartico': 'antarctic',
    'anteriore': 'anterior; front',
    'anti': 'anti-; against',
    'antiaderente': 'non-stick',
    'anticipare': 'early; brought forward',
    'antico': 'ancient; old',
    'antincendio': 'firefighting; fireproof',
    'antipatico': 'unpleasant; unlikeable',
    'antiquato': 'outdated; old-fashioned',
    'antitraspirante': 'antiperspirant',
}

SPECIAL_NOTES = {
    "altr'": 'before a vowel',
}


def parse_feat(feats: str, key: str) -> str | None:
    if not feats:
        return None
    for part in feats.split('|'):
        if part.startswith(key + '='):
            return part.split('=', 1)[1]
    return None


def collect_lemma_genders() -> dict[str, set[str]]:
    genders: dict[str, set[str]] = defaultdict(set)
    for path in DATA_DIR.glob('*.tsv'):
        if path.name in SKIP_FILES:
            continue
        if not path.name.lower().startswith(TARGET_PREFIXES):
            continue
        with path.open(encoding='utf-8') as handle:
            reader = csv.DictReader(handle, delimiter='\t')
            for row in reader:
                gender = parse_feat(row.get('feats', ''), 'Gender')
                if gender:
                    genders[row['lemma']].add(gender)
    return genders


def format_translation(base: str, row: dict[str, str], gender_map: dict[str, set[str]]) -> str:
    lemma = row['lemma']
    notes = []
    gender = parse_feat(row.get('feats', ''), 'Gender')
    number = parse_feat(row.get('feats', ''), 'Number')
    genders = gender_map.get(lemma, set())
    gender_note = None
    if gender and len(genders) > 1:
        gender_note = 'masculine' if gender == 'Masc' else 'feminine'
    plural_note = 'plural' if number == 'Plur' else None
    combined_note = None
    if gender_note and plural_note:
        combined_note = f'{gender_note} {plural_note}'
    elif gender_note:
        combined_note = gender_note
    elif plural_note:
        combined_note = plural_note
    if combined_note:
        notes.append(combined_note)
    extra = SPECIAL_NOTES.get(row['text'])
    if extra:
        notes.append(extra)
    if notes:
        return f"{base} ({', '.join(notes)})"
    return base


def main() -> None:
    gender_map = collect_lemma_genders()
    target_files = [
        path for path in sorted(DATA_DIR.glob('*.tsv'))
        if path.name.lower().startswith(TARGET_PREFIXES) and path.name not in SKIP_FILES
    ]
    for path in target_files:
        with path.open(encoding='utf-8') as handle:
            reader = csv.DictReader(handle, delimiter='\t')
            fieldnames = reader.fieldnames or []
            rows = list(reader)
        if not rows:
            continue
        lemma = rows[0]['lemma']
        if lemma not in LEMMA_TRANSLATIONS:
            raise KeyError(f'Missing translation for lemma {lemma} in {path.name}')
        base_translation = LEMMA_TRANSLATIONS[lemma]
        for row in rows:
            row['translation_en'] = format_translation(base_translation, row, gender_map)
            row['pronunciation'] = pronounce_word(row['text'])
        with path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)


if __name__ == '__main__':
    main()
