from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word

BASE_PATH = Path('vocabulary/dataframes/ADJ')

LEMMA_SENSES = {
    'diabetico': 'diabetic',
    'diagnostico': 'diagnostic',
    'diagonale': 'diagonal',
    'dialettale': 'dialectal; relating to dialects',
    'dichiarato': 'declared; stated',
    'diciannovesimo': 'nineteenth',
    'diciassettesimo': 'seventeenth',
    'diciottesimo': 'eighteenth',
    'didattica': 'didactic; educational',
    'difendibile': 'defensible',
    'difensivo': 'defensive',
    'difensore': 'defense; defending',
    'difettoso': 'defective; faulty',
    'differente': 'different; distinct',
    'differenziale': 'differential',
    'differenziare': 'differentiated',
    'differenziato': 'differentiated',
    'diffidente': 'suspicious; mistrustful',
    'diffuso': 'widespread; diffuse',
    'digitale': 'digital',
    'dilagante': 'rampant; spreading',
    'diligente': 'diligent; hardworking',
    'dimostrabile': 'demonstrable; provable',
    'dipendente': 'dependent; reliant',
    'diplomatico': 'diplomatic',
    'diretto': 'direct; straightforward',
    'disarmante': 'disarming',
    'disarmato': 'unarmed',
    'disastroso': 'disastrous; catastrophic',
    'disattento': 'careless; inattentive',
    'discografico': 'record-label; recording-industry',
    'discreto': 'discreet; decent',
    'discutibile': 'debatable; questionable',
    'diseguale': 'unequal; uneven',
    'disgustato': 'disgusted',
    'disgusto': 'disgusting; gross',
    'disgustoso': 'disgusting; gross',
    'disinformato': 'uninformed; misinformed',
    'disinteressato': 'disinterested; selfless',
    'disinvolto': 'carefree; nonchalant',
    'disobbediente': 'disobedient',
    'disoccupato': 'unemployed',
    'disonesto': 'dishonest',
    'disordinato': 'messy; untidy',
    'disorganizzato': 'disorganized',
    'disorientato': 'disoriented; confused',
    'dispari': 'odd-numbered; uneven',
    'disperato': 'desperate; hopeless',
    'dispettoso': 'spiteful; mischievous',
    'dispiacere': 'sorry; regretful',
    'dispiaciuta': 'sorry; regretful',
    'disponibile': 'available; willing',
    'disposto': 'willing; prepared',
    'dispotico': 'despotic; tyrannical',
    'disprezzabile': 'despicable; contemptible',
    'dissociativo': 'dissociative',
    'distaccato': 'detached; aloof',
    'distante': 'distant; far away',
    'distinguibile': 'distinguishable',
    'distintivo': 'distinctive; identifying',
    'distinto': 'distinguished; separate',
    'distorcere': 'distorted; twisted',
    'distratto': 'distracted; absent-minded',
    'disturbante': 'disturbing',
    'disturbare': 'disturbed',
    'diver': 'different; varied',
    'divertito': 'amused; entertained',
    'divino': 'divine; heavenly',
    'divorziato': 'divorced',
    'docile': 'docile; gentle',
    'dodicenne': 'twelve-year-old',
    'dodicesimo': 'twelfth',
    'dolce': 'sweet; gentle',
    'dolente': 'sorrowful; aching',
    'doloroso': 'painful; sore',
    'domestico': 'domestic; household',
    'dominante': 'dominant; prevailing',
    'donante': 'giving; generous',
    'donare': 'given; donated',
    'doppio': 'double; twofold',
    'dorato': 'golden; gilded',
    'dorsale': 'dorsal; back',
    'dotare': 'equipped; endowed',
    'dotato': 'gifted; well-equipped',
    'dotto': 'learned; erudite',
    'dovuto': 'due; owed',
    'drammatico': 'dramatic',
    'drastico': 'drastic; severe',
    'dritta': 'straight',
    'dritto': 'straight; upright',
    'drogare': 'drugged; high on drugs',
    'drogato': 'drugged; addicted',
    'dubbio': 'dubious; doubtful',
    'dubbioso': 'doubtful; skeptical',
    'duraturo': 'lasting; enduring',
    'ebola': 'Ebola; related to the Ebola virus',
    'ebraico': 'Hebrew; Jewish',
    'ebreo': 'Jewish',
    'eccellente': 'excellent; outstanding',
    'eccento': 'eccentric',
    'eccessivo': 'excessive; too much',
    'eccezionale': 'exceptional; outstanding',
    'eccitante': 'exciting; thrilling',
    'eccitare': 'excited; aroused',
    'eccitato': 'excited; aroused',
    'eccola': 'here she is; there she is',
    'eccolo': 'here he is; here it is',
    'ecologico': 'environmental; eco-friendly',
    'edile': 'building; construction',
    'editore': "publishing; publisher's",
    'educativo': 'educational',
    'educato': 'polite; well-mannered',
    'effeminato': 'effeminate',
    'effemminato': 'effeminate',
    'effettivo': 'effective; actual',
    'efficace': 'effective; efficacious',
    'efficiente': 'efficient',
    'effimero': 'ephemeral; short-lived',
    'egiziano': 'Egyptian',
    'egizio': 'Egyptian; ancient Egyptian',
    'egocentrico': 'egocentric; self-centered',
    'egoista': 'selfish',
    'egoisto': 'selfish',
    'eguale': 'equal',
    'elastico': 'elastic; flexible',
    'elboniano': 'Elbonian; from Elbonia',
    'elegante': 'elegant; stylish',
    'eleggere': 'elected; chosen',
    'elementare': 'elementary; basic',
    'elettorale': 'electoral',
    'elettrico': 'electric; electrical',
    'elettronico': 'electronic',
    'elevato': 'elevated; high',
    'elevatore': 'forklift; lifting',
    'elfico': 'elvish; elf-like',
    'ellittico': 'elliptical',
    'eloquente': 'eloquent',
    'emaciato': 'emaciated',
    'emblematico': 'emblematic; symbolic',
    'emersoniano': 'Emersonian',
    'eminente': 'eminent; distinguished',
    'emotivo': 'emotional; emotive',
    'emozionale': 'emotional; affective',
    'emozionante': 'exciting; thrilling',
    'emozionato': 'excited; moved',
    'enciclopedico': 'encyclopedic',
    'energetico': 'energetic; energy-related',
    'energico': 'energetic; vigorous',
    'enorme': 'huge; enormous',
    'entrata': 'entered; having come in',
    'entusiasmante': 'exciting; inspiring',
    'entusiasta': 'enthusiastic',
    'epicantico': 'epicanthic',
    'epico': 'epic',
    'epidermico': 'epidermal; of the skin',
    'epilettico': 'epileptic',
    'epizootico': 'epizootic',
    'epocale': 'epoch-making; momentous',
    'equilibrato': 'balanced',
    'equivalente': 'equivalent',
    'equivoco': 'ambiguous; equivocal',
    'equo': 'fair; equitable',
    'ereditario': 'hereditary',
    'ermetico': 'hermetic; airtight',
    'eroico': 'heroic',
    'erotico': 'erotic',
    'errato': 'incorrect; wrong',
    'erudito': 'erudite; scholarly',
    'esagerato': 'exaggerated; over the top',
    'esagonale': 'hexagonal',
    'esaminale': 'examine',
    'esaminatore': 'examining; examiner',
    'esatto': 'exact; correct',
    'esausto': 'exhausted',
    'escatologico': 'eschatological',
    'esclamativo': 'exclamatory',
    'esclusivo': 'exclusive',
    'escluso': 'excluded',
    'esente': 'exempt; free from',
    'esigente': 'demanding',
    'esiguo': 'scant; meager',
    'esilarante': 'hilarious',
    'esile': 'slender; thin',
    'esistente': 'existing',
    'esitante': 'hesitant',
    'esotico': 'exotic',
    'esperanto': 'Esperanto; in Esperanto',
    'esperto': 'expert; skilled',
    'esplicativo': 'explanatory',
    'esplicito': 'explicit; clear',
    'esponenziale': 'exponential',
    'esportabile': 'exportable',
    'espressivo': 'expressive',
    'espresso': 'express; express train',
    'essenziale': 'essential; vital',
    'essiccato': 'dried',
    'estensivo': 'extensive',
    'estenuante': 'exhausting; grueling',
    'esteriore': 'external; outward',
    'esterno': 'external; outside',
    'estero': 'foreign; overseas',
    'esterrefatto': 'astonished; stunned',
    'esteso': 'extensive; widespread',
    'estetico': 'aesthetic',
    'estinguere': 'extinct',
    'estivo': 'summer; summery',
    'estone': 'Estonian',
    'estraneo': 'foreign; unrelated',
    'estrarre': 'drawn; pulled out',
    'estremi': 'extreme',
    'estremo': 'extreme; utmost',
    'estroverso': 'outgoing; extroverted',
}

SPECIAL_TRANSLATIONS = {
    'dolcissimo': 'very sweet; delicious (masculine)',
    'eccole': 'here they are (feminine plural)',
    'esaminali': 'examine them (plural object)',
    'esaminale': 'examine her',
}


def parse_feats(feats: str) -> dict[str, str]:
    data: dict[str, str] = {}
    if feats:
        for part in feats.split('|'):
            if '=' in part:
                key, value = part.split('=', 1)
                data[key] = value
    return data


def build_note(feats: dict[str, str], gendered: bool) -> str:
    note_parts: list[str] = []
    gender = feats.get('Gender')
    number = feats.get('Number')
    if gendered:
        if gender == 'Masc':
            note_parts.append('masculine')
        elif gender == 'Fem':
            note_parts.append('feminine')
    if number == 'Plur':
        note_parts.append('plural')
    return ', '.join(note_parts)


def update_file(path: Path, sense: str) -> None:
    with path.open(encoding='utf-8') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    if not rows or 'text' not in fieldnames:
        return
    if 'pronunciation' not in fieldnames:
        insert_idx = fieldnames.index('text') + 1
        fieldnames.insert(insert_idx, 'pronunciation')
    if 'translation_en' not in fieldnames:
        insert_idx = fieldnames.index('pos') if 'pos' in fieldnames else len(fieldnames)
        fieldnames.insert(insert_idx, 'translation_en')
    genders = set()
    for row in rows:
        feats = parse_feats(row.get('feats', ''))
        gender = feats.get('Gender')
        if gender in {'Masc', 'Fem'}:
            genders.add(gender)
    gendered = len(genders) > 1
    for row in rows:
        text = row.get('text', '')
        translation = SPECIAL_TRANSLATIONS.get(text)
        if not translation:
            feats = parse_feats(row.get('feats', ''))
            note = build_note(feats, gendered)
            translation = sense
            if note:
                translation = f'{translation} ({note})'
        row['translation_en'] = translation
        row['pronunciation'] = pronounce_word(text)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    for lemma, sense in LEMMA_SENSES.items():
        path = BASE_PATH / f'{lemma}.tsv'
        if not path.exists():
            continue
        update_file(path, sense)


if __name__ == '__main__':
    main()
