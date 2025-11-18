#!/usr/bin/env python3
import csv
from pathlib import Path
from typing import Dict, Optional

BASE_DIR = Path('vocabulary/dataframes/VERB')

IRREGULAR = {
    'be': {
        'present': {('1', 'Sing'): 'am', ('2', 'Sing'): 'are', ('3', 'Sing'): 'is',
                     ('1', 'Plur'): 'are', ('2', 'Plur'): 'are', ('3', 'Plur'): 'are'},
        'past': {('1', 'Sing'): 'was', ('2', 'Sing'): 'were', ('3', 'Sing'): 'was',
                 ('1', 'Plur'): 'were', ('2', 'Plur'): 'were', ('3', 'Plur'): 'were'},
        'part': 'been',
        'gerund': 'being',
    },
    'have': {'present3': 'has', 'past': 'had', 'part': 'had', 'gerund': 'having'},
    'do': {'present3': 'does', 'past': 'did', 'part': 'done', 'gerund': 'doing'},
    'go': {'past': 'went', 'part': 'gone', 'gerund': 'going'},
    'get': {'present3': 'gets', 'past': 'got', 'part': 'gotten', 'gerund': 'getting'},
    'make': {'present3': 'makes', 'past': 'made', 'part': 'made', 'gerund': 'making'},
    'take': {'present3': 'takes', 'past': 'took', 'part': 'taken', 'gerund': 'taking'},
    'come': {'present3': 'comes', 'past': 'came', 'part': 'come', 'gerund': 'coming'},
    'run': {'present3': 'runs', 'past': 'ran', 'part': 'run', 'gerund': 'running'},
    'build': {'present3': 'builds', 'past': 'built', 'part': 'built', 'gerund': 'building'},
    'write': {'present3': 'writes', 'past': 'wrote', 'part': 'written', 'gerund': 'writing'},
    'read': {'present3': 'reads', 'past': 'read', 'part': 'read', 'gerund': 'reading'},
    'buy': {'present3': 'buys', 'past': 'bought', 'part': 'bought', 'gerund': 'buying'},
    'sell': {'present3': 'sells', 'past': 'sold', 'part': 'sold', 'gerund': 'selling'},
    'pay': {'present3': 'pays', 'past': 'paid', 'part': 'paid', 'gerund': 'paying'},
    'hold': {'present3': 'holds', 'past': 'held', 'part': 'held', 'gerund': 'holding'},
    'leave': {'present3': 'leaves', 'past': 'left', 'part': 'left', 'gerund': 'leaving'},
    'lead': {'present3': 'leads', 'past': 'led', 'part': 'led', 'gerund': 'leading'},
    'meet': {'present3': 'meets', 'past': 'met', 'part': 'met', 'gerund': 'meeting'},
    'hear': {'present3': 'hears', 'past': 'heard', 'part': 'heard', 'gerund': 'hearing'},
    'keep': {'present3': 'keeps', 'past': 'kept', 'part': 'kept', 'gerund': 'keeping'},
    'sleep': {'present3': 'sleeps', 'past': 'slept', 'part': 'slept', 'gerund': 'sleeping'},
    'feel': {'present3': 'feels', 'past': 'felt', 'part': 'felt', 'gerund': 'feeling'},
    'win': {'present3': 'wins', 'past': 'won', 'part': 'won', 'gerund': 'winning'},
    'lose': {'present3': 'loses', 'past': 'lost', 'part': 'lost', 'gerund': 'losing'},
    'teach': {'present3': 'teaches', 'past': 'taught', 'part': 'taught', 'gerund': 'teaching'},
    'say': {'present3': 'says', 'past': 'said', 'part': 'said', 'gerund': 'saying'},
    'see': {'present3': 'sees', 'past': 'saw', 'part': 'seen', 'gerund': 'seeing'},
    'sit': {'present3': 'sits', 'past': 'sat', 'part': 'sat', 'gerund': 'sitting'},
    'stand': {'present3': 'stands', 'past': 'stood', 'part': 'stood', 'gerund': 'standing'},
    'understand': {'present3': 'understands', 'past': 'understood', 'part': 'understood', 'gerund': 'understanding'},
    'wear': {'present3': 'wears', 'past': 'wore', 'part': 'worn', 'gerund': 'wearing'},
    'eat': {'present3': 'eats', 'past': 'ate', 'part': 'eaten', 'gerund': 'eating'},
}

CONFIG: Dict[str, Dict] = {
    'celebrare': {'base': 'celebrate'},
    'celebrire': {'base': 'celebrate'},
    'cena': {'base': 'eat dinner'},
    'cenarare': {'base': 'have dinner'},
    'cenerare': {'base': 'have dinner'},
    'censurare': {'base': 'censor'},
    'cercalare': {'base': 'look up'},
    'cercalere': {'base': 'look for'},
    'cerchiare': {
        'base': 'look for',
        'overrides': {
            'cerchiamolo': "let's look it up",
        },
    },
    'cessare': {'base': 'cease'},
    'cessiare': {'base': 'cease'},
    'chattare': {'base': 'chat'},
    'chiacchiare': {'base': 'chat'},
    'chiacchierare': {'base': 'chat'},
    'chiacchierato': {'base': 'chat'},
    'chiamalare': {
        'base': 'call',
        'overrides': {'chiamala': '(you) call her'},
    },
    'chiamale': {
        'base': 'call',
        'overrides': {'chiamale': '(you) call them'},
    },
    'chiamalere': {
        'base': 'call',
        'overrides': {'chiamale': '(you) call them'},
    },
    'chiamatemere': {
        'base': 'call',
        'overrides': {'chiamatemi': '(you (plural)) call me'},
    },
    'chiamiare': {'base': 'call'},
    'chiarire': {'base': 'clarify'},
    'chiederemelo': {
        'base': 'ask',
        'overrides': {'chiedetemelo': 'ask me again later'},
    },
    'chiediamoci': {
        'base': 'ask ourselves',
        'overrides': {'chiediamoci': "let's ask ourselves"},
    },
    'chinare': {'base': 'bend down'},
    'cibare': {'base': 'feed on'},
    'cicciotere': {
        'base': 'be chubby',
        'overrides': {'cicciottello': 'he/she/it is chubby'},
    },
    'cigolare': {'base': 'creak'},
    'cinare': {'base': 'have dinner'},
    'cinguettare': {'base': 'chirp'},
    'circolare': {'base': 'circulate'},
    'circondare': {'base': 'surround'},
    'circoscrivere': {'base': 'circumscribe'},
    'citamare': {
        'base': 'quote',
        'overrides': {'citami': 'quote me an example'},
    },
    'citare': {'base': 'quote'},
    'citatemere': {
        'base': 'quote',
        'overrides': {'citatemi': 'quote me an example'},
    },
    'ciucciamare': {
        'base': 'suck',
        'overrides': {'ciucciami': 'suck my dick'},
    },
    'ciucciare': {
        'base': 'suck',
        'overrides': {
            'ciucciatemi': 'suck my dick',
            'ciucci': '(you) suck my dick',
        },
    },
    'civilizzare': {'base': 'civilize'},
    'classificare': {'base': 'classify'},
    'cliccamare': {
        'base': 'click',
        'overrides': {'cliccami': 'click me'},
    },
    'cliccare': {'base': 'click'},
    'cliccatemere': {
        'base': 'click',
        'overrides': {'cliccatemi': 'click me'},
    },
    'clonare': {'base': 'clone'},
    'coccare': {'base': 'catch'},
    'coccolare': {'base': 'pamper'},
    'cogliere': {'base': 'catch'},
    'coincidere': {'base': 'coincide'},
    'coinvolgere': {'base': 'involve'},
    'colare': {'base': 'drip'},
    'colgare': {'base': 'take advantage'},
    'collaborare': {'base': 'collaborate'},
    'collassare': {'base': 'collapse'},
    'collaudare': {'base': 'test'},
    'collegare': {'base': 'connect'},
    'collezionare': {'base': 'collect'},
    'colmare': {'base': 'fill'},
    'colonizzare': {'base': 'colonize'},
    'colorare': {'base': 'color'},
    'coltire': {'base': 'cultivate'},
    'coltivare': {'base': 'cultivate'},
    'comandare': {'base': 'command'},
    'combare': {'base': 'fight'},
    'combinare': {'base': 'combine'},
    'commentare': {'base': 'comment'},
    'commento': {'base': 'comment'},
    'commerciare': {'base': 'trade'},
    'commettere': {'base': 'commit'},
    'commiere': {'base': 'commit'},
    'commuovere': {'base': 'move'},
    'comparare': {'base': 'compare'},
    'comparato': {'base': 'compare'},
    'comparire': {'base': 'appear'},
    'compatire': {'base': 'pity'},
    'compattare': {'base': 'compact'},
    'compenire': {'base': 'compensate'},
    'compensare': {'base': 'compensate'},
    'comperare': {'base': 'buy'},
    'compere': {'base': 'go shopping'},
    'competere': {'base': 'compete'},
    'compiacere': {'base': 'please'},
    'compiere': {'base': 'carry out'},
    'compilare': {'base': 'compile'},
    'compile': {'base': 'fill out'},
    'compirare': {'base': 'turn'},
    'completare': {'base': 'complete'},
    'completo': {'base': 'complete'},
    'complicare': {'base': 'complicate'},
    'complicato': {'base': 'complicate'},
    'complottare': {'base': 'plot'},
    'compormiare': {'base': 'dial'},
    'comporre': {'base': 'compose'},
    'compramare': {
        'base': 'buy',
        'overrides': {'comprami': 'buy me one'},
    },
    'comprassimare': {'base': 'buy'},
    'compratemere': {
        'base': 'buy',
        'overrides': {'compratemi': 'buy me one'},
    },
    'comprere': {'base': 'buy'},
    'comunicare': {'base': 'communicate'},
    'concedere': {'base': 'grant'},
    'concentrare': {'base': 'concentrate'},
    'concepire': {'base': 'conceive'},
    'concluderare': {'base': 'finish'},
    'concludere': {'base': 'conclude'},
    'concordare': {'base': 'agree'},
    'condannare': {'base': 'condemn'},
    'condannato': {'base': 'condemn'},
    'condare': {'base': 'sentence'},
    'condire': {'base': 'season'},
    'condurre': {'base': 'lead'},
    'confermare': {'base': 'confirm'},
    'confermerare': {'base': 'confirm'},
    'confermiare': {'base': 'confirm'},
    'confessare': {'base': 'confess'},
    'confesso': {'base': 'confess'},
    'confezionare': {'base': 'pack'},
    'confidare': {'base': 'confide'},
    'confiderare': {'base': 'impart'},
    'configurare': {'base': 'configure'},
    'confinare': {'base': 'confine'},
    'confiscare': {'base': 'confiscate'},
    'confondare': {'base': 'confuse'},
    'confondere': {'base': 'confuse'},
    'conforgere': {'base': 'comfort'},
    'confortare': {'base': 'comfort'},
    'confrontare': {'base': 'confront'},
    'confundere': {'base': 'puzzle'},
    'confuso': {'base': 'confuse'},
    'confutare': {'base': 'refute'},
    'congelare': {'base': 'freeze'},
    'congratulare': {'base': 'congratulate'},
    'coniare': {'base': 'coin'},
    'connettere': {'base': 'connect'},
    'conosciare': {'base': 'know'},
    'conquista': {'base': 'conquer'},
    'conquistare': {'base': 'conquer'},
    'consegnare': {'base': 'deliver'},
    'conseguire': {'base': 'follow'},
    'consentire': {'base': 'allow'},
    'conservare': {'base': 'preserve'},
    'considerale': {'base': 'consider'},
    'consideriare': {'base': 'consider'},
    'consigliere': {'base': 'advise'},
    'consistere': {'base': 'consist'},
    'consolare': {'base': 'console'},
    'consolato': {'base': 'console'},
    'consultare': {'base': 'consult'},
    'consumare': {'base': 'consume'},
    'consumato': {'base': 'wear out'},
    'contaminare': {'base': 'contaminate'},
    'contato': {'base': 'count'},
    'contattare': {'base': 'contact'},
    'contatto': {'base': 'contact'},
    'contemplare': {'base': 'contemplate'},
    'contenente': {'base': 'contain'},
    'contenere': {'base': 'contain'},
    'contento': {'base': 'be content'},
    'contestato': {'base': 'contest'},
    'conto': {'base': 'count'},
    'contorcere': {'base': 'twist'},
    'contraddare': {'base': 'contradict'},
    'contraddire': {'base': 'contradict'},
    'contraffare': {'base': 'counterfeit'},
    'contrare': {'base': 'contract'},
    'contrarre': {'base': 'contract'},
    'contrattaccare': {'base': 'strike back'},
    'contribuire': {'base': 'contribute'},
    'contrire': {'base': 'repent'},
    'controllalare': {'base': 'check'},
    'controllatevere': {'base': 'control yourselves'},
    'controllo': {'base': 'check'},
    'controvere': {'base': 'be controversial'},
    'convalidare': {'base': 'validate'},
    'convenire': {'base': 'behoove'},
    'convergere': {'base': 'converge'},
    'converire': {'base': 'converse'},
    'conversare': {'base': 'converse'},
    'convertire': {'base': 'convert'},
    'convicere': {'base': 'convince'},
    'convivare': {'base': 'live together'},
    'convocare': {'base': 'convene'},
    'convogliare': {'base': 'convey'},
    'cooperare': {'base': 'cooperate'},
    'coordinare': {'base': 'coordinate'},
    'copiare': {'base': 'copy'},
    'copire': {'base': 'copy'},
    'coplire': {'base': 'strike'},
    'coprimere': {'base': 'cover'},
    'coprire': {'base': 'cover'},
    'copritemere': {
        'base': 'cover',
        'overrides': {'copritemi': 'cover my back'},
    },
    'copritevere': {
        'base': 'cover',
        'overrides': {'copritevi': 'cover your eyes'},
    },
    'coricare': {'base': 'lie down'},
    'correggendoce': {
        'base': 'correct',
        'overrides': {'correggendoce': 'correcting one another'},
    },
    'correggetemere': {
        'base': 'correct',
        'overrides': {'correggetemi': '(you (plural)) correct me'},
    },
    'correggile': {
        'base': 'correct',
        'overrides': {'correggile': '(you) correct them'},
    },
    'correggimare': {
        'base': 'correct',
        'overrides': {'correggimi': '(you) correct me'},
    },
    'correlare': {'base': 'correlate'},
    'corrispondere': {'base': 'correspond'},
    'corromperare': {
        'base': 'bribe',
        'overrides': {'corrompermi': 'bribe me'},
    },
    'corrompere': {'base': 'bribe'},
    'corrotto': {'base': 'bribe'},
    'corruppire': {'base': 'bribe'},
    'corsa': {'base': 'take a risk'},
    'corto': {'base': 'be short'},
    'cospirare': {'base': 'conspire'},
    'costeggiare': {'base': 'run along'},
    'costituire': {'base': 'constitute'},
    'costringere': {'base': 'force'},
    'costrutto': {'base': 'construct'},
    'crebrire': {'base': 'grow up'},
    'criptare': {'base': 'encrypt'},
    'criticare': {'base': 'criticize'},
    'critico': {'base': 'criticize'},
    'crogiolare': {'base': 'bask'},
    'crollare': {'base': 'collapse'},
    'crudere': {'base': 'be cruel'},
    'cucina': {'base': 'cook'},
    'cucire': {'base': 'sew'},
    'culminare': {'base': 'culminate'},
    'cuocere': {'base': 'bake'},
    'curare': {'base': 'cure'},
    'curiosare': {'base': 'pry'},
    'curva': {'base': 'curve'},
}

SUBJECTS = {
    ('1', 'Sing'): 'I',
    ('2', 'Sing'): 'you',
    ('3', 'Sing'): 'he/she/it',
    ('1', 'Plur'): 'we',
    ('2', 'Plur'): 'you (plural)',
    ('3', 'Plur'): 'they',
}

IMPERATIVE_SUBJECTS = {
    ('2', 'Sing'): '(you)',
    ('2', 'Plur'): '(you (plural))',
    ('1', 'Plur'): "let's",
    ('3', 'Sing'): 'let him/her/it',
    ('3', 'Plur'): 'let them',
}


def parse_feats(feats: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if feats:
        for item in feats.split('|'):
            if '=' in item:
                key, value = item.split('=', 1)
                result[key] = value
    return result


def split_base_phrase(phrase: str):
    parts = phrase.strip().split()
    if not parts:
        return '', ''
    head = parts[0]
    tail = ' ' + ' '.join(parts[1:]) if len(parts) > 1 else ''
    return head.lower(), tail


def third_person_form(head: str, number: str, person: str) -> str:
    entry = IRREGULAR.get(head)
    if entry and 'present' in entry:
        return entry['present'].get((person, number), head)
    if entry and 'present3' in entry and person == '3' and number == 'Sing':
        return entry['present3']
    if person == '3' and number == 'Sing':
        if head.endswith('y') and head[-2] not in 'aeiou':
            return head[:-1] + 'ies'
        if head.endswith(('s', 'sh', 'ch', 'x', 'z', 'o')):
            return head + 'es'
        return head + 's'
    return head


def simple_past(head: str) -> str:
    entry = IRREGULAR.get(head)
    if entry and 'past' in entry:
        past = entry['past']
        if isinstance(past, dict):
            return past.get(('3', 'Sing'), past.get(('1', 'Sing'), head))
        return past
    if head.endswith('e'):
        return head + 'd'
    if head.endswith('y') and head[-2] not in 'aeiou':
        return head[:-1] + 'ied'
    if (len(head) >= 3 and head[-1] not in 'aeiouwxy' and
            head[-2] in 'aeiou' and head[-3] not in 'aeiou'):
        return head + head[-1] + 'ed'
    return head + 'ed'


def past_participle(head: str) -> str:
    entry = IRREGULAR.get(head)
    if entry and 'part' in entry:
        return entry['part']
    return simple_past(head)


def gerund_form(head: str) -> str:
    entry = IRREGULAR.get(head)
    if entry and 'gerund' in entry:
        return entry['gerund']
    if head.endswith('ie'):
        return head[:-2] + 'ying'
    if head.endswith('e') and head not in {'be', 'see'}:
        return head[:-1] + 'ing'
    if (len(head) >= 3 and head[-1] not in 'aeiouwxy' and
            head[-2] in 'aeiou' and head[-3] not in 'aeiou'):
        return head + head[-1] + 'ing'
    return head + 'ing'


def build_phrase(head_form: str, tail: str) -> str:
    return (head_form + tail).strip()


def add_markers(text: str, feats: Dict[str, str]) -> str:
    gender = feats.get('Gender')
    number = feats.get('Number')
    suffix = ''
    if gender == 'Masc':
        suffix += ' (masculine)'
    elif gender == 'Fem':
        suffix += ' (feminine)'
    if number == 'Plur':
        suffix += ' (plural)'
    return text + suffix


def translate_row(row: Dict[str, str], cfg: Dict[str, str]) -> Optional[str]:
    text = row['text']
    overrides = cfg.get('overrides', {})
    if text in overrides:
        return overrides[text]
    feats = parse_feats(row['feats'])
    base_phrase = cfg['base']
    head, tail = split_base_phrase(base_phrase)
    verb_form = feats.get('VerbForm')
    if verb_form == 'Inf':
        return cfg.get('inf') or f"to {base_phrase}"
    if verb_form == 'Ger':
        ger = cfg.get('gerund') or gerund_form(head)
        return build_phrase(ger, tail)
    if verb_form == 'Part':
        part = cfg.get('participle') or past_participle(head)
        return add_markers(build_phrase(part, tail), feats)
    person = feats.get('Person')
    number = feats.get('Number')
    mood = feats.get('Mood')
    tense = feats.get('Tense')
    if not person or not number or not mood:
        return None
    subject = SUBJECTS.get((person, number))
    if not subject:
        return None
    if mood == 'Imp':
        imp_subj = IMPERATIVE_SUBJECTS.get((person, number))
        if not imp_subj:
            return None
        form = build_phrase(head, tail)
        return f"{imp_subj} {form}!".strip()
    if mood == 'Cnd':
        form = build_phrase(head, tail)
        return f"{subject} would {form}".replace('would to', 'would')
    if mood == 'Sub':
        if tense == 'Imp':
            base = build_phrase(simple_past(head), tail)
        else:
            base = build_phrase(head, tail)
        return f"that {subject.lower()} {base}"
    if mood == 'Ind':
        if tense == 'Fut':
            form = build_phrase(head, tail)
            return f"{subject} will {form}"
        if tense in {'Past', 'Imp'}:
            past = cfg.get('past') or simple_past(head)
            return f"{subject} {build_phrase(past, tail)}"
        present = third_person_form(head, number, person)
        return f"{subject} {build_phrase(present, tail)}"
    return None


def update_file(path: Path) -> None:
    lemma = path.stem
    cfg = CONFIG.get(lemma)
    if not cfg:
        return
    with path.open() as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        rows = list(reader)
    changed = False
    for row in rows:
        if row['translation_en'].strip():
            continue
        translation = translate_row(row, cfg)
        if translation:
            row['translation_en'] = translation
            changed = True
    if changed:
        with path.open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys(), delimiter='\t')
            writer.writeheader()
            writer.writerows(rows)


def main():
    for path in sorted(BASE_DIR.glob('c*.tsv')):
        update_file(path)

if __name__ == '__main__':
    main()
