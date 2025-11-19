#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word

ROOT = Path('vocabulary/dataframes/ADJ')
TRANSLATIONS = {
    'audace': 'bold; daring',
    'audio': 'audio; sound-related',
    'aumentato': 'augmented; increased',
    'auspicabile': 'desirable; advisable',
    'austero': 'austere; strict; somber',
    'australiano': 'Australian',
    'austriaco': 'Austrian',
    'autentico': 'authentic; genuine; real',
    'autobiografico': 'autobiographical',
    'autoctone': 'indigenous; native',
    'automatico': 'automatic',
    'automatizzare': 'automated; mechanized',
    'automatizzato': 'automated; mechanized',
    'automobilistico': 'automotive; motoring',
    'autonomo': 'autonomous; independent',
    'autorevole': 'authoritative; respected',
    'autoritario': 'authoritarian; dictatorial',
    'autosufficiento': 'self-sufficient',
    'autunnale': 'autumnal; fall',
    'avanzato': 'advanced',
    'avario': 'stingy; miserly',
    'avaro': 'stingy; miserly',
    'avido': 'greedy; avid',
    'avvelenato': 'poisoned; tainted',
    'avventuroso': 'adventurous',
    'avvilito': 'dejected; downcast',
    'avvisare': 'forewarned; alerted',
    'aziendale': 'corporate; company-related',
    'azionario': 'stock-market; equity',
    'azteco': 'Aztec',
    'azzurro': 'sky-blue; blue',
    'azzurrognolo': 'bluish; sky-blueish',
    'bagnare': 'wet; soaked',
    'bagnata': 'wet; soaked',
    'bagnate': 'wet; soaked',
    'bagnato': 'wet; soaked',
    'balilla': 'foosball; table-soccer',
    'balistico': 'ballistic',
    'baltico': 'Baltic',
    'banale': 'banal; trivial',
    'bancario': 'banking; financial',
    'bandito': 'outlawed; banned',
    'barbaro': 'barbaric; savage',
    'barcollante': 'staggering; unsteady',
    'baschio': 'Basque',
    'basilare': 'basic; fundamental',
    'basso': 'low; short',
    'bastardo': 'bastardly; vile',
    'beglio': 'beautiful; nice',
    'belga': 'Belgian',
    'belligerante': 'belligerent; warlike',
    'belloccio': 'good-looking; cute',
    'benedetto': 'blessed',
    'benestante': 'well-off; affluent',
    'beninese': 'Beninese',
    'bentornato': 'welcome back',
    'benvenuto': 'welcome',
    'berbero': 'Berber',
    'bevetto': 'drank',
    'biblico': 'biblical',
    'bielorussa': 'Belarusian',
    'bielorusso': 'Belarusian',
    'bilingue': 'bilingual',
    'binaurale': 'binaural',
    'biochimico': 'biochemical',
    'biologico': 'biological',
    'biondo': 'blond; fair-haired',
    'birichino': 'mischievous; naughty',
    'bisessuale': 'bisexual',
    'bisestile': 'leap-year',
    'bisognoso': 'needy; destitute',
    'biunivoco': 'one-to-one; bijective',
    'bizzarro': 'bizarre; eccentric',
    'blasfemo': 'blasphemous; profane',
    'blatto': 'cockroach',
    'boccalone': 'gullible; big-mouthed',
    'bolla': 'bubble; blistered',
    'bollente': 'boiling; scalding',
    'bollito': 'boiled',
    'boreale': 'northern; boreal',
    'borioso': 'boastful; arrogant',
    'boschivo': 'wooded; forest',
    'bosniaco': 'Bosnian',
    'bovino': 'bovine; cattle',
    'bramoso': 'eager; craving',
    'brasiliano': 'Brazilian',
    'brillante': 'brilliant; bright',
    'brillo': 'tipsy; buzzed',
    'britannico': 'British',
    'brontolone': 'grumpy; grouchy',
    'bruciato': 'burnt; scorched',
    'brulicante': 'teeming; swarming',
    'brusco': 'abrupt; curt',
    'brutale': 'brutal; cruel',
    'buddista': 'Buddhist',
    'buffo': 'funny; comical',
    'bugiardo': 'lying; deceitful',
    'bui': 'dark',
    'buio': 'dark',
    'bulgaro': 'Bulgarian',
    'burbero': 'gruff; surly',
    'burocratico': 'bureaucratic',
    'cadente': 'crumbling; decaying',
    'cadere': 'fallen; dropped',
    'caduto': 'fallen; fallen in battle',
    'calloso': 'calloused; thick-skinned',
    'calmo': 'calm; quiet',
    'caloroso': 'warm; heartfelt',
    'calvo': 'bald',
    'calzante': 'well-fitting; apt',
    'cameriera': 'waitress',
    'canadese': 'Canadian',
    'canaglia': 'rascally; rogue',
    'cancerogeno': 'carcinogenic',
    'candidare': 'nominated; put forward',
    'candido': 'candid; pure; white',
    'cangiante': 'iridescent; shifting',
    'cannibale': 'cannibalistic',
    'canoro': 'melodious; singing',
    'cantonese': 'Cantonese',
    'caotico': 'chaotic',
    'caparbio': 'stubborn; obstinate',
    'capitale': 'capital; chief',
    'capitalista': 'capitalist',
    'capriccioso': 'capricious; whimsical',
    'caratteristico': 'characteristic; typical',
    'carbonico': 'carbonic; carbon-based',
    'carcerario': 'prison; penal',
    'cardiaco': 'cardiac; heart-related',
    'carente': 'lacking; deficient',
    'cargo': 'cargo; freight',
    'carico': 'loaded; charged; full',
    'carina': 'pretty; cute',
    'carine': 'pretty; cute',
    'carismatico': 'charismatic',
    'carnale': 'carnal; fleshly',
    'carnivore': 'carnivorous',
    'caro': 'dear; expensive',
    'cartaceo': 'paper; on-paper',
    'casalingo': 'homemade; household',
    'casereccio': 'homemade; rustic',
    'castano': 'brown-haired; chestnut',
    'castigliano': 'Castilian; Spanish',
    'casuale': 'random; casual',
    'catastrofico': 'catastrophic; disastrous',
    'cattolica': 'Catholic',
    'cattolico': 'Catholic',
    'catturalo': 'catch it',
    'caucasico': 'Caucasian',
    'causale': 'causal',
    'caustico': 'caustic; biting',
    'cauto': 'cautious; careful',
    'cavalleresco': 'chivalrous',
    'cazzuto': 'badass; gutsy',
    'celebre': 'famous; renowned',
    'celesto': 'celestial; heavenly',
    'celibo': 'celibate; unmarried',
    'cellulare': 'cellular; mobile',
    'celtico': 'Celtic',
    'censurato': 'censored; banned',
    'centesimo': 'hundredth; cent',
    'centigrado': 'centigrade; Celsius',
    'centrale': 'central; main',
    'centrifugo': 'centrifugal',
    'cerebrale': 'cerebral; brain',
    'cerebroleso': 'brain-damaged',
    'certo': 'certain; sure',
    'cervicale': 'cervical; neck',
    'chiacchierone': 'talkative; chatty',
    'chiacchierono': 'talkative; chatty',
    'chimerico': 'chimerical; fanciful',
    'chimico': 'chemical',
    'chirurgico': 'surgical',
    'chiuso': 'closed; shut',
    'ciarliero': 'garrulous; talkative',
    'ciccì': 'sweetie; cutie',
    'ciclabile': 'cycle-friendly; bikeable',
    'ciclico': 'cyclical',
    'ciclistico': 'cycling; bike-related',
    'cieco': 'blind',
    'cinematografico': 'cinematic; film',
    'cinico': 'cynical',
    'cinquantenne': 'fifty-year-old',
    'cinquemillesimo': 'five-thousandth',
    'cinto': 'belted; girded',
    'ciondolante': 'dangling; drooping',
    'circadiano': 'circadian',
    'circospetto': 'cautious; wary',
    'circostante': 'surrounding; nearby',
    'cirillico': 'Cyrillic',
    'cisterna': 'tanker',
    'citato': 'cited; quoted',
    'cittadina': 'town; civic',
    'cittadino': 'civic; citizen',
    'civico': 'civic',
    'civile': 'civil',
    'civilizzato': 'civilized',
}

SHORT_FORM = {'beglio'}


def parse_feats(feats: str) -> dict[str, str]:
    data: dict[str, str] = {}
    if feats:
        for part in feats.split('|'):
            if '=' in part:
                key, value = part.split('=', 1)
                data[key] = value
    return data


def format_note(gender: str | None, number: str | None, text: str, include_gender: bool, extra: list[str]) -> str:
    bits: list[str] = []
    if number == 'Plur':
        if include_gender and gender in {'Fem', 'Masc'}:
            bits.append(('feminine' if gender == 'Fem' else 'masculine') + ' plural')
        else:
            bits.append('plural')
    else:
        if include_gender and gender in {'Fem', 'Masc'}:
            bits.append('feminine' if gender == 'Fem' else 'masculine')
    bits.extend(extra)
    return f" ({', '.join(bits)})" if bits else ''


def update_file(stem: str) -> None:
    path = ROOT / f"{stem}.tsv"
    base = TRANSLATIONS[stem]
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    genders = set()
    for row in rows:
        feats = parse_feats(row.get('feats', ''))
        gender = feats.get('Gender')
        if gender in {'Fem', 'Masc'}:
            genders.add(gender)
    include_gender = len(genders) > 1
    for row in rows:
        text = row.get('text', '')
        feats = parse_feats(row.get('feats', ''))
        gender = feats.get('Gender')
        number = feats.get('Number')
        extra: list[str] = []
        if stem in SHORT_FORM:
            extra.append('short form')
        if "'" in text:
            extra.append('before a vowel')
        note = format_note(gender, number, text, include_gender, extra)
        row['translation_en'] = base + note
        row['pronunciation'] = pronounce_word(text)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    targets = sorted(TRANSLATIONS)
    for stem in targets:
        update_file(stem)


if __name__ == '__main__':
    main()
