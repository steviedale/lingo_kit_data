#!/usr/bin/env python3
"""Fill translation_en and pronunciation for IO–MI adjective TSVs."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Tuple

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word
DATA_DIR = REPO_ROOT / 'vocabulary' / 'dataframes' / 'ADJ'

LEMMA_TRANSLATIONS: Dict[str, str] = {
    'iperattivo': 'hyperactive',
    'iperemotivo': 'overly emotional; hyperemotional',
    'ipersensibile': 'hypersensitive',
    'ipocrita': 'hypocritical',
    'iracheno': 'Iraqi',
    'iraniano': 'Iranian',
    'irascibile': 'irascible; quick-tempered',
    'irideo': 'rainbow-colored',
    'irlandese': 'Irish',
    'ironico': 'ironic; sarcastic',
    'irragionevole': 'unreasonable',
    'irrazionale': 'irrational',
    'irreale': 'unreal; dreamlike',
    'irrealistico': 'unrealistic',
    'irrealizzabile': 'unachievable; impossible to realize',
    'irregolare': 'irregular',
    'irremovibile': 'immovable; unshakable',
    'irreparabile': 'irreparable',
    'irrequieto': 'restless; fidgety',
    'irresistibile': 'irresistible',
    'irresponsabile': 'irresponsible',
    'irriguardoso': 'disrespectful; discourteous',
    'irrilevante': 'irrelevant',
    'irrisolto': 'unsolved; unresolved',
    'irrispettoso': 'disrespectful; irreverent',
    'irritabile': 'irritable; easily annoyed',
    'irritante': 'irritating; annoying',
    'irritato': 'irritated; inflamed',
    'irto': 'bristling; studded',
    'islandese': 'Icelandic',
    'isolato': 'isolated; remote',
    'isomorfo': 'isomorphic',
    'israeliano': 'Israeli',
    'isterico': 'hysterical',
    'istruito': 'educated; learned',
    'istruttivo': 'instructive; educational',
    'italiano': 'Italian',
    'iugoslavo': 'Yugoslav',
    'iv': 'IV; fourth',
    'keniota': 'Kenyan',
    'laborioso': 'hardworking; industrious',
    'lacrimogeno': 'tear-inducing; tear-gas',
    'ladro': 'thieving; thievish',
    'lamentoso': 'whiny; plaintive',
    'largo': 'wide; broad',
    'lasciamico': 'let me',
    'lascivo': 'lascivious; lewd',
    'latente': 'latent; dormant',
    'laterale': 'lateral; side',
    'latino': 'Latin',
    'laudabilissimo': 'very praiseworthy; highly commendable',
    'laureato': 'college-educated; university graduate',
    'lavorativo': 'work-related; workday',
    'leale': 'loyal; faithful',
    'lecito': 'lawful; permissible',
    'legale': 'legal; lawful',
    'legalizzale': 'legalize it (feminine object)',
    'legalizzalo': 'legalize it (masculine object)',
    'legato': 'tied; bound; attached',
    'leggendario': 'legendary',
    'leggero': 'light; lightweight; gentle',
    'leggiadro': 'graceful; dainty',
    'leggibile': 'readable; legible',
    'legislativo': 'legislative',
    'legittimo': 'legitimate; lawful',
    'lentament': 'slowly',
    'lente': 'slow',
    'lento': 'slow',
    'leporino': 'hare-lipped; with a cleft lip',
    'lesbico': 'lesbian',
    'letale': 'lethal; deadly',
    'letargico': 'lethargic',
    'letterale': 'literal',
    'letterario': 'literary',
    'letto': 'sleeping; sleeper',
    'lettone': 'Latvian',
    'lezioso': 'affected; precious',
    'liberale': 'liberal',
    'libico': 'Libyan',
    'lieto': 'glad; joyful',
    'lieve': 'light; slight; mild',
    'limitato': 'limited; narrow',
    'limpido': 'clear; limpid',
    'lineare': 'linear',
    'linguistico': 'linguistic',
    'liquido': 'liquid',
    'liscio': 'smooth',
    'lituano': 'Lithuanian',
    'locale': 'local',
    'localizzato': 'localized; targeted',
    'logico': 'logical',
    'londinese': 'London; from London',
    'loquace': 'talkative; loquacious',
    'losco': 'shady; suspicious',
    'low-cost': 'low-cost; budget',
    'luccicante': 'sparkling; glittering',
    'lucido': 'shiny; glossy; lucid',
    'lugubro': 'gloomy; lugubrious',
    'luminoso': 'bright; luminous',
    'lunare': 'lunar; moonlike',
    'lunaro': 'lunar; moon-related',
    'lunatico': 'moody; erratic',
    'lungi': 'far away; distant',
    'lurido': 'filthy; squalid',
    'lusingato': 'flattered; pleased',
    'lussureggiante': 'lush; luxuriant',
    'macedone': 'Macedonian',
    'macinato': 'ground; minced',
    'madido': 'soaked; dripping wet',
    'madre': 'mother; native',
    'madrelinguo': 'native-speaking; mother-tongue',
    'maggiorenne': 'of age; adult',
    'magico': 'magical; magic',
    'magnetico': 'magnetic',
    'magnifico': 'magnificent',
    'magre': 'thin; skinny',
    'magro': 'thin; skinny',
    'maiuscolo': 'uppercase; capitalized',
    'malaccorto': 'imprudent; careless',
    'maldestro': 'clumsy; awkward',
    'maledetto': 'damned; cursed',
    'maleducato': 'rude; ill-mannered',
    'malevolo': 'malevolent; spiteful',
    'malfermo': 'unsteady; shaky',
    'maligno': 'malicious; malignant',
    'malinconico': 'melancholic; wistful',
    'maltese': 'Maltese',
    'malvage': 'evil; wicked',
    'malvagio': 'evil; wicked',
    'mancante': 'missing; lacking',
    'mancato': 'failed; missed',
    'mancino': 'left-handed',
    'mangiabile': 'edible',
    'maniacale': 'maniacal; obsessive',
    'manifesto': 'manifest; obvious',
    'mannaro': 'werewolf; lycanthropic',
    'mano': 'gradually; little by little',
    'manuale': 'manual; hand-operated',
    'maore': 'Maori',
    'marcato': 'marked; pronounced',
    'marcia': 'rotten; corrupt',
    'marcio': 'rotten; spoiled',
    'marcito': 'rotted; decayed',
    'marina': 'naval; marine',
    'marino': 'marine; seawater',
    'marittimo': 'maritime; nautical',
    'marrone': 'brown',
    'marsupiale': 'marsupial',
    'martino': 'martini',
    'marziale': 'martial; warlike',
    'maschile': 'masculine',
    'maschio': 'male; masculine',
    'mascolino': 'masculine; manly',
    'masochista': 'masochistic',
    'massiccio': 'massive; solid',
    'massimo': 'maximum; utmost',
    'matematica': 'mathematical',
    'matematico': 'mathematical',
    'materiale': 'material; physical',
    'materialista': 'materialistic',
    'materno': 'maternal',
    'matto': 'crazy; nuts',
    'mattutino': 'morning; early-morning',
    'maturo': 'mature; ripe',
    'medesimo': 'same; very same',
    'mediano': 'middle; central',
    'medico': 'medical',
    'medievale': 'medieval',
    'medio': 'average; middle',
    'mediocro': 'mediocre',
    'mediterraneo': 'Mediterranean',
    'melodrammatico': 'melodramatic',
    'mensile': 'monthly',
    'mentale': 'mental',
    'mento': 'less',
    'meraviglioso': 'wonderful; marvelous',
    'mercantile': 'mercantile; commercial',
    'mere': 'mute; speechless',
    'meridionale': 'southern',
    'meritevole': 'worthy; deserving',
    'mero': 'mere; pure',
    'meschino': 'mean; petty',
    'messicano': 'Mexican',
    'mestruale': 'menstrual',
    'meteo': 'weather; meteorological',
    'meteorologico': 'meteorological',
    'metereologico': 'meteorological',
    'meticoloso': 'meticulous; thorough',
    'metodico': 'methodical',
    'metropolitano': 'metropolitan',
    'mezza': 'half',
    'mezzo': 'half; mid',
    'microscopico': 'microscopic',
    'migliorare': 'improved',
    'migratorio': 'migratory',
    'miliardario': 'billionaire',
    'milionario': 'millionaire',
    'militare': 'military',
    'millenovecentosettano': '1979; nineteen seventy-nine',
    'milonguero': 'milonga-style; tango-loving',
    'minaccioso': 'menacing; threatening',
    'minerale': 'mineral',
    'mini': 'mini; miniature',
    'minimo': 'minimal; least',
    'minore': 'minor; lesser',
    'minorenne': 'underage; minor',
    'minorile': 'juvenile; youth',
    'minoritario': 'minority',
    'minuscolo': 'tiny; minuscule',
    'minuzioso': 'meticulous; detailed',
    'miope': 'nearsighted; myopic',
    'mirabile': 'admirable; wondrous',
    'miracoloso': 'miraculous',
    'mirare': 'aimed; targeting',
    'miserabile': 'miserable; wretched',
    'misero': 'poor; wretched; meager',
    'misterioso': 'mysterious',
    'mistico': 'mystical',
    'misto': 'mixed; assorted',
    'mite': 'gentle; mild',
    'mitico': 'mythical; legendary',
    'mitigato': 'softened; mitigated',
}

SPECIAL_TRANSLATIONS: Dict[Tuple[str, str], str] = {
    ('legalizzale', 'legalizzala'): 'legalize it (feminine object)',
    ('legalizzalo', 'legalizzalo'): 'legalize it (masculine object)',
}


def parse_feats(text: str) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if text:
        for part in text.split('|'):
            if '=' in part:
                key, value = part.split('=', 1)
                data[key] = value
    return data


def build_note(feats: Dict[str, str], gendered: bool) -> str:
    note = ''
    gender = feats.get('Gender')
    number = feats.get('Number')
    gender_note = ''
    if gendered and gender == 'Masc':
        gender_note = 'masculine'
    elif gendered and gender == 'Fem':
        gender_note = 'feminine'
    if number == 'Plur':
        if gender_note:
            return f'{gender_note} plural'
        return 'plural'
    return gender_note


def needs_update(rows):
    return any(not (row.get('translation_en') or '').strip() or not (row.get('pronunciation') or '').strip() for row in rows)


def in_range(name: str) -> bool:
    low = name.lower()
    prefix = low[:2]
    return 'io' <= prefix <= 'mi'


def main() -> None:
    for path in sorted(DATA_DIR.glob('*.tsv')):
        if not in_range(path.name):
            continue
        with path.open(newline='') as handle:
            reader = csv.DictReader(handle, delimiter='\t')
            rows = list(reader)
            fieldnames = reader.fieldnames or []
        if not rows or not needs_update(rows):
            continue
        lemma = path.stem
        if lemma not in LEMMA_TRANSLATIONS:
            raise KeyError(f'Missing base translation for lemma {lemma}')
        genders = set()
        for row in rows:
            feats = parse_feats(row.get('feats', ''))
            gender = feats.get('Gender')
            if gender in {'Masc', 'Fem'}:
                genders.add(gender)
        gendered = len(genders) > 1
        base = LEMMA_TRANSLATIONS[lemma]
        for row in rows:
            text = row.get('text', '')
            feats = parse_feats(row.get('feats', ''))
            translation = SPECIAL_TRANSLATIONS.get((lemma, text))
            if not translation:
                translation = base
                note = build_note(feats, gendered)
                if note:
                    translation = f"{translation} ({note})"
            row['translation_en'] = translation
            pron = (row.get('pronunciation') or '').strip()
            if not pron:
                row['pronunciation'] = pronounce_word(text)
            else:
                row['pronunciation'] = pron
        with path.open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)


if __name__ == '__main__':
    main()
