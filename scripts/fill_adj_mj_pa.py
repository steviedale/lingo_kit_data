from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word

BASE_DIR = Path('vocabulary/dataframes/ADJ')

RAW_TRANSLATIONS = """
moderato\tmoderate; restrained
moderno\tmodern; contemporary
modesto\tmodest; humble
modificato\tmodified; altered
momentaneo\tmomentary; temporary
mondano\tworldly; sophisticated
mondiale\tglobal; worldwide
monetario\tmonetary; financial
mongolo\tMongolian; Mongol
monolitico\tmonolithic
monoteiste\tmonotheistic
monotono\tmonotonous; dull
monouso\tsingle-use; disposable
montagnoso\tmountainous; hilly
montuoso\tmountainous; rugged
morale\tmoral; ethical
morbido\tsoft; tender
mordere\tbit
morente\tdying; near death
moriato\tyou (plural) die (subjunctive)
mortale\tmortal; deadly
morto\tdead; deceased
mostruoso\tmonstrous; hideous
motivato\tmotivated; driven
motorizzato\tmotorized; mechanized
movimentare\teventful; hectic
mulatto\tmulatto
multicolorato\tmulticolored; colorful
multiculturale\tmulticultural
multilingue\tmultilingual
multilinguo\tmultilingual
multinazionale\tmultinational
muovere\twavy; choppy
muscolare\tmuscular
musicale\tmusical
musulmano\tMuslim; Islamic
mutageno\tmutagenic
n\ta; an (colloquial contraction)
nanno\tlullaby
nano\tdwarf; tiny
narcisistico\tnarcissistic; self-absorbed
nascente\tnascent; emerging
nascosto\thidden; concealed
natale\tChristmas; natal
natalizia\tChristmas; festive
natalizio\tChristmas; holiday
nativo\tnative; indigenous
natto\tnatto; fermented soybeans
naturale\tnatural; genuine
naturle\tnatural; genuine
nauseabondo\tnauseating; sickening
nauseante\tnauseating; revolting
nauseato\tnauseated; queasy
nazionale\tnational
nazionalista\tnationalist
naïf\tnaive; artless
nebbioso\tfoggy; misty
negativo\tnegative; pessimistic
negligente\tnegligent; careless
negro\tnegro; black
nemico\tenemy; hostile
neo-eletto\tnewly elected
neoclassico\tneoclassical
neozelandese\tNew Zealand; Kiwi
netto\tnet; clear
neutrale\tneutral; impartial
neutro\tneutral
nevrotico\tneurotic
newtoniano\tNewtonian
nigeriano\tNigerian
nitido\tsharp; clear
nobile\tnoble; aristocratic
nobiliare\tnoble; of the nobility
nocivo\tharmful; noxious
noncurante\tnonchalant; careless
nono\tninth
nord-\tnorthern
nord\tnorthern; north
normale\tnormal; ordinary
norvegese\tNorwegian
notevole\tnotable; remarkable
noto\tknown; famous
notturno\tnighttime; nocturnal
novantenne\tninety-year-old
novello\tnew; fresh
nubile\tsingle; unmarried
nucleare\tnuclear
nudista\tnudist
nudo\tnaked; nude
numeroso\tnumerous; large
nutriente\tnutritious; nourishing
nuvolo\tcloudy; overcast
nuvoloso\tcloudy; gloomy
nuziale\tbridal; wedding
obbediente\tobedient
obbigatore\tmandatory; compulsory
obbligatorio\tmandatory; compulsory
obeso\tobese
obesogeno\tobesogenic; fattening
obietto\tI object; I protest
oblungo\toblong; elongated
obsoleto\tobsolete
occasionale\toccasional; casual
occidentale\twestern; occidental
occupato\tbusy; occupied
occupazionale\toccupational; job-related
oculare\tocular; eye-related
odierno\ttoday's; present-day
odioso\thateful; odious
offensiva\toffensive; insulting
offensivo\toffensive; insulting
offline\toffline
oggettivo\tobjective; factual
olandese\tDutch
olimpico\tOlympic
oltraggioso\toutrageous; insulting
ombreggiato\tshaded; shadowy
ombroso\tshady; shadowy
omissivo\tomissive; neglectful
omofobo\thomophobic
omogeneo\thomogeneous; uniform
omonimo\thomonymous; namesake
omosessuale\thomosexual
onirico\tdreamlike; oneiric
online\tonline
onnivore\tomnivorous
onnivoro\tomnivorous
onorato\thonored; proud
operaio\tworking-class; blue-collar
operativo\toperational; working
operoso\thard-working; industrious
opinabile\tdebatable; arguable
opposto\topposite
opprimente\toppressive; stifling
orale\toral
orario\tscheduled; hourly
orbitante\torbiting; revolving
ordinare\ttidy; orderly
ordinario\tordinary; common
orfano\torphaned; parentless
organico\torganic
organizzato\torganized
orientale\teastern; oriental
originale\toriginal; unique
originario\toriginal; native
orizzontale\thorizontal
orrende\thorrendous; dreadful
orrendo\thorrendous; awful
orribile\thorrible; awful
orripilante\thorrifying; gruesome
orripilato\thorrified
osceno\tobscene
oscillante\toscillating; swinging
oscuro\tdark; obscure
ospitale\thospitable; welcoming
ospitante\thosting; accommodating
ossequioso\tobsequious; fawning
osservabile\tobservable; noticeable
ossessionante\tobsessive; haunting
ossessivo\tobsessive; compulsive
ossuto\tbony; gaunt
ostentato\tostentatious; showy
ostile\thostile
ostinato\tstubborn; obstinate
ottavo\teighth
ottenibile\tobtainable; attainable
ottico\toptic; optical
ottimista\toptimistic
ottimistico\toptimistic; upbeat
ottimo\texcellent; great
ottomano\tOttoman; Turkish
ottuso\tdull; obtuse
ovale\toval
ovvio\tobvious; evident
ovvo\tobvious; evident
pacifico\tpeaceful; pacific
pacifista\tpacifist
paesaggistico\tscenic; landscape
paffuto\tchubby; plump
pagabile\tpayable
pagandone\tpaying for it; paying the price
pagano\tpagan
pallido\tpale
palpabile\tpalpable; tangible
panoramico\tpanoramic; scenic
paracadutista\tparatrooper; parachuting
paragonabile\tcomparable
paralizzare\tparalyzed; immobilized
parallela\tparallel
parallelo\tparallel
paranoici\tparanoid
paranoico\tparanoid
pari\tequal; even
parlante\tspeaking; talking
parlare\tyou (plural) speak
parlato\tspoken; oral
part-timo\tpart-time
particolare\tparticular; special
parziale\tpartial; biased
passante\tpassing; transient
passato\tpast; former
passeggero\ttemporary; passing
passibile\tliable; subject to
passionale\tpassionate; impulsive
passivo\tpassive
paternalistico\tpaternalistic
patetico\tpathetic; pitiful
patologico\tpathological
patriottico\tpatriotic
pauroso\tfearful; scary
paziente\tpatient; calm
"""

TRANSLATIONS: dict[str, dict] = {}
for line in RAW_TRANSLATIONS.strip().splitlines():
    name, default = line.split('\t', 1)
    TRANSLATIONS[name] = {'default': default}

EXTRA_CONFIG = {
    'mordere': {'gender_notes': 'never'},
    'moriato': {'gender_notes': 'never', 'plural_notes': 'never'},
    'n': {'gender_notes': 'never', 'plural_notes': 'never'},
    'nanno': {'gender_notes': 'never'},
    'natto': {'gender_notes': 'never'},
    'negro': {'form_notes': {'negro': ['offensive']}},
    'nord-': {'gender_notes': 'never', 'plural_notes': 'never', 'form_notes': {'nord-': ['prefix']}},
    'pagandone': {'gender_notes': 'never', 'plural_notes': 'never'},
    'parlare': {'gender_notes': 'never', 'plural_notes': 'never'},
}

for name, updates in EXTRA_CONFIG.items():
    if name in TRANSLATIONS:
        TRANSLATIONS[name].update(updates)

def parse_feats(feat_str: str) -> dict[str, str]:
    result: dict[str, str] = {}
    if not feat_str:
        return result
    for part in feat_str.split('|'):
        if '=' in part:
            key, value = part.split('=', 1)
            result[key] = value
    return result


def gender_map(rows: list[dict[str, str]]) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    for row in rows:
        feats = parse_feats(row.get('feats', ''))
        gender = feats.get('Gender')
        if gender:
            mapping.setdefault(row['text'], set()).add(gender)
    return mapping


def build_translation(row: dict[str, str], entry: dict, gender_mapping: dict[str, set[str]]) -> str:
    text = row['text']
    translation = entry.get('forms', {}).get(text)
    if translation is None:
        translation = entry.get('default')
    if translation is None:
        raise ValueError(f"No translation configured for {text}")
    feats = parse_feats(row.get('feats', ''))
    note_parts: list[str] = []
    gender = feats.get('Gender')
    number = feats.get('Number')
    if entry.get('gender_notes') == 'always':
        gender_flag = True
    elif entry.get('gender_notes') == 'never':
        gender_flag = False
    else:
        genders_for_text = gender_mapping.get(text, set())
        gender_flag = bool(genders_for_text) and len(genders_for_text) == 1
    if gender_flag and gender in ('Masc', 'Fem'):
        label = 'masculine' if gender == 'Masc' else 'feminine'
        if number == 'Plur':
            note_parts.append(f"{label} plural")
        else:
            note_parts.append(label)
    elif number == 'Plur' and entry.get('plural_notes', 'auto') != 'never':
        note_parts.append('plural')
    if "'" in text:
        note_parts.append('before a vowel')
    extra_notes = entry.get('form_notes', {}).get(text)
    if extra_notes:
        note_parts.extend(extra_notes)
    if entry.get('common_notes'):
        note_parts.extend(entry['common_notes'])
    if note_parts:
        translation = f"{translation} ({', '.join(note_parts)})"
    return translation


def process_file(name: str, entry: dict) -> None:
    path = BASE_DIR / f"{name}.tsv"
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    if not rows:
        return
    genders = gender_map(rows)
    for row in rows:
        row['translation_en'] = build_translation(row, entry, genders)
        row['pronunciation'] = pronounce_word(row['text'])
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    missing = [name for name in sorted(TRANSLATIONS) if not (BASE_DIR / f"{name}.tsv").exists()]
    if missing:
        raise SystemExit(f"Missing TSV files: {missing}")
    for name, entry in TRANSLATIONS.items():
        process_file(name, entry)


if __name__ == '__main__':
    main()
