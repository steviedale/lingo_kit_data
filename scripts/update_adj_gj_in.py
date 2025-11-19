from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict

DATA_DIR = Path('vocabulary/dataframes/ADJ')

LEMMA_TRANSLATIONS: Dict[str, str] = {
    # G-start lemmas
    'glaciale': 'glacial; icy',
    'glielo': 'to her it; to him it',
    'globale': 'global; worldwide',
    'goffo': 'clumsy; awkward',
    'goloso': 'greedy for sweets; gluttonous',
    'gonfiato': 'inflated; overblown',
    'gonfie': 'swollen; puffy',
    'gotico': 'Gothic',
    'governativo': 'government; governmental',
    'gradevole': 'pleasant; agreeable',
    'gradito': 'welcome; appreciated',
    'graduale': 'gradual; step-by-step',
    'grafomane': 'graphomaniac; obsessed with writing',
    'grammaticale': 'grammatical; grammar-related',
    'grandioso': 'grand; magnificent',
    'grata': 'grateful; thankful',
    'grate': 'grateful; thankful',
    'gratificante': 'rewarding; gratifying',
    'grato': 'grateful; thankful',
    'grattugiato': 'grated; shredded',
    'gratuito': 'free of charge; gratuitous',
    'grave': 'serious; severe',
    'gravitazionale': 'gravitational',
    'grazioso': 'graceful; pretty',
    'greca': 'Greek',
    'greco': 'Greek',
    'greggio': 'crude; raw',
    'gretto': 'narrow-minded; petty',
    'grezzo': 'rough; crude',
    'grigio': 'gray; dull',
    'grossolano': 'coarse; crude',
    'grottesco': 'grotesque; absurd',
    'guasto': 'broken; out of order',
    'gustativo': 'gustatory; taste-related',
    'gustoso': 'tasty; flavorful',
    # H-start lemmas
    'haitiano': 'Haitian',
    'hawaiano': 'Hawaiian',
    'heavy': 'heavy',
    'himalayano': 'Himalayan',
    'hollywoodiano': 'Hollywood',
    'horror': 'horror-themed',
    # I-start lemmas before il...
    'i': 'roman numeral i; first',
    'iberico': 'Iberian',
    'iconico': 'iconic',
    'ideale': 'ideal; perfect',
    'idealistico': 'idealistic',
    'identico': 'identical; the same',
    'identificabile': 'identifiable; recognizable',
    'ideologico': 'ideological',
    'idiomatico': 'idiomatic; natural',
    'idiota': 'idiotic; foolish',
    'idoneo': 'suitable; fit',
    'idrico': 'water-related; hydric',
    'igienico': 'hygienic; sanitary',
    'ignaro': 'unaware; ignorant',
    'ignobile': 'ignoble; vile',
    'ignorale': 'ignore it',
    'ignoralo': 'ignore it',
    'ignorante': 'ignorant; uneducated',
    'ii': 'roman numeral ii; second',
    'iii.': 'roman numeral iii; third',
    'ii°': 'second; number two',
    'ilegale': 'illegal',
    'illegale': 'illegal; unlawful',
    'illeggibile': 'illegible; unreadable',
    'illegittimo': 'illegitimate; unlawful',
    'illetterato': 'illiterate; uneducated',
    'illogico': 'illogical; irrational',
    'illustre': 'illustrious; renowned',
    'imbarazzante': 'embarrassing; awkward',
    'imbarazzato': 'embarrassed; ill at ease',
    'imbattibile': 'unbeatable; invincible',
    'imbecille': 'imbecilic; stupid',
    'imbottigliato': 'bottled',
    'imbranato': 'clumsy; awkward',
    'immagazzinato': 'stored; warehoused',
    'immaginabile': 'imaginable; conceivable',
    'immaginare': 'to imagine',
    'immaginario': 'imaginary; make-believe',
    'immaginativo': 'imaginative; creative',
    'immaginoso': 'imaginative; fanciful',
    'immature': 'immature',
    'immaturo': 'immature; undeveloped',
    'immediato': 'immediate; prompt',
    'immenso': 'immense; vast',
    'imminente': 'imminent; impending',
    'immobile': 'motionless; immobile',
    'immobiliare': 'real-estate; property-related',
    'immorale': 'immoral; unethical',
    'immortale': 'immortal; undying',
    'immune': 'immune; exempt',
    'immunitario': 'immune-system; immunological',
    'immutabile': 'unchangeable; immutable',
    'impacciato': 'awkward; self-conscious',
    'impagabile': 'priceless; invaluable',
    'impallidito': 'pale; blanched',
    'impallidì': 'grew pale; turned pale',
    'imparare': 'to learn',
    'impareggiabile': 'incomparable; matchless',
    'imparziale': 'impartial; unbiased',
    'impaurito': 'frightened; scared',
    'impavido': 'fearless; bold',
    'impaziente': 'impatient; eager',
    'impazzire': 'to go crazy; to go mad',
    'impegnare': 'to engage; to commit',
    'impegnativo': 'demanding; challenging',
    'impegnato': 'busy; committed',
    'impensabile': 'unthinkable; inconceivable',
    'impercettibile': 'imperceptible; subtle',
    'imperdonabile': 'unforgivable; inexcusable',
    'impermeabile': 'waterproof; impervious',
    'impertinente': 'impertinent; sassy',
    'impetuoso': 'impetuous; rash',
    'implacabile': 'relentless; implacable',
    'implicante': 'implying; involving',
    'implicito': 'implicit; implied',
    'impolverato': 'dusty',
    'imponente': 'imposing; impressive',
    'impopolare': 'unpopular',
    'importato': 'imported',
    'importatore': 'importing; import-related',
    'imposto': 'imposed; enforced',
    'impotente': 'impotent; powerless',
    'impoverire': 'to impoverish; to make poor',
    'imprendibile': 'unattainable; uncatchable',
    'impreparato': 'unprepared; unready',
    'impressionante': 'impressive; astonishing',
    'impressionato': 'impressed; amazed',
    'impressionista': 'impressionist; impressionistic',
    'imprevedibile': 'unpredictable',
    'imprevisto': 'unexpected; unforeseen',
    'improbabile': 'improbable; unlikely',
    'improduttivo': 'unproductive',
    'improponibile': 'unacceptable; untenable',
    'improprio': 'improper; inappropriate',
    'improvviso': 'sudden; abrupt',
    'imprudente': 'reckless; imprudent',
    'impudente': 'impudent; cheeky',
    'impulsivo': 'impulsive',
    'impunito': 'unpunished',
    'inabile': 'unable; incapable',
    'inaccettabile': 'unacceptable',
    'inadeguato': 'inadequate; insufficient',
    'inaffidabile': 'unreliable',
    'inalienabile': 'inalienable; nontransferable',
    'inammissibile': 'inadmissible; unacceptable',
    'inanimato': 'inanimate',
    'inappropriato': 'inappropriate',
    'inarrestabile': 'unstoppable; relentless',
    'inarticolato': 'inarticulate',
    'inaspettato': 'unexpected',
    'inattaccabile': 'unassailable; unassailable',
    'inatteso': 'unexpected; unforeseen',
    'inaudito': 'unheard-of; outrageous',
    'inavvicinabile': 'unapproachable; unreachable',
    'incandescente': 'incandescent; white-hot',
    'incantevole': 'enchanting; lovely',
    'incapace': 'incapable; unable',
    'incauto': 'careless; incautious',
    'incazzato': 'pissed off; furious',
    'incerto': 'uncertain; unsure',
    'incessante': 'ceaseless; relentless',
    'incivile': 'uncivil; rude',
    'incline': 'inclined; prone',
    'incollo': 'gluey; stuck',
    'incolore': 'colorless',
    'incolto': 'uncultivated; uncultured',
    'incolume': 'unharmed; uninjured',
    'incomparabile': 'incomparable; matchless',
    'incompetente': 'incompetent; unskilled',
    'incompiuto': 'unfinished; incomplete',
    'incompleto': 'incomplete; lacking',
    'incomprensibile': 'incomprehensible; baffling',
    'inconfutabile': 'irrefutable; undeniable',
    'inconsapevole': 'unaware; unconscious',
    'inconscio': 'unconscious; unaware',
    'incoraggiante': 'encouraging',
    'incorporeo': 'bodiless; incorporeal',
    'incorreggibile': 'incorrigible',
    'incorretto': 'incorrect; improper',
    'incosciente': 'reckless; unconscious',
    'incostituzionale': 'unconstitutional',
    'incredibile': 'incredible; unbelievable',
    'incredulo': 'incredulous; doubtful',
    'incrociato': 'crossed; crisscross',
    'incurabile': 'incurable; hopeless',
    'incurante': 'careless; heedless',
    'incuriosito': 'curious; intrigued',
    'incustodito': 'unattended; unsupervised',
    'indebitato': 'in debt; indebted',
    'indecente': 'indecent; improper',
    'indeciso': 'undecided; hesitant',
    'indegne': 'unworthy; undeserving',
    'indegno': 'unworthy; shameful',
    'indelicato': 'tactless; insensitive',
    'indenne': 'unscathed; unharmed',
    'indescrivibile': 'indescribable',
    'indesiderato': 'unwanted; undesired',
    'indiano': 'Indian',
    'indicare': 'to indicate; to point out',
    'indifese': 'defenseless',
    'indifeso': 'defenseless',
    'indifferente': 'indifferent; uninterested',
    'indigeno': 'indigenous; native',
    'indignato': 'indignant; outraged',
    'indipendente': 'independent',
    'indisciplinato': 'undisciplined',
    'indiscreto': 'indiscreet',
    'indispensabile': 'indispensable; essential',
    'indistinguibile': 'indistinguishable',
    'indisturbato': 'undisturbed; undisturbed',
    'individuale': 'individual; personal',
    'individualista': 'individualistic',
    'indolente': 'indolent; lazy',
    'indolenzita': 'sore; aching',
    'indolenzito': 'sore; aching',
    'indolore': 'painless',
    'indonesiano': 'Indonesian',
    'industriale': 'industrial',
    'industrializzato': 'industrialized',
    'inefficace': 'ineffective',
    'inefficiente': 'inefficient',
    'ineguagliabile': 'unequaled; unsurpassed',
    'ineguale': 'unequal; uneven',
    'inequivocabile': 'unequivocal; unmistakable',
    'inerme': 'defenseless; unarmed',
    'inesauribile': 'inexhaustible; endless',
    'inesistente': 'nonexistent',
    'inesperto': 'inexperienced; green',
    'inestimabile': 'inestimable; priceless',
    'inetto': 'inept; clumsy',
    'inevitabile': 'inevitable; unavoidable',
    'infallibile': 'infallible',
    'infantile': 'childish; childlike',
    'infastidito': 'annoyed; bothered',
    'infecondo': 'infertile; barren',
    'infedele': 'unfaithful; disloyal',
    'infelice': 'unhappy; unfortunate',
    'inferiore': 'inferior; lower',
    'infermiera': 'nurse (feminine)',
    'infermiere': 'nurse (masculine)',
    'infermieristico': 'nursing; nurse-related',
    'infernale': 'infernal; hellish',
    'inferocito': 'infuriated; enraged',
    'infettare': 'to infect',
    'infettato': 'infected',
    'infettivo': 'infectious; contagious',
    'infiammabile': 'flammable',
    'infiammato': 'inflamed; irritated',
    'infido': 'treacherous; deceitful',
    'infinito': 'infinite; endless',
    'inflessibile': 'inflexible; unyielding',
    'influente': 'influential',
    'infondato': 'unfounded; baseless',
    'informale': 'informal; casual',
    'informare': 'to inform; to notify',
    'informatico': 'computer; IT-related',
    'informativo': 'informative; informational',
    'informato': 'informed; knowledgeable',
    'infrangere': 'to break; to shatter',
    'infrante': 'shattered; broken',
    'infranto': 'broken; shattered',
    'infruttuoso': 'fruitless; unproductive',
    'ingegnoso': 'ingenious; clever',
    'ingente': 'considerable; massive',
    'ingenuo': 'naive; innocent',
    'ingerente': 'meddlesome; interfering',
    'inginocchiato': 'kneeling',
    'ingiusto': 'unjust; unfair',
    'ingombrante': 'bulky; cumbersome',
    'ingordo': 'gluttonous; greedy',
    'ingrassato': 'fattened; put on weight',
    'ingrossato': 'swollen; enlarged',
    'inimitabile': 'inimitable; unique',
    'inimmaginabile': 'unimaginable',
    'ininfluente': 'irrelevant; negligible',
    'innamorare': 'to fall in love; to make fall in love',
    'innamorato': 'in love; enamored',
    'innato': 'innate; inborn',
    'innaturale': 'unnatural',
    'innegabile': 'undeniable',
    'innervosito': 'irritated; made nervous',
    'innocuo': 'harmless',
    'innovativo': 'innovative',
    'innumerevole': 'innumerable; countless',
    'inoffensivo': 'inoffensive; harmless',
    'inorganico': 'inorganic',
    'inorridito': 'horrified',
    'inosservato': 'unnoticed; unobserved',
    'inquietante': 'disturbing; unsettling',
    'inquieto': 'restless; uneasy',
    'inquinato': 'polluted; contaminated',
    'insaponato': 'soapy; lathered',
    'insapore': 'tasteless; bland',
    'insensato': 'senseless; foolish',
    'insensibile': 'insensitive; numb',
    'inseparabile': 'inseparable',
    'insicuro': 'insecure; unsafe',
    'insignificante': 'insignificant; trivial',
    'insincero': 'insincere',
    'insistente': 'insistent; persistent',
    'insoddisfacente': 'unsatisfying; unsatisfactory',
    'insoddisfatto': 'unsatisfied; dissatisfied',
    'insofferente': 'intolerant; impatient',
    'insolente': 'insolent; rude',
    'insolito': 'unusual; uncommon',
    'insolubile': 'insoluble; unsolvable',
    'insoluto': 'unresolved; unsettled',
    'insonne': 'sleepless; insomniac',
    'insopportabile': 'unbearable; intolerable',
    'insormontabile': 'insurmountable',
    'inspiegabile': 'inexplicable',
    'instabile': 'unstable; shaky',
    'insufficiente': 'insufficient; inadequate',
    'insulare': 'insular; island-based',
    'intatto': 'intact; untouched',
    'integrale': 'integral; wholegrain',
    'integrato': 'integrated; built-in',
    'integro': 'whole; intact',
    'intellettuale': 'intellectual',
    'intensivo': 'intensive',
    'intenso': 'intense; deep',
    'intenzionato': 'intent on; determined',
    'interconnessa': 'interconnected',
    'interdentale': 'interdental',
    'interdipendente': 'interdependent',
    'interdisciplinare': 'interdisciplinary',
    'interinale': 'interim; temporary',
    'interiore': 'inner; interior',
    'interminabile': 'endless; interminable',
    'internazionale': 'international',
    'interno': 'internal; interior',
    'intero': 'entire; whole',
    'interrogativo': 'questioning; interrogative',
    'interrompere': 'to interrupt; to stop',
    'inteso': 'meant; intended',
    'intimidito': 'intimidated; cowed',
    'intimo': 'intimate; close',
    'intimorito': 'frightened; intimidated',
    'intollerabile': 'intolerable; unbearable',
    'intollerante': 'intolerant',
    'intontito': 'dazed; stunned',
    'intorpidite': 'numb; numbed',
    'intraducibile': 'untranslatable',
    'intransigente': 'intransigent; uncompromising',
    'intraprendente': 'enterprising; resourceful',
    'intrecciato': 'interwoven; intertwined',
    'intrepido': 'intrepid; fearless',
    'intressato': 'interested; concerned',
    'intrigante': 'intriguing; fascinating',
    'intrigato': 'intrigued; entangled',
    'introvabile': 'hard to find; unavailable',
    'introverso': 'introverted',
    'intuitivo': 'intuitive',
    'inumano': 'inhuman; cruel',
    'inusuale': 'unusual',
    'inutile': 'useless',
    'inutilizzato': 'unused; not utilized',
    'invadente': 'intrusive; pushy',
    'invalicabile': 'impassable',
    'invendibile': 'unsellable; unable to sell',
    'inventare': 'to invent; to make up',
    'inventato': 'invented; made-up',
    'inventivo': 'inventive; creative',
    'invernale': 'winter; wintry',
    'inverosimile': 'implausible; unlikely',
    'inverso': 'inverse; reverse',
    'invertire': 'to invert; to reverse',
    'investigativo': 'investigative',
    'invidiabile': 'enviable; desirable',
    'invidioso': 'envious; jealous',
    'invincibile': 'invincible',
    'invisibile': 'invisible',
    'invitante': 'inviting; appealing',
    'invulnerabile': 'invulnerable',
}

SPECIAL_TEXT_TRANSLATIONS: Dict[str, str] = {
    # text-specific overrides when needed
}

VOWELS = "aeiouàèéìòù"

VOWEL_SOUNDS = {
    'a': 'ah',
    'à': 'ah',
    'e': 'eh',
    'è': 'eh',
    'é': 'ay',
    'i': 'ee',
    'ì': 'ee',
    'o': 'oh',
    'ò': 'oh',
    'u': 'oo',
    'ù': 'oo',
}


def syllabify(word: str) -> list[str]:
    syllables: list[str] = []
    current = ''
    for char in word:
        current += char
        if char in VOWELS:
            syllables.append(current)
            current = ''
    if current:
        if syllables:
            syllables[-1] += current
        else:
            syllables.append(current)
    return syllables or [word]


def transliterate_syllable(syll: str) -> str:
    s = syll.lower()
    result = ''
    i = 0
    while i < len(s):
        char = s[i]
        nxt = s[i + 1] if i + 1 < len(s) else ''
        nxt2 = s[i + 2] if i + 2 < len(s) else ''
        pair = s[i:i + 2]
        triple = s[i:i + 3]
        if triple == 'gli':
            result += 'lyee'
            i += 3
            continue
        if pair == 'gn':
            result += 'ny'
            i += 2
            continue
        if pair == 'sc' and nxt in 'eiéè':
            result += 'sh'
            i += 2
            continue
        if s[i:i + 3] == 'sch' and nxt2:
            result += 'sk'
            i += 3
            continue
        if char == 'q' and nxt == 'u':
            if nxt2 == 'i' and i + 3 < len(s):
                result += 'kw'
                i += 3
            else:
                result += 'kw'
                i += 2
            continue
        if char == 'g' and nxt == 'u' and nxt2 in VOWELS:
            result += 'gw'
            i += 2
            continue
        if char == 'c':
            if nxt == 'h':
                result += 'k'
                i += 2
                continue
            if nxt == 'i' and nxt2 in 'aeou':
                result += 'ch'
                i += 2
                continue
            if nxt == 'i':
                result += 'ch'
                i += 1
                continue
            if nxt in 'eiéè':
                result += 'ch'
                i += 1
                continue
            result += 'k'
            i += 1
            continue
        if char == 'g':
            if nxt == 'h':
                result += 'g'
                i += 2
                continue
            if nxt == 'i' and nxt2 in 'aeou':
                result += 'j'
                i += 2
                continue
            if nxt == 'i':
                result += 'j'
                i += 1
                continue
            if nxt in 'eiéè':
                result += 'j'
                i += 1
                continue
            result += 'g'
            i += 1
            continue
        if char == 'z':
            result += 'ts'
            i += 1
            continue
        if char == 'h':
            i += 1
            continue
        if char in VOWEL_SOUNDS:
            result += VOWEL_SOUNDS[char]
            i += 1
            continue
        result += char
        i += 1
    return result


def build_pronunciation(word: str) -> str:
    syllables = syllabify(word)
    if not syllables:
        syllables = [word]
    stress_index = max(0, len(syllables) - 2)
    for idx, syll in enumerate(syllables):
        spoken = transliterate_syllable(syll)
        if idx == stress_index:
            spoken = spoken.upper()
        syllables[idx] = spoken
    return '-'.join(syllables)


def build_note(feats: str) -> str:
    gender = None
    number = None
    if 'Gender=Masc' in feats:
        gender = 'masculine'
    elif 'Gender=Fem' in feats:
        gender = 'feminine'
    if 'Number=Plur' in feats:
        number = 'plural'
    note_parts = []
    if gender:
        note_parts.append(gender)
    if number:
        note_parts.append(number)
    if not note_parts:
        return ''
    return ' '.join(note_parts)


def update_files() -> None:
    for file_path in sorted(DATA_DIR.glob('*.tsv')):
        name = file_path.stem
        key = name.lower()
        prefix = key[:2]
        if prefix < 'gj' or prefix > 'in':
            continue
        rows = []
        changed = False
        with file_path.open(encoding='utf-8') as fh:
            reader = csv.DictReader(fh, delimiter='\t')
            fieldnames = reader.fieldnames
            if not fieldnames:
                continue
            for row in reader:
                base = SPECIAL_TEXT_TRANSLATIONS.get(row['text']) or LEMMA_TRANSLATIONS.get(row['lemma'])
                if not base:
                    rows.append(row)
                    continue
                note = build_note(row.get('feats', ''))
                translation = base
                if note:
                    translation = f"{translation} ({note})"
                pronunciation = build_pronunciation(row['text'])
                if row.get('translation_en') != translation or row.get('pronunciation') != pronunciation:
                    row['translation_en'] = translation
                    row['pronunciation'] = pronunciation
                    changed = True
                rows.append(row)
        if changed:
            with file_path.open('w', encoding='utf-8', newline='') as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
                writer.writeheader()
                writer.writerows(rows)


if __name__ == '__main__':
    update_files()
