from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word

DATA_DIR = Path('vocabulary/dataframes/ADJ')

TRANSLATIONS = {
    'eterno.tsv': {'default': 'eternal; everlasting'},
    'eterogeneo.tsv': {'default': 'heterogeneous; diverse'},
    'etico.tsv': {'default': 'ethical; moral'},
    'etiope.tsv': {'default': 'Ethiopian'},
    'etnico.tsv': {'default': 'ethnic'},
    'etrusco.tsv': {'default': 'Etruscan'},
    'eurocentrico.tsv': {'default': 'Eurocentric'},
    'europeo.tsv': {'default': 'European'},
    'evadere.tsv': {'default': 'escaped; on the run'},
    'evidente.tsv': {'default': 'obvious; evident'},
    'evitabile.tsv': {'default': 'avoidable'},
    'ex.tsv': {'default': 'ex; former'},
    'extra.tsv': {'default': 'extra; additional'},
    'extragalattico.tsv': {'default': 'extragalactic'},
    'faceto.tsv': {'default': 'facetious; witty'},
    'fallo.tsv': {'forms': {'fallo': 'do it; make it (masculine object)'}},
    'falso.tsv': {'default': 'false; fake'},
    'famelico.tsv': {'default': 'starving; ravenous'},
    'famigerato.tsv': {'default': 'notorious; infamous'},
    'famigliare.tsv': {'default': 'family; familiar'},
    'familiare.tsv': {'default': 'family; familiar'},
    'fangoso.tsv': {'default': 'muddy'},
    'fannullone.tsv': {'default': 'lazy; idle'},
    'fantascientifico.tsv': {'default': 'science-fiction; sci-fi'},
    'fantasioso.tsv': {'default': 'imaginative; fanciful'},
    'fantastico.tsv': {'default': 'fantastic; great'},
    'fargle.tsv': {'forms': {'fargli': 'to him; for him'}},
    'fascista.tsv': {'default': 'fascist'},
    'fastidoso.tsv': {'default': 'annoying; bothersome'},
    'fatale.tsv': {'default': 'fatal; fateful'},
    'fattibile.tsv': {'default': 'feasible; doable'},
    'favoloso.tsv': {'default': 'fabulous; wonderful'},
    'favorire.tsv': {'default': 'favorite'},
    'fazioso.tsv': {'default': 'biased; partisan'},
    'fedele.tsv': {'default': 'faithful; loyal'},
    'federale.tsv': {'default': 'federal'},
    'fedifrago.tsv': {'default': 'unfaithful; treacherous'},
    'femminile.tsv': {'default': 'feminine'},
    'femminista.tsv': {'default': 'feminist'},
    'fenomenale.tsv': {'default': 'phenomenal; amazing'},
    'ferire.tsv': {'default': 'injured; hurt'},
    'ferito.tsv': {'default': 'injured; wounded'},
    'fermo.tsv': {'default': 'still; motionless; firm'},
    'feroce.tsv': {'default': 'fierce; ferocious'},
    'ferroviario.tsv': {'default': 'railway; rail'},
    'fertile.tsv': {'default': 'fertile'},
    'fervido.tsv': {'default': 'vivid; fervent'},
    'fessacchiotto.tsv': {'default': 'gullible; dopey'},
    'festivo.tsv': {'default': 'holiday; festive'},
    'festoso.tsv': {'default': 'festive; merry'},
    'feudale.tsv': {'default': 'feudal'},
    'fiammingo.tsv': {'default': 'Flemish'},
    'fidanzato.tsv': {'default': 'engaged'},
    'fidato.tsv': {'default': 'trusted; reliable'},
    'fiduciario.tsv': {'default': 'fiduciary'},
    'fiducioso.tsv': {'default': 'confident; hopeful'},
    'fiera.tsv': {'default': 'proud'},
    'figo.tsv': {'default': 'cool; awesome'},
    'figurato.tsv': {'default': 'figurative'},
    'finale.tsv': {'default': 'final; last'},
    'finanziario.tsv': {'default': 'financial'},
    'fine.tsv': {'default': 'fine; delicate'},
    'finito.tsv': {'default': 'finished; done'},
    'finlandese.tsv': {'default': 'Finnish'},
    'finlando.tsv': {'default': 'Finnish'},
    'fino.tsv': {'default': 'final; ultimate'},
    'finto.tsv': {'default': 'fake; pretend'},
    'fiorire.tsv': {'default': 'in bloom; blossoming'},
    'fiscale.tsv': {'default': 'fiscal; tax'},
    'fisico.tsv': {'default': 'physical'},
    'fisso.tsv': {'default': 'fixed; steady'},
    'fitto.tsv': {'default': 'dense; thick'},
    'flaccido.tsv': {'default': 'flabby; flaccid'},
    'flebile.tsv': {'default': 'faint; feeble'},
    'flessibile.tsv': {'default': 'flexible'},
    'floreale.tsv': {'default': 'floral'},
    'fluente.tsv': {'default': 'flowing'},
    'fluento.tsv': {'default': 'fluent'},
    'fluttuante.tsv': {'default': 'floating; fluctuating'},
    'focoso.tsv': {'default': 'fiery; ardent'},
    'fognario.tsv': {'default': 'sewer; drainage'},
    'folkloristica.tsv': {'default': 'folkloric; folk'},
    'folle.tsv': {'default': 'crazy; foolish'},
    'folo.tsv': {'default': 'folk'},
    'folto.tsv': {'default': 'thick; dense'},
    'fondamentale.tsv': {'default': 'fundamental; essential'},
    'fondere.tsv': {'default': 'burned out; exhausted'},
    'fondo.tsv': {'default': 'late; deep'},
    'forato.tsv': {'default': 'punctured; perforated'},
    'formale.tsv': {'default': 'formal'},
    'formidabile.tsv': {'default': 'formidable; terrific'},
    'forzare.tsv': {'default': 'forced; coerced'},
    'fosco.tsv': {'default': 'gloomy; dusky'},
    'fossile.tsv': {'default': 'fossil'},
    'fotochimico.tsv': {'default': 'photochemical'},
    'fotogenico.tsv': {'default': 'photogenic'},
    'fottuto.tsv': {'default': 'damned; fucking'},
    'fradice.tsv': {'default': 'soaked; drenched'},
    'fradicio.tsv': {'default': 'soaking wet; plastered'},
    'fradico.tsv': {'default': 'soaked; drenched'},
    'fragile.tsv': {'default': 'fragile; delicate'},
    'franco.tsv': {'default': 'frank; candid'},
    'francobollo.tsv': {'default': 'postage stamps'},
    'frastornato.tsv': {'default': 'dazed; bewildered'},
    'fratturato.tsv': {'default': 'fractured; broken'},
    'fraudolento.tsv': {'default': 'fraudulent'},
    'freelance.tsv': {'default': 'freelance'},
    'frenetico.tsv': {'default': 'frantic; frenetic'},
    'frequentato.tsv': {'default': 'busy; well-frequented'},
    'frequente.tsv': {'default': 'frequent; common'},
    'frettoloso.tsv': {'default': 'hasty; rushed'},
    'friabile.tsv': {'default': 'crumbly; brittle'},
    'friggere.tsv': {'default': 'fried'},
    'frigido.tsv': {'default': 'frigid; icy'},
    'fritto.tsv': {'default': 'fried'},
    'frontale.tsv': {'default': 'frontal; head-on'},
    'frullato.tsv': {'default': 'blended; pureed'},
    'frustrante.tsv': {'default': 'frustrating'},
    'frustrato.tsv': {'default': 'frustrated'},
    'fumatore.tsv': {'default': 'smoking; for smokers'},
    'fumoso.tsv': {'default': 'smoky'},
    'funebre.tsv': {'default': 'funeral; funereal'},
    'funereo.tsv': {'default': 'funereal; gloomy'},
    'funzionante.tsv': {'default': 'working; functioning'},
    'fuore.tsv': {'forms': {'fuor': 'out; outside'}, 'extra_notes': {'fuor': 'short form'}},
    'fuorviante.tsv': {'default': 'misleading'},
    'furbo.tsv': {'default': 'clever; sly'},
    'furioso.tsv': {'default': 'furious; enraged'},
    'furtivo.tsv': {'default': 'stealthy; furtive'},
    'fuso.tsv': {'default': 'burned out; exhausted'},
    'futile.tsv': {'default': 'futile; pointless'},
    'futuro.tsv': {'default': 'future'},
    'gaio.tsv': {'default': 'cheerful; gay'},
    'galante.tsv': {'default': 'gallant; courteous'},
    'galleggiante.tsv': {'default': 'floating'},
    'gallese.tsv': {'default': 'Welsh'},
    'gallico.tsv': {'default': 'Gallic'},
    'ganzo.tsv': {'default': 'cool; neat'},
    'garbato.tsv': {'default': 'polite; courteous'},
    'gassato.tsv': {'default': 'carbonated; fizzy'},
    'gatto.tsv': {'forms': {'gatta': 'cat'}},
    'gelato.tsv': {'default': 'frozen'},
    'gelido.tsv': {'default': 'icy; freezing'},
    'gelo.tsv': {'forms': {'gela': 'freezes'}},
    'gemello.tsv': {'default': 'twin'},
    'genealogico.tsv': {'default': 'genealogical'},
    'generale.tsv': {'default': 'general'},
    'generico.tsv': {'default': 'generic'},
    'generoso.tsv': {'default': 'generous'},
    'genetico.tsv': {'default': 'genetic'},
    'geniale.tsv': {'default': 'brilliant; ingenious'},
    'genuino.tsv': {'default': 'genuine; authentic'},
    'geografico.tsv': {'default': 'geographic'},
    'gergale.tsv': {'default': 'slangy; colloquial'},
    'germanico.tsv': {'default': 'Germanic'},
    'gestire.tsv': {'default': 'managed; run'},
    'ghiacciato.tsv': {'default': 'icy; frozen'},
    'giallo.tsv': {'default': 'yellow'},
    'giallognolo.tsv': {'default': 'yellowish'},
    'giavanese.tsv': {'default': 'Javanese'},
    'gigante.tsv': {'default': 'giant'},
    'gigantesco.tsv': {'default': 'gigantic'},
    'giocoso.tsv': {'default': 'playful; jocular'},
    'gioioso.tsv': {'default': 'joyful; joyous'},
    'giornaliero.tsv': {'default': 'daily'},
    'giornalistico.tsv': {'default': 'journalistic'},
    'giovanile.tsv': {'default': 'youthful'},
    'giuliano.tsv': {'default': 'Julian'},
    'giuridico.tsv': {'default': 'legal; juridical'},
}

SPECIAL_FILES = set(TRANSLATIONS)


def parse_feats(feats: str) -> tuple[str | None, str | None]:
    gender = None
    number = None
    for part in feats.split('|'):
        if part.startswith('Gender='):
            gender = part.split('=', 1)[1]
        elif part.startswith('Number='):
            number = part.split('=', 1)[1]
    return gender, number


def format_note(gender: str | None, number: str | None, add_gender: bool, extra: str | None) -> str:
    pieces = []
    if add_gender and gender:
        pieces.append('masculine' if gender == 'Masc' else 'feminine')
    if number == 'Plur':
        if pieces:
            pieces[-1] = pieces[-1] + ' plural'
        else:
            pieces.append('plural')
    if extra:
        pieces.append(extra)
    if not pieces:
        return ''
    return ' (' + ', '.join(pieces) + ')'


def update_file(filename: str, config: dict[str, object]) -> None:
    path = DATA_DIR / filename
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    if not rows:
        return

    genders = set()
    for row in rows:
        gender, _ = parse_feats(row.get('feats', ''))
        if gender:
            genders.add(gender)
    add_gender_note = len(genders) > 1

    default = config.get('default')
    form_trans = config.get('forms', {})
    extra_notes = config.get('extra_notes', {})

    for row in rows:
        text = row.get('text', '')
        translation = form_trans.get(text)
        if translation is None:
            translation = default
        if translation is None:
            raise ValueError(f'Missing translation for {filename} / {text}')
        gender, number = parse_feats(row.get('feats', ''))
        extra = extra_notes.get(text)
        note = format_note(gender, number, add_gender_note, extra)
        row['translation_en'] = f'{translation}{note}' if note else translation
        row['pronunciation'] = pronounce_word(text)

    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t', lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    for filename in sorted(SPECIAL_FILES):
        update_file(filename, TRANSLATIONS[filename])


if __name__ == '__main__':
    main()
