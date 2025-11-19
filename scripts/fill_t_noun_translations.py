"""Fill translation_en and pronunciation for NOUN TSVs starting with T."""
from __future__ import annotations

import csv
from pathlib import Path
import sys
from typing import Dict

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.add_pronunciation_column import pronounce_word

REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET_DIR = REPO_ROOT / "vocabulary" / "dataframes" / "NOUN"

# Maps lemma -> translation data.
# Each entry can specify keys:
# - singular: translation string for singular forms
# - plural: translation string for plural forms
# - invariant: translation string for invariant forms (used when Number missing)
# - notes: optional mapping of specific text -> translation override
LEMMA_TRANSLATIONS: Dict[str, Dict[str, str]] = {
    'ta': {'invariant': 'such (before a noun)'},
    'tabacco': {'singular': 'tobacco'},
    'tabella': {'singular': 'table; chart'},
    'tablet': {'invariant': 'tablet'},
    'tabloid': {'invariant': 'tabloid'},
    'tabula': {'singular': 'tablet; slate'},
    'taccheggio': {'singular': 'shoplifting', 'plural': 'shoplifting cases'},
    'tacchino': {'singular': 'turkey', 'plural': 'turkeys'},
    'tacco': {'singular': 'heel', 'plural': 'heels'},
    'taccuino': {'singular': 'notebook'},
    'tacos': {'invariant': 'tacos'},
    'tagalog': {'invariant': 'Tagalog (language)'},
    'taglia': {'singular': 'size'},
    'tagliatella': {'plural': 'tagliatelle noodles'},
    'taglie': {'plural': 'bounties; sizes'},
    'taglio': {'singular': 'cut; haircut', 'plural': 'cuts; haircuts'},
    'tagliola': {'singular': 'trap', 'plural': 'traps'},
    'tagliolino': {'singular': 'tagliolino noodle', 'plural': 'tagliolini noodles'},
    'tailandese': {'singular': 'Thai (person)', 'plural': 'Thai people'},
    'talent': {'invariant': 'talent show'},
    'talento': {'singular': 'talent', 'plural': 'talents'},
    'talk': {'invariant': 'talk show'},
    'tallero': {'singular': 'thaler coin'},
    'tallone': {'singular': "heel; Achilles' heel"},
    'talpa': {'singular': 'mole'},
    'tamburello': {'singular': 'tambourine'},
    'tamburo': {'singular': 'drum'},
    'tamil': {'invariant': 'Tamil (language)'},
    'tampone': {'singular': 'tampon', 'plural': 'tampons'},
    'tangente': {'singular': 'bribe', 'plural': 'bribes'},
    'tangerino': {'singular': 'tangerine', 'plural': 'tangerines'},
    'tango': {'singular': 'tango'},
    'tanru': {'invariant': 'tanru (Lojban metaphor)'},
    'tantino': {'singular': 'tiny bit'},
    'tanto': {'singular': 'time (occasion)', 'plural': 'times (occasions)'},
    'tapis': {'invariant': 'treadmill'},
    'tappa': {'singular': 'stage; stopover', 'plural': 'stages; stopovers'},
    'tappetino': {'singular': 'mat; small rug'},
    'tappeto': {'singular': 'carpet; rug', 'plural': 'carpets; rugs'},
    'tappo': {'singular': 'cork; stopper', 'plural': 'corks; stoppers'},
    'tarantola': {'singular': 'tarantula'},
    'targa': {'singular': 'plaque; license plate', 'plural': 'plaques; license plates'},
    'tariffa': {'singular': 'fare; fee; rate', 'plural': 'fares; fees; rates'},
    'tarocco': {'singular': 'tarot card', 'plural': 'tarot cards'},
    'tartaro': {'singular': 'tartar (dental)'},
    'tartaruga': {'singular': 'tortoise; turtle', 'plural': 'tortoises; turtles'},
    'tartifletta': {'invariant': 'tartiflette'},
    'tarto': {'singular': 'late hour', 'plural': 'late hours'},
    'tartufo': {'singular': 'truffle', 'plural': 'truffles'},
    'tasca': {'singular': 'pocket', 'plural': 'pockets'},
    'tassa': {'singular': 'tax', 'plural': 'taxes'},
    'tassazione': {'singular': 'taxation'},
    'tassista': {'singular': 'taxi driver', 'plural': 'taxi drivers'},
    'tasso': {'singular': 'rate', 'plural': 'rates'},
    'tassì': {'invariant': 'taxi'},
    'tastiera': {'singular': 'keyboard', 'plural': 'keyboards'},
    'tastierista': {'singular': 'keyboardist', 'plural': 'keyboardists'},
    'tasto': {'singular': 'key; button', 'plural': 'keys; buttons'},
    'tastoni': {'plural': 'groping motions'},
    'tatame': {'singular': 'tatami mat', 'plural': 'tatami mats'},
    'tatoeba': {'invariant': 'Tatoeba (project)'},
    'tattico': {'singular': 'tactic', 'plural': 'tactics'},
    'tatto': {'singular': 'sense of touch; tact'},
    'tatuaggio': {'singular': 'tattoo', 'plural': 'tattoos'},
    'tauromachia': {'singular': 'bullfighting'},
    'taverna': {'singular': 'tavern'},
    'tavola': {'singular': 'table; board', 'plural': 'tables; boards'},
    'taxista': {'singular': 'taxi driver', 'plural': 'taxi drivers'},
    'tazzina': {'singular': 'demitasse; small cup'},
    'teatro': {'singular': 'theater', 'plural': 'theaters'},
    'tecnica': {'singular': 'technique', 'plural': 'techniques'},
    'tecnico': {'singular': 'technician', 'plural': 'technicians'},
    'tecnologia': {'singular': 'technology', 'plural': 'technologies'},
    'teenager': {'invariant': 'teenager'},
    'tegame': {'singular': 'casserole pan'},
    'teglia': {'singular': 'baking tray'},
    'teiera': {'singular': 'teapot'},
    'tela': {'singular': 'canvas; cloth', 'plural': 'canvases; cloths'},
    'telecamera': {'singular': 'video camera', 'plural': 'video cameras'},
    'telecomando': {'singular': 'remote control', 'plural': 'remote controls'},
    'telecomunicazione': {'singular': 'telecommunication', 'plural': 'telecommunications'},
    'telefonata': {'singular': 'phone call', 'plural': 'phone calls'},
    'telegiornale': {'singular': 'TV news program', 'plural': 'TV news programs'},
    'telegrafo': {'singular': 'telegraph'},
    'telegramma': {'singular': 'telegram', 'plural': 'telegrams'},
    'telenovela': {'singular': 'telenovela; soap opera', 'plural': 'telenovelas; soap operas'},
    'telescopio': {'singular': 'telescope', 'plural': 'telescopes'},
    'teletrasporto': {'singular': 'teleportation'},
    'televisore': {'singular': 'television set', 'plural': 'television sets'},
    'tema': {'singular': 'essay topic; theme', 'plural': 'essay topics; themes'},
    'tempera': {'singular': 'tempera paint'},
    'temperamento': {'singular': 'temperament'},
    'temperatura': {'singular': 'temperature', 'plural': 'temperatures'},
    'tempesta': {'singular': 'storm', 'plural': 'storms'},
    'tempia': {'singular': 'temple (head)', 'plural': 'temples (head)'},
    'tempio': {'singular': 'temple', 'plural': 'temples'},
    'tempismo': {'singular': 'timing'},
    'tempistica': {'singular': 'timeline; scheduling'},
    'templio': {'singular': 'temple', 'plural': 'temples'},
    'temporale': {'singular': 'thunderstorm', 'plural': 'thunderstorms'},
    'tempura': {'invariant': 'tempura'},
    'tenda': {'singular': 'tent; curtain', 'plural': 'tents; curtains'},
    'tendenza': {'singular': 'trend; tendency', 'plural': 'trends; tendencies'},
    'tendina': {'singular': 'small curtain', 'plural': 'small curtains'},
    'tendinita': {'singular': 'tendinitis'},
    'tenebra': {'singular': 'darkness', 'plural': 'darkness'},
    'tennista': {'singular': 'tennis player', 'plural': 'tennis players'},
    'tenore': {'singular': 'tenor', 'plural': 'tenors'},
    'tensione': {'singular': 'tension; voltage', 'plural': 'tensions; voltages'},
    'tentacolo': {'singular': 'tentacle', 'plural': 'tentacles'},
    'tentativo': {'singular': 'attempt', 'plural': 'attempts'},
    'tentazione': {'singular': 'temptation', 'plural': 'temptations'},
    'tenuta': {'singular': 'estate; seal', 'plural': 'estates; seals'},
    'teologia': {'singular': 'theology'},
    'teoria': {'singular': 'theory', 'plural': 'theories'},
    "teorico": {'singular': 'theory section', 'plural': 'theory sections'},
    'teppanyakio': {'invariant': 'teppanyaki'},
    'tequila': {'invariant': 'tequila'},
    'terapeuta': {'singular': 'therapist', 'plural': 'therapists'},
    'terapia': {'singular': 'therapy', 'plural': 'therapies'},
    'tergicristallo': {'singular': 'windshield wiper', 'plural': 'windshield wipers'},
    'terme': {'plural': 'thermal baths'},
    'terminale': {'singular': 'terminal', 'plural': 'terminals'},
    'termine': {'singular': 'term; deadline', 'plural': 'terms; deadlines'},
    'termometro': {'singular': 'thermometer', 'plural': 'thermometers'},
    'termosifone': {'singular': 'radiator', 'plural': 'radiators'},
    'termostato': {'singular': 'thermostat', 'plural': 'thermostats'},
    'terpomo': {'singular': 'potato (Esperanto term)'},
    'terracotta': {'singular': 'terracotta'},
    'terraferma': {'singular': 'mainland'},
    'terrazzo': {'singular': 'terrace', 'plural': 'terraces'},
    'terremoto': {'singular': 'earthquake', 'plural': 'earthquakes'},
    'terreno': {'singular': 'plot of land', 'plural': 'plots of land'},
    'territorio': {'singular': 'territory', 'plural': 'territories'},
    'terrore': {'singular': 'terror'},
    'terrorismo': {'singular': 'terrorism'},
    'terrorista': {'singular': 'terrorist', 'plural': 'terrorists'},
    'teschio': {'singular': 'skull', 'plural': 'skulls'},
    'tesi': {'invariant': 'thesis; theses'},
    'tesore': {'singular': 'treasure', 'plural': 'treasures'},
    'tesoro': {'singular': 'treasure', 'plural': 'treasures'},
    'tessera': {'singular': 'membership card', 'plural': 'membership cards'},
    'tessuto': {'singular': 'fabric', 'plural': 'fabrics'},
    'test': {'singular': 'test', 'plural': 'tests'},
    'testamento': {'singular': 'will; testament'},
    'testicolo': {'singular': 'testicle', 'plural': 'testicles'},
    'testimone': {'singular': 'witness', 'plural': 'witnesses'},
    'testimonianza': {'singular': 'testimony', 'plural': 'testimonies'},
    'testuggine': {'singular': 'tortoise', 'plural': 'tortoises'},
    'tetano': {'singular': 'tetanus'},
    'tetta': {'singular': 'breast; boob', 'plural': 'breasts; boobs'},
    'tetto': {'singular': 'roof', 'plural': 'roofs'},
    'thailandese': {'singular': 'Thai (person)', 'plural': 'Thai people'},
    'the': {'singular': 'tea'},
    'thriller': {'singular': 'thriller'},
    'ticchettio': {'singular': 'ticking; clicking'},
    'tifo': {'singular': 'cheering; rooting'},
    'tifone': {'singular': 'typhoon', 'plural': 'typhoons'},
    'tigre': {'singular': 'tiger', 'plural': 'tigers'},
    'tigrino': {'singular': 'Tigrinya (language)'},
    'timbro': {'singular': 'stamp', 'plural': 'stamps'},
    'timidezza': {'singular': 'shyness'},
    'timore': {'singular': 'fear', 'plural': 'fears'},
    'timpano': {'singular': 'eardrum', 'plural': 'eardrums'},
    'tinta': {'singular': 'tint; dye', 'plural': 'tints; dyes'},
    'tipa': {'singular': 'girl; chick', 'plural': 'girls; chicks'},
    "tipo'": {'singular': 'guy; type', 'plural': 'guys; types'},
    'tiranno': {'singular': 'tyrant', 'plural': 'tyrants'},
    'tiratore': {'singular': 'shooter; marksman', 'plural': 'shooters; marksmen'},
    'tiro': {'singular': 'shot; throw', 'plural': 'shots; throws'},
    'tirocinante': {'singular': 'intern; trainee', 'plural': 'interns; trainees'},
    'titolo': {'singular': 'title', 'plural': 'titles'},
    'tizio': {'singular': 'guy; fellow', 'plural': 'guys; fellows'},
    'tizo': {'singular': 'guy', 'plural': 'guys'},
    'toast': {'singular': 'toasted sandwich', 'plural': 'toasted sandwiches'},
    'tocco': {'singular': 'touch; stroke', 'plural': 'touches; strokes'},
    'tofu': {'invariant': 'tofu'},
    'toilette': {'singular': 'powder room; toilet'},
    'tolleranza': {'singular': 'tolerance'},
    'tomba': {'singular': 'tomb', 'plural': 'tombs'},
    'tondo': {'singular': 'circle; round shape', 'plural': 'circles; round shapes'},
    'tonic': {'singular': 'tonic'},
    'tonnellata': {'singular': 'metric ton', 'plural': 'metric tons'},
    'tonno': {'singular': 'tuna'},
    'tono': {'singular': 'tone', 'plural': 'tones'},
    'tonsilla': {'singular': 'tonsil', 'plural': 'tonsils'},
    'tonsillite': {'singular': 'tonsillitis'},
    'tonto': {'singular': 'fool; idiot', 'plural': 'fools; idiots'},
    'topa': {'singular': 'beaver (feminine)'},
    'topo': {'singular': 'mouse', 'plural': 'mice'},
    'toponimo': {'singular': 'toponym', 'plural': 'toponyms'},
    'torcia': {'singular': 'flashlight; torch', 'plural': 'flashlights; torches'},
    'torcicollo': {'singular': 'stiff neck', 'plural': 'stiff necks'},
    'tore': {'singular': 'bull', 'plural': 'bulls'},
    'tormentare': {'singular': 'blizzard', 'plural': 'blizzards'},
    'tormento': {'singular': 'torment', 'plural': 'torments'},
    'torneo': {'singular': 'tournament', 'plural': 'tournaments'},
    'toro': {'singular': 'bull', 'plural': 'bulls'},
    'torre': {'singular': 'tower', 'plural': 'towers'},
    'torte': {'plural': 'cakes'},
    'tortina': {'singular': 'cupcake', 'plural': 'cupcakes'},
    'torto': {'singular': 'wrong; injustice', 'plural': 'wrongs; injustices'},
    'tortura': {'singular': 'torture', 'plural': 'tortures'},
    'tosse': {'singular': 'cough', 'plural': 'coughs'},
    'tossicità': {'singular': 'toxicity'},
    'tossicodipendente': {'singular': 'drug addict', 'plural': 'drug addicts'},
    'tostapana': {'singular': 'toaster', 'plural': 'toasters'},
    'totale': {'singular': 'total', 'plural': 'totals'},
    'tour': {'singular': 'tour', 'plural': 'tours'},
    'tovaglia': {'singular': 'tablecloth', 'plural': 'tablecloths'},
    'tovagliolo': {'singular': 'napkin', 'plural': 'napkins'},
    'traccia': {'singular': 'trace; track', 'plural': 'traces; tracks'},
    'tradimento': {'singular': 'betrayal', 'plural': 'betrayals'},
    'traditore': {'singular': 'traitor', 'plural': 'traitors'},
    'tradizione': {'singular': 'tradition', 'plural': 'traditions'},
    'traduttore': {'singular': 'translator', 'plural': 'translators'},
    'trafficante': {'singular': 'trafficker', 'plural': 'traffickers'},
    'traffico': {'singular': 'traffic'},
    'traforo': {'singular': 'tunnel', 'plural': 'tunnels'},
    'tragedia': {'singular': 'tragedy', 'plural': 'tragedies'},
    'traghetto': {'singular': 'ferry', 'plural': 'ferries'},
    'tragitto': {'singular': 'journey; route', 'plural': 'journeys; routes'},
    'traguardo': {'singular': 'finish line; milestone', 'plural': 'finish lines; milestones'},
    'trainer': {'invariant': 'trainer'},
    'tram': {'singular': 'tram', 'plural': 'trams'},
    'trama': {'singular': 'plot; storyline', 'plural': 'plots; storylines'},
    'trambusto': {'singular': 'commotion', 'plural': 'commotions'},
    'tramezzino': {'singular': 'sandwich', 'plural': 'sandwiches'},
    'tramonto': {'singular': 'sunset', 'plural': 'sunsets'},
    'trampolo': {'singular': 'stilt', 'plural': 'stilts'},
    'tran-tran': {'singular': 'daily grind', 'plural': 'daily grinds'},
    'tranche': {'singular': 'tranche', 'plural': 'tranches'},
    'trancio': {'singular': 'thick slice', 'plural': 'thick slices'},
    'transazione': {'singular': 'transaction', 'plural': 'transactions'},
    'transessuale': {'singular': 'transsexual person', 'plural': 'transsexual people'},
    'transitterazione': {'singular': 'transliteration', 'plural': 'transliterations'},
    'transizione': {'singular': 'transition', 'plural': 'transitions'},
    'translitterazione': {'singular': 'transliteration', 'plural': 'transliterations'},
    'trapiante': {'singular': 'transplant', 'plural': 'transplants'},
    'trapianto': {'singular': 'transplant', 'plural': 'transplants'},
    'trappola': {'singular': 'trap', 'plural': 'traps'},
    'trascrizione': {'singular': 'transcription', 'plural': 'transcriptions'},
    'trasferimento': {'singular': 'transfer', 'plural': 'transfers'},
    'trasferta': {'singular': 'away trip; business trip', 'plural': 'away trips; business trips'},
    'trasformazione': {'singular': 'transformation', 'plural': 'transformations'},
    'trasfusione': {'singular': 'transfusion', 'plural': 'transfusions'},
    'trasgressione': {'singular': 'violation; offense', 'plural': 'violations; offenses'},
    'trasgressore': {'singular': 'offender', 'plural': 'offenders'},
    'trasloco': {'singular': 'move; relocation', 'plural': 'moves; relocations'},
    'trasmigrazione': {'singular': 'transmigration'},
    'trasmissione': {'singular': 'broadcast; transmission', 'plural': 'broadcasts; transmissions'},
    'trasparenza': {'singular': 'transparency'},
    'trasporto': {'singular': 'transport', 'plural': 'transports'},
    'tratta': {'singular': 'route; slave trade', 'plural': 'routes; slave trades'},
    'trattamento': {'singular': 'treatment', 'plural': 'treatments'},
    'trattato': {'singular': 'treaty', 'plural': 'treaties'},
    'tratto': {'singular': 'stroke; stretch', 'plural': 'strokes; stretches'},
    'trattore': {'singular': 'tractor', 'plural': 'tractors'},
    'trauma': {'singular': 'trauma', 'plural': 'traumas'},
    'trave': {'singular': 'beam', 'plural': 'beams'},
    'travestimento': {'singular': 'disguise', 'plural': 'disguises'},
    'travestito': {'singular': 'cross-dresser', 'plural': 'cross-dressers'},
    'treccia': {'singular': 'braid', 'plural': 'braids'},
    'trekking': {'invariant': 'trekking'},
    'trentina': {'singular': 'thirtysomething (feminine)'},
    'trepidazione': {'singular': 'trepidation'},
    'triangolo': {'singular': 'triangle', 'plural': 'triangles'},
    'triathlon': {'singular': 'triathlon', 'plural': 'triathlons'},
    'tribunale': {'singular': 'court', 'plural': 'courts'},
    'tribù': {'invariant': 'tribe; tribes'},
    'triciclo': {'singular': 'tricycle', 'plural': 'tricycles'},
    'tricipite': {'singular': 'triceps muscle', 'plural': 'triceps muscles'},
    'tricipito': {'plural': 'triceps muscles'},
    'tricolore': {'singular': 'tricolour flag', 'plural': 'tricolour flags'},
    'trifoglio': {'singular': 'clover', 'plural': 'clovers'},
    'trimestre': {'singular': 'quarter; trimester', 'plural': 'quarters; trimesters'},
    'triplo': {'singular': 'triple', 'plural': 'triples'},
    'tristezza': {'singular': 'sadness'},
    'tritone': {'singular': 'newt', 'plural': 'newts'},
    'trochilide': {'singular': 'hummingbird'},
    'trochilidio': {'plural': 'hummingbirds'},
    'trofeo': {'singular': 'trophy', 'plural': 'trophies'},
    'troia': {'singular': 'slut; bitch', 'plural': 'sluts; bitches'},
    'troll': {'singular': 'troll', 'plural': 'trolls'},
    'tromba': {'singular': 'trumpet', 'plural': 'trumpets'},
    'trombone': {'singular': 'trombone', 'plural': 'trombones'},
    'tronco': {'singular': 'trunk', 'plural': 'trunks'},
    'troncone': {'singular': 'stump', 'plural': 'stumps'},
    'trono': {'singular': 'throne', 'plural': 'thrones'},
    'trote': {'plural': 'trout'},
    'trucco': {'singular': 'makeup; trick', 'plural': 'makeup looks; tricks'},
    'truciolo': {'singular': 'wood shaving', 'plural': 'wood shavings'},
    'truffa': {'singular': 'scam', 'plural': 'scams'},
    'truffatore': {'singular': 'con artist', 'plural': 'con artists'},
    'truppa': {'singular': 'troop', 'plural': 'troops'},
    'tsez': {'invariant': 'Tsez (language)'},
    'tsuname': {'singular': 'tsunami', 'plural': 'tsunamis'},
    'tuba': {'singular': 'tuba', 'plural': 'tubas'},
    'tubatura': {'singular': 'piping', 'plural': 'pipelines'},
    'tubercolina': {'singular': 'tuberculin'},
    'tubercolosi': {'singular': 'tuberculosis'},
    'tubo': {'singular': 'tube; pipe', 'plural': 'tubes; pipes'},
    'tugurio': {'singular': 'hovel', 'plural': 'hovels'},
    'tulipano': {'singular': 'tulip', 'plural': 'tulips'},
    'tumore': {'singular': 'tumor', 'plural': 'tumors'},
    'tumulto': {'singular': 'riot; upheaval', 'plural': 'riots; upheavals'},
    'tunica': {'singular': 'tunic', 'plural': 'tunics'},
    'tunnel': {'singular': 'tunnel', 'plural': 'tunnels'},
    'tuono': {'singular': 'thunderclap', 'plural': 'thunderclaps'},
    'tuorli': {'plural': 'egg yolks'},
    'turbante': {'singular': 'turban', 'plural': 'turbans'},
    'turchese': {'singular': 'turquoise', 'plural': 'turquoises'},
    'turismo': {'singular': 'tourism'},
    'turista': {'singular': 'tourist', 'plural': 'tourists'},
    'turno': {'singular': 'shift; turn', 'plural': 'shifts; turns'},
    'tuta': {'singular': 'coveralls; tracksuit', 'plural': 'coveralls; tracksuits'},
    'tuttologo': {'singular': 'know-it-all', 'plural': 'know-it-alls'},
}

# Specific text overrides (used when a form needs unique translation or note).
TEXT_TRANSLATIONS: Dict[str, str] = {
    'telefonagli': 'call him',
    'telefonai': 'I phoned',
    'tetra': 'gloomy (feminine)',
    'tienlo': 'keep it',
    'torno': 'away',
    'tradirti': 'betray you',
    'traduciamole': "let's translate them",
    'tradurlo': 'translate it',
    'tram.': 'tram',
    'turco': 'Turkish (language)',
    'turchi': 'Turks',
    'tv.': 'TV',
}


def get_translation(text: str, lemma: str, feats: str) -> str:
    if text in TEXT_TRANSLATIONS:
        return TEXT_TRANSLATIONS[text]
    info = LEMMA_TRANSLATIONS.get(lemma)
    if not info:
        raise KeyError(f"Missing lemma translation for {lemma}")
    feats = feats or ""
    if "Number=Plur" in feats and info.get("plural"):
        return info["plural"]
    if "Number=Sing" in feats and info.get("singular"):
        return info["singular"]
    if info.get("invariant"):
        return info["invariant"]
    # Fallback to singular if plural not specified.
    if "Number=Plur" in feats and info.get("singular"):
        return info["singular"]
    if info.get("singular"):
        return info["singular"]
    raise KeyError(f"Cannot determine translation for {text} ({lemma})")


def update_file(path: Path) -> None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    if not rows:
        return
    if "pronunciation" not in fieldnames:
        insert_idx = fieldnames.index("text") + 1
        fieldnames = fieldnames[:insert_idx] + ["pronunciation"] + fieldnames[insert_idx:]
        for row in rows:
            row.setdefault("pronunciation", "")
    for row in rows:
        text = row.get("text", "") or ""
        lemma = row.get("lemma", "") or ""
        feats = row.get("feats", "") or ""
        translation = (row.get("translation_en") or "").strip()
        pronunciation = (row.get("pronunciation") or "").strip()
        if not translation:
            row["translation_en"] = get_translation(text, lemma, feats)
        if not pronunciation:
            row["pronunciation"] = pronounce_word(text)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    for path in sorted(TARGET_DIR.glob("t*.tsv")):
        update_file(path)


if __name__ == "__main__":
    main()
