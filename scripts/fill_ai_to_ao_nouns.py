"""Fill translation_en and pronunciation for NOUN TSVs from AI through AO."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word
TARGET_DIR = REPO_ROOT / "vocabulary" / "dataframes" / "NOUN"
PREFIXES = ("ai", "aj", "ak", "al", "am", "an", "ao")

TRANSLATIONS: Dict[str, str] = {
    "aia": "threshing floor; farmyard",
    "aiuole": "flowerbeds",
    "aiutanti": "assistants; helpers",
    "aiutante": "assistant; helper",
    "ali": "wings",
    "ala": "wing",
    "alba": "dawn; sunrise",
    "albanese": "Albanian (person)",
    "alberelli": "saplings; little trees",
    "alberello": "sapling; little tree",
    "alberghetto": "small hotel; inn",
    "albicocche": "apricots",
    "albicocca": "apricot",
    "albicocchi": "apricot trees",
    "albino": "albino (person)",
    "album": "album",
    "alci": "moose (plural)",
    "alchimia": "alchemy",
    "alcol": "alcohol",
    "alcolici": "alcoholic drinks",
    "alcolizzata": "alcoholic (feminine)",
    "alcolizzato": "alcoholic (masculine)",
    "alcolizzati": "alcoholics (masculine)",
    "alcool": "alcohol",
    "ale": "ale",
    "alfa": "alpha",
    "alfabeti": "alphabets",
    "alfabetizzazione": "literacy; alphabetization",
    "alfabeto": "alphabet",
    "algebra": "algebra",
    "algebre": "algebras",
    "algerini": "Algerians",
    "algoritmo": "algorithm",
    "alieni": "aliens",
    "alimentazione": "nutrition; diet",
    "alimento": "food; foodstuff",
    "alimenti": "foods; foodstuffs",
    "alito": "breath",
    "allarme": "alarm; alert",
    "alleanza": "alliance",
    "alleanze": "alliances",
    "alleati": "allies",
    "alleato": "ally",
    "allegria": "joy; cheerfulness",
    "allenamento": "training; workout",
    "allenamenti": "training sessions; workouts",
    "allenatore": "coach; trainer (masculine)",
    "allenatrice": "coach; trainer (feminine)",
    "allergie": "allergies",
    "allerta": "alert; warning",
    "allievi": "students; pupils",
    "alligatore": "alligator",
    "alloggi": "lodgings; accommodations",
    "allontanamento": "separation; estrangement",
    "allori": "laurels",
    "allucinazioni": "hallucinations",
    "alluminio": "aluminum",
    "allunaggio": "moon landing",
    "alluvione": "flood",
    "alluvioni": "floods",
    "almanacco": "almanac",
    "alpinismo": "mountaineering; alpinism",
    "alpinista": "mountaineer; alpinist",
    "altare": "altar",
    "alternative": "alternatives",
    "alternativa": "alternative",
    "altezza": "height",
    "altezze": "heights",
    "altitudine": "altitude",
    "alto": "height; high place",
    "altroieri": "day before yesterday",
    "altruismo": "altruism",
    "alunni": "students; pupils",
    "alunno": "student; pupil",
    "alveare": "beehive",
    "amante": "lover",
    "amanti": "lovers",
    "amaro": "bitterness; bitter aftertaste",
    "amato": "beloved one (masculine)",
    "amate": "beloved ones (feminine)",
    "amarezza": "bitterness",
    "amarmi": "loving me",
    "amasse": "loved (subjunctive)",
    "amassi": "loved (subjunctive)",
    "amaste": "you loved (plural)",
    "amatemi": "love me",
    "amata": "beloved one (feminine)",
    "ambarabà": "eeny meeny counting rhyme",
    "ambasciata": "embassy",
    "ambasciate": "embassies",
    "ambasciatore": "ambassador (masculine)",
    "ambasciatrice": "ambassador (feminine)",
    "ambiente": "environment",
    "ambiguità": "ambiguity",
    "ambito": "field; sphere",
    "ambizioni": "ambitions",
    "ambizione": "ambition",
    "ambulanza": "ambulance",
    "ameba": "amoeba",
    "amercani": "Americans",
    "americani": "Americans (masculine)",
    "americano": "American (masculine)",
    "americana": "American (feminine)",
    "amido": "starch",
    "ammasso": "heap; pile",
    "ammenda": "fine; penalty",
    "amministrazione": "administration",
    "ammiraglio": "admiral",
    "ammiro": "I admire",
    "ammiratore": "admirer (masculine)",
    "ammiratrice": "admirer (feminine)",
    "ammiratori": "admirers (masculine)",
    "ammiratrici": "admirers (feminine)",
    "ammirazione": "admiration",
    "ammisi": "I admitted",
    "ammissione": "admission",
    "ammoniaca": "ammonia",
    "ammonimento": "warning; admonition",
    "ammonio": "ammonium",
    "ammontare": "amount; total",
    "amnesie": "amnesias",
    "amo": "hook; fishhook",
    "amplificatore": "amplifier",
    "amputazione": "amputation",
    "amuleto": "amulet; charm",
    "anacronismo": "anachronism",
    "anagramma": "anagram",
    "analgesici": "painkillers; analgesics",
    "analisi": "analysis",
    "ananas": "pineapple",
    "anatomia": "anatomy",
    "anatra": "duck",
    "anatre": "ducks",
    "anca": "hip",
    "and-roll": "rock and roll",
    "andarsene": "to go away; to leave",
    "andata": "outbound trip; departure",
    "andateglielo": "go tell him",
    "andatura": "gait; walk",
    "androide": "android",
    "androidi": "androids",
    "anello": "ring",
    "anelli": "rings",
    "anestesia": "anesthesia",
    "anfibi": "amphibians",
    "angelo": "angel",
    "angeli": "angels",
    "angioletto": "cherub; little angel",
    "anglicismo": "anglicism",
    "anglofoni": "English speakers",
    "anglosassone": "Anglo-Saxon",
    "angolazione": "angle; viewpoint",
    "angolo": "corner; angle",
    "angoli": "corners; angles",
    "angosce": "anguishes; anxieties",
    "anguille": "eels",
    "anguria": "watermelon",
    "anidride": "anhydride",
    "anima": "soul",
    "anime": "souls",
    "animalisti": "animal-rights activists",
    "animazione": "animation",
    "animo": "spirit; mood",
    "annaffia": "she waters",
    "annaffiatoio": "watering can",
    "annaffi": "you water",
    "annegamento": "drowning",
    "anniversario": "anniversary",
    "annotazioni": "notes; annotations",
    "annuii": "I nodded",
    "annunciatrice": "announcer (feminine)",
    "annunciatore": "announcer (masculine)",
    "annuncio": "announcement; notice",
    "anoressia": "anorexia",
    "ansia": "anxiety",
    "antagonista": "antagonist",
    "anteguerra": "prewar period",
    "antenati": "ancestors",
    "antenne": "antennas",
    "antibiotici": "antibiotics",
    "antichità": "antiquity; antiques",
    "anticipo": "advance payment; advance",
    "antichi": "ancients",
    "anticonstitutionnellement": "unconstitutionally",
    "antidolorifici": "painkillers; analgesics",
    "antidoto": "antidote",
    "antimateria": "antimatter",
    "antiquariato": "antique trade; antiques market",
    "antitesi": "antithesis",
    "antivirus": "antivirus",
    "antropologia": "anthropology",
    "antropologa": "anthropologist (feminine)",
    "anulare": "ring finger",
}


def should_process(path: Path) -> bool:
    stem = path.stem.lower()
    return any(stem.startswith(prefix) for prefix in PREFIXES)


def update_file(path: Path) -> bool:
    changed = False
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    for row in rows:
        text = row.get("text", "")
        if (row.get("translation_en") or "").strip() == "":
            if text not in TRANSLATIONS:
                raise KeyError(f"Missing translation for '{text}' in {path.name}")
            row["translation_en"] = TRANSLATIONS[text]
            changed = True
        if (row.get("pronunciation") or "").strip() == "":
            row["pronunciation"] = pronounce_word(text)
            changed = True
    if changed:
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    return changed


def main() -> None:
    paths = sorted(p for p in TARGET_DIR.glob("*.tsv") if should_process(p))
    for path in paths:
        update_file(path)


if __name__ == "__main__":
    main()
