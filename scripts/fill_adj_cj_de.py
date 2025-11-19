"""Fill translations and pronunciations for ADJ TSV files from CJ to DE."""

from __future__ import annotations

import csv
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.add_pronunciation_column import pronounce_word


ADJ_DIR = REPO_ROOT / "vocabulary" / "dataframes" / "ADJ"


TRANSLATIONS: dict[str, str] = {
    "clandestino": "clandestine; underground; illegal",
    "classico": "classic; classical; traditional",
    "claustrofobico": "claustrophobic",
    "clientelare": "clientelist; patronage-based",
    "climatico": "climatic; climate-related",
    "clinico": "clinical; medical",
    "cocainomane": "cocaine-addicted; cocaine-dependent",
    "cocciuto": "stubborn; pigheaded",
    "codardo": "cowardly; gutless",
    "coerente": "coherent; consistent",
    "coeso": "cohesive; unified",
    "coinvolgente": "engaging; compelling",
    "col": "with the",
    "colla": "with the",
    "collaborativo": "collaborative; cooperative",
    "collassare": "collapsed",
    "collaterale": "collateral; secondary",
    "collettivo": "collective; group",
    "colombiano": "Colombian",
    "colorare": "colorful; colored",
    "colore": "colored; in the color of",
    "colpevole": "guilty; culpable",
    "colposo": "negligent; culpable",
    "colto": "cultured; learned",
    "comico": "comic; funny",
    "commerciale": "commercial; business",
    "commestibile": "edible",
    "commissivo": "commissive; active",
    "commovente": "moving; touching",
    "commutativo": "commutative",
    "comodo": "comfortable; convenient",
    "comparato": "comparative",
    "compatibile": "compatible",
    "competente": "competent; skilled",
    "competitivo": "competitive",
    "compiaciuta": "pleased; self-satisfied",
    "compiaciuto": "pleased; self-satisfied",
    "complementare": "complementary",
    "complessivo": "overall; total",
    "complesso": "complex; complicated",
    "completo": "complete; full",
    "complicato": "complicated; tricky",
    "complice": "complicit; accomplice",
    "comporre": "compound; compounded",
    "comprensibile": "understandable; comprehensible",
    "comprensivo": "understanding; supportive",
    "compreso": "understood; grasped",
    "comprimere": "compressed; zipped",
    "compulsivo": "compulsive",
    "computazionale": "computational",
    "comune": "common; ordinary",
    "comunista": "communist",
    "concentrato": "concentrated; focused",
    "conciliante": "conciliatory; accommodating",
    "conciso": "concise; brief",
    "conclusivo": "conclusive; final",
    "concorrente": "competing; rival",
    "concorrenziale": "competitive; market-based",
    "concreto": "concrete; real",
    "condividere": "shared",
    "condiviso": "shared",
    "condizionale": "conditional",
    "condizionato": "conditioned; dependent",
    "confidenziale": "confidential",
    "confinante": "bordering; adjacent",
    "confondere": "confused; mixed up",
    "conforme": "conforming; compliant",
    "conformista": "conformist",
    "confortante": "comforting; reassuring",
    "confortevole": "comfortable; cozy",
    "confuciano": "Confucian",
    "confuso": "confused; muddled",
    "congelare": "frozen",
    "congelato": "frozen",
    "congestionato": "congested; jammed",
    "congolese": "Congolese",
    "congregazionalista": "Congregationalist",
    "coniugato": "married",
    "connesso": "connected; linked",
    "connotato": "connoted; nuanced",
    "conoscere": "known; well-known",
    "conosciuto": "known; familiar",
    "consapevole": "aware; conscious",
    "conscio": "conscious; aware",
    "consecutivo": "consecutive; successive",
    "conseguente": "consequent; resulting",
    "conservativo": "conservative; preservative",
    "conservatore": "conservative",
    "considerevole": "considerable; significant",
    "consigliabile": "advisable; recommendable",
    "consueto": "usual; customary",
    "contagioso": "contagious; infectious",
    "contato": "numbered",
    "contemporaneo": "contemporary; modern",
    "contento": "happy; pleased; content",
    "contestabile": "contestable; debatable",
    "continuo": "continuous; constant",
    "contorto": "twisted; contorted",
    "contraffare": "counterfeit; fake",
    "contraffatto": "counterfeit; fake",
    "contrario": "contrary; opposite",
    "controcorrente": "against the current; against the grain",
    "controproduttivo": "counterproductive",
    "controverso": "controversial",
    "conveniente": "convenient; affordable",
    "convenzionale": "conventional; standard",
    "convesso": "convex",
    "convincente": "convincing; persuasive",
    "convinto": "convinced; sure",
    "cool": "cool; trendy",
    "coperto": "covered; sheltered",
    "coraggiosi": "brave; courageous",
    "coraggioso": "brave; courageous; bold",
    "corallino": "coral; coral-colored",
    "cordiale": "cordial; friendly",
    "coreane": "Korean",
    "coreano": "Korean",
    "corporale": "corporal; bodily",
    "corporeo": "corporeal; bodily",
    "corposo": "full-bodied; rich",
    "correggile": "correct them",
    "correlato": "related; correlated",
    "corrente": "current; present",
    "corretto": "correct; proper",
    "corrispondente": "corresponding; matching",
    "corroso": "corroded; eaten away",
    "corrotto": "corrupt; tainted",
    "cortese": "courteous; polite",
    "corto": "short; brief",
    "cosciente": "conscious; aware",
    "coscienzioso": "conscientious; diligent",
    "cosiddetto": "so-called",
    "cosmetico": "cosmetic; beauty-related",
    "cospicuo": "conspicuous; substantial",
    "costante": "constant; steady",
    "costiero": "coastal; seaside",
    "costipato": "constipated; congested",
    "costituzionale": "constitutional",
    "costoso": "expensive; costly",
    "costruttivo": "constructive",
    "costrutto": "constructed; invented",
    "covariante": "covariant",
    "cranico": "cranial",
    "creativo": "creative; inventive",
    "credente": "believing; faithful",
    "credibile": "credible; believable",
    "credulone": "gullible; naive",
    "creduloso": "gullible; credulous",
    "creolo": "Creole",
    "crescente": "growing; increasing",
    "crescere": "raised; grown up",
    "crespe": "frizzy; kinky",
    "cretino": "idiotic; stupid",
    "criminale": "criminal; felonious",
    "cristallino": "crystalline; crystal-clear",
    "cristiano": "Christian",
    "critico": "critical; crucial",
    "cronico": "chronic",
    "cruciale": "crucial",
    "crude": "raw; uncooked",
    "crudele": "cruel",
    "crudo": "raw; uncooked",
    "cubano": "Cuban",
    "culinario": "culinary",
    "culturale": "cultural",
    "cuocere": "cooked",
    "cuoco": "cook; chef",
    "cupo": "gloomy; dark",
    "curabile": "curable; treatable",
    "curativo": "healing; therapeutic",
    "curioso": "curious; inquisitive",
    "custodire": "guarded; watched",
    "cutaneo": "cutaneous; skin-related",
    "daltonico": "color-blind",
    "danese": "Danish",
    "dannato": "damned; accursed",
    "danneggiare": "damaged",
    "danneggiato": "damaged",
    "dannoso": "harmful; damaging",
    "datema": "give me",
    "datemene": "give me some of it",
    "dato": "given",
    "debito": "due; proper",
    "debole": "weak; frail",
    "deceduto": "deceased",
    "decente": "decent; respectable",
    "decimo": "tenth",
    "decisionale": "decision-making",
    "decisivo": "decisive; crucial",
    "deciso": "decided; resolute",
    "decorare": "decorated; honored",
    "deficiente": "deficient; lacking",
    "definitivo": "definitive; final",
    "defunto": "deceased; late",
    "degno": "worthy; deserving",
    "degradante": "degrading; demeaning",
    "deliberato": "deliberate; intentional",
    "delicato": "delicate; sensitive",
    "delirante": "delirious; raving",
    "delizioso": "delicious; delightful",
    "deludente": "disappointing",
    "deluso": "disappointed; let down",
    "deluxe": "deluxe; high-end",
    "demente": "demented; insane",
    "democratica": "democratic",
    "democratico": "democratic",
    "demoniaco": "demonic",
    "denso": "dense; thick",
    "dentato": "toothed; serrated",
    "deplorevole": "deplorable; lamentable",
    "depresso": "depressed",
    "deprimente": "depressing",
    "deprimere": "depressed; downcast",
    "desertico": "desert; arid",
    "deserto": "deserted; empty",
    "desiderare": "desired; wanted",
    "desiderato": "desired",
    "desideroso": "eager; desirous",
    "desolato": "desolate; bleak",
    "desto": "awake; alert",
    "destro": "right; right-hand",
    "destrorso": "right-handed",
    "determinante": "decisive; determining",
    "determinato": "determined; specific",
    "detestabile": "detestable; hateful",
    "dettagliato": "detailed; thorough",
    "detto": "said; aforementioned",
    "devoto": "devout; devoted",
}


SPECIAL_NOTES = {
    "col": "short form",
    "colla": "short form",
}


def parse_feats(value: str) -> dict[str, str]:
    result: dict[str, str] = {}
    if not value:
        return result
    for chunk in value.split("|"):
        if "=" not in chunk:
            continue
        key, val = chunk.split("=", 1)
        result[key] = val
    return result


def build_note(row: dict[str, str], genders_present: set[str]) -> str:
    feats = parse_feats(row.get("feats", ""))
    gender = feats.get("Gender")
    number = feats.get("Number")
    pieces: list[str] = []
    if gender in {"Masc", "Fem"} and len({g for g in genders_present if g in {"Masc", "Fem"}}) > 1:
        pieces.append("masculine" if gender == "Masc" else "feminine")
    if number == "Plur":
        if pieces:
            pieces[-1] = f"{pieces[-1]} plural"
        else:
            pieces.append("plural")
    special = SPECIAL_NOTES.get(row.get("text", ""))
    if special:
        pieces.append(special)
    if not pieces:
        return ""
    return f" ({', '.join(pieces)})"


def update_file(path: Path) -> bool:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if not rows:
        return False

    lemma = rows[0].get("lemma", "")
    base = TRANSLATIONS.get(lemma)
    if not base:
        return False

    genders_present = {
        parse_feats(row.get("feats", "")).get("Gender", "")
        for row in rows
        if row.get("feats")
    }

    updated = False
    for row in rows:
        translation = (row.get("translation_en") or "").strip()
        pronunciation = (row.get("pronunciation") or "").strip()
        if translation and pronunciation:
            continue
        note = build_note(row, genders_present)
        row["translation_en"] = f"{base}{note}"
        row["pronunciation"] = pronounce_word(row.get("text", ""))
        updated = True

    if not updated:
        return False

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    return True


def main() -> None:
    files = sorted(p for p in ADJ_DIR.iterdir() if p.suffix == ".tsv")
    changed = 0
    for path in files:
        name = path.name.upper()
        if not ("CJ" <= name[:2] <= "DE"):
            continue
        if update_file(path):
            changed += 1
    print(f"Updated {changed} files")


if __name__ == "__main__":
    main()
