"""Fill missing English translations and pronunciations for verb tables."""

from __future__ import annotations

import csv
import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple
import unicodedata

import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.add_pronunciation_column import pronounce_word


@dataclass(frozen=True)
class Sense:
    base: str
    third: str | None = None
    past: str | None = None
    participle: str | None = None
    gerund: str | None = None

    def present(self, person: str | None, number: str | None) -> str:
        if person == "3" and number == "Sing" and self.third:
            return self.third
        if person == "3" and number == "Sing":
            return conjugate_phrase(self.base, "present3", person, number)
        return conjugate_phrase(self.base, "present", person, number)

    def past_form(self, person: str | None, number: str | None) -> str:
        if self.past:
            return self.past
        return conjugate_phrase(self.base, "past", person, number)

    def participle_form(self, person: str | None, number: str | None) -> str:
        if self.participle:
            return self.participle
        return conjugate_phrase(self.base, "participle", person, number)

    def gerund_form(self, person: str | None, number: str | None) -> str:
        if self.gerund:
            return self.gerund
        return conjugate_phrase(self.base, "gerund", person, number)


@dataclass
class VerbConfig:
    senses: Sequence[Sense]
    reflexive_senses: Sequence[Sense] | None = None
    reflexive_prons: set[str] = field(default_factory=set)
    impersonal_subject: str | None = None
    drop_reflexive_objects: bool = False


REFLEXIVE_PRON_SET = {"mi", "me", "ti", "te", "si", "se", "ci", "ce", "vi", "ve"}

VOWELS = set("aeiou")


IRREGULAR_FORMS: Dict[str, Dict[str, object]] = {
    "be": {
        "present": {
            ("1", "Sing"): "am",
            ("2", "Sing"): "are",
            ("3", "Sing"): "is",
            ("1", "Plur"): "are",
            ("2", "Plur"): "are",
            ("3", "Plur"): "are",
        },
        "past": {
            ("1", "Sing"): "was",
            ("2", "Sing"): "were",
            ("3", "Sing"): "was",
            ("1", "Plur"): "were",
            ("2", "Plur"): "were",
            ("3", "Plur"): "were",
        },
        "participle": "been",
        "gerund": "being",
        "third": "is",
    },
    "have": {"third": "has", "past": "had", "participle": "had", "gerund": "having"},
    "do": {"third": "does", "past": "did", "participle": "done", "gerund": "doing"},
    "go": {"third": "goes", "past": "went", "participle": "gone", "gerund": "going"},
    "make": {"third": "makes", "past": "made", "participle": "made", "gerund": "making"},
    "spend": {"third": "spends", "past": "spent", "participle": "spent", "gerund": "spending"},
}

PERSONAL_MAP = {
    "mi": "me",
    "me": "me",
    "ti": "you",
    "te": "you",
    "ci": "us",
    "ce": "us",
    "vi": "you (plural)",
    "ve": "you (plural)",
}

REFLEXIVE_MAP = {
    "mi": "myself",
    "me": "myself",
    "ti": "yourself",
    "te": "yourself",
    "si": "himself/herself/itself",
    "se": "himself/herself/itself",
    "ci": "ourselves",
    "ce": "ourselves",
    "vi": "yourselves",
    "ve": "yourselves",
}

DIRECT_MAP = {
    "lo": "him/it",
    "la": "her/it",
    "li": "them",
    "le": "them",
}

INDIRECT_MAP = {
    "gli": "to him",
    "ghe": "to him",
    "loro": "to them",
}

PARTITIVE_MAP = {
    "ne": "some (of it)",
}

SUBJECT_MAP = {
    ("1", "Sing"): "I",
    ("2", "Sing"): "you",
    ("3", "Sing"): "he/she/it",
    ("1", "Plur"): "we",
    ("2", "Plur"): "you (plural)",
    ("3", "Plur"): "they",
}


def split_phrase(phrase: str) -> Tuple[str, str]:
    parts = phrase.split()
    if not parts:
        return phrase, ""
    first = parts[0]
    rest = " ".join(parts[1:])
    return first, rest


def default_third(word: str) -> str:
    lower = word.lower()
    if lower.endswith("y") and len(lower) > 1 and lower[-2] not in VOWELS:
        return word[:-1] + "ies"
    if lower.endswith(("s", "x", "z", "ch", "sh", "o")):
        return word + "es"
    return word + "s"


def default_past(word: str) -> str:
    lower = word.lower()
    if lower.endswith("e"):
        return word + "d"
    if lower.endswith("y") and len(lower) > 1 and lower[-2] not in VOWELS:
        return word[:-1] + "ied"
    return word + "ed"


def default_participle(word: str) -> str:
    return default_past(word)


def default_gerund(word: str) -> str:
    lower = word.lower()
    if lower.endswith("ie"):
        return word[:-2] + "ying"
    if lower.endswith("e") and lower not in {"be", "see"}:
        return word[:-1] + "ing"
    return word + "ing"


def conjugate_word(word: str, form: str, person: str | None, number: str | None) -> str:
    base = word
    lower = word.lower()
    data = IRREGULAR_FORMS.get(lower)
    if data:
        if lower == "be":
            if form == "present":
                return data["present"].get((person or "3", number or "Sing"), "are")
            if form == "past":
                return data["past"].get((person or "3", number or "Sing"), "were")
        else:
            if form == "present3" and data.get("third"):
                return data["third"]
            if form == "past" and data.get("past"):
                return data["past"]  # type: ignore[return-value]
            if form == "participle" and data.get("participle"):
                return data["participle"]  # type: ignore[return-value]
            if form == "gerund" and data.get("gerund"):
                return data["gerund"]  # type: ignore[return-value]
    if form == "present3":
        return default_third(base)
    if form == "past":
        return default_past(base)
    if form == "participle":
        return default_participle(base)
    if form == "gerund":
        return default_gerund(base)
    return base


def conjugate_phrase(phrase: str, form: str, person: str | None, number: str | None) -> str:
    first, rest = split_phrase(phrase)
    conj = conjugate_word(first, form, person, number)
    if rest:
        return f"{conj} {rest}"
    return conj


def parse_feats(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    if not raw:
        return result
    for chunk in raw.split("|"):
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        result[key] = value
    return result


def load_rows(path: Path) -> tuple[list[dict[str, str]], List[str]]:
    with path.open() as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = list(reader)
        fieldnames = reader.fieldnames or []
        return rows, list(fieldnames)


def write_rows(path: Path, rows: Sequence[dict[str, str]], fieldnames: Sequence[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def ensure_pronunciation_field(fieldnames: List[str]) -> List[str]:
    if "pronunciation" not in fieldnames:
        if "text" in fieldnames:
            idx = fieldnames.index("text") + 1
            fieldnames = list(fieldnames)
            fieldnames.insert(idx, "pronunciation")
        else:
            fieldnames = list(fieldnames) + ["pronunciation"]
    return fieldnames


def needs_update(rows: Iterable[dict[str, str]]) -> bool:
    for row in rows:
        if not (row.get("translation_en") or "").strip():
            return True
    return False


def strip_accents(text: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", text) if unicodedata.category(ch) != "Mn")


CONSONANTS = set("bcdfghjklmnpqrstvwxyz")


CLITIC_SEQUENCE_MAP: List[tuple[str, List[str], bool]] = []
CLITIC_BASES = ["me", "te", "se", "ci", "vi"]
CLITIC_TAILS = ["lo", "la", "li", "le", "ne"]
for base in CLITIC_BASES:
    for tail in CLITIC_TAILS:
        spelled = base
        token = base
        if base == "ci" and tail.startswith(("l", "n")):
            spelled = "ce"
        elif base == "vi" and tail.startswith(("l", "n")):
            spelled = "ve"
        pattern = spelled + tail
        needs_consonant = token in {"me", "te", "se"}
        CLITIC_SEQUENCE_MAP.append((pattern, [token, tail], needs_consonant))

GLI_PATTERNS = [
    ("gliene", ["gli", "ne"]),
    ("glielo", ["gli", "lo"]),
    ("gliela", ["gli", "la"]),
    ("glieli", ["gli", "li"]),
    ("gliele", ["gli", "le"]),
]
for pattern, seq in GLI_PATTERNS:
    CLITIC_SEQUENCE_MAP.append((pattern, seq, False))

CLITIC_SEQUENCE_MAP.sort(key=lambda item: len(item[0]), reverse=True)

SIMPLE_CLITICS = {
    "mi",
    "ti",
    "si",
    "ci",
    "vi",
    "gli",
    "loro",
    "lo",
    "la",
    "li",
    "le",
    "ne",
}


def extract_clitics_from_parts(parts_lemma: str) -> List[str]:
    if not parts_lemma or "+" not in parts_lemma:
        return []
    bits = [chunk.strip() for chunk in parts_lemma.split("+") if chunk.strip()]
    return bits[1:]


def extract_clitics_from_text(text: str) -> List[str]:
    cleaned = strip_accents(text.lower().replace("'", "").replace("’", ""))
    tokens: List[str] = []
    while cleaned:
        matched = False
        for pattern, seq, needs_consonant in CLITIC_SEQUENCE_MAP:
            if cleaned.endswith(pattern):
                remaining = cleaned[: -len(pattern)]
                if not remaining:
                    break
                if needs_consonant:
                    prev = strip_accents(remaining[-1].lower()) if remaining else ""
                    if prev not in CONSONANTS:
                        continue
                cleaned = remaining
                tokens = seq + tokens
                matched = True
                break
        if matched:
            continue
        for simple in sorted(SIMPLE_CLITICS, key=len, reverse=True):
            if cleaned.endswith(simple) and len(cleaned) > len(simple):
                cleaned = cleaned[: -len(simple)]
                tokens = [simple] + tokens
                matched = True
                break
        if not matched:
            break
    return tokens


def extract_clitics(parts_lemma: str, text: str) -> List[str]:
    tokens = extract_clitics_from_parts(parts_lemma)
    if tokens:
        return tokens
    return extract_clitics_from_text(text)


def build_clitic_phrase(tokens: Sequence[str], feats: dict[str, str], config: VerbConfig) -> str:
    if not tokens:
        return ""
    has_combo = len(tokens) > 1
    reflexive_parts: List[str] = []
    direct_parts: List[str] = []
    partitive_parts: List[str] = []
    personal_parts: List[str] = []
    indirect_parts: List[str] = []
    for token in tokens:
        low = token.lower()
        if low in config.reflexive_prons:
            if config.drop_reflexive_objects:
                continue
            reflexive_parts.append(REFLEXIVE_MAP.get(low, "oneself"))
            continue
        if low in DIRECT_MAP:
            direct_parts.append(DIRECT_MAP[low])
            continue
        if low in PARTITIVE_MAP:
            partitive_parts.append(PARTITIVE_MAP[low])
            continue
        if low in INDIRECT_MAP:
            indirect_parts.append(INDIRECT_MAP[low])
            continue
        if low in PERSONAL_MAP:
            base = PERSONAL_MAP[low]
            if has_combo:
                if base == "me":
                    personal_parts.append("to me")
                elif base == "you":
                    personal_parts.append("to you")
                elif base == "us":
                    personal_parts.append("to us")
                elif base.startswith("you (plural)"):
                    personal_parts.append("to you (plural)")
                else:
                    personal_parts.append(f"to {base}")
            else:
                personal_parts.append(base)
            continue
        if low in REFLEXIVE_MAP and not config.drop_reflexive_objects:
            reflexive_parts.append(REFLEXIVE_MAP[low])
            continue
        personal_parts.append(low)
    parts = reflexive_parts + direct_parts + partitive_parts + personal_parts
    if indirect_parts:
        if not (direct_parts or partitive_parts or personal_parts):
            adjusted = [chunk.replace("to ", "", 1) if chunk.startswith("to ") else chunk for chunk in indirect_parts]
            parts += adjusted
        else:
            parts += indirect_parts
    return " ".join(parts).strip()


def subject_from_feats(feats: dict[str, str], config: VerbConfig) -> tuple[str | None, str | None, str | None]:
    if config.impersonal_subject:
        return config.impersonal_subject, "3", "Sing"
    person = feats.get("Person")
    number = feats.get("Number")
    return SUBJECT_MAP.get((person, number)), person, number


def gender_presence(rows: Sequence[dict[str, str]]) -> dict[str, bool]:
    presence = {"Masc": False, "Fem": False}
    for row in rows:
        feats = parse_feats(row.get("feats", ""))
        gender = feats.get("Gender")
        if gender in presence:
            presence[gender] = True
    return presence


def append_gender_markers(text: str, feats: dict[str, str], presence: dict[str, bool]) -> str:
    markers: List[str] = []
    gender = feats.get("Gender")
    number = feats.get("Number")
    if gender == "Masc" and presence.get("Fem"):
        markers.append("(masculine)")
    elif gender == "Fem":
        markers.append("(feminine)")
    if number == "Plur":
        markers.append("(plural)")
    if markers:
        return f"{text} {' '.join(markers)}".strip()
    return text


def sense_phrase(
    sense: Sense,
    verb_form: str,
    mood: str,
    tense: str,
    subject_label: str | None,
    person: str | None,
    number: str | None,
) -> str:
    if verb_form == "inf":
        return f"to {sense.base}"
    if verb_form == "ger":
        return sense.gerund_form(person, number)
    if verb_form == "part":
        return sense.participle_form(person, number)
    if mood == "cnd":
        if subject_label:
            return f"{subject_label} would {sense.base}"
        return f"would {sense.base}"
    if mood == "sub":
        if tense == "imp":
            body = sense.past_form(person, number)
        else:
            body = sense.present(person, number)
        if subject_label:
            return f"that {subject_label} {body}"
        return f"that {body}"
    if mood == "imp":
        base = sense.base
        if person == "1" and number == "Plur":
            return f"let's {base}"
        if person == "2" and number == "Plur":
            return f"you (plural) {base}"
        if person == "2":
            return f"you {base}"
        if subject_label:
            return f"{subject_label} {base}"
        return base
    if tense in {"imp", "past"}:
        body = sense.past_form(person, number)
        if subject_label:
            return f"{subject_label} {body}"
        return body
    if tense == "fut":
        if subject_label:
            return f"{subject_label} will {sense.base}"
        return f"will {sense.base}"
    if tense in {"pqp", "pastperf"}:
        part = sense.participle_form(person, number)
        if subject_label:
            return f"{subject_label} had {part}"
        return f"had {part}"
    body = sense.present(person, number)
    if subject_label:
        return f"{subject_label} {body}"
    return body


def compute_translation(row: dict[str, str], config: VerbConfig, presence: dict[str, bool]) -> str:
    feats = parse_feats(row.get("feats", ""))
    verb_form = (feats.get("VerbForm") or "fin").lower()
    mood = (feats.get("Mood") or ("ind" if verb_form == "fin" else "")).lower()
    tense = (feats.get("Tense") or ("pres" if verb_form == "fin" else "")).lower()
    subject_label, person, number = subject_from_feats(feats, config)
    clitic_tokens = extract_clitics(row.get("parts_lemma", ""), row.get("text", ""))
    clitic_suffix = build_clitic_phrase(clitic_tokens, feats, config)
    phrases = []
    for sense in config.senses:
        phrase = sense_phrase(sense, verb_form, mood, tense, subject_label, person, number)
        if clitic_suffix:
            phrase = f"{phrase} {clitic_suffix}".strip()
        phrases.append(phrase.strip())
    translation = "; ".join(phrases).strip()
    translation = append_gender_markers(translation, feats, presence)
    return translation


def process_file(path: Path) -> bool:
    rows, fieldnames = load_rows(path)
    if not rows or not needs_update(rows):
        return False
    config = VERB_CONFIGS.get(path.stem)
    if not config:
        raise KeyError(f"Missing verb configuration for {path.stem}")
    presence = gender_presence(rows)
    changed = False
    for row in rows:
        row.setdefault("pronunciation", "")
        row.setdefault("translation_en", "")
        if not (row["translation_en"] or "").strip():
            row["translation_en"] = compute_translation(row, config, presence)
            changed = True
        if not (row["pronunciation"] or "").strip():
            row["pronunciation"] = pronounce_word(row.get("text", ""))
            changed = True
    if changed:
        fieldnames = ensure_pronunciation_field(fieldnames)
        write_rows(path, rows, fieldnames)
    return changed


def make_config(*bases: str | Sense, **kwargs) -> VerbConfig:
    sense_list: List[Sense] = []
    for base in bases:
        if isinstance(base, Sense):
            sense_list.append(base)
        else:
            sense_list.append(Sense(base=base))
    return VerbConfig(senses=sense_list, **kwargs)


GIVE_SENSE = Sense("give", third="gives", past="gave", participle="given", gerund="giving")
TELL_SENSE = Sense("tell", third="tells", past="told", participle="told", gerund="telling")
SAY_SENSE = Sense("say", third="says", past="said", participle="said", gerund="saying")


VERB_CONFIGS: Dict[str, VerbConfig] = {
    "abituare": make_config(
        Sense("accustom"),
        Sense("get used to"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "accadere": make_config("happen"),
    "accettare": make_config("accept"),
    "accusare": make_config("accuse"),
    "addormentare": make_config(
        Sense("put to sleep"),
        Sense("fall asleep"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "adorare": make_config("adore"),
    "aggiungere": make_config("add"),
    "alzare": make_config(
        Sense("raise"),
        Sense("lift"),
        Sense("get up"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "ammalare": make_config(
        Sense("make ill"),
        Sense("get sick"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "ammettere": make_config("admit", "allow"),
    "appartenere": make_config("belong"),
    "apprezzare": make_config("appreciate"),
    "aprire": make_config("open"),
    "arrabbiare": make_config(
        Sense("anger"),
        Sense("get angry"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "arrestare": make_config("arrest", "stop"),
    "ascoltare": make_config("listen"),
    "assicurare": make_config("assure", "insure"),
    "assomigliare": make_config("resemble"),
    "assumere": make_config("hire", "assume"),
    "attaccare": make_config("attack", "attach"),
    "attraversare": make_config("cross"),
    "avvisare": make_config("warn", "notify"),
    "baciare": make_config("kiss"),
    "ballare": make_config("dance"),
    "bastare": make_config(Sense("be enough"), impersonal_subject="it"),
    "battere": make_config(Sense("beat", past="beat", participle="beaten", gerund="beating")),
    "cadere": make_config(Sense("fall", past="fell", participle="fallen", gerund="falling")),
    "cambiare": make_config("change"),
    "camminare": make_config("walk"),
    "cantare": make_config(Sense("sing", past="sang", participle="sung", gerund="singing")),
    "capitare": make_config("happen"),
    "cenare": make_config("have dinner"),
    "cercare": make_config("look for"),
    "chiudere": make_config("close", Sense("shut", past="shut", participle="shut", gerund="shutting")),
    "colpire": make_config(Sense("hit", past="hit", participle="hit", gerund="hitting"), "strike"),
    "combattere": make_config(Sense("fight", past="fought", participle="fought", gerund="fighting")),
    "cominciare": make_config(Sense("begin", past="began", participle="begun", gerund="beginning"), "start"),
    "comportare": make_config("involve", "entail"),
    "comprendere": make_config("understand", "include"),
    "condividere": make_config("share"),
    "considerare": make_config("consider"),
    "consigliare": make_config("advise", "recommend"),
    "contare": make_config("count", "matter"),
    "continuare": make_config("continue"),
    "controllare": make_config("check", "control"),
    "convincere": make_config("convince", "persuade"),
    "correggere": make_config("correct", "fix"),
    "correre": make_config(Sense("run", past="ran", participle="run", gerund="running")),
    "costare": make_config(Sense("cost", past="cost", participle="cost", gerund="costing")),
    "costruire": make_config(Sense("build", past="built", participle="built", gerund="building")),
    "creare": make_config("create"),
    "crescere": make_config(Sense("grow", past="grew", participle="grown", gerund="growing")),
    "cucinare": make_config("cook"),
    "danzare": make_config("dance"),
    "decidere": make_config(Sense("decide", past="decided", participle="decided", gerund="deciding")),
    "dimenticare": make_config(Sense("forget", past="forgot", participle="forgotten", gerund="forgetting")),
    "discutere": make_config("discuss", "argue"),
    "dispiacere": make_config(Sense("be sorry"), Sense("mind"), impersonal_subject="it"),
    "distinguere": make_config("distinguish"),
    "disturbare": make_config("disturb", "bother"),
    "divertire": make_config("entertain", "amuse"),
    "dormire": make_config(Sense("sleep", past="slept", participle="slept", gerund="sleeping")),
    "dovere": make_config(
        Sense("have to", third="has to", past="had to", participle="had to", gerund="having to"),
        Sense("owe"),
    ),
    "durare": make_config("last"),
    "da": make_config(GIVE_SENSE),
    "dandolare": make_config(GIVE_SENSE),
    "dannato": make_config("damn"),
    "danneggiare": make_config("damage"),
    "danza": make_config("dance"),
    "danzerare": make_config("dance"),
    "danzere": make_config("dance"),
    "dare": make_config(GIVE_SENSE),
    "dategliare": make_config(GIVE_SENSE),
    "datele": make_config(GIVE_SENSE),
    "datemere": make_config(GIVE_SENSE),
    "debuggare": make_config("debug"),
    "decapitare": make_config("decapitate"),
    "decedere": make_config(Sense("die", past="died", participle="died", gerund="dying")),
    "decentralizzare": make_config("decentralize"),
    "decifrare": make_config("decipher"),
    "declinare": make_config("decline"),
    "decollare": make_config(
        Sense("take off", third="takes off", past="took off", participle="taken off", gerund="taking off")
    ),
    "decorare": make_config("decorate"),
    "decorato": make_config("decorate"),
    "dedere": make_config("deduce"),
    "dedicare": make_config("dedicate"),
    "dedurre": make_config("deduce"),
    "defenire": make_config("defenestrate"),
    "defenistrare": make_config("defenestrate"),
    "definire": make_config("define"),
    "definiscimare": make_config("define"),
    "deformare": make_config("deform", "distort"),
    "deformato": make_config("deform", "distort"),
    "degenerare": make_config("degenerate"),
    "degnare": make_config("deign"),
    "delegare": make_config("delegate"),
    "delimitare": make_config("delimit"),
    "deliziare": make_config("delight"),
    "delocalizzare": make_config("relocate", "offshore"),
    "deludere": make_config("disappoint"),
    "deluso": make_config("disappoint"),
    "demolire": make_config("demolish"),
    "denunciare": make_config("report", "denounce"),
    "depenalizzare": make_config("decriminalize"),
    "depilare": make_config("shave"),
    "deporre": make_config(Sense("lay", past="laid", participle="laid", gerund="laying")),
    "depositare": make_config("deposit"),
    "depostare": make_config("depose"),
    "depravare": make_config("deprave", "corrupt"),
    "deprimere": make_config("depress"),
    "deprire": make_config("depress"),
    "deragliare": make_config("derail"),
    "dere": make_config(GIVE_SENSE),
    "deridere": make_config("mock", "laugh at"),
    "derivante": make_config("derive"),
    "derivare": make_config("derive"),
    "derubare": make_config("rob"),
    "descrivere": make_config("describe"),
    "descrivimare": make_config("describe"),
    "desiderare": make_config("desire", "want"),
    "desideroso": make_config(Sense("be eager", participle="eager")),
    "designare": make_config("designate"),
    "desistere": make_config("desist"),
    "desistiare": make_config("desist"),
    "destinare": make_config(Sense("destine", participle="destined"), "assign"),
    "destrimare": make_config(
        Sense(
            "be right-handed",
            third="is right-handed",
            past="was right-handed",
            participle="been right-handed",
            gerund="being right-handed",
        )
    ),
    "detenere": make_config("detain", "hold"),
    "determinare": make_config("determine"),
    "detestare": make_config("detest"),
    "dettagliato": make_config(Sense("detail", participle="detailed")),
    "devastare": make_config("devastate"),
    "deviare": make_config("divert", "deviate"),
    "devolere": make_config("devote", "donate"),
    "dia": make_config(GIVE_SENSE),
    "diadere": make_config(GIVE_SENSE),
    "diagnosticare": make_config("diagnose"),
    "diamocare": make_config("get to work"),
    "dibattere": make_config("debate", "discuss"),
    "dibattettare": make_config("debate", "discuss"),
    "dicestare": make_config(Sense("say", past="said", participle="said", gerund="saying")),
    "dichiarare": make_config("declare", "state"),
    "diedere": make_config(GIVE_SENSE),
    "difendere": make_config("defend"),
    "difendilare": make_config("defend"),
    "difesa": make_config("defend"),
    "diffamare": make_config("defame"),
    "differenziare": make_config("differentiate"),
    "differire": make_config("differ"),
    "diffidare": make_config("distrust"),
    "diffondare": make_config("spread"),
    "diffondere": make_config("spread", "disseminate"),
    "digerire": make_config("digest"),
    "digitalizzare": make_config("digitize"),
    "digitare": make_config("dial", "type"),
    "digiunare": make_config("fast"),
    "digliere": make_config(TELL_SENSE),
    "dilettare": make_config("delight"),
    "dillo": make_config(SAY_SENSE),
    "diluire": make_config("dilute"),
    "diluito": make_config("dilute"),
    "diluviare": make_config(
        Sense("pour down", third="pours down", past="poured down", participle="poured down", gerund="pouring down")
    ),
    "dimagrire": make_config("lose weight"),
    "dimenare": make_config("wiggle"),
    "dimentere": make_config("forget"),
    "dimenticateverticticare": make_config("forget"),
    "dimentichiamocinte": make_config("forget"),
    "dimentichiamocire": make_config("forget"),
    "dimettere": make_config("resign", "dismiss"),
    "dimezzare": make_config("halve"),
    "diminuire": make_config("decrease"),
    "diminuito": make_config("decrease"),
    "dimmelare": make_config(TELL_SENSE),
    "dimmi": make_config(TELL_SENSE),
    "dimmiare": make_config(TELL_SENSE),
    "dimora": make_config("dwell", "reside"),
    "dimostrare": make_config("prove", "demonstrate"),
    "dipendere": make_config("depend"),
    "dipingere": make_config("paint"),
    "dipinto": make_config("paint"),
    "diplomare": make_config("graduate"),
    "diramare": make_config("branch", "spread"),
    "dire": make_config(Sense("say", past="said", participle="said", gerund="saying")),
    "dirglielere": make_config(TELL_SENSE),
    "dirigere": make_config("direct", "manage"),
    "dirle": make_config(TELL_SENSE),
    "dirmalere": make_config(TELL_SENSE),
    "dirottare": make_config("reroute", "hijack"),
    "dirtelere": make_config(TELL_SENSE),
    "dirvilere": make_config(TELL_SENSE),
    "disappare": make_config("disappear"),
    "disapprovare": make_config("disapprove"),
    "disarmare": make_config("disarm"),
    "disarmato": make_config(Sense("disarm", participle="disarmed")),
    "disattivare": make_config("deactivate"),
    "discendere": make_config("descend"),
    "disciogliere": make_config("dissolve"),
    "disconnetitere": make_config("disconnect", "log off"),
    "discorrere": make_config("talk", "discuss"),
    "discreditare": make_config("discredit"),
    "disegnamere": make_config("draw"),
    "disegnare": make_config("draw"),
    "disfare": make_config("undo", "unpack"),
    "disgungere": make_config(Sense("disgust", participle="disgusting")),
    "disgustare": make_config("disgust"),
    "disidratare": make_config("dehydrate"),
    "disinfettare": make_config("disinfect"),
    "disobbedire": make_config("disobey"),
    "disorganizzato": make_config(Sense("disorganize", participle="disorganized")),
    "disorientare": make_config("disorient"),
    "disperdere": make_config("disperse"),
    "disperso": make_config("disperse"),
    "disporre": make_config(Sense("be willing", participle="willing"), "arrange", "have available"),
    "disprezzare": make_config("despise"),
    "dissipare": make_config("dissipate"),
    "dissociare": make_config("dissociate"),
    "dissolvere": make_config("dissolve"),
    "dissuadere": make_config("dissuade"),
    "distaccato": make_config(
        Sense("detach", participle="detached"),
        Sense("outdistance", past="outdistanced", participle="outdistanced", gerund="outdistancing"),
    ),
    "distanziare": make_config("distance", "outdistance"),
    "distare": make_config(
        Sense("be far", third="is far", past="was far", participle="been far", gerund="being far")
    ),
    "distillare": make_config("distill"),
    "distinguo": make_config("distinguish"),
    "distogliere": make_config("divert", "look away"),
    "distorcare": make_config("distort"),
    "distorcere": make_config("distort"),
    "distorcire": make_config("distort"),
    "distraggiare": make_config("distract"),
    "distrarre": make_config("distract"),
    "distrarree": make_config(Sense("distract", participle="distracted")),
    "distribuire": make_config("distribute"),
    "distriggere": make_config("destroy", "crush"),
    "distruggere": make_config("destroy"),
    "distruo": make_config("destroy"),
    "distrussare": make_config("destroy", "crush"),
    "distrutto": make_config("destroy"),
    "disturbatemare": make_config("bother"),
    "disturbatevere": make_config("bother"),
    "disturbo": make_config("bother"),
    "ditecere": make_config(TELL_SENSE),
    "ditegliare": make_config(TELL_SENSE),
    "ditelare": make_config(SAY_SENSE),
    "ditele": make_config(TELL_SENSE),
    "ditemere": make_config(TELL_SENSE),
    "ditenle": make_config(TELL_SENSE),
    "divenire": make_config(Sense("become", past="became", participle="become", gerund="becoming")),
    "diventante": make_config(Sense("become", participle="becoming")),
    "diventare": make_config(Sense("become", past="became", participle="become", gerund="becoming")),
    "diverire": make_config("have fun"),
    "divertare": make_config("amuse"),
    "divertiamoci": make_config("have fun"),
    "divertiare": make_config("have fun"),
    "dividere": make_config("divide"),
    "dividiamoci": make_config("divide"),
    "dividilare": make_config("divide"),
    "divorare": make_config("devour"),
    "divorzare": make_config("divorce"),
    "divorziare": make_config("divorce"),
    "divorziata": make_config("divorce"),
    "divorziato": make_config("divorce"),
    "do": make_config(GIVE_SENSE),
    "documentare": make_config("document"),
    "domandare": make_config("ask"),
    "domare": make_config("tame", "extinguish"),
    "dominare": make_config("dominate"),
    "donare": make_config("donate"),
    "dondolare": make_config("swing", "wobble"),
    "doppiare": make_config("dub"),
    "dore": make_config("sleep"),
    "dossire": make_config(GIVE_SENSE),
    "dotare": make_config("endow"),
    "driggere": make_config("go straight"),
    "drogare": make_config("drug"),
    "dubire": make_config("doubt"),
    "dubitare": make_config("doubt"),
    "duolere": make_config("ache"),
    "durire": make_config("last"),
    "entrare": make_config("enter", "go in"),
    "esistere": make_config("exist"),
    "fallire": make_config("fail"),
    "farmare": make_config("farm"),
    "ferire": make_config("injure", "hurt"),
    "fermare": make_config(
        Sense("stop"),
        Sense("halt"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "fidare": make_config(
        Sense("trust"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "fissare": make_config("fix", "stare at"),
    "fornire": make_config("provide", "supply"),
    "fumare": make_config("smoke"),
    "funzionare": make_config("work", "function"),
    "galleggiare": make_config("float"),
    "galoppare": make_config("gallop"),
    "garantire": make_config("guarantee", "ensure"),
    "gareggiare": make_config("compete"),
    "gelare": make_config("freeze"),
    "gemellare": make_config("twin"),
    "generare": make_config("generate"),
    "germogliare": make_config("sprout", "bud"),
    "gestare": make_config("gestate"),
    "gesticolare": make_config("gesture"),
    "gestire": make_config("manage", "run"),
    "gettare": make_config("throw"),
    "ghiacciare": make_config("freeze"),
    "giacere": make_config("lie"),
    "giocherellare": make_config("play around"),
    "girare": make_config("turn", "go around"),
    "giudicare": make_config("judge"),
    "giungere": make_config("arrive", "reach"),
    "giurare": make_config("swear"),
    "giustificare": make_config("justify"),
    "giustiziare": make_config("execute"),
    "glorificare": make_config("glorify"),
    "godere": make_config("enjoy"),
    "gonfiare": make_config("inflate", "swell"),
    "governare": make_config("govern", "rule"),
    "gradire": make_config("appreciate", "like"),
    "graffiare": make_config("scratch"),
    "grandinare": make_config("hail"),
    "grattare": make_config("scrape", "scratch"),
    "gridare": make_config("shout", "yell"),
    "grigliare": make_config("grill"),
    "grugnire": make_config("grunt"),
    "guadagnare": make_config("earn", "gain"),
    "guarire": make_config("heal", "get better"),
    "guastare": make_config("ruin", "spoil"),
    "guidare": make_config("drive", "guide"),
    "gustare": make_config("taste", "enjoy"),
    "ignorare": make_config("ignore"),
    "immaginare": make_config("imagine"),
    "impegnare": make_config("commit", "engage"),
    "importare": make_config("matter", "import"),
    "incolpare": make_config("blame"),
    "incontrare": make_config(Sense("meet", past="met", participle="met", gerund="meeting")),
    "indossare": make_config(Sense("wear", past="wore", participle="worn", gerund="wearing")),
    "infastidire": make_config("annoy", "bother"),
    "iniziare": make_config("start", Sense("begin", past="began", participle="begun", gerund="beginning")),
    "innamorare": make_config(
        Sense("fall in love"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "insegnare": make_config(Sense("teach", past="taught", participle="taught", gerund="teaching")),
    "intendere": make_config("mean", "intend"),
    "interessare": make_config("interest", "matter"),
    "inventare": make_config("invent"),
    "invitare": make_config("invite"),
    "lamentare": make_config("complain", "lament"),
    "lavare": make_config(
        Sense("wash"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
    ),
    "licenziare": make_config("fire", "dismiss"),
    "mancare": make_config("miss", "lack"),
    "mandare": make_config(Sense("send", past="sent", participle="sent", gerund="sending")),
    "mantenere": make_config("maintain", Sense("keep", past="kept", participle="kept", gerund="keeping")),
    "mentire": make_config(Sense("lie", past="lied", participle="lied", gerund="lying")),
    "meritare": make_config("deserve"),
    "migliorare": make_config("improve"),
    "mostrare": make_config(Sense("show", past="showed", participle="shown", gerund="showing")),
    "nascere": make_config("be born"),
    "nascondere": make_config(Sense("hide", past="hid", participle="hidden", gerund="hiding")),
    "negare": make_config("deny"),
    "nevicare": make_config("snow", impersonal_subject="it"),
    "nuotare": make_config(Sense("swim", past="swam", participle="swum", gerund="swimming")),
    "occupare": make_config("occupy", "take up"),
    "odiare": make_config("hate"),
    "offrire": make_config("offer"),
    "ordinare": make_config("order"),
    "ottenere": make_config("obtain", Sense("get", past="got", participle="gotten", gerund="getting")),
    "pagare": make_config(Sense("pay", past="paid", participle="paid", gerund="paying")),
    "partecipare": make_config("participate", "attend"),
    "partire": make_config(Sense("leave", past="left", participle="left", gerund="leaving"), Sense("depart")),
    "perdonare": make_config("forgive"),
    "permettere": make_config("allow", "permit"),
    "pescare": make_config("fish"),
    "piangere": make_config(Sense("cry", past="cried", participle="cried", gerund="crying")),
    "piovere": make_config("rain", impersonal_subject="it"),
    "possedere": make_config("own", "possess"),
    "potere": make_config(Sense("be able to", third="is able to", past="was able to", participle="been able to", gerund="being able to")),
    "pranzare": make_config("have lunch"),
    "preferire": make_config("prefer"),
    "pregare": make_config("pray", "beg"),
    "preoccupare": make_config(
        Sense("worry"),
        Sense("concern"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "preparare": make_config("prepare"),
    "presentare": make_config("present", "introduce"),
    "prestare": make_config("lend", "loan"),
    "promettere": make_config("promise"),
    "proteggere": make_config("protect"),
    "pulire": make_config("clean"),
    "raccontare": make_config("tell", "narrate"),
    "raggiungere": make_config("reach", "attain"),
    "rendere": make_config(Sense("make", past="made", participle="made", gerund="making"), "render"),
    "restare": make_config("stay", "remain"),
    "ricevere": make_config("receive"),
    "riconoscere": make_config("recognize"),
    "ricordare": make_config("remember"),
    "ridere": make_config("laugh"),
    "rifiutare": make_config("refuse", "reject"),
    "ringraziare": make_config("thank"),
    "riparare": make_config("repair"),
    "ripetere": make_config("repeat"),
    "riposare": make_config("rest"),
    "risolvere": make_config("solve", "resolve"),
    "rispondere": make_config("answer", "respond"),
    "ritornare": make_config("return", "come back"),
    "rompere": make_config(Sense("break", past="broke", participle="broken", gerund="breaking")),
    "rubare": make_config(Sense("steal", past="stole", participle="stolen", gerund="stealing")),
    "salire": make_config("go up", "climb"),
    "saltare": make_config("jump", "skip"),
    "salvare": make_config("save", "rescue"),
    "sapete": make_config(Sense("know", past="knew", participle="known", gerund="knowing")),
    "sbagliare": make_config("make a mistake", "be wrong"),
    "sbrigare": make_config("take care of", "hurry"),
    "scambiare": make_config("exchange", "trade"),
    "scappare": make_config("escape", "run away"),
    "scegliere": make_config(Sense("choose", past="chose", participle="chosen", gerund="choosing")),
    "scoppiare": make_config("explode", "burst"),
    "scoprire": make_config("discover", "uncover"),
    "scordare": make_config(Sense("forget", past="forgot", participle="forgotten", gerund="forgetting")),
    "scusare": make_config("excuse", "forgive"),
    "sedere": make_config(Sense("sit", past="sat", participle="sat", gerund="sitting")),
    "seguire": make_config("follow"),
    "servire": make_config("serve"),
    "significare": make_config("mean"),
    "smettere": make_config("stop", "quit"),
    "soddisfare": make_config("satisfy", "fulfill"),
    "soffrire": make_config("suffer"),
    "sognare": make_config("dream"),
    "sopportare": make_config("endure", "tolerate"),
    "sorprendere": make_config("surprise"),
    "sorridere": make_config("smile"),
    "sparare": make_config(Sense("shoot", past="shot", participle="shot", gerund="shooting")),
    "spegnere": make_config("turn off", "switch off"),
    "spendere": make_config(Sense("spend", past="spent", participle="spent", gerund="spending")),
    "sperare": make_config("hope"),
    "spiegare": make_config("explain"),
    "sposare": make_config(
        Sense("marry"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "succedere": make_config("happen"),
    "suggerire": make_config("suggest"),
    "suicidare": make_config(
        Sense("kill oneself"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "suonare": make_config("play", Sense("ring", past="rang", participle="rung", gerund="ringing")),
    "superare": make_config(Sense("overcome", past="overcame", participle="overcome", gerund="overcoming"), "surpass"),
    "svegliare": make_config(
        Sense("wake"),
        Sense("wake up"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "tagliare": make_config(Sense("cut", past="cut", participle="cut", gerund="cutting")),
    "temere": make_config("fear"),
    "tenere": make_config(Sense("keep", past="kept", participle="kept", gerund="keeping"), Sense("hold")),
    "tirare": make_config("pull", "throw"),
    "toccare": make_config("touch"),
    "togliere": make_config("remove", "take off"),
    "tradurre": make_config("translate"),
    "trascorrere": make_config(Sense("spend", past="spent", participle="spent", gerund="spending"), Sense("pass")),
    "trasferire": make_config("transfer", "relocate"),
    "trattare": make_config("treat", "deal with"),
    "uccidere": make_config("kill"),
    "unire": make_config("unite", "join"),
    "urlare": make_config("shout", "yell"),
    "usare": make_config("use"),
    "uscire": make_config("go out", "exit"),
    "utilizzare": make_config("use", "utilize"),
    "valere": make_config(Sense("be worth"), Sense("matter")),
    "vedertare": make_config(Sense("see", past="saw", participle="seen", gerund="seeing")),
    "vendere": make_config(Sense("sell", past="sold", participle="sold", gerund="selling")),
    "vestire": make_config(
        Sense("dress"),
        Sense("get dressed"),
        reflexive_prons=set(REFLEXIVE_PRON_SET),
        drop_reflexive_objects=True,
    ),
    "viaggiare": make_config("travel"),
    "vincere": make_config(Sense("win", past="won", participle="won", gerund="winning")),
    "visitare": make_config("visit"),
    "vivo": make_config("live"),
    "volare": make_config(Sense("fly", past="flew", participle="flown", gerund="flying")),
    "votare": make_config("vote"),
}


def main() -> None:
    base_dir = Path("vocabulary/dataframes/VERB")
    updated = 0
    for path in sorted(base_dir.glob("*.tsv")):
        try:
            changed = process_file(path)
        except KeyError as exc:
            print(exc)
            continue
        if changed:
            updated += 1
            print(f"Updated {path.name}")
    print(f"Completed {updated} verb files")


if __name__ == "__main__":
    main()
