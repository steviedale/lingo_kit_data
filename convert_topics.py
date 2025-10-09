"""Utility for consolidating topic tags across dataframe CSVs.

This script reads every CSV located in ``dataframes/dataframes_by_pos`` and
replaces the granular topic tags with a curated set of broader, user-friendly
categories. The mapping from the original topics to the new categories is
captured in :data:`OLD_TO_NEW_TOPIC`, while :data:`CATEGORY_PRIORITY` controls
how categories are prioritised when limiting each term to at most three
topics.

Run the script directly to update the CSV files in place. Use ``--dry-run`` to
preview the derived topics without modifying any files.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

import pandas as pd

# Root directory containing the part-of-speech specific dataframe folders.
DATAFRAME_ROOT = Path(__file__).resolve().parent / "dataframes" / "dataframes_by_pos"

# Curated list of higher-level, user-friendly topics (20-50 categories).
CATEGORY_PRIORITY: List[str] = [
    "Food & Drink",
    "Travel & Transportation",
    "Shopping & Money",
    "Home & Household",
    "Places & Infrastructure",
    "Nature & Environment",
    "Weather & Seasons",
    "Time & Calendar",
    "Numbers & Measurement",
    "Science & Math",
    "Work & Professions",
    "Education & Knowledge",
    "Technology & Media",
    "Arts & Entertainment",
    "Sports & Fitness",
    "Government & Law",
    "Religion & Culture",
    "Services & Logistics",
    "Events & Celebrations",
    "People & Society",
    "Family & Relationships",
    "Emotions & Personality",
    "Body & Health",
    "Clothing & Appearance",
    "Objects & Materials",
    "Descriptions & Qualities",
    "Ideas & Concepts",
    "Communication & Conversation",
    "Grammar & Structure",
    "Core Language",
]

# Mapping from the original topic tags to their broader category.
OLD_TO_NEW_TOPIC: Dict[str, str] = {
    "abstract": "Ideas & Concepts",
    "accommodation": "Travel & Transportation",
    "achievement": "Ideas & Concepts",
    "action": "Ideas & Concepts",
    "actions": "Ideas & Concepts",
    "addition": "Science & Math",
    "administration": "Government & Law",
    "affection": "Emotions & Personality",
    "affirmation": "Communication & Conversation",
    "age": "People & Society",
    "air": "Nature & Environment",
    "airport": "Travel & Transportation",
    "apology": "Communication & Conversation",
    "appearance": "Clothing & Appearance",
    "approximation": "Numbers & Measurement",
    "architecture": "Places & Infrastructure",
    "articles": "Grammar & Structure",
    "arts": "Arts & Entertainment",
    "aspect": "Grammar & Structure",
    "assessment": "Education & Knowledge",
    "availability": "Services & Logistics",
    "banking": "Shopping & Money",
    "bathroom": "Home & Household",
    "behavior": "Emotions & Personality",
    "belief": "Religion & Culture",
    "body": "Body & Health",
    "books": "Education & Knowledge",
    "buildings": "Places & Infrastructure",
    "business": "Work & Professions",
    "calendar": "Time & Calendar",
    "cause": "Ideas & Concepts",
    "celebrations": "Events & Celebrations",
    "certainty": "Communication & Conversation",
    "character": "Emotions & Personality",
    "city": "Places & Infrastructure",
    "clarity": "Descriptions & Qualities",
    "clause": "Grammar & Structure",
    "clothing": "Clothing & Appearance",
    "color": "Clothing & Appearance",
    "commerce": "Shopping & Money",
    "common": "Core Language",
    "communication": "Communication & Conversation",
    "community": "People & Society",
    "comparison": "Grammar & Structure",
    "concepts": "Ideas & Concepts",
    "condition": "Descriptions & Qualities",
    "connector": "Grammar & Structure",
    "connectors": "Grammar & Structure",
    "construction": "Places & Infrastructure",
    "contrast": "Grammar & Structure",
    "conversation": "Communication & Conversation",
    "cooking": "Food & Drink",
    "core": "Core Language",
    "correlative": "Grammar & Structure",
    "counting": "Numbers & Measurement",
    "crime": "Government & Law",
    "culture": "Religion & Culture",
    "currency": "Shopping & Money",
    "dayparts": "Time & Calendar",
    "debate": "Communication & Conversation",
    "degree": "Numbers & Measurement",
    "deixis": "Grammar & Structure",
    "demonstratives": "Grammar & Structure",
    "description": "Descriptions & Qualities",
    "descriptions": "Descriptions & Qualities",
    "descriptive": "Descriptions & Qualities",
    "design": "Arts & Entertainment",
    "determiner": "Grammar & Structure",
    "dining": "Food & Drink",
    "direction": "Travel & Transportation",
    "directions": "Travel & Transportation",
    "discourse": "Communication & Conversation",
    "documents": "Services & Logistics",
    "drink": "Food & Drink",
    "drinks": "Food & Drink",
    "driving": "Travel & Transportation",
    "economy": "Shopping & Money",
    "education": "Education & Knowledge",
    "emotion": "Emotions & Personality",
    "emphasis": "Communication & Conversation",
    "environment": "Nature & Environment",
    "ethics": "Religion & Culture",
    "events": "Events & Celebrations",
    "everyday": "Core Language",
    "fabric": "Objects & Materials",
    "family": "Family & Relationships",
    "feelings": "Emotions & Personality",
    "finance": "Shopping & Money",
    "fitness": "Sports & Fitness",
    "focus": "Ideas & Concepts",
    "food": "Food & Drink",
    "formal": "Communication & Conversation",
    "fraction": "Science & Math",
    "frequency": "Numbers & Measurement",
    "furniture": "Home & Household",
    "geography": "Education & Knowledge",
    "geometry": "Science & Math",
    "government": "Government & Law",
    "grammar": "Grammar & Structure",
    "greetings": "Communication & Conversation",
    "habits": "People & Society",
    "health": "Body & Health",
    "history": "Education & Knowledge",
    "home": "Home & Household",
    "household": "Home & Household",
    "hygiene": "Body & Health",
    "ideas": "Ideas & Concepts",
    "identity": "People & Society",
    "immediacy": "Communication & Conversation",
    "importance": "Ideas & Concepts",
    "industry": "Work & Professions",
    "informal": "Communication & Conversation",
    "infrastructure": "Places & Infrastructure",
    "instruments": "Arts & Entertainment",
    "intensity": "Descriptions & Qualities",
    "jewelry": "Clothing & Appearance",
    "kitchen": "Home & Household",
    "knowledge": "Education & Knowledge",
    "language": "Education & Knowledge",
    "law": "Government & Law",
    "learning": "Education & Knowledge",
    "leisure": "Arts & Entertainment",
    "life": "People & Society",
    "light": "Nature & Environment",
    "linking": "Grammar & Structure",
    "location": "Places & Infrastructure",
    "logic": "Science & Math",
    "logistics": "Services & Logistics",
    "mail": "Services & Logistics",
    "manner": "Descriptions & Qualities",
    "material": "Objects & Materials",
    "materials": "Objects & Materials",
    "math": "Science & Math",
    "meals": "Food & Drink",
    "measurement": "Numbers & Measurement",
    "media": "Technology & Media",
    "mind": "Ideas & Concepts",
    "modification": "Grammar & Structure",
    "money": "Shopping & Money",
    "motion": "Travel & Transportation",
    "movement": "Travel & Transportation",
    "music": "Arts & Entertainment",
    "nationality": "People & Society",
    "nature": "Nature & Environment",
    "nautical": "Travel & Transportation",
    "needs": "People & Society",
    "negation": "Grammar & Structure",
    "news": "Technology & Media",
    "noun": "Grammar & Structure",
    "number": "Numbers & Measurement",
    "numbers": "Numbers & Measurement",
    "objects": "Objects & Materials",
    "obligation": "Government & Law",
    "opinion": "Communication & Conversation",
    "order": "Services & Logistics",
    "ordering": "Services & Logistics",
    "organization": "Work & Professions",
    "origin": "People & Society",
    "ownership": "Shopping & Money",
    "people": "People & Society",
    "perception": "Body & Health",
    "permission": "Government & Law",
    "physical": "Body & Health",
    "place": "Places & Infrastructure",
    "places": "Places & Infrastructure",
    "planning": "Services & Logistics",
    "plants": "Nature & Environment",
    "politeness": "Communication & Conversation",
    "politics": "Government & Law",
    "position": "Places & Infrastructure",
    "possessive": "Grammar & Structure",
    "preposition": "Grammar & Structure",
    "prepositions": "Grammar & Structure",
    "price": "Shopping & Money",
    "profession": "Work & Professions",
    "professions": "Work & Professions",
    "pronoun": "Grammar & Structure",
    "pronouns": "Grammar & Structure",
    "property": "Places & Infrastructure",
    "proximity": "Places & Infrastructure",
    "quality": "Descriptions & Qualities",
    "quantifier": "Numbers & Measurement",
    "quantifiers": "Numbers & Measurement",
    "quantities": "Numbers & Measurement",
    "quantity": "Numbers & Measurement",
    "question": "Communication & Conversation",
    "questions": "Communication & Conversation",
    "reading": "Education & Knowledge",
    "reason": "Ideas & Concepts",
    "reference": "Communication & Conversation",
    "regulation": "Government & Law",
    "relation": "Family & Relationships",
    "relationships": "Family & Relationships",
    "religion": "Religion & Culture",
    "restaurant": "Food & Drink",
    "rooms": "Home & Household",
    "royalty": "Government & Law",
    "science": "Science & Math",
    "seasons": "Weather & Seasons",
    "senses": "Body & Health",
    "sequence": "Grammar & Structure",
    "service": "Services & Logistics",
    "services": "Services & Logistics",
    "shopping": "Shopping & Money",
    "size": "Descriptions & Qualities",
    "social": "People & Society",
    "society": "People & Society",
    "sound": "Nature & Environment",
    "space": "Science & Math",
    "spatial": "Descriptions & Qualities",
    "speed": "Numbers & Measurement",
    "sports": "Sports & Fitness",
    "state": "Government & Law",
    "stationery": "Education & Knowledge",
    "status": "People & Society",
    "storage": "Services & Logistics",
    "style": "Clothing & Appearance",
    "taste": "Food & Drink",
    "technology": "Technology & Media",
    "temperature": "Weather & Seasons",
    "texture": "Descriptions & Qualities",
    "time": "Time & Calendar",
    "transport": "Travel & Transportation",
    "travel": "Travel & Transportation",
    "uncertainty": "Communication & Conversation",
    "vegetables": "Food & Drink",
    "vehicles": "Travel & Transportation",
    "verb": "Grammar & Structure",
    "water": "Nature & Environment",
    "wealth": "Shopping & Money",
    "weather": "Weather & Seasons",
    "web": "Technology & Media",
    "work": "Work & Professions",
}

# Topics that can be dropped when a term is already covered by more specific
# categories. These are applied when limiting each term to 1-3 topics.
FALLBACK_TOPICS: Sequence[str] = (
    "Core Language",
    "Ideas & Concepts",
    "Descriptions & Qualities",
)


def extract_topics(values: Iterable[str]) -> Set[str]:
    """Return the set of unique topic labels contained in *values*."""

    topics: Set[str] = set()
    for raw in values:
        if not isinstance(raw, str):
            continue
        for part in raw.split(","):
            label = part.strip()
            if label:
                topics.add(label)
    return topics


def map_to_general_topics(old_topics: Set[str]) -> List[str]:
    """Translate granular topic labels to the curated general categories."""

    if not old_topics:
        return ["Core Language"]

    missing = sorted(topic for topic in old_topics if topic not in OLD_TO_NEW_TOPIC)
    if missing:
        raise KeyError(f"Topics missing from mapping: {', '.join(missing)}")

    general_topics = {OLD_TO_NEW_TOPIC[topic] for topic in old_topics}

    # Drop fallback buckets when other categories are available so that we can
    # keep each term to a concise set of topics.
    for fallback in FALLBACK_TOPICS:
        if len(general_topics) > 3 and fallback in general_topics and len(general_topics) > 1:
            general_topics.remove(fallback)

    ordered = [topic for topic in CATEGORY_PRIORITY if topic in general_topics]
    if len(ordered) > 3:
        ordered = ordered[:3]
    return ordered or ["Core Language"]


def load_topics_from_csv(csv_path: Path) -> Set[str]:
    """Collect the original topic tags from *csv_path* using the csv module."""

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        try:
            topics_index = header.index("topics")
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Missing 'topics' column in {csv_path}") from exc

        values = [row[topics_index] for row in reader if topics_index < len(row)]

    return extract_topics(values)


def update_topics(csv_path: Path, dry_run: bool = False) -> List[str]:
    """Update the topics column for *csv_path* and return the new topics."""

    old_topics = load_topics_from_csv(csv_path)
    new_topics = map_to_general_topics(old_topics)
    new_value = ",".join(new_topics)

    if not dry_run:
        df = pd.read_csv(csv_path, engine="python")
        if "topics" not in df.columns:
            raise ValueError(f"Missing 'topics' column in {csv_path}")
        df["topics"] = new_value
        df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)

    return new_topics


def build_topics_map(root: Path, dry_run: bool = False) -> Dict[Path, List[str]]:
    """Generate the mapping from CSV path to its curated topic list."""

    mapping: Dict[Path, List[str]] = {}
    for csv_path in sorted(root.rglob("*.csv")):
        topics = update_topics(csv_path, dry_run=dry_run)
        mapping[csv_path.relative_to(root)] = topics
    return mapping


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=DATAFRAME_ROOT,
        help="Root directory containing the dataframe CSV files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the curated topics without modifying any CSV files.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    root = args.root
    if not root.exists():
        raise FileNotFoundError(f"Dataframe root does not exist: {root}")

    topics_map = build_topics_map(root, dry_run=args.dry_run)

    if args.dry_run:
        for relative_path, topics in topics_map.items():
            display_topics = ", ".join(topics)
            print(f"{relative_path}: {display_topics}")
    else:
        print(f"Updated topics for {len(topics_map)} CSV files under {root}.")


if __name__ == "__main__":
    main()
