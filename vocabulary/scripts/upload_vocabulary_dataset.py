#!/usr/bin/env python3
"""Upload TSV vocabulary data into the backend via the public API."""

from __future__ import annotations

import argparse
import csv
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests


LOGGER = logging.getLogger(__name__)


@dataclass
class APIConfig:
    base_api_url: str
    prune_missing: bool = False


def api_url(base_api_url: str, path: str) -> str:
    """Join base_api_url and a relative path ensuring a trailing slash.

    Django (with APPEND_SLASH=True) expects collection and detail endpoints to
    end with a slash (e.g., "/auth/login/", "/terms/123/"). If a POST hits
    the non-slashed URL, Django tries to redirect and loses the POST body.

    This helper guarantees the URL we produce ends with a slash before any
    query or fragment component.
    """
    path = path.lstrip("/")
    if not path.endswith("/"):
        # Preserve query/fragment when appending the trailing slash
        main = path.split("?", 1)[0].split("#", 1)[0]
        suffix = path[len(main) :]
        if not main.endswith("/"):
            main = f"{main}/"
        path = main + suffix
    return urljoin(base_api_url, path)


def optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def optional_int(value: Any) -> int | None:
    text = optional_str(value)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        LOGGER.debug("Could not convert %s to int", text)
        return None


def optional_float(value: Any) -> float | None:
    text = optional_str(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        LOGGER.debug("Could not convert %s to float", text)
        return None


def optional_decimal_str(value: Any) -> str | None:
    text = optional_str(value)
    if text is None:
        return None
    try:
        return str(Decimal(text))
    except (InvalidOperation, ValueError):
        LOGGER.debug("Could not convert %s to decimal", text)
        return None


def get_paginated(
    session: requests.Session,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    url = endpoint
    request_params = params
    while url:
        response = session.get(url, params=request_params)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and "results" in payload:
            results.extend(payload["results"])
            url = payload.get("next")
            request_params = None
        else:
            if isinstance(payload, list):
                results.extend(payload)
            else:
                results.append(payload)
            break
    return results


def login(session: requests.Session, config: APIConfig, *, email: str, password: str) -> None:
    LOGGER.info("Authenticating as %s", email)
    response = session.post(
        api_url(config.base_api_url, "auth/login"),
        json={"email": email, "password": password},
    )
    response.raise_for_status()
    data = response.json()
    tokens = data.get("tokens", {})
    access = tokens.get("access")
    if not access:
        raise RuntimeError("Login response did not include an access token.")
    session.headers.update({"Authorization": f"Bearer {access}"})


def ensure_topic(
    session: requests.Session,
    config: APIConfig,
    *,
    topic_name: str,
) -> int:
    endpoint = api_url(config.base_api_url, "topics")
    records = get_paginated(session, endpoint, params={"topic": topic_name})
    if records:
        return records[0]["id"]
    LOGGER.debug("Creating topic %s", topic_name)
    response = session.post(endpoint, json={"topic": topic_name})
    response.raise_for_status()
    return response.json()["id"]


def ensure_group(
    session: requests.Session,
    config: APIConfig,
    *,
    payload: dict[str, Any],
) -> int:
    endpoint = api_url(config.base_api_url, "groups")
    query = {
        "lemma": payload.get("lemma"),
        "part_of_speech": payload.get("part_of_speech"),
    }
    records = get_paginated(session, endpoint, params=query)
    if records:
        group_id = records[0]["id"]
        LOGGER.debug("Updating group %s (%s)", payload.get("lemma"), payload.get("part_of_speech"))
        response = session.patch(api_url(config.base_api_url, f"groups/{group_id}"), json=payload)
        response.raise_for_status()
        return group_id
    LOGGER.info("Creating group %s (%s)", payload.get("lemma"), payload.get("part_of_speech"))
    response = session.post(endpoint, json=payload)
    response.raise_for_status()
    data = response.json()
    return data["id"]


def ensure_term(
    session: requests.Session,
    config: APIConfig,
    *,
    payload: dict[str, Any],
) -> int:
    endpoint = api_url(config.base_api_url, "terms")
    records = get_paginated(session, endpoint, params={"term_italian": payload.get("term_italian")})
    if records:
        term_id = records[0]["id"]
        LOGGER.debug("Updating term %s", payload.get("term_italian"))
        response = session.patch(api_url(config.base_api_url, f"terms/{term_id}"), json=payload)
        response.raise_for_status()
        return term_id
    LOGGER.debug("Creating term %s", payload.get("term_italian"))
    response = session.post(endpoint, json=payload)
    response.raise_for_status()
    return response.json()["id"]


def sync_group_topics(
    session: requests.Session,
    config: APIConfig,
    *,
    group_id: int,
    topic_names: Iterable[str],
) -> None:
    desired_topic_ids = set()
    for name in topic_names:
        topic_id = ensure_topic(session, config, topic_name=name)
        desired_topic_ids.add(topic_id)

    endpoint = api_url(config.base_api_url, "group-topics")
    existing = get_paginated(session, endpoint, params={"group": group_id})
    existing_by_topic = {item["topic"]: item for item in existing}

    for topic_id in desired_topic_ids - existing_by_topic.keys():
        LOGGER.debug("Linking group %s to topic %s", group_id, topic_id)
        response = session.post(endpoint, json={"group": group_id, "topic": topic_id})
        if response.status_code not in (200, 201):
            response.raise_for_status()

    if not config.prune_missing:
        return

    for topic_id, record in existing_by_topic.items():
        if topic_id in desired_topic_ids:
            continue
        record_id = record["id"]
        LOGGER.debug("Removing obsolete topic %s from group %s", topic_id, group_id)
        response = session.delete(api_url(config.base_api_url, f"group-topics/{record_id}"))
        if response.status_code not in (200, 202, 204):
            response.raise_for_status()


def parse_feats(feats: str | None) -> dict[str, str]:
    result: dict[str, str] = {}
    if not feats:
        return result
    for item in feats.split("|"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def translation_key_from_row(row: dict[str, str]) -> tuple[str, ...]:
    token_hash = optional_str(row.get("token_hash"))
    if token_hash:
        return ("hash", token_hash)
    return (
        "term_translation",
        row.get("text", "").lower(),
        (optional_str(row.get("translation_en")) or "").lower(),
    )


def translation_key_from_record(record: dict[str, Any]) -> tuple[str, ...]:
    token_hash = optional_str(record.get("token_hash"))
    if token_hash:
        return ("hash", token_hash)
    return (
        "term_translation",
        record.get("term_detail", {}).get("term_italian", "").lower(),
        (optional_str(record.get("translation_english")) or "").lower(),
    )


def sync_translations(
    session: requests.Session,
    config: APIConfig,
    *,
    group_id: int,
    rows: list[dict[str, str]],
) -> None:
    endpoint = api_url(config.base_api_url, "term-translations")
    existing = get_paginated(session, endpoint, params={"group": group_id})
    existing_map = {translation_key_from_record(item): item for item in existing}

    seen_keys: set[tuple[str, ...]] = set()
    for position, row in enumerate(rows, start=1):
        term_payload = {
            "term_italian": row["text"],
            "audio_hash_italian": optional_str(row.get("italian_audio_hash")),
            "pronunciation": optional_str(row.get("pronunciation")),
        }
        term_id = ensure_term(session, config, payload=term_payload)

        feats = row.get("feats")
        feats_map = parse_feats(feats)
        payload = {
            "term": term_id,
            "group": group_id,
            "translation_english": row.get("translation_en"),
            "part_of_speech": row.get("pos"),
            "parts_of_speech": optional_str(row.get("parts_pos")),
            "parts_of_lemma": optional_str(row.get("parts_lemma")),
            "gender": optional_str(feats_map.get("Gender")),
            "number": optional_str(feats_map.get("Number")),
            "mood": optional_str(feats_map.get("Mood")),
            "person": optional_str(feats_map.get("Person")),
            "tense": optional_str(feats_map.get("Tense")),
            "verb_form": optional_str(feats_map.get("VerbForm")),
            "feats": optional_str(feats),
            "xpos": optional_str(row.get("xpos")),
            "dependency_relation": optional_str(row.get("deprel")),
            "token_count": optional_int(row.get("token_count")),
            "token_pct": optional_decimal_str(row.get("token_pct")),
            "percentile": optional_decimal_str(row.get("percentile")),
            "token_hash": optional_str(row.get("token_hash")),
            "group_hash": optional_str(row.get("group_hash")),
            "sentence_1_italian": optional_str(row.get("sentence_1_it")),
            "sentence_1_english": optional_str(row.get("sentence_1_en")),
            "sentence_2_italian": optional_str(row.get("sentence_2_it")),
            "sentence_2_english": optional_str(row.get("sentence_2_en")),
            "sentence_3_italian": optional_str(row.get("sentence_3_it")),
            "sentence_3_english": optional_str(row.get("sentence_3_en")),
            "english_audio_hash": optional_str(row.get("english_audio_hash")),
            "english_audio_duration_ms": optional_float(row.get("english_duration_ms")),
            "italian_audio_hash": optional_str(row.get("italian_audio_hash")),
            "italian_audio_duration_ms": optional_float(row.get("italian_duration_ms")),
            "position": position,
        }

        key = translation_key_from_row(row)
        seen_keys.add(key)
        if key in existing_map:
            record_id = existing_map[key]["id"]
            LOGGER.debug(
                "Updating translation %s for term %s",
                record_id,
                row["text"],
            )
            response = session.patch(
                api_url(config.base_api_url, f"term-translations/{record_id}"),
                json=payload,
            )
            response.raise_for_status()
        else:
            LOGGER.debug(
                "Creating translation for term %s (%s)",
                row["text"],
                row.get("translation_en"),
            )
            response = session.post(endpoint, json=payload)
            response.raise_for_status()

    if not config.prune_missing:
        return

    for key, record in existing_map.items():
        if key in seen_keys:
            continue
        record_id = record["id"]
        LOGGER.debug("Deleting obsolete translation %s", record_id)
        response = session.delete(api_url(config.base_api_url, f"term-translations/{record_id}"))
        if response.status_code not in (200, 202, 204):
            response.raise_for_status()


def load_group_info(path: Path) -> dict[str, dict[str, Any]]:
    with path.open() as handle:
        data = json.load(handle)
    info: dict[str, dict[str, Any]] = {}
    for key, value in data.items():
        if key == "all_topics":
            continue
        if not isinstance(value, dict):
            continue
        info[key.lower()] = value
    return info


def process_tsv(
    session: requests.Session,
    config: APIConfig,
    *,
    tsv_path: Path,
    group_meta: dict[str, Any] | None,
) -> None:
    LOGGER.info("Processing %s", tsv_path)
    with tsv_path.open() as handle:
        reader = list(csv.DictReader(handle, delimiter="\t"))
    if not reader:
        LOGGER.warning("File %s is empty; skipping", tsv_path)
        return

    first_row = reader[0]
    group_payload = {
        "lemma": first_row.get("lemma"),
        "english": optional_str((group_meta or {}).get("english")),
        "part_of_speech": first_row.get("pos"),
        "parts_of_speech": optional_str(first_row.get("parts_pos")),
        "parts_of_lemma": optional_str(first_row.get("parts_lemma")),
        "group_count": optional_int(first_row.get("group_count")),
        "group_pct": optional_decimal_str(first_row.get("group_pct")),
        "percentile": optional_decimal_str(first_row.get("percentile")),
        "group_hash": optional_str(first_row.get("group_hash")),
    }

    group_id = ensure_group(session, config, payload=group_payload)

    topics = (group_meta or {}).get("topics", [])
    if topics:
        sync_group_topics(session, config, group_id=group_id, topic_names=topics)

    sync_translations(session, config, group_id=group_id, rows=reader)


def iter_dataset_files(data_dir: Path) -> Iterable[tuple[Path, str]]:
    for pos_dir in sorted(p for p in data_dir.iterdir() if p.is_dir()):
        yield pos_dir, pos_dir.name.lower()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000/",
        help=(
            "Root URL for the backend host (default: %(default)s). "
            "Note: API paths are versioned under /api/v1/."
        ),
    )
    parser.add_argument(
        "--email",
        required=True,
        help="Email address for an API user with permissions to manage vocabulary",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="Password for the API user",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("lingo_kit_data/vocabulary/dataframes"),
        help="Directory containing part-of-speech subfolders of TSV files",
    )
    parser.add_argument(
        "--group-info",
        type=Path,
        default=Path("lingo_kit_data/vocabulary/group_info.json"),
        help="Path to the group metadata JSON file",
    )
    parser.add_argument(
        "--prune-missing",
        action="store_true",
        help="Delete translations/topics not present in the TSV files",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    if not args.data_dir.exists():
        raise SystemExit(f"Data directory {args.data_dir} does not exist")
    if not args.group_info.exists():
        raise SystemExit(f"Group info file {args.group_info} does not exist")

    base_url = args.base_url.rstrip("/") + "/"
    # The backend mounts its API under /api/v1/ (see lingo_kit_backend/src/config/urls.py)
    config = APIConfig(
        base_api_url=urljoin(base_url, "api/v1/"),
        prune_missing=args.prune_missing,
    )

    session = requests.Session()
    login(session, config, email=args.email, password=args.password)

    group_info = load_group_info(args.group_info)

    for pos_dir, pos_key in iter_dataset_files(args.data_dir):
        meta_for_pos = group_info.get(pos_key, {})
        for tsv_path in sorted(pos_dir.glob("*.tsv")):
            lemma_key = tsv_path.stem.lower()
            group_meta = meta_for_pos.get(lemma_key)
            if group_meta is None:
                LOGGER.warning(
                    "No metadata found for %s (%s) in group_info.json",
                    tsv_path.stem,
                    pos_dir.name,
                )
            process_tsv(session, config, tsv_path=tsv_path, group_meta=group_meta)


if __name__ == "__main__":
    main()
