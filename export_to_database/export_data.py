#!/usr/bin/env python3
"""
Import LingoKit base data (Topics, BaseLemmas, Terms, TermTranslations) from a CSV
into your Django REST API.

Usage:
  python import_lingokit_data.py \
    --base-url https://yourdomain.com \
    --csv ./data.csv \
    --auth token --token YOUR_DRF_OR_JWT_TOKEN

Alternative auth:
  # Basic auth
  --auth basic --username alice --password secret

  # Session cookie present in env/CI (already logged in)
  --auth session --cookie "sessionid=...; csrftoken=..."

CSV columns (example provided in the prompt) are mapped 1:1 where possible.

Idempotency:
- The script "get-or-create"s each entity using lookups:
  Topic: by exact `topic`
  BaseLemma: by exact `(base_lemma_italian, base_lemma_english)`
  Term: by exact `term_italian`
  TermTranslation: by key `(term_id, base_lemma_id, translation_english, part_of_speech)`
    (adjust this if your true uniqueness differs)

Endpoints expected (adjust with CLI flags if your paths differ):
  /api/topics/
  /api/baselemmas/
  /api/terms/
  /api/term-translations/
  /api/baselemma-topics/   # optional; used if --link-lemma-topics=api
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import requests
from tqdm import tqdm

# -----------------------
# Helpers
# -----------------------

def booly(v: str) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None

def empty_to_none(v: str) -> Optional[str]:
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None

def split_csv_list(v: str) -> List[str]:
    if not v:
        return []
    # topics field expected like: "quantity,manner"
    return [t.strip() for t in v.split(",") if t.strip()]

def die(msg: str, code: int = 2):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

# -----------------------
# API Client
# -----------------------

class Api:
    def __init__(
        self,
        base_url: str,
        auth_mode: str,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        cookie: Optional[str] = None,
        paths: Dict[str, str] = None,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth_mode = auth_mode
        self.session = requests.Session()
        self.timeout = timeout

        # default paths (adjust via CLI if needed)
        self.paths = {
            "topics": "/api/topics/",
            "baselemmas": "/api/baselemmas/",
            "terms": "/api/terms/",
            "term_translations": "/api/term-translations/",
            "baselemma_topics": "/api/baselemma-topics/",  # optional
        }
        if paths:
            self.paths.update(paths)

        # Auth headers / cookies
        if auth_mode == "token":
            # Works for DRF tokens (Token <key>) or JWT (Bearer <jwt>)
            # Try to detect format – you can force with --token-prefix
            if token is None:
                die("token auth selected but no --token provided")
            # Heuristic: if token has dots, treat as JWT
            if "." in token:
                self.session.headers["Authorization"] = f"Bearer {token}"
            else:
                self.session.headers["Authorization"] = f"Token {token}"
        elif auth_mode == "basic":
            if not username or not password:
                die("basic auth selected but missing --username/--password")
            self.session.auth = (username, password)
        elif auth_mode == "session":
            if not cookie:
                die("session auth selected but missing --cookie")
            self.session.headers["Cookie"] = cookie
        else:
            die(f"Unknown auth mode: {auth_mode}")

        self.session.headers["Accept"] = "application/json"
        # Allow either form-encoded or JSON. We'll use JSON by default.
        self.session.headers["Content-Type"] = "application/json"

    def url(self, key: str) -> str:
        return self.base_url + self.paths[key]

    def get(self, key: str, params: Dict[str, Any]) -> requests.Response:
        return self.session.get(self.url(key), params=params, timeout=self.timeout)

    def post(self, key: str, payload: Dict[str, Any]) -> requests.Response:
        return self.session.post(self.url(key), data=json.dumps(payload), timeout=self.timeout)

    # Generic paginate-through helper (DRF-style pagination optional)
    def list_all(self, key: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        items = []
        url = self.url(key)
        while True:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            if resp.status_code != 200:
                raise RuntimeError(f"GET {url} failed: {resp.status_code} {resp.text}")
            data = resp.json()
            if isinstance(data, dict) and "results" in data:
                items.extend(data["results"])
                url = data.get("next")
                if not url:
                    break
                params = {}  # next already encodes query
            elif isinstance(data, list):
                items.extend(data)
                break
            else:
                # Non-standard shape; just return as-is
                if isinstance(data, dict):
                    items.append(data)
                break
        return items

# -----------------------
# Upsert functions
# -----------------------

def upsert_topic(api: Api, topic: str, cache: Dict[str, int]) -> int:
    if topic in cache:
        return cache[topic]

    # Try GET by exact name (adjust the filter param if your API differs)
    resp = api.get("topics", params={"topic": topic})
    if resp.status_code == 200:
        data = resp.json()
        # handle both list and DRF-paginated
        if isinstance(data, list):
            for t in data:
                if t.get("topic") == topic:
                    cache[topic] = t["id"]
                    return t["id"]
        elif isinstance(data, dict):
            results = data.get("results", [])
            for t in results:
                if t.get("topic") == topic:
                    cache[topic] = t["id"]
                    return t["id"]

    # Create
    resp = api.post("topics", {"topic": topic})
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Create topic '{topic}' failed: {resp.status_code} {resp.text}")
    tid = resp.json()["id"]
    cache[topic] = tid
    return tid

def upsert_baselemma(api: Api, bl_it: str, bl_en: str, cache: Dict[Tuple[str, str], int]) -> int:
    key = (bl_it, bl_en)
    if key in cache:
        return cache[key]

    resp = api.get("baselemmas", params={
        "base_lemma_italian": bl_it,
        "base_lemma_english": bl_en
    })
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("results", data if isinstance(data, list) else [])
        for b in items:
            if b.get("base_lemma_italian") == bl_it and b.get("base_lemma_english") == bl_en:
                cache[key] = b["id"]
                return b["id"]

    resp = api.post("baselemmas", {
        "base_lemma_italian": bl_it,
        "base_lemma_english": bl_en
    })
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Create BaseLemma '{bl_it} / {bl_en}' failed: {resp.status_code} {resp.text}")
    bid = resp.json()["id"]
    cache[key] = bid
    return bid

def upsert_term(api: Api, term_it: str, cache: Dict[str, int], audio_hash_italian: Optional[str], pronunciation: Optional[str]) -> int:
    if term_it in cache:
        return cache[term_it]

    resp = api.get("terms", params={"term_italian": term_it})
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("results", data if isinstance(data, list) else [])
        for t in items:
            if t.get("term_italian") == term_it:
                cache[term_it] = t["id"]
                return t["id"]

    payload = {
        "term_italian": term_it,
        "audio_hash_italian": audio_hash_italian,
        "pronunciation": pronunciation,
    }
    resp = api.post("terms", payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Create Term '{term_it}' failed: {resp.status_code} {resp.text}")
    tid = resp.json()["id"]
    cache[term_it] = tid
    return tid

def upsert_baselemma_topic_link(api: Api, baselemma_id: int, topic_id: int, mode: str):
    """
    mode = 'api' -> POST to /api/baselemma-topics/
    mode = 'skip' -> do nothing (e.g., your API auto-links on BaseLemma save)
    """
    if mode == "skip":
        return
    payload = {"base_lemma": baselemma_id, "topic": topic_id}
    resp = api.post("baselemma_topics", payload)
    # Allow both 201 created and 200/409 for already exists
    if resp.status_code not in (200, 201, 409):
        raise RuntimeError(f"Link BaseLemma({baselemma_id}) to Topic({topic_id}) failed: {resp.status_code} {resp.text}")

def upsert_term_translation(
    api: Api,
    term_id: int,
    baselemma_id: int,
    tt_payload: Dict[str, Any],
    unique_key=("translation_english", "part_of_speech"),
) -> int:
    """
    Upsert TermTranslation by (term, base_lemma, translation_english, part_of_speech).
    Adjust `unique_key` to match your actual uniqueness constraints if needed.
    """
    # Try to find existing
    params = {
        "term": term_id,
        "base_lemma": baselemma_id,
    }
    for k in unique_key:
        params[k] = tt_payload.get(k)

    resp = api.get("term_translations", params=params)
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("results", data if isinstance(data, list) else [])
        if items:
            return items[0]["id"]

    payload = {"term": term_id, "base_lemma": baselemma_id}
    payload.update(tt_payload)
    resp = api.post("term_translations", payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Create TermTranslation failed ({term_id}, {baselemma_id}): {resp.status_code} {resp.text}"
        )
    return resp.json()["id"]

# -----------------------
# Row mapping
# -----------------------

def map_row_to_models(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Map raw CSV row to model payloads & linkage.
    Returns dict with:
      - topics: List[str]
      - baselemma: {base_lemma_italian, base_lemma_english}
      - term: {term_italian, audio_hash_italian?, pronunciation?}
      - term_translation: dict of TT fields
    """
    topics = split_csv_list(row.get("topics"))

    baselemma = {
        "base_lemma_italian": row.get("base_lemma_italian", "").strip(),
        "base_lemma_english": row.get("base_lemma_english", "").strip(),
    }

    term = {
        "term_italian": row.get("term_italian", "").strip(),
        "audio_hash_italian": empty_to_none(row.get("italian_audio_hash")),
        "pronunciation": empty_to_none(row.get("pronunciation")),  # optional column
    }

    # Map TT fields (respect your model names)
    tt = {
        "translation_english": empty_to_none(row.get("translation_english")),
        "part_of_speech": empty_to_none(row.get("part_of_speech")),
        "gender": empty_to_none(row.get("gender")),
        "plurality": empty_to_none(row.get("plurality")),
        "article_type": empty_to_none(row.get("article_type")),
        "article_italian": empty_to_none(row.get("article_italian")),
        "preposition": empty_to_none(row.get("preposition")),
        "notes": empty_to_none(row.get("notes")),
        "example_sentence_italian": empty_to_none(row.get("example_sentence_italian")),
        "example_sentence_english": empty_to_none(row.get("example_sentence_english")),
        "audio_hash_english": empty_to_none(row.get("english_audio_hash")),
        # Optional fields in CSV that don't map to the provided TermTranslation model
        # are read but ignored unless you expand your model:
        # subtype, person_number, mood, tense, is_comparative, is_superlative, is_compound,
        # added_particle_italian
    }

    return {
        "topics": topics,
        "baselemma": baselemma,
        "term": term,
        "term_translation": tt,
    }

# -----------------------
# Main import routine
# -----------------------

def import_csv(
    api: Api,
    csv_path: Path,
    link_lemma_topics_mode: str = "api",  # 'api' or 'skip'
):
    # Caches to avoid redundant GETs/POSTs
    topic_cache: Dict[str, int] = {}
    baselemma_cache: Dict[Tuple[str, str], int] = {}
    term_cache: Dict[str, int] = {}

    created_counts = {
        "topics": 0,
        "baselemmas": 0,
        "terms": 0,
        "term_translations": 0,
        "links": 0,
        "rows": 0,
    }

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in tqdm(enumerate(reader, start=1)):
            mapped = map_row_to_models(row)

            # Validate minimal required values
            bl_it = mapped["baselemma"]["base_lemma_italian"]
            bl_en = mapped["baselemma"]["base_lemma_english"]
            term_it = mapped["term"]["term_italian"]
            if not bl_it or not bl_en or not term_it:
                print(f"[row {idx}] Skipping: missing base lemma or term_italian")
                continue

            # Upserts
            # 1) Topics
            topic_ids = []
            for t in mapped["topics"]:
                prior = topic_cache.get(t)
                tid = upsert_topic(api, t, topic_cache)
                if prior is None and tid:
                    created_counts["topics"] += 1
                topic_ids.append(tid)

            # 2) BaseLemma
            prior = baselemma_cache.get((bl_it, bl_en))
            baselemma_id = upsert_baselemma(api, bl_it, bl_en, baselemma_cache)
            if prior is None:
                created_counts["baselemmas"] += 1

            # 3) Link BaseLemma <-> Topics
            for tid in topic_ids:
                upsert_baselemma_topic_link(api, baselemma_id, tid, link_lemma_topics_mode)
                created_counts["links"] += 1

            # 4) Term
            prior = term_cache.get(term_it)
            term_id = upsert_term(
                api,
                term_it,
                term_cache,
                mapped["term"]["audio_hash_italian"],
                mapped["term"]["pronunciation"],
            )
            if prior is None:
                created_counts["terms"] += 1

            # 5) TermTranslation
            tt_payload = mapped["term_translation"]
            if not tt_payload.get("translation_english") or not tt_payload.get("part_of_speech"):
                print(f"[row {idx}] Skipping TermTranslation: requires translation_english and part_of_speech")
                continue

            tt_id = upsert_term_translation(
                api,
                term_id=term_id,
                baselemma_id=baselemma_id,
                tt_payload=tt_payload,
            )
            if tt_id:
                created_counts["term_translations"] += 1
            created_counts["rows"] += 1

            if idx % 200 == 0:
                print(f"Processed {idx} rows...")

    print("Done.")
    print(json.dumps(created_counts, indent=2))


def parse_args():
    p = argparse.ArgumentParser(description="Import LingoKit data into Django via REST API")

    p.add_argument("--base-url", required=True, help="Base URL like https://example.com")
    p.add_argument("--csv", required=True, help="Path to CSV file")

    # Auth
    p.add_argument("--auth", choices=["token", "basic", "session"], default="token")
    p.add_argument("--token", help="DRF token or JWT (auto-detected)")
    p.add_argument("--username")
    p.add_argument("--password")
    p.add_argument("--cookie", help='Entire Cookie header, e.g. "sessionid=...; csrftoken=..."')

    # Endpoint path overrides (if your API uses different paths)
    p.add_argument("--topics-path", default="/api/topics/")
    p.add_argument("--baselemmas-path", default="/api/baselemmas/")
    p.add_argument("--terms-path", default="/api/terms/")
    p.add_argument("--term-translations-path", default="/api/term-translations/")
    p.add_argument("--baselemma-topics-path", default="/api/baselemma-topics/")

    # How to link BaseLemma<->Topic
    p.add_argument("--link-lemma-topics", choices=["api", "skip"], default="api",
                   help="Use 'api' if you expose /api/baselemma-topics/; 'skip' if your backend auto-links via BaseLemma payload.")

    args = p.parse_args()

    paths = {
        "topics": args.topics_path,
        "baselemmas": args.baselemmas_path,
        "terms": args.terms_path,
        "term_translations": args.term_translations_path,
        "baselemma_topics": args.baselemma_topics_path,
    }

    return args, paths


def main():
    args, paths = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        die(f"CSV does not exist: {csv_path}")

    api = Api(
        base_url=args.base_url,
        auth_mode=args.auth,
        token=args.token,
        username=args.username,
        password=args.password,
        cookie=args.cookie,
        paths=paths,
    )
    import_csv(api, csv_path, link_lemma_topics_mode=args.link_lemma_topics)


if __name__ == "__main__":
    main()
