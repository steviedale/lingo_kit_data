# %% [markdown]
# # Export DataFrames to Backend
# 
# This notebook scans `dataframes/dataframes_by_pos` for CSVs, loads rows, and publishes them to the backend via the API.
# 
# Notes:
# - Ignores the `is_base` column.
# - Uses `tqdm` to show progress for files and rows.
# - Creates topics, base lemmas, terms, baselemma-topic links, and term translations as needed.
# - See `export_to_database/example_database_api_calls.ipynb` for reference API patterns.
# 
# Configuration via env vars (optional):
# - `LINGOKIT_BASE_URL` (default: `http://127.0.0.1:8000`)
# - `LINGOKIT_AUTH_USER` (default: `stevie`)
# - `LINGOKIT_AUTH_PASS` (default: `lingokit2025!`)
# 

# %%
import os
os.environ["PATH_TO_REPO"] = "/Users/stevie/repos/lingo_kit_data"
# os.environ["PATH_TO_REPO"] = "/home/ubuntu/busy_bees/lingo_kit_data"

# %%
# load in environment variable
import os
PATH_TO_REPO = os.getenv('PATH_TO_REPO')
PATH_TO_REPO

# %%
import os
import csv
from collections import defaultdict
from typing import Optional, Dict, Any, Tuple

import requests
from tqdm.auto import tqdm

# --- Config ---
BASE_URL = os.getenv('LINGOKIT_BASE_URL', 'http://127.0.0.1:8000')
AUTH = (
    os.getenv('LINGOKIT_AUTH_USER', 'stevie'),
    os.getenv('LINGOKIT_AUTH_PASS', 'lingokit2025!'),
)
DATA_DIR = os.path.join(PATH_TO_REPO, 'dataframes', 'dataframes_by_pos')
assert os.path.isdir(DATA_DIR), f'Expected directory not found: {DATA_DIR}'

# --- Value normalization ---
ALLOWED_POS = {'adj', 'adv', 'art', 'conj', 'det', 'noun', 'prep', 'pron', 'verb'}
POS_MAP: Dict[str, str] = {
    'adj': 'adj', 'adjective': 'adj', 'adjectives': 'adj',
    'adv': 'adv', 'adverb': 'adv', 'adverbs': 'adv',
    'art': 'art', 'article': 'art', 'articles': 'art',
    'conj': 'conj', 'conjunction': 'conj', 'conjunctions': 'conj',
    'det': 'det', 'determiner': 'det', 'determiners': 'det',
    'noun': 'noun', 'nouns': 'noun',
    'prep': 'prep', 'preposition': 'prep', 'prepositions': 'prep',
    'pron': 'pron', 'pronoun': 'pron', 'pronouns': 'pron',
    'verb': 'verb', 'verbs': 'verb',
}
GENDER_MAP: Dict[str, str] = {'masculine': 'm', 'feminine': 'f', 'n/a': 'n/a', 'none': 'n/a', '': 'n/a'}
PLURALITY_MAP: Dict[str, str] = {'singular': 's', 'plural': 'p', 'n/a': 'n/a', 'none': 'n/a', '': 'n/a'}

def normalize_pos(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    key = str(value).strip().lower()
    if not key:
        return None
    return POS_MAP.get(key, key if key in ALLOWED_POS else None)

def normalize_gender(value: Optional[str]) -> str:
    key = 'n/a' if value is None else str(value).strip().lower()
    return GENDER_MAP.get(key, key if key in {'m', 'f', 'n/a'} else 'n/a')

def normalize_plurality(value: Optional[str]) -> str:
    key = 'n/a' if value is None else str(value).strip().lower()
    return PLURALITY_MAP.get(key, key if key in {'s', 'p', 'n/a'} else 'n/a')

# --- Caches ---
topic_cache: Dict[str, int] = {}
baselemma_cache: Dict[Tuple[str, str], int] = {}
term_cache: Dict[str, int] = {}
baselemma_topic_cache = set()
position_tracker = defaultdict(int)  # tracks per (base_it, base_en) position

def _extract_results(payload: Any):
    if isinstance(payload, dict):
        results = payload.get('results')
        if isinstance(results, list):
            return results
        return []
    if isinstance(payload, list):
        return payload
    return []

# --- HTTP error handling helpers ---
def _redact_auth(auth):
    try:
        user, _ = auth or (None, None)
        return f"{user}:***" if user else None
    except Exception:
        return "***"

def _json_preview(text, limit=2000):
    try:
        import json as _json
        obj = _json.loads(text)
        return _json.dumps(obj, ensure_ascii=False, indent=2)[:limit]
    except Exception:
        return (text or "")[:limit]

def raise_for_status_detailed(
    resp: requests.Response,
    *,
    method: Optional[str] = None,
    url: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    data: Optional[Any] = None,
    auth: Optional[Any] = None,
    context: Optional[str] = None,
) -> None:
    """Raise an HTTPError with rich details if response is not OK.

    Includes request/response metadata and a preview of the response body.
    """
    if resp.status_code < 400:
        return

    try:
        req = getattr(resp, "request", None)
        method = method or (getattr(req, "method", None)) or "?"
        url = url or (getattr(req, "url", None)) or "?"
    except Exception:
        method = method or "?"
        url = url or "?"

    body_text = None
    try:
        body_text = resp.text
    except Exception:
        body_text = "<no text>"

    detail = _json_preview(body_text)

    msg_lines = [
        f"HTTP {resp.status_code} {resp.reason or ''} while {method} {url}",
    ]
    if context:
        msg_lines.append(f"Context: {context}")
    if params:
        msg_lines.append(f"Params: {params}")
    if json_body is not None:
        msg_lines.append(f"JSON: {json_body}")
    if data is not None and json_body is None:
        msg_lines.append(f"Data: {str(data)[:1000]}")
    if auth is not None:
        msg_lines.append(f"Auth: {_redact_auth(auth)}")
    try:
        msg_lines.append(f"Response headers: {dict(resp.headers)})")
    except Exception:
        pass
    msg_lines.append("Response body preview:" + detail)

    import requests as _requests
    err = _requests.HTTPError("\n".join(msg_lines), response=resp)
    raise err

# --- API ensure helpers ---
def ensure_topic(name: str) -> Optional[int]:
    name = name.strip()
    if not name:
        return None
    if name in topic_cache:
        return topic_cache[name]
    resp = requests.get(f"{BASE_URL}/api/topics/", params={'topic': name}, auth=AUTH)
    raise_for_status_detailed(resp, method='GET', url=f"{BASE_URL}/api/topics/", params={'topic': name}, auth=AUTH, context='ensure_topic:get')
    results = _extract_results(resp.json())
    if results:
        topic_cache[name] = results[0]['id']
        return topic_cache[name]
    resp = requests.post(f"{BASE_URL}/api/topics/", json={'topic': name}, auth=AUTH)
    raise_for_status_detailed(resp, method='POST', url=f"{BASE_URL}/api/topics/", json_body={'topic': name}, auth=AUTH, context='ensure_topic:create')
    topic_cache[name] = resp.json()['id']
    return topic_cache[name]

def ensure_baselemma(base_it: str, base_en: str) -> int:
    key = (base_it, base_en)
    if key in baselemma_cache:
        return baselemma_cache[key]
    resp = requests.get(
        f"{BASE_URL}/api/baselemmas/",
        params={'base_lemma_italian': base_it, 'base_lemma_english': base_en},
        auth=AUTH,
    )
    raise_for_status_detailed(resp, method='GET', url=f"{BASE_URL}/api/baselemmas/", params={'base_lemma_italian': base_it, 'base_lemma_english': base_en}, auth=AUTH, context='ensure_baselemma:get')
    results = _extract_results(resp.json())
    if results:
        baselemma_cache[key] = results[0]['id']
        return baselemma_cache[key]
    resp = requests.post(
        f"{BASE_URL}/api/baselemmas/",
        json={'base_lemma_italian': base_it, 'base_lemma_english': base_en},
        auth=AUTH,
    )
    raise_for_status_detailed(resp, method='POST', url=f"{BASE_URL}/api/baselemmas/", json_body={'base_lemma_italian': base_it, 'base_lemma_english': base_en}, auth=AUTH, context='ensure_baselemma:create')
    baselemma_cache[key] = resp.json()['id']
    return baselemma_cache[key]

def ensure_term(term_it: str, audio_hash: str, pronunciation: str) -> int:
    if term_it in term_cache:
        return term_cache[term_it]
    resp = requests.get(f"{BASE_URL}/api/terms/", params={'term_italian': term_it}, auth=AUTH)
    raise_for_status_detailed(resp, method='GET', url=f"{BASE_URL}/api/terms/", params={'term_italian': term_it}, auth=AUTH, context='ensure_term:get')
    results = _extract_results(resp.json())
    if results:
        term_cache[term_it] = results[0]['id']
        return term_cache[term_it]
    body = {'term_italian': term_it}
    if audio_hash:
        body['audio_hash_italian'] = audio_hash
    if pronunciation:
        body['pronunciation'] = pronunciation
    resp = requests.post(f"{BASE_URL}/api/terms/", json=body, auth=AUTH)
    raise_for_status_detailed(resp, method='POST', url=f"{BASE_URL}/api/terms/", json_body=body, auth=AUTH, context='ensure_term:create')
    term_cache[term_it] = resp.json()['id']
    return term_cache[term_it]

def ensure_baselemma_topic(base_lemma_id: int, topic_id: Optional[int]) -> None:
    if topic_id is None:
        return
    key = (base_lemma_id, topic_id)
    if key in baselemma_topic_cache:
        return
    resp = requests.post(
        f"{BASE_URL}/api/baselemma-topics/",
        json={'base_lemma': base_lemma_id, 'topic': topic_id},
        auth=AUTH,
    )
    if resp.status_code not in (200, 201):
        raise_for_status_detailed(resp, method='POST', url=f"{BASE_URL}/api/baselemma-topics/", json_body={'base_lemma': base_lemma_id, 'topic': topic_id}, auth=AUTH, context='ensure_baselemma_topic:create')
    baselemma_topic_cache.add(key)

def ensure_term_translation(payload: Dict[str, Any]) -> int:
    params = {
        'term': payload['term'],
        'base_lemma': payload['base_lemma'],
        'translation_english': payload['translation_english'],
        'part_of_speech': payload['part_of_speech'],
    }
    resp = requests.get(f"{BASE_URL}/api/term-translations/", params=params, auth=AUTH)
    raise_for_status_detailed(resp, method='GET', url=f"{BASE_URL}/api/term-translations/", params=params, auth=AUTH, context='ensure_term_translation:get')
    existing = _extract_results(resp.json())
    if existing:
        return existing[0]['id']
    resp = requests.post(f"{BASE_URL}/api/term-translations/", json=payload, auth=AUTH)
    raise_for_status_detailed(resp, method='POST', url=f"{BASE_URL}/api/term-translations/", json_body=payload, auth=AUTH, context='ensure_term_translation:create')
    return resp.json()['id']

# --- Utility to collect CSV files ---
def iter_csv_files(root_dir: str):
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.lower().endswith('.csv'):
                yield os.path.join(dirpath, fn)

# --- Main export loop ---
csv_files = sorted(iter_csv_files(DATA_DIR))
print(f'Found {len(csv_files)} CSV files under {DATA_DIR}')
created_tt_ids = []
for csv_path in tqdm(csv_files, desc='CSV files'):
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, desc=os.path.basename(csv_path), leave=False):
            # Basic fields
            base_it = (row.get('base_lemma_italian') or '').strip()
            base_en = (row.get('base_lemma_english') or '').strip()
            term_it = (row.get('term_italian') or '').strip()
            translation_en = (row.get('translation_english') or '').strip()
            if not (base_it and base_en and term_it and translation_en):
                # Skip incomplete rows
                continue

            # Ensure base lemma and term exist
            base_lemma_id = ensure_baselemma(base_it, base_en)
            term_id = ensure_term(
                term_it,
                (row.get('italian_audio_hash') or '').strip(),
                (row.get('pronunciation') or '').strip(),
            )

            # Topics and link to base lemma
            topics = [t.strip() for t in (row.get('topics') or '').split(',') if t.strip()]
            for t in topics:
                topic_id = ensure_topic(t)
                ensure_baselemma_topic(base_lemma_id, topic_id)

            # Part of speech and position within lemma group
            pos_value = normalize_pos(row.get('part_of_speech'))
            if not pos_value:
                raise ValueError(f"Unhandled part_of_speech: {row.get('part_of_speech')} in {csv_path}")
            key = (base_it, base_en)
            position = position_tracker[key]
            position_tracker[key] += 1

            payload = {
                'term': term_id,
                'base_lemma': base_lemma_id,
                'translation_english': translation_en,
                'part_of_speech': pos_value,
                'position': position,
            }

            # Optional attributes
            gender = normalize_gender(row.get('gender'))
            if gender:
                payload['gender'] = gender
            plurality = normalize_plurality(row.get('plurality'))
            if plurality:
                payload['plurality'] = plurality
            if (row.get('article_type') or '').strip():
                payload['article_type'] = row['article_type'].strip()
            if (row.get('article_italian') or '').strip():
                payload['article_italian'] = row['article_italian'].strip()
            if (row.get('preposition') or '').strip():
                payload['preposition'] = row['preposition'].strip()
            if (row.get('example_sentence_english') or '').strip():
                payload['example_sentence_english'] = row['example_sentence_english'].strip()
            if (row.get('example_sentence_italian') or '').strip():
                payload['example_sentence_italian'] = row['example_sentence_italian'].strip()
            if (row.get('english_audio_hash') or '').strip():
                payload['audio_hash_english'] = row['english_audio_hash'].strip()
            if (row.get('notes') or '').strip():
                payload['notes'] = row['notes'].strip()

            tt_id = ensure_term_translation(payload)
            created_tt_ids.append(tt_id)

print(f'Created/verified {len(created_tt_ids)} term translations across {len(position_tracker)} base lemma groups.')


# %%



