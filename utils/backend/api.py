# Setup auth and base URL
import requests


BASE_URL = 'http://127.0.0.1:8000'
AUTH = ('stevie', 'lingokit2025!')


# Helpers to GET a Term and TermTranslation by id
def _iter_list_endpoint(url):
    r = requests.get(url, auth=AUTH)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        for item in data:
            yield item
    elif isinstance(data, dict) and 'results' in data:
        while True:
            for item in data['results']:
                yield item
            next_url = data.get('next')
            if not next_url:
                break
            r = requests.get(next_url, auth=AUTH)
            r.raise_for_status()
            data = r.json()
    else:
        return


def get_term_by_id(term_id: int):
    for item in _iter_list_endpoint(f'{BASE_URL}/api/terms/'):
        if item.get('id') == term_id:
            return item
    return None


def get_term_translation_by_id(tt_id: int):
    for item in _iter_list_endpoint(f'{BASE_URL}/api/term-translations/'):
        if item.get('id') == tt_id:
            return item
    return None


def get_term_id(term: str):
    response = requests.get(f'{BASE_URL}/api/terms/', params={'term_italian': term}, auth=AUTH)
    response.raise_for_status()
    data = response.json()
    results = data['results']
    if not results:
        raise Exception(f'Term "{term}" not found.')
    if len(results) > 1:
        raise Exception(f'Warning: multiple Terms found for "{term}": {results}')
    # assert(len(results) == 1)
    term_id = results[0]
    return term_id['id']


def get_term_translation_id(term_italian_id: int, part_of_speech: str, translation_english: str):
    # GET /api/term-translations/?term=ID&base_lemma=ID&translation_english=...&part_of_speech=...
    response = requests.get(f'{BASE_URL}/api/term-translations', params={
        'term': term_italian_id,
        'part_of_speech': part_of_speech,
        'translation_english': translation_english
    }, auth=AUTH)
    response.raise_for_status()
    data = response.json()
    results = data['results']
    if not results:
        raise Exception(f'No TermTranslations found for "{term_italian_id}" ({part_of_speech}) -> "{translation_english}".')
    # assert(len(results) == 1)
    if len(results) > 1:
        raise Exception(f'Warning: multiple TermTranslations found for "{term_italian_id}" ({part_of_speech}) -> "{translation_english}": {results}')

    tt_id = results[0]['id']
    return tt_id


def get_term_translation_id_list(term_italian_id: int, part_of_speech: str, translation_english: str):
    # GET /api/term-translations/?term=ID&base_lemma=ID&translation_english=...&part_of_speech=...
    response = requests.get(f'{BASE_URL}/api/term-translations', params={
        'term': term_italian_id,
        'part_of_speech': part_of_speech,
        'translation_english': translation_english
    }, auth=AUTH)
    response.raise_for_status()
    data = response.json()
    results = data['results']
    if not results:
        raise Exception(f'No TermTranslations found for "{term_italian_id}" ({part_of_speech}) -> "{translation_english}".')

    tt_ids = [] 
    for result in results:
        tt_ids.append(result['id'])
    return tt_ids


# Helper to PATCH a Term's audio_hash_italian
def update_term_audio(term_id: int, audio_hash: str):
    url = f'{BASE_URL}/api/terms/{term_id}/'
    return requests.patch(url, json={'audio_hash_italian': audio_hash}, auth=AUTH)


# Helper to PATCH a TermTranslation's audio_hash_english
def update_term_translation_audio(tt_id: int, audio_hash: str):
    url = f'{BASE_URL}/api/term-translations/{tt_id}/'
    return requests.patch(url, json={'audio_hash_english': audio_hash}, auth=AUTH)


if __name__ == '__main__':
    # Confirm before/after for Term id=1
    tid = 10
    before = get_term_by_id(tid)
    print('Before:', before)
    resp = update_term_audio(tid, 'test3')
    print('Update status:', resp.status_code)
    resp.content
    after = get_term_by_id(tid)
    print('After:', after)
    if before and after:
        print('audio_hash_italian:', before.get('audio_hash_italian'), '->', after.get('audio_hash_italian'))

    # Confirm before/after for TermTranslation id=1
    ttid = 20
    before = get_term_translation_by_id(ttid)
    print('Before:', before)
    resp = update_term_translation_audio(ttid, 'english_test')
    print('Update status:', resp.status_code)
    after = get_term_translation_by_id(ttid)
    print('After:', after)
    if before and after:
        print('audio_hash_english:', before.get('audio_hash_english'), '->', after.get('audio_hash_english'))
