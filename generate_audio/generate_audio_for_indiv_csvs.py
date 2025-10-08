# load in environment variable
import os
PATH_TO_REPO = os.getenv('PATH_TO_REPO')
assert PATH_TO_REPO is not None, "Please set PATH_TO_REPO environment variable"

# %%
import os
import pandas as pd
from tqdm import tqdm
import yaml
import requests
import hashlib
import uuid

import sys
sys.path.append(PATH_TO_REPO)
from utils.audio.text_to_speech import TextToSpeech, VOICES
from utils.s3.upload_to_s3 import upload_file
from utils.csv_helper import get_all_csv_files_rec

# %%
df_path = os.path.join(PATH_TO_REPO, f'dataframes/dataframes_by_pos')
all_csv_files = get_all_csv_files_rec(df_path)

# %%
tts = TextToSpeech()

# %%
def get_term_hash(english_text, italian_text):
    hash = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{english_text}-{italian_text}"))
    return hash
print(get_term_hash('person', 'persone'))

# %%
def parse_topics(topics_str):
    assert(topics_str[0] == '[' and topics_str[-1] == ']')
    topics_str = topics_str[1:-1]  # remove brackets
    topics = topics_str.split(',')
    return topics

def get_language_key(lang):
    if lang == 'english':
        return 'en'
    elif lang == 'italian':
        return 'it'
    else:
        raise ValueError(f"Unknown language: {lang}")

def get_audio_hash(text, voice_name, speaking_rate, pitch):
    param_str = f"{text}{voice_name}{speaking_rate:.2f}{pitch:.2f}"
    hash_object = hashlib.sha256(param_str.encode('utf-8'))
    hash_key = hash_object.hexdigest()
    return hash_key

# %%
def is_csv_processed(df):
    for lang in 'english', 'italian':
        if f'{lang}_audio_hash' not in df.columns:
            return False
        if f"{lang}_duration_ms" not in df.columns:
            return False
        if sum(df[f'{lang}_audio_hash'].isna()) > 0:
            return False
        if sum(df[f'{lang}_duration_ms'].isna()) > 0:
            return False
    return True

for csv_path in tqdm(all_csv_files):
    if not os.path.exists(csv_path):
        print(f"File no longer exists, skipping: {csv_path}")
        continue
    print(f"Processing file: {csv_path}")
    assert(os.path.exists(csv_path)), f"File does not exist: {csv_path}"
    df = pd.read_csv(csv_path)
    if is_csv_processed(df):
        print(f"File already processed: {csv_path}")
        continue
    for i, (index, row) in enumerate(df.iterrows()):
        for lang in 'english', 'italian':
            if lang == 'english':
                gender = 'female'
                speaking_rate = 0.92
                text = row['translation_english']
            else:
                assert(lang == 'italian')
                gender = 'male'
                speaking_rate = 0.7
                text = row['term_italian']

            # this is a fix to make short words like "a" or "the" followed by parenthesis to sound too short
            synth_obj = tts.synthesize(
                text=text,
                voice_name=VOICES[lang][gender],
                speaking_rate=speaking_rate,
                verbose=False,
            )

            local_hash = get_audio_hash(
                text=text,
                voice_name=VOICES[lang][gender],
                speaking_rate=speaking_rate,
                pitch=synth_obj['pitch'],
            )
            assert(local_hash == synth_obj['hash']), f"Hash mismatch: {local_hash} != {synth_obj['hash']}"

            # upload audio file to s3
            file_path = os.path.join(PATH_TO_REPO, synth_obj['audio_file'])
            assert(os.path.exists(file_path)), f"File does not exist: {file_path}"
            upload_file(file_path=file_path, verbose=False)

            df.loc[index, f'{lang}_audio_hash'] = local_hash
            df.loc[index, f'{lang}_duration_ms'] = synth_obj['duration_ms']
        print(f"Processing {i+1}/{len(df)}: {row['term_italian']} / {row['translation_english']} -> {df.loc[index, 'italian_audio_hash']} / {df.loc[index, 'english_audio_hash']}")
    df.to_csv(csv_path, index=False)
    tts.save()