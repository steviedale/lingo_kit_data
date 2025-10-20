# %%
import time
import numpy as np
import json
from tqdm import tqdm
import uuid

import pandas as pd

from stanza_tokenizer import StanzaTokenizer

# %%
tokenizer = StanzaTokenizer()

# %%
path = '/Users/stevie/repos/lingo_kit_combined/lingo_kit_data/word_analysis/datasets/tatoeba/dataframe.tsv'
df = pd.read_csv(path, sep='\t')
len(df), df.columns

# %%
# start_i = 180000
# end_i =  200000
# df = df.iloc[start_i:end_i]
# print(f"Processing rows {start_i} to {end_i}")
# print(f"Total rows: {len(df)}")

# %%
def normalize(text):
    assert(text.lower() == text), f"Text is not lowercase: {text}"
    assert(text.strip() == text), f"Text is not stripped: '{text}'"
    bad_chars = {
        "“": '"',
        "”": '"',
        "’": "'",
    }
    for bad_c, good_c in bad_chars.items():
        assert(bad_c not in text), f"Text contains bad character {bad_c}: {text}"
    # text = text.lower()
    # text = text.replace("’", "'")
    # text = text.replace("“", '"').replace("”", '"')
    # text = text.strip()
    return text

# %%
data = {}
for _, row in tqdm(df.iterrows(), total=len(df)):
    tokens = tokenizer.tokenize(row['text_it'])
    for token in tokens:
        text = normalize(token['text'])
        token_hash = token['token_hash']
        group_hash = token['group_hash']
        if token_hash not in data:
            data[token_hash] = {
                'text': text,
                'lemma': token['lemma'],
                'pos': token['pos'],
                'xpos': token['xpos'],
                'deprel': token['deprel'],
                'count': 0,
                'sentences': set(),
                'group_hash': group_hash,
            }
        data[token_hash]['count'] += 1
        data[token_hash]['sentences'].add(row['hash'])

# %%
token_df_data = {'token_hash': [], 'text': [], 'lemma': [], 'pos': [], 'xpos': [], 'deprel': [], 'count': [], 'sentences': [], 'group_hash': []}
for token_hash, info in data.items():
    if info['pos'] == 'PUNCT' or info['pos'] == "PROPN":
        continue
    token_df_data['token_hash'].append(token_hash)
    token_df_data['text'].append(info['text'])
    token_df_data['lemma'].append(info['lemma'])
    token_df_data['pos'].append(info['pos'])
    token_df_data['xpos'].append(info['xpos'])
    token_df_data['deprel'].append(info['deprel'])
    token_df_data['count'].append(info['count'])
    token_df_data['sentences'].append(list(info['sentences']))
    token_df_data['group_hash'].append(info['group_hash'])
token_df = pd.DataFrame(token_df_data)

# %%
token_df.sort_values(by='count', ascending=False, inplace=True)

# %%
# save_path = f'/Users/stevie/repos/lingo_kit_combined/lingo_kit_data/word_analysis/datasets/tatoeba//token_data_{start_i}_{end_i}.tsv'
save_path = f'/Users/stevie/repos/lingo_kit_combined/lingo_kit_data/word_analysis/datasets/tatoeba/token_data/token_data_all.tsv'
token_df.to_csv(save_path, sep='\t', index=False)


