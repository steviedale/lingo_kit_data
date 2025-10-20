# %%
import os
from tqdm import tqdm
import pandas as pd

from stanza_tokenizer import StanzaTokenizer

# %%
tokenizer = StanzaTokenizer()

# %%
path = 'dataframe.tsv'
df = pd.read_csv(path, sep='\t')
len(df), df.columns

# %%
save_path = f'token_data/token_data_all.tsv'
assert(os.path.exists(os.path.dirname(save_path))), f"Directory does not exist: {os.path.dirname(save_path)}"

# %%
def sanity_check(text):
    assert(text.lower() == text), f"Text is not lowercase: {text}"
    assert(text.strip() == text), f"Text is not stripped: '{text}'"
    bad_chars = {
        "“": '"',
        "”": '"',
        "’": "'",
    }
    for bad_c, good_c in bad_chars.items():
        assert(bad_c not in text), f"Text contains bad character {bad_c}: {text}"

# %%
chunk_size = 20000
if len(df) % chunk_size == 0:
    n = len(df) // chunk_size
else:
    n = len(df) // chunk_size + 1
for chunk_i in range(n):
    start_index = chunk_i*chunk_size
    end_index = (chunk_i+1)*chunk_size
    chunk_df = df.iloc[start_index:end_index]
    print(f"Processing chunk {chunk_i+1}/{n}, rows {start_index} to {end_index}...")

    data = {}
    for i, (_, row) in enumerate(tqdm(chunk_df.iterrows(), total=len(chunk_df))):
        # normalize text
        text = row['text_it']
        text = text.lower()
        text = text.replace("’", "'")
        text = text.replace("“", '"').replace("”", '"')
        text = text.strip()

        tokens = tokenizer.tokenize(row['text_it'])
        for token in tokens:
            sanity_check(text)
            lemma = token['lemma']
            lemma = lemma.lower()
            token_hash = token['token_hash']
            group_hash = token['group_hash']
            if token_hash not in data:
                data[token_hash] = {
                    'text': text,
                    'lemma': lemma,
                    'pos': token['pos'],
                    'xpos': token['xpos'],
                    'deprel': token['deprel'],
                    'feats': token['feats'],
                    'count': 0,
                    'sentences': set(),
                    'group_hash': group_hash,
                }
            data[token_hash]['count'] += 1
            if len(data[token_hash]['sentences']) < 20:
                data[token_hash]['sentences'].add(row['hash'])
    
    token_df_data = {'token_hash': [], 'text': [], 'lemma': [], 'pos': [], 'xpos': [], 'deprel': [], 'feats': [], 'count': [], 'sentences': [], 'group_hash': []}
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
    token_df.sort_values(by='count', ascending=False, inplace=True)
    save_path = 'new_token_data/token_data_chunk_' + str(chunk_i) + '.tsv'
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))
    token_df.to_csv(save_path, sep='\t', index=False)

# %%


# %%


# %%



