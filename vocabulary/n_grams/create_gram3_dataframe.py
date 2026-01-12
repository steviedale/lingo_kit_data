# %%
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm

# %%
BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from utils.tokenization import StanzaTokenizer

# %%
bad_pos = ['X', 'PUNCT', 'PROPN', 'NUM']

# %%
df = pd.read_csv('../sentence_dataframe.tsv', sep='\t')
len(df), df.columns

# %%
df['term_length_it'] = df['text_it'].str.split().apply(len)
df['term_length_en'] = df['text_en'].str.split().apply(len)
df['char_length_it'] = df['text_it'].str.len()
df['char_length_en'] = df['text_en'].str.len()

# %%
print(len(df))
df = df[df['char_length_it'] < 50]
print(len(df))

# %%
tokenizer = StanzaTokenizer()

# %%
def get_token_key(token_dict):
    return f"{token_dict['text']}_{token_dict['lemma']}_{token_dict['pos']}"

# %%
gram3_dict = {}
iter_df = df.sample(frac=1, random_state=42).reset_index(drop=True)
#iter_df = df.sample(n=100000, random_state=42).reset_index(drop=True)
#iter_df = df.sample(n=10, random_state=42).reset_index(drop=True)

errors = 0
for sentence in tqdm(iter_df['text_it'], total=len(iter_df)):
    try:
        start_word_count = {}
        end_word_count = {}
        tokens = tokenizer.tokenize(sentence)
        for i in range(len(tokens)-2):
            t1 = tokens[i]
            t2 = tokens[i+1]
            t3 = tokens[i+2]

            # keep track of the index of 'find' words
            if t1['text'].lower() not in start_word_count:
                start_word_count[t1['text'].lower()] = -1
            start_word_count[t1['text'].lower()] += 1
            if t3['text'].lower() not in end_word_count:
                end_word_count[t3['text'].lower()] = -1
            end_word_count[t3['text'].lower()] += 1

            if t1['pos'] in bad_pos or t2['pos'] in bad_pos or t3['pos'] in bad_pos:
                continue

            gram3 = (get_token_key(t1), get_token_key(t2), get_token_key(t3))
            
            start_index = sentence.lower().find(t1['text'].lower(), start_word_count[t1['text'].lower()])
            end_index = sentence.lower().find(t3['text'].lower(), end_word_count[t3['text'].lower()]) + len(t3['text'])
            substr = sentence[start_index:end_index]

            if gram3 not in gram3_dict:
                gram3_dict[gram3] = []

            gram3_dict[gram3].append({
                'sentence': sentence,
                'substr': substr,
                'start_index': start_index,
                'end_index': end_index,
                'token_1': t1,
                'token_2': t2,
                'token_3': t3, 
            })
    except Exception as e:
        errors += 1
        print(f"Error processing sentence: {e}")
        continue
print(f"Total errors in tokenization: {errors}")


# %%
gram3_items = sorted(gram3_dict.items(), key=lambda x: len(x[1]), reverse=True)
for i in range(20):
    print(gram3_items[i][1][0]['token_1']['text'])
    print(gram3_items[i][1][0]['token_2']['text'])
    print(gram3_items[i][1][0]['token_3']['text'])
    for j, t in enumerate(gram3_items[i][1]):
        print(t['substr'])
        if j > 2:
            break
        # break

# %%
len(gram3_dict)

# %%
gram3_dict[list(gram3_dict.keys())[0]][0].keys()

# %%
data = {
    'gram3_key': [], 
    'count': [],
    'substr': [],
    'term_1': [],
    'term_2': [],
    'term_3': [],
    'lemma_1': [],
    'lemma_2': [],
    'lemma_3': [],
    'pos_1': [],
    'pos_2': [],
    'pos_3': [],
}
new_gram3_dict = {}
len(gram3_dict)
errors = 0
for gram_key, gram_list in tqdm(gram3_dict.items(), total=len(gram3_dict.keys())):
    substr_votes = {}
    if len(gram_list) < 10:
        continue
    try:
        for entry in gram_list:
            substr = entry['substr']
            if substr not in substr_votes:
                substr_votes[substr] = 0
            substr_votes[substr] += 1
        sorted_substrs = sorted(substr_votes.items(), key=lambda x: x[1], reverse=True)
        best_substr, best_votes = sorted_substrs[0]
        data['gram3_key'].append(gram_key)
        data['substr'].append(best_substr.lower())
        data['term_1'].append(gram_key[0].split('_')[0])
        data['term_2'].append(gram_key[1].split('_')[0])
        data['term_3'].append(gram_key[2].split('_')[0])
        data['lemma_1'].append(gram_key[0].split('_')[1])
        data['lemma_2'].append(gram_key[1].split('_')[1])
        data['lemma_3'].append(gram_key[2].split('_')[1])
        data['pos_1'].append(gram_key[0].split('_')[2])
        data['pos_2'].append(gram_key[1].split('_')[2])
        data['pos_3'].append(gram_key[2].split('_')[2])
        data['count'].append(len(gram_list))
    except Exception as e:
        errors += 1
        print(f"Error processing gram_key {gram_key}: {e}")
        # if error make sure that the data lists are not appended to
        max_length = max(len(v) for v in data.values())
        min_length = min(len(v) for v in data.values())
        if max_length != min_length:
            for key in data:
                while len(data[key]) > min_length:
                    data[key].pop()
        max_length = max(len(v) for v in data.values())
        min_length = min(len(v) for v in data.values())
        assert(max_length == min_length)
        continue
print(f"Total errors in dataframe creation: {errors}")
gram3_df = pd.DataFrame(data)

# %%
len(gram3_df)

# %%
gram3_df.sort_values(by='count', ascending=False, inplace=True)

# %%
gram3_df.to_csv('gram3.csv', sep='\t', index=False)

# %%


# %%


