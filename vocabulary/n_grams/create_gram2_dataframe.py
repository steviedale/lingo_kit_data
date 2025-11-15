# %%
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import pandas as pd

# %%
sys.path.append('/Users/stevie/repos/lingo_kit_data/vocabulary')
from stanza_tokenizer import StanzaTokenizer

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
gram2_dict = {}
iter_df = df.sample(frac=1, random_state=42).reset_index(drop=True)
# iter_df = df.sample(n=5000, random_state=42).reset_index(drop=True)
for sentence in tqdm(iter_df['text_it'], total=len(iter_df)):
    start_word_count = {}
    end_word_count = {}
    tokens = tokenizer.tokenize(sentence)
    for i in range(len(tokens)-2):
        t1 = tokens[i]
        t2 = tokens[i+1]

        # keep track of the index of 'find' words
        if t1['text'].lower() not in start_word_count:
            start_word_count[t1['text'].lower()] = -1
        start_word_count[t1['text'].lower()] += 1
        if t2['text'].lower() not in end_word_count:
            end_word_count[t2['text'].lower()] = -1
        end_word_count[t2['text'].lower()] += 1

        if t1['pos'] in bad_pos or t2['pos'] in bad_pos:
            continue

        gram2 = (get_token_key(t1), get_token_key(t2))
        
        start_index = sentence.lower().find(t1['text'].lower(), start_word_count[t1['text'].lower()])
        end_index = sentence.lower().find(t2['text'].lower(), end_word_count[t2['text'].lower()]) + len(t2['text'])
        substr = sentence[start_index:end_index]

        if gram2 not in gram2_dict:
            gram2_dict[gram2] = []

        gram2_dict[gram2].append({
            'sentence': sentence,
            'substr': substr,
            'start_index': start_index,
            'end_index': end_index,
            'token_1': t1,
            'token_2': t2,
        })

# %%
gram2_items = sorted(gram2_dict.items(), key=lambda x: len(x[1]), reverse=True)
for i in range(20):
    print(gram2_items[i][1][0]['token_1']['text'])
    print(gram2_items[i][1][0]['token_2']['text'])
    for j, t in enumerate(gram2_items[i][1]):
        print(t['substr'])
        if j > 2:
            break
        # break

# %%
len(gram2_dict)

# %%
gram2_dict[list(gram2_dict.keys())[0]][0].keys()

# %%
data = {
    'gram2_key': [], 
    'count': [],
    'substr': [],
    'term_1': [],
    'term_2': [],
    'lemma_1': [],
    'lemma_2': [],
    'pos_1': [],
    'pos_2': [],
}
new_gram2_dict = {}
len(gram2_dict)
for gram_key, gram_list in gram2_dict.items():
    substr_votes = {}
    if len(gram_list) < 5:
        continue
    for entry in gram_list:
        substr = entry['substr']
        if substr not in substr_votes:
            substr_votes[substr] = 0
        substr_votes[substr] += 1
    sorted_substrs = sorted(substr_votes.items(), key=lambda x: x[1], reverse=True)
    best_substr, best_votes = sorted_substrs[0]
    data['gram2_key'].append(gram_key)
    data['substr'].append(best_substr.lower())
    data['term_1'].append(gram_key[0].split('_')[0])
    data['term_2'].append(gram_key[1].split('_')[0])
    data['lemma_1'].append(gram_key[0].split('_')[1])
    data['lemma_2'].append(gram_key[1].split('_')[1])
    data['pos_1'].append(gram_key[0].split('_')[2])
    data['pos_2'].append(gram_key[1].split('_')[2])
    data['count'].append(len(gram_list))
gram2_df = pd.DataFrame(data)

# %%
len(gram2_df)

# %%
gram2_df.sort_values(by='count', ascending=False, inplace=True)

# %%
gram2_df.to_csv('gram2.csv', sep='\t', index=False)

# %%


# %%



