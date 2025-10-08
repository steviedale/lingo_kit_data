# load in environment variable
import os
PATH_TO_REPO = os.getenv('PATH_TO_REPO')

# %%
from tqdm import tqdm
import pandas as pd
import sys
sys.path.append(PATH_TO_REPO)
from utils.chatgpt.generate_with_chatgpt import generate_csv
from utils.csv_helper import get_all_terms_df

# %%
path = os.path.join(PATH_TO_REPO, 'dataframes/old_dataframes/spotify_lessons.csv')
assert(os.path.exists(path))
df = pd.read_csv(path)
len(df), df.columns

# %%
punctuations = '''!()-[]{};:"\,<>./?@#$%^&*_~'''
word_set = set()
for i, row in tqdm(df.iterrows(), total=len(df)):
    words = row['italian_term'].lower().split(' ')
    for word in words:
        for p in punctuations:
            word = word.replace(p, '')
        if word != '':
            word_set.add(word)
len(word_set)

# %%
italian_words = list(sorted(word_set))

# %%
new_list = []
all_df = get_all_terms_df()
for term_it in tqdm(italian_words):
    if term_it.lower() in all_df['term_italian'].str.lower().values:
        print(f"Skipping term '{term_it}' as it already exists in the dataset.")
    else:
        new_list.append(term_it)
len(new_list)

# %%
italian_words = sorted(new_list)

# %%
print(len(italian_words))
italian_words = [x for x in italian_words if 'mille' not in x]
print(len(italian_words))

#italian_words = list(reversed(italian_words))

# %%
blacklist = ['guardia', 'joe']

# %%
response_list = []
generated_files_list = []
cost_info_list = []
for term_it in tqdm(italian_words):
    if term_it in blacklist:
        print(f"Skipping blacklisted term '{term_it}'.")
        continue

    all_df = get_all_terms_df()
    if term_it.lower() in all_df['term_italian'].str.lower().values:
        print(f"Skipping term '{term_it}' as it already exists in the dataset.")
        continue

    attempt_count = 0
    for attempt_count in range(3):
        try:
            print(f"Attempt {attempt_count + 1} for term: {term_it}")
            response, generated_files, cost_info = generate_csv(
                italian_term=term_it,
                model='gpt-5-mini',
                # model='gpt-5',
                reasoning_effort='medium',
            )
            response_list.append(response)
            generated_files_list.extend(generated_files)
            cost_info_list.append(cost_info)
            print(f"cost of this call: {cost_info['total_cost']}")
            break  # Exit the retry loop if successful
        except pd.errors.ParserError as e:
            print(f"ParserError on attempt {attempt_count + 1} for term '{term_it}': {e}")
            if attempt_count == 2:
                print(f"Failed to process term '{term_it}' after 3 attempts.")
        except Exception as e:
            print(f"Error on attempt {attempt_count + 1} for term '{term_it}': {e}")
            if attempt_count == 2:
                print(f"Failed to process term '{term_it}' after 3 attempts.")

# %%



