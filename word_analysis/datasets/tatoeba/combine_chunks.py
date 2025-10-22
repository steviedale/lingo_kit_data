import os
import pandas as pd
import random
from tqdm import tqdm


# def consolidate_token_data(df_paths):
def consolidate_token_data(chunks_i):

    save_path = f'new_token_data/combined/combined_tokens_0_to_{chunks_i*10000}.tsv'
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))
    if os.path.exists(save_path):
        df = pd.read_csv(save_path, sep='\t')
        return df

    df_paths = []
    for j in range(chunks_i):
        df_paths.append(f'new_token_data/token_data_chunk_{j}.tsv')

    token_df = pd.DataFrame()
    for df_path in df_paths: 
        df = pd.read_csv(df_path, sep='\t')
        token_df = pd.concat([token_df, df], axis=0, ignore_index=True)
    len(token_df), token_df.columns
    # (4769,
    #  Index(['token_hash', 'text', 'lemma', 'pos', 'xpos', 'deprel', 'feats',
    #         'count', 'sentences', 'group_hash'],
    #        dtype='object'))

    # %%
    new_token_df = pd.DataFrame()
    for token_hash in tqdm(token_df['token_hash'].unique()):
        token_subset = token_df[token_df['token_hash'] == token_hash]
        count = token_subset['count'].sum()
        sentences = []
        for s in token_subset['sentences']:
            sentences.extend(eval(s))
        a = len(sentences)
        sentences = list(set(sentences))
        b = len(sentences)
        # since each df was created with different sentences, the number of sentences should be the same
        assert(a == b)
        if len(sentences) > 20:
            random.shuffle(sentences)
            sentences = sentences[:20]
        new_row = token_subset.iloc[0].copy()
        new_row['count'] = count
        new_row['sentences'] = str(sentences)
        new_token_df = pd.concat([new_token_df, pd.DataFrame([new_row])], axis=0, ignore_index=True)

    # %%
    assert(new_token_df['group_hash'].isna().sum() == 0)
    assert(new_token_df['token_hash'].isna().sum() == 0)

    # %%
    new_token_df['token_pct'] = new_token_df['count'] / new_token_df['count'].sum()

    # %%
    # sanity check
    new_token_df['token_pct'].sum()

    # %%
    group_counts = {}
    for group in tqdm(new_token_df['group_hash'].unique()):
        mask = new_token_df['group_hash'] == group
        group_df = new_token_df[mask]
        group_count = group_df['count'].sum()
        new_token_df.loc[mask, 'group_count'] = group_count
        group_counts[group] = group_df['count'].sum()
    new_token_df['group_pct'] = new_token_df['group_count'] / new_token_df['count'].sum()

    # %%
    # sanity check
    total = 0
    for group in tqdm(new_token_df['group_hash'].unique()):
        mask = new_token_df['group_hash'] == group
        group_df = new_token_df[mask]
        total += group_df.iloc[0]['group_pct']
    total

    # %%
    new_token_df.sort_values(by=['group_pct', 'token_pct'], ascending=False, inplace=True)

    # %%
    new_token_df.head(20)

    # %%
    new_token_df.rename(columns={'count': 'token_count'}, inplace=True)

    # %%
    ordered_columns = [
        'text', 
        'lemma',
        'pos',
        'token_count',
        'token_pct',
        'group_count',
        'group_pct',
        'xpos',
        'deprel',
        'feats',
        'token_hash',
        'group_hash',
        'sentences',
    ]
    assert(set(ordered_columns) == set(new_token_df.columns))
    new_token_df = new_token_df[ordered_columns]

    # %%
    for col in ['token_pct', 'group_pct']:
        new_token_df[col] = new_token_df[col].astype(float).round(6)
    for col in ['token_count', 'group_count']:
        new_token_df[col] = new_token_df[col].astype(int)

    new_token_df.to_csv(save_path, sep='\t', index=False)
    return new_token_df



if __name__ == '__main__':
    token_dir = 'new_token_data'
    df_paths = [os.path.join(token_dir, f) for f in os.listdir(token_dir) if f.endswith('.tsv')]
    token_df = consolidate_token_data(df_paths)
    print(len(token_df))
