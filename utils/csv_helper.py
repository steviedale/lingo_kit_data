# load in environment variable
import os
PATH_TO_REPO = os.getenv('PATH_TO_REPO')
assert PATH_TO_REPO is not None, "Please set PATH_TO_REPO environment variable"

import os
import pandas as pd


def get_all_csv_files_rec(dir):
    """Ritorna una lista di tutti i file .csv nella directory e nelle sottodirectory."""
    csv_files = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files


def get_all_terms_df():
    all_csv_files = get_all_csv_files_rec(os.path.join(PATH_TO_REPO, 'dataframes/dataframes_by_pos'))
    all_df = pd.DataFrame()
    for path in all_csv_files:
        assert(os.path.exists(path))
        # print(path)
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"Error reading {path}: {e}")
            raise e
        all_df = pd.concat([all_df, df], ignore_index=True)
    # len(all_df), all_df.columns
    return all_df