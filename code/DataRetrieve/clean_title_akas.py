import dask.dataframe as dd

''' cleaning title.akas to only include movies in English

Working order:
1. clean_name_basics.py
2. clean_title_akas.py
3. clean_knownfortitles.py
'''

def load_data(file_path, columns=None, chunk_size=None):
    return dd.read_csv(file_path, delimiter='\t', usecols=columns, na_values='\\N', blocksize=chunk_size)


def filter_english_movies(df):
    return df[df['language'] == 'en']


def write_to_file(df, output_file):
    df.to_csv(output_file, sep='\t', index=False, single_file=True)


if __name__ == "__main__":
    title_akas_columns = ['titleId', 'language']

    title_akas_dask = load_data('datasets/title.akas.tsv', columns=title_akas_columns)

    title_akas_dask_filtered = filter_english_movies(title_akas_dask)

    write_to_file(title_akas_dask_filtered, 'datasets/title.akas_processed.tsv')

    print("\nProcessing Complete.")
