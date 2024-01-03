import dask.dataframe as dd

''' cleaning name.basics to remove any unneeded columns and non latin actor names
whic we are only left with actor names and their known for movies

Working order:
1. clean_name_basics.py
2. clean_title_akas.py
3. clean_knownfortitles.py
'''


def load_data(file_path, chunk_size=None):
    return dd.read_csv(file_path, delimiter='\t', na_values='\\N', blocksize=chunk_size)


def filter_latin_characters(df):
    return df[df['primaryName'].str.encode('ascii', 'ignore').str.decode('ascii') == df['primaryName']]


def remove_columns_and_filter_known_for_titles(df):
    return df[['primaryName', 'knownForTitles']].dropna(subset=['knownForTitles'])


def write_to_file(df, output_file):
    df.to_csv(output_file, sep='\t', index=False, single_file=True)


if __name__ == "__main__":
    name_basics_dask = load_data('datasets/name.basics.tsv')

    name_basics_dask_filtered = filter_latin_characters(name_basics_dask)

    name_basics_dask_processed = remove_columns_and_filter_known_for_titles(name_basics_dask_filtered)

    write_to_file(name_basics_dask_processed, 'datasets/name.basics_processed.tsv')

    print("\nProcessing Complete.")
