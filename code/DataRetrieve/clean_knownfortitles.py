import pandas as pd

''' cleaning name.basics once more to remove non english movies retrieved from
filtered title.akas from actors' known for movies

Working order:
1. clean_name_basics.py
2. clean_title_akas.py
3. clean_knownfortitles.py
'''


def load_data(file_path):
    return pd.read_csv(file_path, delimiter='\t', na_values='\\N')

def filter_known_for_titles(actors_df, english_movies_set):
    def filter_titles(row):
        known_for_titles = str(row['knownForTitles']).split(',')
        filtered_titles = [title for title in known_for_titles if title in english_movies_set]
        return ','.join(filtered_titles)

    actors_df['knownForTitles'] = actors_df.apply(filter_titles, axis=1)
    return actors_df

def write_to_file(data_frame, output_file):
    data_frame.to_csv(output_file, sep='\t', index=False)

if __name__ == "__main__":
    name_basics_df = load_data('datasets/name.basics_processed.tsv')
    title_akas_set = set(load_data('datasets/title.akas_processed.tsv')['titleId'])

    name_basics_filtered = filter_known_for_titles(name_basics_df, title_akas_set)

    write_to_file(name_basics_filtered, 'datasets/name.basics_processed_filtered.tsv')
