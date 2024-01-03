import pandas as pd


def load_data(file_path, chunk_size=None):
    chunks = pd.read_csv(file_path, delimiter='\t', na_values='\\N', chunksize=chunk_size)
    return chunks


def process_chunk(chunk):
    actor_lists = []

    for _, row in chunk.iterrows():
        known_for_titles = str(row['knownForTitles']).split(',')

        actors_in_same_movie = set()
        for title in known_for_titles:
            movie_row = chunk[chunk['knownForTitles'].str.contains(title, na=False)]
            
            actors_in_same_movie.update(movie_row['primaryName'].tolist())

        actors_in_same_movie.discard(row['primaryName'])
        actor_lists.append([row['primaryName']] + list(actors_in_same_movie))

    return actor_lists


def write_to_file(actor_lists, output_file):
    with open(output_file, 'w') as file:
        for actor_list in actor_lists:
            file.write(','.join(actor_list) + ',\n')



if __name__ == "__main__":
    chunk_size = 10000
    data_chunks = load_data('datasets/name.basics_processed_filtered.tsv', chunk_size=chunk_size)

    total_chunks = 0
    total_actors_processed = 0

    for chunk_number, chunk in enumerate(data_chunks):
        print(f"\nProcessing Chunk {chunk_number + 1}...")

        actor_lists = process_chunk(chunk)

        write_to_file(actor_lists, f'output_chunk_{chunk_number + 1}.txt')

        total_chunks += 1
        total_actors_processed += len(chunk)

        print(f"\nProcessed {total_actors_processed} actors across {total_chunks} chunks.")

    print("\nProcessing Complete.")
