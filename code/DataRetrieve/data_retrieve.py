import pandas as pd
import os
import shutil

def load_data(file_path):
    return pd.read_csv(file_path, delimiter='\t', na_values='\\N')

def process_data(data, checkpoint_interval=100, checkpoint_file='checkpoint.txt', output_file='output.txt'):
    actor_lists = []

    if os.path.exists(checkpoint_file):
        try:
            checkpoint = pd.read_csv(checkpoint_file, delimiter='\t', na_values='\\N')
            processed_rows = checkpoint['processed_rows'].values[0]
            print(f"Resuming from checkpoint. Processed {processed_rows} rows.")
        except Exception as e:
            print(f"Error reading checkpoint file: {e}")
            return actor_lists
    else:
        processed_rows = 0

    for idx, row in data.iloc[processed_rows:].iterrows():
        known_for_titles = str(row['knownForTitles']).split(',')

        actors_in_same_movie = set()
        for title in known_for_titles:
            print(f'\ncurrent title: {title}')
            movie_row = data[data['knownForTitles'].str.contains(title, na=False)]
            actors_in_same_movie.update(movie_row['primaryName'].tolist())

        actors_in_same_movie.discard(row['primaryName'])
        actor_lists.append([row['primaryName']] + list(actors_in_same_movie))

        if idx % checkpoint_interval == 0:
            # Saving checkpoint to a temp file
            temp_checkpoint_file = 'checkpoint_temp.txt'
            checkpoint_data = pd.DataFrame({'processed_rows': [idx]})
            checkpoint_data.to_csv(temp_checkpoint_file, sep='\t', index=False)

            # Moving the temp file to the final checkpoint file
            try:
                shutil.move(temp_checkpoint_file, checkpoint_file)
            except Exception as e:
                print(f"Error moving checkpoint file: {e}")
                os.remove(temp_checkpoint_file)

            # Writing to the output file and clear the actor_lists
            write_to_file(actor_lists, output_file)
            actor_lists = []

            print(f"Checkpoint: Processed {idx} rows.")

    # Writing any remaining data to the output file
    write_to_file(actor_lists, output_file)

    return actor_lists

def write_to_file(actor_lists, output_file):
    with open(output_file, 'a') as file:
        for actor_list in actor_lists:
            file.write(','.join(actor_list) + ',\n')


if __name__ == "__main__":
    input_file = 'datasets/name.basics_processed_filtered_final.tsv'
    output_file = 'output.txt'
    checkpoint_file = 'checkpoint.txt'

    print("\nStarting load_data()\n")
    data = load_data(input_file)

    print("\nStarting process_data()\n")
    actor_lists = process_data(data, checkpoint_file=checkpoint_file, output_file=output_file)

    total_actors_processed = len(data)

    print(f"\nProcessed {total_actors_processed} actors.\n")

    print("\nProcessing Complete.")
