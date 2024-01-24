import dask.dataframe as dd

def load_data(file_path):
    return dd.read_csv(file_path, delimiter='\t', na_values='\\N')

def filter_empty_values(data):
    return data.dropna(subset=['knownForTitles'])

def write_to_file(data_frame, output_file):
    data_frame.to_csv(output_file, sep='\t', index=False, single_file=True)

if __name__ == "__main__":
    input_file = 'name.basics_processed_filtered.tsv'
    output_file = 'datasets/name.basics_processed_filtered_final.tsv'

    data = load_data(input_file)

    filtered_data = filter_empty_values(data)

    write_to_file(filtered_data, output_file)

    total_actors_processed = len(data)

    print(f"\nProcessed {total_actors_processed} actors.\n")

    print("\nProcessing Complete.")
