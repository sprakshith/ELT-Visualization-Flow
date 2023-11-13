import os
import pandas as pd

def clean_and_save_data(input_file='events_data.json', output_file='cleaned.json'):
    if not os.path.exists(input_file):
        print(f"Error: The input file '{input_file}' does not exist.")
        return

    df = pd.read_json(input_file)
    columns_to_drop = ['relevance', 'phq_attendance', 'geo', 'place_hierarchies','alternate_titles','local_rank', 'entities', "impact_patterns"]
    df = df.drop(columns=columns_to_drop)


    # Remove newline characters from the 'description' field
    df['description'] = df['description'].str.replace('\n', ' ')
    df.rename(columns={'end': 'ended'}, inplace=True)
    df.rename(columns={'rank': 'ranks'}, inplace=True)
    print('Cleaning completed')
    df.to_json(output_file, orient='records', lines=True)
