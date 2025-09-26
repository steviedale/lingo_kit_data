import os
from openai import OpenAI
import pandas as pd


api_key_path = '/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.1/generator/openaiapikey.txt'
api_key = open(api_key_path).read().strip()
client = OpenAI(api_key=api_key)


def generate_csv(italian_term, model='gpt-5-mini', reasoning_effort='low'):
    prompt_file = f'/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.1/generator/prompts/merged_prompt.txt'
    assert(os.path.exists(prompt_file)), f"Prompt file {prompt_file} does not exist"
    prompt = open(prompt_file).read()

    response = client.responses.create(
        model=model,
        input=[
            {
                'role': "developer",
                'content': prompt,
            },
            {
                'role': "user",
                'content': f"The italian word is {italian_term}",
            },
        ],
        reasoning={
            "effort": reasoning_effort,
        },
    )

    temp_path = 'temp.csv'

    with open(temp_path, "w") as file:
        file.write(response.output_text)

    df = pd.read_csv(temp_path)

    assert('part_of_speech' in df.columns), f"Generated CSV must contain 'part_of_speech' column: {df.columns}"
    assert('base_lemma_italian' in df.columns), f"Generated CSV must contain 'base_lemma_italian' column: {df.columns}"

    generate_files = []

    for pos in df['part_of_speech'].unique():
        pos_df = df[df['part_of_speech'] == pos]

        for base_term in pos_df['base_lemma_italian'].unique():
            base_df = pos_df[pos_df['base_lemma_italian'] == base_term]

            output_path = f"/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.1/dataframes/{pos}/{base_term}.csv"
            base_df.to_csv(output_path, index=False)
            generate_files.append(output_path)
            print(f"Saved {len(pos_df)} rows to {output_path}")
    
    os.remove(temp_path)

    return response, generate_files
