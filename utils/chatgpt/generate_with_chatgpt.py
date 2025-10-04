import os
from openai import OpenAI
import pandas as pd
import yaml


api_key_path = '/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.1/generator/openaiapikey.txt'
api_key = open(api_key_path).read().strip()
client = OpenAI(api_key=api_key)


COST_PATH = '/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.1/generator/cost.yaml'


# costs (per million tokens)
COST_TABLE = {
    'gpt-5-mini': {
        'input': 0.25,
        'cached-input': 0.025,
        'output': 2.0
    },
    'gpt-5': {
        'input': 1.25,
        'cached-input': 0.125,
        'output': 10.0
    }
}


def get_cost(response, model='gpt-5-mini'):
    total_input_tokens = response.usage.input_tokens
    cached_input_tokens = response.usage.input_tokens_details.cached_tokens
    non_cached_input_tokens = total_input_tokens - cached_input_tokens
    output_tokens = response.usage.output_tokens
    non_cached_input_token_cost = non_cached_input_tokens * (COST_TABLE[model]['input'] / 1_000_000)
    cached_input_token_cost = cached_input_tokens * (COST_TABLE[model]['cached-input'] / 1_000_000)
    output_token_cost = output_tokens * (COST_TABLE[model]['output'] / 1_000_000)
    total_cost = non_cached_input_token_cost + cached_input_token_cost + output_token_cost
    return {
        'non_cached_input_token_cost': non_cached_input_token_cost,
        'cached_input_token_cost': cached_input_token_cost,
        'output_token_cost': output_token_cost,
        'total_cost': total_cost
    }


def generate_csv(italian_term, model='gpt-5-mini', reasoning_effort='low'):
    current_cost = yaml.load(open(COST_PATH), Loader=yaml.FullLoader)['total_spent']
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

    cost_info = get_cost(response, model=model)

    current_cost += cost_info['total_cost']
    yaml.dump({'total_spent': current_cost}, open(COST_PATH, 'w'))

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
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # if the csv file already exists, append a number (so that it doesn't overwrite) 
            # for example for "test.csv", if it exists, try "test_1.csv", "test_2.csv", etc.
            if os.path.exists(output_path):
                i = 1
                while os.path.exists(output_path.replace('.csv', f'_{i}.csv')):
                    i += 1
                output_path = output_path.replace('.csv', f'_{i}.csv')

            base_df.to_csv(output_path, index=False)
            generate_files.append(output_path)
            print(f"Saved {len(pos_df)} rows to {output_path}")

    os.remove(temp_path)


    return response, generate_files, cost_info
