import os
from openai import OpenAI
api_key_path = '/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.0/generator/openaiapikey.txt'
api_key = open(api_key_path).read().strip()
client = OpenAI(api_key=api_key)


def generate_csv(part_of_speech, base_term, english_translation, output_path, model='gpt-5-mini', prompt_file=None):
    if prompt_file is None:
        prompt_file = f'/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.0/generator/prompts/{part_of_speech}_prompt_with_sentences.txt'
    assert(os.path.exists(prompt_file)), f"Prompt file {prompt_file} does not exist"
    prompt = open(prompt_file).read()
    prompt = prompt.replace(f'[{part_of_speech.upper()}]', base_term).replace('[ENGLISH TRANSLATION]', english_translation)
    
    response = client.responses.create(
        model=model,
        input=prompt
    )
    
    with open(output_path, "w") as file:
        file.write(response.output_text)
    
    return response.output_text
