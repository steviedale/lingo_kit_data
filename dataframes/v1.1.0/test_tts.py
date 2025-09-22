# %%
import asyncio

from openai import AsyncOpenAI
from openai.helpers import LocalAudioPlayer

# %%
api_key_path = '/Users/stevie/repos/lingo_kit_data/dataframes/v1.1.0/generator/openaiapikey.txt'
api_key = open(api_key_path).read().strip()
openai = AsyncOpenAI(api_key=api_key)

# %%
async def main() -> None:
    async with openai.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="echo",
        # input="arrivaderci",
        # input="ciao",
        # input="tu",
        # input="lei",
        # input="il",
        # input='lui',
        # when does the last bus for Milan depart?
        # input="quando parte l'ultimo autobus per Milano?",
        input="Buongiorno, come stai oggi?",
        instructions="Speak slowly, calmly, and clearly in Italian. This will be used for language learning.",
        response_format="pcm",
    ) as response:
        await LocalAudioPlayer().play(response)

if __name__ == "__main__":
    asyncio.run(main())
