from google.cloud import texttospeech
import pandas as pd
import os
import time
# for guid generation
import uuid
import numpy as np
import hashlib
from pydub import AudioSegment
import requests
import base64


API_KEY = open("/Users/stevie/repos/lingo_kit_data/utils/google_cloud_api_key.txt").read().strip()

SAVE_DIR = '/Users/stevie/repos/lingo_kit_data/data/audio'
DF_PATH = '/Users/stevie/repos/lingo_kit_data/data/dataframe.csv'

ENDPOINT = f"https://texttospeech.googleapis.com/v1/text:synthesize?key={API_KEY}"
VOICES = {
    'english': {
        'male': 'en-US-Neural2-D',
        'female': 'en-US-Neural2-C',
    },
    'italian': {
        'male': 'it-IT-Neural2-F',
        'female': 'it-IT-Neural2-A',
    }
}

def ssml_single_word(word, rate, pitch, pause_ms, slash_pause_ms):
    # Add a period to encourage natural sentence prosody
    safe = word.strip()
    safe = safe.replace("/", f'.<break time="{slash_pause_ms}ms"/>')
    safe = safe.replace(" (", f'. (')
    safe = safe.replace("you. (formal)", "you (formal)")
    if safe[-1] not in ".!?":
        safe += "."
    speach_ssml = f"""
        <speak>
            <break time="{pause_ms}ms"/>
            <prosody rate="{rate}" pitch="{pitch}">
                <p><s>{safe}</s></p>
            </prosody>
            <break time="{pause_ms}ms"/>
        </speak>
    """.strip()
    print(speach_ssml)
    return speach_ssml


def synthesize_word(word, voice_name, speaking_rate, outfile):
    # right now, let's only support the following settings
    pitch = None
    pause_ms = 120
    lang = voice_name.split("-")[0]+"-"+voice_name.split("-")[1]
    assert(lang in ['en-US', 'it-IT'])
    if lang == 'en-US':
        assert(voice_name in VOICES['english'].values())
        assert(speaking_rate == 0.92)
    elif lang == 'it-IT':
        assert(voice_name in VOICES['italian'].values())
        assert(speaking_rate == 0.7)

    ssml = ssml_single_word(word, rate=speaking_rate, pitch="-1st", pause_ms=pause_ms, slash_pause_ms=500)

    # Optional: also set global audioConfig tweaks (mild adjustments)
    audio_cfg = {"audioEncoding": "MP3"}
    if speaking_rate is not None:
        audio_cfg["speakingRate"] = speaking_rate
    if pitch is not None:
        audio_cfg["pitch"] = pitch

    payload = {
        "input": {"ssml": ssml},
        "voice": {"languageCode": lang, "name": voice_name},
        "audioConfig": audio_cfg
    }

    r = requests.post(ENDPOINT, json=payload, timeout=30)
    r.raise_for_status()
    audio_b64 = r.json()["audioContent"]
    with open(outfile, "wb") as f:
        f.write(base64.b64decode(audio_b64))


def get_duration_ms(path):
    # print('getting duration of', path)
    audio = AudioSegment.from_file(path)
    return len(audio)  # duration in milliseconds

def get_audio_hash(text, voice_name, speaking_rate, pitch):
    param_str = f"{text}{voice_name}{speaking_rate:.2f}{pitch:.2f}"
    hash_object = hashlib.sha256(param_str.encode('utf-8'))
    hash_key = hash_object.hexdigest()
    return hash_key


class TextToSpeech:

    def __init__(self):

        if not os.path.exists(SAVE_DIR):
            os.makedirs(SAVE_DIR)

        if not os.path.exists(DF_PATH):
            self.df = pd.DataFrame(columns=[
                'hash', 'text', 'audio_file', 'synthesis_time', 
                'voice_name', 'speaking_rate', 'pitch', 'duration_ms'
            ])
        else:
            self.df = pd.read_csv(DF_PATH)

    # destructor
    def __del__(self):
        self.save()

    def synthesize(self, text, voice_name, speaking_rate, verbose=False, force_generate=False):
        # pitch is no longer supported for configuration
        pitch = 0

        # hash text
        hash_key = get_audio_hash(text, voice_name, speaking_rate, pitch)

        audio_file = f'{SAVE_DIR}/{hash_key}.mp3'

        # check if text already exists
        match_df = self.df[self.df['hash'] == hash_key]
        if not force_generate and len(match_df) > 0:
            if verbose:
                print(f"found {hash_key} in dataframe")
            row = match_df.iloc[0]
            assert(len(match_df) == 1)
            assert(row['text'] == text)
            assert(row['speaking_rate'] == speaking_rate)
            assert(row['pitch'] == pitch)
            assert(row['voice_name'] == voice_name)
            assert(row['hash'] == hash_key)
            assert(row['audio_file'] == audio_file)

        else:
            if not force_generate and os.path.exists(audio_file):
                print(f"WARNING: file {audio_file} exists but not in dataframe, adding to dataframe")
                # since we don't know the synthesis time, just make it -1
                synthesis_time = -1
            else:
                if verbose:
                    print("synthesizing...")

                # raise Exception(
                #     "WARNING: about to synthesize new audio, did you add new words? " + 
                #     "If you have just added new words, this is expeceted, just comment out this raise Exception line")

                # Synthesize speech
                t0 = time.perf_counter()
                synthesize_word(
                    text, voice_name=voice_name,
                    speaking_rate=speaking_rate, outfile=audio_file
                )
                t1 = time.perf_counter()
                synthesis_time = t1 - t0

            duration_ms = get_duration_ms(audio_file)

            # store in dataframe
            if len(match_df) == 0:
                self.df.loc[self.df.shape[0]] = [
                    hash_key, text, audio_file, synthesis_time, voice_name, speaking_rate, pitch, duration_ms
                ]
                row = self.df.iloc[-1]
            else:
                assert(force_generate)
                assert(len(match_df) == 1)
                row = match_df.iloc[0]
        data = dict(row)
        # convert np.float to float
        # convert np.int to int
        for k, v in data.items():
            if type(v) is np.float64 or type(v) is np.float32 or type(v) is np.float16:
                data[k] = float(v)
            if type(v) is np.int64 or type(v) is np.int32 or type(v) is np.int16:
                data[k] = int(v)
        return data

    def save(self):
        self.df.to_csv(DF_PATH, index=False)
        print(f"Dataframe saved to {DF_PATH}")


if __name__ == '__main__':
    tts = TextToSpeech()

    # phrase = 'vorrei un tavolo per due vicino alla finestra'
    phrase = 'this is a test script that should hopefully take a few seconds to finish. Which will give me enought time to debug.'
    ret = tts.synthesize(phrase, speaking_rate=1.0, voice_name=VOICES['english']['male'], pitch=0.0, verbose=True)

    print(ret)