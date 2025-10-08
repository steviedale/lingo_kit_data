from pygame import mixer
import time


def play_audio(file_path):
    mixer.init()
    mixer.music.load(file_path)
    mixer.music.play()
    # wait for music to finish playing
    while mixer.music.get_busy():
        time.sleep(0.1)


if __name__ == '__main__':
    file_path = ''  # specify your audio file path here
    play_audio(file_path)