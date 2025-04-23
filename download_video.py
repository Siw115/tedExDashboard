from pytubefix import YouTube
import moviepy.editor as mp
import os


def download_youtube_video(video_id, download_path="downloads/"):
    url = f"https://www.youtube.com/watch?v={video_id}"
    yt = YouTube(url)
    video = yt.streams.filter(only_audio=True).first()

    if not os.path.exists(download_path):
        os.makedirs(download_path)

    output_file = video.download(output_path=download_path)
    base, ext = os.path.splitext(output_file)
    audio_file = base + '.mp3'

    clip = mp.AudioFileClip(output_file)
    clip.write_audiofile(audio_file)

    return audio_file
