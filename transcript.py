from youtube_transcript_api import YouTubeTranscriptApi
import logging


def fetch_transcript_for_videos(video_ids):
    """
    Fetches transcripts for a list of YouTube video IDs.
    :param video_ids: List of video IDs.
    :return: Dictionary of video ID to transcript text.
    """
    transcripts = {}

    for video_id in video_ids:
        try:
            logging.info(f"Fetching transcript for video: {video_id}")
            transcript_data = YouTubeTranscriptApi.get_transcript(video_id)
            transcript_text = ' '.join([entry['text'] for entry in transcript_data])
            transcripts[video_id] = transcript_text
            logging.info(f"Successfully fetched transcript for video {video_id}.")
        except Exception as e:
            logging.error(f"Error fetching transcript for video {video_id}: {e}")
            transcripts[video_id] = None

    return transcripts
