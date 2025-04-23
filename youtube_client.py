import logging
import os

import pandas as pd
from textblob import TextBlob

from googleapiclient.discovery import build
from googleapiclient.http import HttpRequest

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

if not YOUTUBE_API_KEY:
    logging.error("YouTube API key is not set. Please set the YOUTUBE_API_KEY environment variable.")


def no_cache_decorator(func):
    def wrapper(*args, **kwargs):
        kwargs['cache_discovery'] = False
        return func(*args, **kwargs)

    return wrapper


HttpRequest = no_cache_decorator(HttpRequest)


def setup_youtube_client():
    """
    Set up the YouTube API client using the API key from the environment variable.
    :return: YouTube API client object.
    """
    return build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)


def compute_sentiment(text):
    """
    Computes the sentiment polarity of a given text (-1 to 1).
    If the text is empty or NaN, it returns 0.
    """
    if not text or pd.isna(text):
        return 0
    return TextBlob(text).sentiment.polarity



def fetch_video_details(video_ids):
    """
    Fetches video details from YouTube for a list of video IDs.
    :param video_ids: List of video IDs.
    :return: DataFrame with video details.
    """
    youtube = setup_youtube_client()
    video_data = []

    try:
        for video_id in video_ids:
            video_response = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            ).execute()

            for video in video_response.get('items', []):
                video_title = video['snippet'].get('title', 'No Title')
                video_description = video['snippet'].get('description', 'No Description')
                video_published_at = video['snippet'].get('publishedAt', None)
                video_statistics = video.get('statistics', {})
                view_count = int(video_statistics.get('viewCount', 0))
                like_count = int(video_statistics.get('likeCount', 0))
                comment_count = int(video_statistics.get('commentCount', 0))
                duration = video['contentDetails'].get('duration', 'N/A')
                category_id = video['snippet'].get('categoryId', 'Unknown')
                tags = video['snippet'].get('tags', [])

                sentiment = compute_sentiment(video_description)

                video_data.append({
                    'Video ID': video_id,
                    'Title': video_title,
                    'Description': video_description,
                    'Published At': video_published_at,
                    'View Count': view_count,
                    'Like Count': like_count,
                    'Comment Count': comment_count,
                    'Duration': duration,
                    'Category': category_id,
                    'Tags': tags if tags else [],
                    'Sentiment': sentiment
                })

        logging.info(f"Fetched details for {len(video_data)} videos.")
        return video_data

    except Exception as e:
        logging.error(f"Error fetching video details from YouTube: {e}")
        return []

