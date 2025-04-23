import logging
import os
import pandas as pd
import psycopg2
import concurrent.futures
from datetime import datetime
from audit import log_audit_event
from dotenv import load_dotenv
from connection import connect_to_postgres, create_fact_table, create_dimension_tables, insert_video_metrics, \
    insert_video_info, insert_transcripts, close_connection
from youtube_client import fetch_video_details
from transcript import fetch_transcript_for_videos

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

VIDEO_IDS_DIRECTORY = "/app/INDATAD"


def insert_history_row(cursor, row, snapshot_date):
    try:
        cursor.execute(
            """
            INSERT INTO fact_video_metrics_history (video_id, snapshot_date, view_count, like_count, comment_count)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (row['Video ID'], snapshot_date, row['View Count'], row['Like Count'], row['Comment Count'])
        )
        logging.info(f"Inserted history row for video {row['Video ID']} at snapshot date {snapshot_date}.")
    except psycopg2.Error as e:
        logging.error(f"Error inserting historical video metrics for {row['Video ID']}: {e}")
        raise


def fetch_missing_transcripts(cursor, video_ids):
    cursor.execute(
        "SELECT video_id FROM dim_transcripts WHERE video_id = ANY(%s)",
        (video_ids,)
    )
    existing_transcripts = {row[0] for row in cursor.fetchall()}
    missing_transcripts = [video_id for video_id in video_ids if video_id not in existing_transcripts]
    return missing_transcripts


def fetch_transcripts_concurrently(video_ids):
    transcripts = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        logging.info(f"Fetching transcripts for {len(video_ids)} videos.")
        future_to_video = {executor.submit(fetch_transcript_for_videos, [video_id]): video_id for video_id in video_ids}
        for future in concurrent.futures.as_completed(future_to_video):
            video_id = future_to_video[future]
            try:
                result = future.result()
                transcript = result.get(video_id, None)
                transcripts[video_id] = transcript if transcript is not None else ''
                logging.info(f"Transcript fetched for video {video_id}.")
            except Exception as e:
                logging.error(f"Error fetching transcript for video {video_id}: {e}")
    return transcripts


def save_video_metrics_to_history(cursor, video_data, weeks=1):
    snapshot_date = datetime.now()
    total_videos = len(video_data)
    inserted_videos = 0

    for _, row in video_data.iterrows():
        video_id = row['Video ID']
        cursor.execute("""
            SELECT view_count, like_count, comment_count 
            FROM fact_video_metrics_history 
            WHERE video_id = %s 
            ORDER BY snapshot_date DESC 
            LIMIT 1;
        """, (video_id,))
        latest_history_metrics = cursor.fetchone()

        if latest_history_metrics is None or (
            latest_history_metrics[0] != row['View Count'] or
            latest_history_metrics[1] != row['Like Count'] or
            latest_history_metrics[2] != row['Comment Count']
        ):
            insert_history_row(cursor, row, snapshot_date)
            inserted_videos += 1

    logging.info(f"Inserted {inserted_videos} video metrics into history for the latest week.")


if __name__ == "__main__":
    if not os.path.exists(VIDEO_IDS_DIRECTORY):
        logging.error(f"The directory {VIDEO_IDS_DIRECTORY} does not exist.")
        exit()

    tedx_video_ids = [filename.split('.')[0] for filename in os.listdir(VIDEO_IDS_DIRECTORY) if os.path.isfile(os.path.join(VIDEO_IDS_DIRECTORY, filename))]
    logging.info(f"Extracted {len(tedx_video_ids)} TEDx video IDs.")

    if not tedx_video_ids:
        logging.error("No TEDx video IDs were found.")
        exit()

    conn = connect_to_postgres()
    if not conn:
        logging.error("Failed to connect to PostgreSQL.")
        exit()

    try:
        cursor = conn.cursor()
        logging.info("Creating fact and dimension tables if they don't exist.")
        create_fact_table(cursor)
        create_dimension_tables(cursor)
        conn.commit()

        logging.info("Fetching video details for all videos.")
        video_details = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_video = {executor.submit(fetch_video_details, [video_id]): video_id for video_id in tedx_video_ids}
            for future in concurrent.futures.as_completed(future_to_video):
                try:
                    video_details.extend(future.result())
                except Exception as e:
                    logging.error(f"Error fetching details: {e}")

        video_details_df = pd.DataFrame(video_details)
        if video_details_df.empty:
            logging.error("No video details were fetched.")
            exit()

        video_details_df.fillna({
            'Transcript': '',
            'View Count': 0,
            'Like Count': 0,
            'Comment Count': 0,
            'Published At': datetime.now()
        }, inplace=True)
        video_details_df['Published At'] = pd.to_datetime(video_details_df['Published At'], errors='coerce').dt.tz_localize(None)

        logging.info("Inserting video information into dim_video_info.")
        insert_video_info(cursor, video_details_df)
        conn.commit()

        logging.info("Processing videos for potential metric updates.")
        save_video_metrics_to_history(cursor, video_details_df, weeks=1)
        conn.commit()

        missing_transcript_ids = fetch_missing_transcripts(cursor, tedx_video_ids)
        logging.info(f"Fetching transcripts for {len(missing_transcript_ids)} videos without existing transcripts.")
        transcripts = fetch_transcripts_concurrently(missing_transcript_ids)
        video_details_df['Transcript'] = video_details_df['Video ID'].map(transcripts)

        logging.info("Inserting video metrics and info.")
        insert_video_metrics(cursor, video_details_df)
        insert_transcripts(cursor, video_details_df[['Video ID', 'Transcript']])
        conn.commit()

        for video_id in tedx_video_ids:
            log_audit_event(cursor, "system", "INSERT", "fact_video_metrics", video_id)

        conn.commit()
        logging.info("New videos and historical metrics have been successfully inserted into the database.")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        conn.rollback()
    finally:
        if conn:
            cursor.close()
            close_connection(conn)
