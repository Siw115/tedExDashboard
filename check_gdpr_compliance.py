import logging

import psycopg2


def delete_user_data(cursor, video_id):
    try:
        cursor.execute("DELETE FROM fact_video_metrics WHERE video_id = %s;", (video_id,))
        cursor.execute("DELETE FROM dim_video_info WHERE video_id = %s;", (video_id,))
        cursor.execute("DELETE FROM dim_transcripts WHERE video_id = %s;", (video_id,))
        cursor.connection.commit()
        logging.info(f"Data for video {video_id} has been deleted.")
    except psycopg2.Error as e:
        logging.error(f"Error deleting data for video {video_id}: {e}")
