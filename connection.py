import numpy as np
import pandas as pd
import psycopg2
import logging
from dotenv import load_dotenv
import os

from audit import log_audit_event

# Load environment variables
load_dotenv()

# Set up logging for database actions and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_postgres():
    db_host = os.getenv('POSTGRES_HOST')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_port = os.getenv('POSTGRES_PORT')
    db_name = os.getenv('POSTGRES_DB')

    logging.info(f"Connecting to PostgreSQL with host: {db_host}, port: {db_port}, dbname: {db_name}, user: {db_user}")

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        logging.info(f"Successfully connected to PostgreSQL at {db_host}:{db_port}, database: {db_name}")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None


def close_connection(conn):
    if conn:
        try:
            conn.close()
            logging.info("Database connection closed.")
        except psycopg2.Error as e:
            logging.error(f"Error closing the database connection: {e}")


def create_fact_table(cursor):
    """
    Creates the fact table 'fact_video_metrics' for storing video metrics.
    """
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_video_metrics (
                video_id TEXT PRIMARY KEY,
                published_at TIMESTAMP NOT NULL,
                view_count INT,
                like_count INT,
                comment_count INT,
                duration TEXT
            );
        """)
        cursor.connection.commit()
        logging.info("Table 'fact_video_metrics' created or already exists.")
    except psycopg2.Error as e:
        logging.error(f"Error creating table 'fact_video_metrics': {e}")
        cursor.connection.rollback()


def create_dimension_tables(cursor):
    """
    Creates the dimension tables 'dim_video_info' and 'dim_transcripts'.
    """
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_video_info (
                video_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                category TEXT,
                tags TEXT[]
            );
        """)
        logging.info("Table 'dim_video_info' created or already exists.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_transcripts (
                video_id TEXT PRIMARY KEY,
                transcript TEXT,
                FOREIGN KEY (video_id) REFERENCES fact_video_metrics (video_id) ON DELETE CASCADE
            );
        """)
        logging.info("Table 'dim_transcripts' created or already exists.")
        cursor.connection.commit()
    except psycopg2.Error as e:
        logging.error(f"Error creating dimension tables: {e}")
        cursor.connection.rollback()


def insert_video_metrics(cursor, videos_df, user_id="system"):
    if videos_df.empty:
        logging.warning("No video data to insert.")
        return

    total_videos = len(videos_df)
    inserted_videos = 0

    for _, row in videos_df.iterrows():
        try:
            row['View Count'] = 0 if pd.isna(row['View Count']) else row['View Count']
            row['Like Count'] = 0 if pd.isna(row['Like Count']) else row['Like Count']
            row['Comment Count'] = 0 if pd.isna(row['Comment Count']) else row['Comment Count']
            row['Duration'] = '' if pd.isna(row['Duration']) else row['Duration']

            popularity_default = 'Not Rated Yet'

            cursor.execute("SELECT video_id FROM dim_stats WHERE video_id = %s;", (row['Video ID'],))
            if not cursor.fetchone():
                logging.info(f"Video ID {row['Video ID']} not found in 'dim_stats'. Inserting into dim_stats.")
                cursor.execute("""
                    INSERT INTO dim_stats (video_id, popularity) 
                    VALUES (%s, %s) 
                    ON CONFLICT (video_id) DO NOTHING;
                """, (row['Video ID'], popularity_default))

            cursor.execute("SELECT * FROM fact_video_metrics WHERE video_id = %s;", (row['Video ID'],))
            old_record = cursor.fetchone()

            if old_record:
                old_view_count, old_like_count, old_comment_count, old_duration = old_record[2], old_record[3], \
                old_record[4], old_record[5]

                if (old_view_count != row['View Count'] or
                        old_like_count != row['Like Count'] or
                        old_comment_count != row['Comment Count'] or
                        old_duration != row['Duration']):
                    cursor.execute(
                        """
                        UPDATE fact_video_metrics 
                        SET view_count = %s, like_count = %s, comment_count = %s, duration = %s
                        WHERE video_id = %s
                        """,
                        (row['View Count'], row['Like Count'], row['Comment Count'], row['Duration'], row['Video ID'])
                    )

                    log_audit_event(
                        cursor, user_id,
                        action="UPDATE",
                        table_name="fact_video_metrics",
                        record_id=row['Video ID'],
                        old_values={
                            'view_count': old_view_count,
                            'like_count': old_like_count,
                            'comment_count': old_comment_count,
                            'duration': old_duration
                        },
                        new_values=row.to_dict()
                    )
            else:
                cursor.execute(
                    """
                    INSERT INTO fact_video_metrics (video_id, published_at, view_count, like_count, comment_count, duration)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (row['Video ID'], row['Published At'], row['View Count'], row['Like Count'], row['Comment Count'],
                     row['Duration'])
                )

                log_audit_event(
                    cursor, user_id,
                    action="INSERT",
                    table_name="fact_video_metrics",
                    record_id=row['Video ID'],
                    old_values=None,
                    new_values=row.to_dict()
                )

            inserted_videos += 1

        except psycopg2.Error as e:
            logging.error(f"Error inserting/updating video metrics for {row['Video ID']}: {e}")
            cursor.connection.rollback()

    logging.info(f"Attempted to insert/update {total_videos} videos. Successfully processed {inserted_videos}.")
    cursor.connection.commit()


def insert_video_info(cursor, videos_df, user_id="system"):
    """
    Inserts video information into the 'dim_video_info' table and logs the action in the audit log.
    """
    if videos_df.empty:
        logging.warning("No video info to insert.")
        return

    videos_df = videos_df.where(pd.notnull(videos_df), None)

    total_videos = len(videos_df)
    inserted_videos = 0

    for _, row in videos_df.iterrows():
        try:
            cursor.execute("SELECT * FROM dim_video_info WHERE video_id = %s;", (row['Video ID'],))
            old_record = cursor.fetchone()

            cursor.execute(
                """
                INSERT INTO dim_video_info (video_id, title, description, category, tags)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET 
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    tags = EXCLUDED.tags
                """,
                (row['Video ID'], row['Title'], row['Description'], row['Category'], row['Tags'])
            )
            inserted_videos += 1

            log_audit_event(
                cursor, user_id,
                action="INSERT" if not old_record else "UPDATE",
                table_name="dim_video_info",
                record_id=row['Video ID'],
                old_values=old_record,
                new_values=row.to_dict()
            )

        except psycopg2.Error as e:
            logging.error(f"Error inserting video info {row['Video ID']}: {e}")
            cursor.connection.rollback()

    logging.info(f"Attempted to insert {total_videos} video info records. Successfully inserted {inserted_videos}.")
    cursor.connection.commit()


def insert_transcripts(cursor, transcripts_df, user_id="system"):
    """
    Inserts transcript data into the 'dim_transcripts' table and logs the action in the audit log.
    """
    if transcripts_df.empty:
        logging.warning("No transcripts to insert.")
        return

    total_transcripts = len(transcripts_df)
    inserted_transcripts = 0

    for _, row in transcripts_df.iterrows():
        try:
            cursor.execute("SELECT video_id FROM fact_video_metrics WHERE video_id = %s;", (row['Video ID'],))
            if not cursor.fetchone():
                logging.warning(
                    f"Video ID {row['Video ID']} not found in 'fact_video_metrics'. Skipping transcript insert.")
                continue

            # Proceed with insert if video ID exists
            cursor.execute("SELECT * FROM dim_transcripts WHERE video_id = %s;", (row['Video ID'],))
            old_record = cursor.fetchone()

            cursor.execute(
                """
                INSERT INTO dim_transcripts (video_id, transcript)
                VALUES (%s, %s)
                ON CONFLICT (video_id) DO UPDATE SET transcript = EXCLUDED.transcript
                """,
                (row['Video ID'], row['Transcript'])
            )
            inserted_transcripts += 1

            log_audit_event(
                cursor, user_id,
                action="INSERT" if not old_record else "UPDATE",
                table_name="dim_transcripts",
                record_id=row['Video ID'],
                old_values=old_record,
                new_values=row.to_dict()
            )

        except psycopg2.Error as e:
            logging.error(f"Error inserting transcript for video {row['Video ID']}: {e}")
            cursor.connection.rollback()

    logging.info(f"Attempted to insert {total_transcripts} transcripts. Successfully inserted {inserted_transcripts}.")
    cursor.connection.commit()
