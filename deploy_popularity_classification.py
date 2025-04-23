import pandas as pd
import numpy as np
import psycopg2.extras
import joblib
import os
from dotenv import load_dotenv
import logging
from audit import log_audit_event

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

try:
    model_dir = os.getenv('MODEL_DIR', 'models')
    scaler = joblib.load(os.path.join(model_dir, 'scaler_ted_model_balanced.pkl'))
    logging.info("Scaler model loaded successfully.")
except Exception as e:
    logging.error(f"Error loading scaler model: {e}")
    exit(1)

db_host = os.getenv('POSTGRES_HOST')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_port = os.getenv('POSTGRES_PORT')
db_name = os.getenv('POSTGRES_DB')

try:
    with psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            sslmode='require'
    ) as connection:
        with connection.cursor() as cursor:
            logging.info("Successfully connected to the database.")

            video_data_query = "SELECT * FROM fact_video_metrics;"
            video_data = pd.read_sql(video_data_query, connection)
            logging.info(f"Fetched {len(video_data)} records from the database.")

            if video_data.empty or 'view_count' not in video_data.columns:
                logging.error("The 'view_count' column is missing or data is empty.")
                exit(1)

            # Feature engineering
            video_data['log_views'] = np.log1p(video_data['view_count'])
            video_data['log_likes'] = np.log1p(video_data['like_count'])
            video_data['log_comments'] = np.log1p(video_data['comment_count'])
            features_to_use = ['log_views', 'log_likes', 'log_comments']

            df_scaled = scaler.transform(video_data[features_to_use])

            popularity_threshold = np.percentile(video_data['view_count'], 60)
            video_data['popularity'] = np.where(video_data['view_count'] >= popularity_threshold, 'Popular',
                                                'Not Popular')
            logging.info("Assigned popularity using the 60th percentile of view counts.")

            cursor.execute("""CREATE TABLE IF NOT EXISTS dim_stats (
                                video_id TEXT PRIMARY KEY,
                                popularity TEXT NOT NULL
                              );""")
            connection.commit()

            stats_data = video_data[['video_id', 'popularity']]
            for index, row in stats_data.iterrows():
                cursor.execute("SELECT * FROM dim_stats WHERE video_id = %s;", (row['video_id'],))
                old_record = cursor.fetchone()
                insert_query = """
                    INSERT INTO dim_stats (video_id, popularity)
                    VALUES (%s, %s)
                    ON CONFLICT (video_id) DO UPDATE SET popularity = EXCLUDED.popularity;
                """
                cursor.execute(insert_query, (row['video_id'], row['popularity']))
                log_audit_event(cursor, "system", "INSERT" if old_record is None else "UPDATE", "dim_stats",
                                row['video_id'], old_record, row.to_dict())

            connection.commit()
            logging.info(f"Inserted popularity ratings for {len(stats_data)} videos into 'dim_stats' table.")

except Exception as e:
    logging.error(f"Unexpected error: {e}")
finally:
    logging.info("Database connection closed.")
