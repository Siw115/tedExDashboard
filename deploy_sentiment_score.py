import pandas as pd
import joblib
import psycopg2
from dotenv import load_dotenv
import os
from audit import log_audit_event

# Load environment variables
load_dotenv()

# Step 1: Load Pre-trained Model and Vectorizer
try:
    classifier = joblib.load('models/sentiment_model.pkl')
    vectorizer = joblib.load('models/tfidf_vectorizer.pkl')
    print("Model and vectorizer loaded successfully!")
except Exception as e:
    print(f"Error loading model/vectorizer: {e}")
    exit()

# Step 2: Connect to PostgreSQL Database using psycopg2
db_host = os.getenv('POSTGRES_HOST')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_port = os.getenv('POSTGRES_PORT')
db_name = os.getenv('POSTGRES_DB')

try:
    connection = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        sslmode='require'
    )
    cursor = connection.cursor()
    print("Successfully connected to the database.")
except psycopg2.Error as e:
    print(f"Error connecting to the database: {e}")
    exit()

# Step 3: Fetch Video Data from the 'dim_transcripts' Table
try:
    video_data_query = "SELECT video_id, transcript FROM dim_transcripts;"
    video_data = pd.read_sql(video_data_query, connection)
    print(f"Fetched {len(video_data)} records from the database.")
except Exception as e:
    print(f"Error fetching data from the database: {e}")
    exit()

# Step 4: Pre-process and Vectorize the Data
try:
    print("Vectorizing transcript data...")
    X = vectorizer.transform(video_data['transcript'])
    print("Transcript data successfully vectorized.")
except Exception as e:
    print(f"Error during vectorization: {e}")
    exit()

# Step 5: Predict Sentiment using the Pre-trained Model
try:
    print("Predicting sentiment labels...")
    predicted_labels = classifier.predict(X)
    video_data['sentiment'] = ['positive' if label == 1 else 'negative' for label in predicted_labels]
    print("Sentiment labels predicted successfully.")
except Exception as e:
    print(f"Error during model prediction: {e}")
    exit()

# Step 6: Create or Update Sentiment Data in 'dim_sentiment'
try:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_sentiment (
            video_id TEXT PRIMARY KEY,
            sentiment TEXT NOT NULL
        );
    """)
    connection.commit()
    print("Table 'dim_sentiment' has been created successfully.")
except psycopg2.Error as e:
    print(f"Error creating table 'dim_sentiment': {e}")
    connection.close()
    exit()

try:
    for _, row in video_data.iterrows():
        cursor.execute("SELECT * FROM dim_sentiment WHERE video_id = %s;", (row['video_id'],))
        old_record = cursor.fetchone()

        cursor.execute("""
            INSERT INTO dim_sentiment (video_id, sentiment)
            VALUES (%s, %s)
            ON CONFLICT (video_id) DO UPDATE SET sentiment = EXCLUDED.sentiment;
        """, (row['video_id'], row['sentiment']))

        log_audit_event(
            cursor, "system", 
            action="INSERT" if old_record is None else "UPDATE",
            table_name="dim_sentiment",
            record_id=row['video_id'],
            old_values=old_record,
            new_values=row.to_dict()
        )

    connection.commit()
    print(f"Inserted sentiment results for {len(video_data)} records into 'dim_sentiment' table.")
except psycopg2.Error as e:
    print(f"Error inserting data into 'dim_sentiment' table: {e}")
    connection.rollback()
finally:
    connection.close()
    print("Database connection closed.")
