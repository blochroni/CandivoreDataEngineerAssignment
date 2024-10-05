import gspread
import requests
from google.oauth2.service_account import Credentials
from snowflake.connector import connect
import datetime
import config
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import regexp_replace, col, to_timestamp
from tenacity import retry, stop_after_attempt, wait_exponential


# Initialize a Spark session
def get_spark_session():
    """
    Initializes and configures a Spark session.
    Configurations include setting memory limits for both executor and driver,
    and increasing network timeout to handle larger datasets.
    """
    return SparkSession.builder \
        .appName("InfluencerCampaignData") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()


# Retry decorator for handling API failures with exponential backoff
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
def extract_google_sheet_data():
    """
    Extracts data from a Google Sheet using Google Sheets API.
    Uses the service account credentials stored in a JSON file.
    Returns all records from the first worksheet of the Google Sheet.
    """
    creds = Credentials.from_service_account_file(config.GOOGLE_SERVICE_ACCOUNT_KEY_PATH,
                                                  scopes=config.GOOGLE_API_SCOPES)
    client = gspread.authorize(creds)
    #sheet = client.open_by_key(config.GOOGLE_SHEET_ID) # For testing
    sheet = client.open("Influencer_Campaign_Data")  # Opening by sheet name
    worksheet = sheet.get_worksheet(0)
    data = worksheet.get_all_records()  # Extract all Google Sheets data
    return data


# Retry decorator for YouTube API failures
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
def scrape_youtube_data(video_url):
    """
    Given a YouTube video URL, this function extracts the video's ID,
    then calls the YouTube API to fetch the publish date and current likes count.
    Returns None, None if the video ID is invalid or the API call fails.
    """
    video_id = extract_video_id(video_url)
    if not video_id:
        return None, None

    # Request video details from the YouTube API
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={config.YOUTUBE_API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        video_info = response.json()['items'][0]
        publish_date = video_info['snippet']['publishedAt']
        likes_count = video_info['statistics'].get('likeCount', 0)
        return publish_date, likes_count
    else:
        return None, None


# Helper function to extract video ID from a YouTube URL
def extract_video_id(video_url):
    """
    Extracts the video ID from a YouTube URL using a regular expression.
    The video ID is a unique identifier used by YouTube to point to a specific video.
    """
    pattern = r'(?:v=|\/)([0-9A-Za-z_-]{11})(?:[&?]|$)'
    match = re.search(pattern, video_url)
    if match:
        return match.group(1)
    return None


# Connect to Snowflake with error handling
def connect_snowflake():
    """
    Establishes a connection to Snowflake using credentials from the config file.
    Returns a Snowflake connection object.
    """
    conn = connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        account=config.SNOWFLAKE_ACCOUNT,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA
    )
    return conn


# Function to create Snowflake table dynamically
def create_snowflake_table(conn, data):
    """
    Dynamically creates a Snowflake table if it doesn't already exist.
    The table is named 'INFLUENCER_CAMPAIGN_METRICS' and contains columns
    based on the data structure passed from the Spark DataFrame.
    """
    dtype_mapping = {
        'string': 'STRING',
        'int': 'INT',
        'double': 'FLOAT',
        'timestamp': 'TIMESTAMP',
        'boolean': 'BOOLEAN'
    }

    # Create columns for Snowflake based on Spark DataFrame schema
    columns = [f"{col} {dtype_mapping.get(dtype, 'STRING')}" for col, dtype in data.dtypes]
    columns_definition = ', '.join(columns)

    # SQL query to create the table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS INFLUENCER_CAMPAIGN_METRICS (
        {columns_definition}
    );
    """
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()


# Function to insert enriched data into Snowflake dynamically
def insert_into_snowflake(conn, data, batch_size=1000):
    """
    Inserts enriched data from the Spark DataFrame into the Snowflake table.
    The table is dynamically updated with the current data.

    :param conn: Snowflake connection object.
    :param data: Spark DataFrame containing the enriched data.
    :param batch_size: Number of rows to insert per batch (default is 1000).
    """
    # Build the query to insert data
    columns = ', '.join(data.columns)
    placeholders = ', '.join(['%s'] * len(data.columns))

    insert_query = f"""
    INSERT INTO INFLUENCER_CAMPAIGN_METRICS 
    ({columns})
    VALUES ({placeholders});
    """

    cur = conn.cursor()

    # Convert Spark DataFrame rows to a local iterator to process rows in batches
    data_batches = data.toLocalIterator()

    batch = []
    for row in data_batches:
        # Convert the Spark Row object to a tuple
        batch.append(tuple(row.asDict().values()))

        # When batch reaches the specified size, insert into Snowflake
        if len(batch) == batch_size:
            cur.executemany(insert_query, batch)  # Execute the batch insert
            batch = []  # Reset the batch after insertion

    # Insert any remaining rows that didn't fill a complete batch
    if batch:
        cur.executemany(insert_query, batch)

    # Commit the transaction to finalize the changes in Snowflake
    conn.commit()

    print(f"Data inserted successfully in batches of {batch_size} rows.")


# Main function that runs the entire pipeline
def run():
    """
    The main pipeline function. It does the following:
    1. Initializes a Spark session.
    2. Extracts data from Google Sheets.
    3. Enriches the data by scraping video details from YouTube.
    4. Inserts the enriched data into a Snowflake table.
    """
    try:
        # Step 1: Initialize Spark session
        spark = get_spark_session()

        # Step 2: Extract data from Google Sheets
        google_sheet_data = extract_google_sheet_data()

        # Step 3: Define the schema for the Google Sheets data and create Spark DataFrame
        schema = StructType([StructField(col, StringType(), True) for col in google_sheet_data[0].keys()] +
                            [StructField("Publish_Date", StringType(), True),
                             StructField("Current_Likes_Count", IntegerType(), True),
                             StructField("Interval_Date", StringType(), True)])

        # Step 4: Enrich the data with YouTube video details
        enriched_data = []
        for row in google_sheet_data:
            video_url = row.get("Video_Url").strip() if row.get("Video_Url") else None
            if video_url:
                publish_date, likes_count = scrape_youtube_data(video_url)
                row["Publish_Date"] = publish_date or ""
                row["Current_Likes_Count"] = int(likes_count) if likes_count is not None else 0
            else:
                row["Publish_Date"] = ""
                row["Current_Likes_Count"] = 0
            row["Interval_Date"] = datetime.datetime.now().isoformat()  # Add current timestamp
            enriched_data.append(row)

        # Convert enriched data to Spark DataFrame
        enriched_df = spark.createDataFrame(enriched_data, schema)
        enriched_df = enriched_df.withColumn("Publish_Date", to_timestamp(col("Publish_Date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        enriched_df = enriched_df.withColumn("Interval_Date",to_timestamp(col("Interval_Date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        enriched_df = enriched_df.withColumn("Cost", regexp_replace(col("Cost"), "\\$", "").cast(FloatType()))

        # Step 5: Connect to Snowflake and insert the data

        conn = connect_snowflake()
        create_snowflake_table(conn, enriched_df)
        insert_into_snowflake(conn, enriched_df)

        print("Pipeline ran successfully!")
    except Exception as e:
        print(f"Error occurred: {e}")

