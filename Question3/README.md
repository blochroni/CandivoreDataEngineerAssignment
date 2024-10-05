
# Influencer Marketing Dashboard Pipeline

This project automates the process of extracting data from a Google Sheet, enriching it with additional data from the YouTube API, and inserting the final data into a Snowflake database. This is designed to run daily as an Airflow DAG for a scalable data pipeline.

## Objective

The pipeline achieves the following:

1. **Extract Data**: Fetch influencer campaign data from a Google Sheet.
2. **Enrich Data**: For each video URL in the Google Sheet, the pipeline scrapes the YouTube video’s publish date and the current number of likes.
3. **Store Data**: Inserts the enriched data into a Snowflake table called `INFLUENCER_CAMPAIGN_METRICS`.
4. **Scalability**: The pipeline is designed to be scalable using Apache Spark, making it efficient for large datasets.

## Requirements

To run the pipeline, the following dependencies are required:

- `tenacity`
- `google-auth`
- `google-auth-oauthlib`
- `google-auth-httplib2`
- `gspread`
- `snowflake-connector-python`
- `pyspark`
- `requests`
- `apache-airflow`
- `python-dotenv`

You can install these dependencies using the following command:

```bash
pip install -r requirements.txt
```

## How It Works

### Spark Session

The pipeline uses Spark for distributed processing, making it scalable for large datasets. The Spark session is initialized with memory and network timeout configurations for better performance:

```python
def get_spark_session():
    return SparkSession.builder         .appName("InfluencerCampaignData")         .config("spark.executor.memory", "4g")         .config("spark.driver.memory", "2g")         .config("spark.network.timeout", "600s")         .getOrCreate()
```

### Extracting Google Sheet Data

Google Sheets data is extracted using the `gspread` library. This function fetches all the records from the specified sheet.

```python
def extract_google_sheet_data():
    creds = Credentials.from_service_account_file(config.GOOGLE_SERVICE_ACCOUNT_KEY_PATH)
    client = gspread.authorize(creds)
    sheet = client.open("Influencer_Campaign_Data")
    worksheet = sheet.get_worksheet(0)
    data = worksheet.get_all_records()
    return data
```

### Scraping YouTube Data

Each video URL is passed to the YouTube API to retrieve the publish date and likes count. The `tenacity` library is used to retry failed requests with exponential backoff.

```python
@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=10))
def scrape_youtube_data(video_url):
    video_id = extract_video_id(video_url)
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={config.YOUTUBE_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        video_info = response.json()['items'][0]
        publish_date = video_info['snippet']['publishedAt']
        likes_count = video_info['statistics'].get('likeCount', 0)
        return publish_date, likes_count
    else:
        return None, None
```

### Snowflake Integration

The pipeline connects to Snowflake, creates the `INFLUENCER_CAMPAIGN_METRICS` table if it doesn’t exist, and inserts the enriched data.

```python
def insert_into_snowflake(conn, data):
    columns = ', '.join(data.columns)
    placeholders = ', '.join(['%s'] * len(data.columns))
    insert_query = f"INSERT INTO INFLUENCER_CAMPAIGN_METRICS ({columns}) VALUES ({placeholders})"
    cur = conn.cursor()
    rows_to_insert = [tuple(row) for row in data.collect()]
    cur.executemany(insert_query, rows_to_insert)
    conn.commit()
```

## Scalability Improvements

To scale the pipeline and make it more efficient, Spark workers can be used to distribute the workload of enriching the YouTube video data. Instead of running the enrichment on the driver node, each Spark worker can scrape YouTube data for its assigned partition. This can be done by broadcasting the YouTube API key and using distributed UDFs.

## Running the Pipeline

The `run()` function executes the entire pipeline, making it easy to trigger from Airflow. This function can be scheduled to run daily by an Airflow DAG.

### Example Airflow DAG:

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pipeline import run

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'influencer_campaign_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

run_task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run,
    dag=dag
)
```

## Environment Configuration

Store your environment-specific variables in a `.env` file or configuration file. These include:
- Google Service Account credentials
- YouTube API key
- Snowflake credentials

