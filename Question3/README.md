
# Influencer Campaign Data Pipeline

## Overview
This project implements a data pipeline that extracts data from a Google Sheet, enriches it with additional information from YouTube, and inserts the enriched data into a Snowflake table. The pipeline is designed to run daily, integrating with Airflow to automatically refresh the data. The enriched data is used for marketing analytics purposes.

## Architecture
The data pipeline follows a simple Extract-Transform-Load (ETL) architecture:

1. **Extract**: 
   - Data is extracted from a Google Sheet using the `gspread` Python library, which accesses the sheet via Google API credentials.
   
2. **Transform**:
   - The video URLs from the Google Sheet are used to fetch additional metadata (like publish date and current likes) using the YouTube Data API.
   - The data is enriched by adding these details, along with a timestamp of when the pipeline ran (`Interval_Date`).
   - The `Cost` column is sanitized by removing dollar signs and casting it to a float type.
   
3. **Load**:
   - The enriched data is inserted into a Snowflake table. The table is dynamically created if it does not exist, and data is inserted in batches for scalability.

## Assumptions
- Google Sheets and Snowflake credentials are provided as environment variables or in a configuration file.
- The Google Sheet contains relevant columns like `Influencer_Name`, `Video_Url`, `Campaign_Name`, and others as defined by the marketing team.
- Snowflake credentials and YouTube API keys are stored securely in a configuration file and not hardcoded.
- API rate limits are handled by retry mechanisms to prevent failures in the event of temporary outages or limits.

## Setup Instructions

### 1. Prerequisites
- **Python 3.8+**
- **Pip** for installing dependencies
- A Google service account JSON file for accessing Google Sheets
- Snowflake account credentials
- YouTube API key

### 2. Installation

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/your-username/influencer-campaign-data-pipeline.git
    cd influencer-campaign-data-pipeline
    ```

2. **Install Required Libraries**:
    Ensure that you have Python 3.8+ installed. Then install the required libraries:
    ```bash
    pip install -r requirements.txt
    ```

3. **Set Up Google Sheets API**:
   - Create a Google service account and download the credentials JSON file.
   - Share the Google Sheet with the service account email address.
   - Ensure that the `GOOGLE_SERVICE_ACCOUNT_KEY_PATH` points to your service account JSON file.

4. **Configure Snowflake Credentials**:
   - Set up Snowflake credentials either via environment variables or by editing the `config.py` file to include:
     ```python
     SNOWFLAKE_USER = '<your_username>'
     SNOWFLAKE_PASSWORD = '<your_password>'
     SNOWFLAKE_ACCOUNT = '<your_account>'
     SNOWFLAKE_WAREHOUSE = '<your_warehouse>'
     SNOWFLAKE_DATABASE = '<your_database>'
     SNOWFLAKE_SCHEMA = '<your_schema>'
     ```

5. **Set Up YouTube API**:
   - Get a YouTube Data API key from the Google Developer Console.
   - Update the `config.py` file with your YouTube API key:
     ```python
     YOUTUBE_API_KEY = '<your_youtube_api_key>'
     ```

### 3. Running the Pipeline

To run the pipeline manually, use the following command:

```bash
python pipeline.py
```

This will:
- Extract the data from Google Sheets.
- Enrich it by fetching YouTube video details.
- Load it into the Snowflake table.

### 4. Scheduling with Airflow
To schedule the pipeline to run daily:
1. Set up Apache Airflow on your system or server.
2. Create a DAG that triggers the `run()` function in `pipeline.py` every day.

## Notes
- The pipeline is designed to handle large datasets by processing the data in batches when inserting it into Snowflake.
- Error handling and retries are implemented to ensure robustness in case of API failures or timeouts.
- Timestamp handling is included for both the `Publish_Date` from YouTube and the `Interval_Date` representing when the pipeline runs.
