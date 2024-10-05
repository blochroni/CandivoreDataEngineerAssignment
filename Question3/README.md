
# Influencer Campaign Data Pipeline

This project is designed to run a data pipeline that extracts influencer marketing data from a Google Sheet, enriches the data by fetching additional details from YouTube, and loads the enriched data into a Snowflake table. The pipeline is intended to be run daily through an Airflow DAG.

## Repository Structure

The code for the pipeline is located in the `Question3` folder within this repository.

```
CandivoreDataEngineerAssignment/
│
└─── Question3/
     │   
     └─── pipeline.py  # Main script containing the pipeline code
     └─── requirements.txt  # Required libraries
     └─── .env  # Environment file (not included, must be created)
```

## Prerequisites

Before running the pipeline, make sure that the following are set up on your system:

- **Python 3.7+**
- **Snowflake Account**: For data loading.
- **Google Cloud Service Account**: To access Google Sheets.
- **YouTube Data API v3**: For fetching video details.
- **Apache Spark**: For handling large datasets.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/blochroni/CandivoreDataEngineerAssignment.git
   cd CandivoreDataEngineerAssignment/Question3
   ```

2. **Create and Set Up Virtual Environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # For Linux/MacOS
   # or
   venv\Scriptsctivate  # For Windows
   ```

3. **Install Dependencies**:
   Install all required libraries by running:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up Environment Variables**:
   You'll need to create a `.env` file in the `Question3` directory to store the necessary credentials for Google Sheets, YouTube API, and Snowflake.

   Create a `.env` file with the following structure:

   ```bash
   SNOWFLAKE_USER=<your_snowflake_username>
   SNOWFLAKE_PASSWORD=<your_snowflake_password>
   SNOWFLAKE_ACCOUNT=<your_snowflake_account>
   SNOWFLAKE_WAREHOUSE=<your_snowflake_warehouse>
   SNOWFLAKE_DATABASE=<your_snowflake_database>
   SNOWFLAKE_SCHEMA=<your_snowflake_schema>
   
   GOOGLE_SERVICE_ACCOUNT_KEY_PATH=<path_to_google_service_account_json>
   GOOGLE_API_SCOPES=<your_google_scopes>
   GOOGLE_SHEET_ID=<your_google_sheet_id>
   
   YOUTUBE_API_KEY=<your_youtube_api_key>
   ```

   This `.env` file is essential for connecting to Snowflake, Google Sheets, and YouTube API, and it is loaded into the pipeline using the `python-dotenv` library.

5. **Configuration (`config.py`)**:
   Make sure the `config.py` file in the `Question3` folder pulls data from the `.env` file correctly. Here's an example of how it should look:

   ```python
   import os
   from dotenv import load_dotenv

   # Load environment variables from .env file
   load_dotenv()

   # Snowflake credentials
   SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
   SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
   SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
   SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
   SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
   SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

   # Google Sheets credentials
   GOOGLE_SERVICE_ACCOUNT_KEY_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_PATH")
   GOOGLE_API_SCOPES = os.getenv("GOOGLE_API_SCOPES")
   GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

   # YouTube API key
   YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
   ```

6. **Running the Pipeline**:
   After setting up the environment and installing dependencies, run the pipeline by executing the `pipeline.py` file:

   ```bash
   python pipeline.py
   ```

## Architecture

The architecture of this pipeline consists of the following components:

1. **Google Sheets Data Extraction**:
   - The `extract_google_sheet_data()` function connects to a Google Sheet and retrieves all records from the first worksheet.
   - Credentials for Google Sheets API access are stored in the `config.py` file and loaded from the `.env` file.

2. **YouTube API Integration**:
   - The pipeline enriches the Google Sheets data with YouTube video details (publish date and likes count) using the YouTube Data API v3.
   - Video IDs are extracted from YouTube URLs, and the API is queried for the corresponding video details.

3. **Data Transformation**:
   - The data is enriched with additional fields (`publish_date`, `current_likes_count`, and `interval_date`) to reflect YouTube data and the current date/time.

4. **Snowflake Table Creation & Insertion**:
   - A Snowflake table named `INFLUENCER_CAMPAIGN_METRICS` is dynamically created if it doesn’t already exist.
   - The enriched data is then inserted into the Snowflake table in batches for efficient processing.

5. **Batch Processing**:
   - The data is inserted into Snowflake in batches (configurable batch size, default is 1000 rows per batch) to optimize performance when handling larger datasets.

## Assumptions

1. **Google Sheet Structure**: It is assumed that the Google Sheet contains the following columns: `Influencer_Name`, `Video_Url`, `Campaign_Name`, etc. All columns from the sheet are included in the final table.
   
2. **API Keys**: It is assumed that valid API keys for Google Sheets and YouTube Data API are available and correctly configured in the `.env` file.

3. **Daily Execution**: This pipeline is designed to be executed daily by an Airflow DAG or another scheduler to keep the Snowflake table up to date.

4. **Data Types**: Columns such as `Cost`, `Likes_Count`, and `Publish_Date` are handled with appropriate data types (e.g., integers, timestamps, floats).

## Future Improvements

- **Error Logging**: Implement error logging for better debugging in case of failures.
- **Parallelization**: Explore parallelizing the data enrichment process for faster performance with larger datasets.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
