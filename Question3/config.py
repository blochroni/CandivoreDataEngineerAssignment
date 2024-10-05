import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Google API configuration
GOOGLE_SHEET_ID = "1UqwIH4xfDOXip3_zGBK5HmARykgFYkwrhQ2nYMaTtoA"
GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
GOOGLE_SERVICE_ACCOUNT_KEY_PATH = os.getenv('GOOGLE_SERVICE_ACCOUNT_KEY_PATH')

# Snowflake credentials (assuming they will be provided later)
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# YouTube API configuration
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
