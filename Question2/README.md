
# Streamlit Snowflake Project for Match Masters Data

## Project Overview

This project implements a Streamlit web application to process data from a Snowflake database. It contains two main features:
1. **General Statistics Tab**: Displays basic statistics and visualizations for player events over selected dates.
2. **Events Tab**: Allows searching for user events by `user_id`, displaying transaction data, detailed in-app purchase information, total purchases, and player information.

## Assumptions

1. The `CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1` column is a JSON object that contains player information, and the `uuid` field within this JSON represents the user_id.
2. The `UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1` column is a JSON object that contains detailed purchase information, including fields such as `amount`, and possibly other transaction details.
3. Queries are constructed to extract data from these structured fields using the JSON extraction operator (`:`).
4. Player names are also assumed to be part of the `CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1` JSON object, accessed via the `player_name` key.
5. **In-App Purchase events** are identified by the `event_name` field being equal to `'in_app_purchase'`. Other types of events (such as logins or gameplay) are displayed separately.

## Setup Instructions

### 1. Clone the repository

Clone the project repository using the following command:

```bash
git clone https://github.com/your-repo-url.git
```

### 2. Install dependencies

Navigate into the project directory and install the required Python packages using `pip`:

```bash
cd your-repo-folder
pip install -r requirements.txt
```

This will install the required libraries, including:
- `streamlit`: To create the web application.
- `snowflake-connector-python`: To connect and interact with Snowflake.
- `pandas`: To handle and manipulate data.
- `matplotlib`: To create visualizations.

### 3. Set up Snowflake connection

Before running the application, make sure to configure the Snowflake connection details in the `snowflake_connector.py` file. Replace the placeholder values with the correct credentials:

```python
conn = snowflake.connector.connect(
    user='YOUR_SNOWFLAKE_USER',
    password='YOUR_SNOWFLAKE_PASSWORD',
    account='YOUR_SNOWFLAKE_ACCOUNT',
    warehouse='YOUR_WAREHOUSE',
    database='YOUR_DATABASE',
    schema='YOUR_SCHEMA'
)
```

### 4. Run the Streamlit app

Once everything is configured, run the Streamlit app locally using the following command:

```bash
streamlit run streamlit_app/app.py
```

This will open the app in your web browser. You should now be able to navigate between the "General Statistics" and "User Events" tabs.

### 5. Architecture Diagram

The architecture for this project is as follows:
- **Streamlit Frontend**: Displays data to the user and allows interactions (e.g., selecting dates, searching user events).
- **Snowflake Backend**: A Snowflake table contains structured and unstructured data about player activity, which is queried via Snowflake’s Python connector.
- **Python Backend**: Handles querying data from Snowflake and feeding it to Streamlit for processing and visualization.

### Detailed Event Handling in the "Events" Tab

- **All Events**: Events related to the specified user are displayed in a table. Events not related to purchases (e.g., logins, gameplay) are displayed with their name and timestamp.
- **In-App Purchases**: For events where `event_name` is `'in_app_purchase'`, the details from `UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1` are displayed, including specific purchase data such as amount.
- **Total Purchases**: The total amount spent by the user on in-app purchases is displayed.
- **Player Information**: The player's name is extracted from `CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1` if available.

## Additional Information

- The application assumes the Snowflake table follows a specific structure, as outlined in the assumptions section above.
- Make sure the Snowflake credentials are securely stored and not hardcoded in production environments.

