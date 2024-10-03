import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from snowflake_connector import query_snowflake
import snowflake.connector.errors


def general_statistics():
    """
    Displays the 'General Statistics' tab in the Streamlit app.

    This function allows users to filter events by selecting a date range.
    It queries the Snowflake database to retrieve events within the selected
    date range, displays detailed basic statistics, and visualizes metrics such as
    event counts over time and event type distributions.
    """
    st.title("General Statistics")

    # Date selection for filtering events
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")

    # Query to retrieve events within the specified date range
    query = f"""
    SELECT event_name, derived_tmstmp
    FROM atomic_table
    WHERE derived_tmstmp BETWEEN '{start_date}' AND '{end_date}'
    """

    try:
        df = query_snowflake(query)  # Execute the query and return results as a DataFrame

        if df.empty:
            st.warning("No data found for the selected date range.")
            return

        # Display basic statistics for the retrieved data
        st.subheader("Basic Statistics")
        st.write(df.describe())

        # Display event counts by event_name
        st.subheader("Event Counts by Type")
        event_counts_by_type = df['event_name'].value_counts()  # Count each event type
        st.bar_chart(event_counts_by_type)  # Display bar chart for event types

        # Visualization of event frequency over time
        st.subheader("Events Over Time")
        df['derived_tmstmp'] = pd.to_datetime(df['derived_tmstmp'])  # Ensure timestamp is in datetime format
        event_counts_over_time = df.groupby('derived_tmstmp').size()  # Group by timestamp

        # Plotting the event counts over time using matplotlib
        plt.figure(figsize=(10, 6))
        plt.plot(event_counts_over_time.index, event_counts_over_time.values)
        plt.title("Number of Events Over Time")
        plt.xlabel("Time")
        plt.ylabel("Event Count")
        st.pyplot(plt)  # Display the plot in Streamlit

    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Error retrieving data: {e}")
        st.stop()  # Stop further execution if an error occurs



def search_user_events():
    """
    Displays the 'User Events' tab in the Streamlit app.

    This function allows users to search for in-app purchase events related to a specific user by inputting a user ID.
    It queries the Snowflake database to retrieve the user's transactions, displays the transaction details,
    the sum of purchases, and shows the player's name (if available).

    Assumptions:
    - `CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1` contains a JSON field from which the `uuid` (user_id) is extracted.
    - `UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1` contains structured data (JSON) that includes purchase details such as the amount.
    """
    st.title("User Transactions")

    # Input field for the user to enter a User ID
    user_id = st.text_input("Enter User ID")

    if user_id:
        try:
            # Query to retrieve purchase-related events and purchase data for the specified user ID
            query = f"""
            SELECT event_name, derived_tmstmp, UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1 as purchase_details
            FROM atomic_table
            WHERE CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1:uuid = '{user_id}'
            AND event_name = 'in_app_purchase'
            """
            df = query_snowflake(query)  # Execute the query and return results as a DataFrame

            if df.empty:
                st.warning("No purchase transactions found for this user.")
            else:
                # Display the purchase transactions for the specified user ID
                st.subheader(f"Purchase Transactions for User ID: {user_id}")

                # Assuming the purchase_details field contains JSON with purchase amount
                purchase_details_df = df['purchase_details'].apply(pd.Series)  # Flatten JSON
                total_purchases = purchase_details_df['amount'].sum()  # Calculate total purchases

                # Display transaction details (event_name, timestamp, and purchase details)
                st.write(df[['event_name', 'derived_tmstmp', 'purchase_details']])

                # Display the total sum of purchases made by the user
                st.subheader(f"Total Purchases: {total_purchases}")

                # Query to fetch the player's name (if available)
                player_name_query = f"""
                SELECT DISTINCT CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1:player_name as player_name
                FROM atomic_table
                WHERE CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1:uuid = '{user_id}'
                """
                player_name_df = query_snowflake(player_name_query)
                if not player_name_df.empty:
                    player_name = player_name_df['player_name'].iloc[0]
                    st.subheader(f"Player Name: {player_name}")
                else:
                    st.warning(f"Player name not found for User ID: {user_id}")

        except snowflake.connector.errors.DatabaseError as e:
            st.error(f"Error retrieving data for User ID: {user_id}. Error: {e}")
            st.stop()  # Stop further execution if an error occurs




def main():
    """
    Main function to run the Streamlit app.

    This function sets up the sidebar navigation for the app, allowing the user to
    switch between the 'General Statistics' and 'User Events' tabs.
    """
    st.sidebar.title("Match Masters Data")

    # Sidebar options for selecting the active tab
    options = ["General Statistics", "Events"]
    choice = st.sidebar.selectbox("Choose Tab", options)

    # Display the corresponding tab based on the user's choice
    if choice == "General Statistics":
        general_statistics()
    elif choice == "Events":
        search_user_events()


if __name__ == '__main__':
    main()  # Run the Streamlit app when the script is executed
