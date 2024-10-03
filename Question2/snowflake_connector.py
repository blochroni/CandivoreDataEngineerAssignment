import snowflake.connector
import pandas as pd


def connect_to_snowflake():
    """
    Establishes a connection to the Snowflake database using provided credentials.

    Returns:
        conn (snowflake.connector.connection.SnowflakeConnection):
            A Snowflake connection object that can be used to execute queries.

    Raises:
        snowflake.connector.errors.DatabaseError: If the connection fails.
    """
    conn = None  # Initialize conn as None to avoid referencing before assignment
    try:
        conn = snowflake.connector.connect(
            user='roni_bloch',
            password='Candivore123!',
            account='MY_ACCOUNT',  # Replace with your account info
            warehouse='MY_WAREHOUSE',  # Replace with your warehouse info
            database='MY_DATABASE',  # Replace with your database info
            schema='MY_SCHEMA'  # Replace with your schema info
        )
        return conn
    except snowflake.connector.errors.DatabaseError as e:
        print(f"Error connecting to Snowflake: {e}")
        raise  # Re-raise the exception so it can be handled elsewhere
    finally:
        if conn is not None:
            conn.close()  # Close the connection if it was established


def query_snowflake(query):
    """
    Executes a SQL query on the connected Snowflake database and returns the results as a Pandas DataFrame.

    Args:
        query (str): SQL query to be executed.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the query results.

    Raises:
        snowflake.connector.errors.DatabaseError: If the query execution fails.
    """
    conn = None  # Initialize conn as None to avoid referencing before assignment
    try:
        conn = connect_to_snowflake()  # Establish connection to Snowflake
        cur = conn.cursor()  # Create a cursor object

        # Execute the query
        try:
            cur.execute(query)
            data = cur.fetchall()  # Fetch all results from the query
            columns = [desc[0] for desc in cur.description]  # Extract column names from the cursor description
            return pd.DataFrame(data, columns=columns)  # Return the result as a Pandas DataFrame
        except snowflake.connector.errors.DatabaseError as e:
            print(f"Error executing query: {e}")
            raise
        finally:
            cur.close()  # Ensure the cursor is closed even if an error occurs

    except snowflake.connector.errors.DatabaseError as e:
        print(f"Error during database operation: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()  # Ensure the connection is closed even if an error occurs
