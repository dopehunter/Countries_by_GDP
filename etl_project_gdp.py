import requests
import pandas as pd
import sqlite3
import json
import logging

# Set up logging
logging.basicConfig(filename='etl_project_log.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

def extract(url, table_attribs):
    ''' Extracts the required information from the website and saves it to a dataframe. '''
    log_progress("Starting data extraction.")
    response = requests.get(url)
    if response.status_code == 200:
        data = pd.read_html(response.text, attrs=table_attribs)[0]
        log_progress("Data extraction successful.")
        return data
    else:
        log_progress("Data extraction failed.")
        raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")

def transform(df):
    ''' Transforms GDP data to billions and rounds it to 2 decimal places. '''
    log_progress("Starting data transformation.")
    df['GDP_USD_billion'] = df['GDP (Millions)'].str.replace(',', '').astype(float) / 1000
    df = df.round({'GDP_USD_billion': 2})
    df = df[['Country', 'GDP_USD_billion']]
    log_progress("Data transformation successful.")
    return df

def load_to_csv(df, csv_path):
    ''' Saves the dataframe as a CSV file. '''
    log_progress(f"Saving data to CSV at {csv_path}.")
    df.to_csv(csv_path, index=False)
    log_progress("Data saved to CSV successfully.")

def load_to_db(df, sql_connection, table_name):
    ''' Saves the dataframe to a database table. '''
    log_progress(f"Saving data to database table {table_name}.")
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data saved to database successfully.")

def run_query(query_statement, sql_connection):
    ''' Executes a query on the database and prints the result. '''
    log_progress("Running query on the database.")
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    result = cursor.fetchall()
    print("Query Result:", result)
    log_progress("Query executed successfully.")

def log_progress(message):
    ''' Logs a message to the log file. '''
    logging.info(message)

# ETL pipeline execution
if __name__ == '__main__':
    # Configuration
    url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"  # Replace with actual URL
    table_attribs = {"class": "data-table"}
    csv_path = "Countries_by_GDP.csv"
    db_path = "World_Economies.db"
    table_name = "Countries_by_GDP"
    
    try:
        # Extract
        raw_data = extract(url, table_attribs)

        # Transform
        processed_data = transform(raw_data)

        # Load to CSV
        load_to_csv(processed_data, csv_path)

        # Load to Database
        conn = sqlite3.connect(db_path)
        load_to_db(processed_data, conn, table_name)

        # Run Query
        query = f"SELECT * FROM {table_name} WHERE GDP_USD_billion > 100"
        run_query(query, conn)

        # Close database connection
        conn.close()

    except Exception as e:
        log_progress(f"Error occurred: {e}")
        print(f"Error: {e}")