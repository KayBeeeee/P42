import requests
import json
import pandas as pd
import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor
import time

#DB credentials:
host = "localhost"
user = "root"
password = "123456"
database = "etl_db"
report_file = "etl_report.txt"

# Function to log to the report and print to the console
def log_to_report(message):
    with open(report_file, "a") as file:
        file.write(f"{message}\n")
    print(message)  # Ensure it prints to console

# Function to fetch data
def fetch_data(start_date, end_date, api_key):
    log_to_report("Starting data fetch...")
    url = "https://8b1gektg00.execute-api.us-east-1.amazonaws.com/default/engineer-test"
    headers = {'x-api-key': 'GSJ3bdgMaoa8bHOXGs5YQHRYxmHMSx96WUmbVAjj'} #API-key received from Thabang via Whatsapp
    payload = {"start_date": start_date, "end_date": end_date}

    try:
        start_time = time.time()
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            elapsed_time = time.time() - start_time
            log_to_report(f"Data successfully fetched in {elapsed_time:.2f} seconds!")
            log_to_report("Raw data preview:")
            log_to_report(json.dumps(response.json()[:5], indent=2))  # log first 5 records of raw data
            return response.json()
        else:
            log_to_report(f"Error: {response.status_code} - {response.text}")
            return None
    except requests.RequestException as e:
        log_to_report(f"An error occurred during the API call: {e}")
        return None

# Function to process the retrieved data
def process_data(data):
    log_to_report("Starting data processing...")
    if not data:
        log_to_report("No data to process.")
        return None

    df = pd.DataFrame(data)
    df = df.dropna(subset=['transaction_date', 'transaction_amount'])
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df = df.dropna(subset=['transaction_date']).drop_duplicates()
    df = df[df['transaction_amount'] >= 0]
    df['spend_category'] = df['spend_category'].fillna('Unknown')
    df['transaction_date'] = df['transaction_date'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Categorize transaction amounts and product categories
    df['transaction_category'] = df['transaction_amount'].apply(lambda x: 'low' if x < 50 else 'medium' if x <= 200 else 'high')
    df['product_category'] = df['spend_category'].apply(
        lambda x: "Tech" if x == "Electronics" else "Apparel" if x == "Clothing" else "Home Goods" if x == "Home" else "Other"
    )

    # Total transaction value per customer
    customer_totals = df.groupby('customer_id')['transaction_amount'].sum().reset_index()
    customer_totals.rename(columns={'transaction_amount': 'total_transaction_value'}, inplace=True)
    df = df.merge(customer_totals, on='customer_id', how='left')

    # Log discrepancies for negative transaction amounts and missing categories
    log_discrepancies(df)

    log_to_report("Data processing completed.")
    log_to_report("Transformed data preview (last 5 rows):")
    log_to_report(df.tail().to_string(index=False))
    return df

# Function to log discrepancies in data
def log_discrepancies(df):
    log_to_report("Logging discrepancies...")
    negative_transactions = df[df['transaction_amount'] < 0]
    if not negative_transactions.empty:
        negative_transactions.to_csv("discrepancies_log.csv", index=False)
        log_to_report("Negative transactions logged in discrepancies_log.csv.")

    missing_categories = df[df['spend_category'] == 'Unknown']
    if not missing_categories.empty:
        missing_categories.to_csv("missing_categories_log.csv", index=False)
        log_to_report("Missing categories logged in missing_categories_log.csv.")

# Function to insert data in parallel
def insert_data_chunk(df_chunk):
    log_to_report(f"Inserting data chunk with {len(df_chunk)} rows...")
    try:
        connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
        cursor = connection.cursor()

        for _, row in df_chunk.iterrows():
            cursor.execute(
                """INSERT INTO transactions (
                    customer_id, transaction_date, transaction_amount,
                    spend_category, transaction_category, product_category,
                    total_transaction_value) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (row['customer_id'], row['transaction_date'], row['transaction_amount'],
                 row['spend_category'], row['transaction_category'], row['product_category'],
                 row['total_transaction_value'])
            )

        connection.commit()
        log_to_report(f"Inserted chunk with {len(df_chunk)} rows.")
    except Error as e:
        log_to_report(f"Error during insertion: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to load data into MySQL with parallel processing
def load_data_to_mysql(df):
    log_to_report("Loading data into MySQL...")
    try:
        chunk_size = 500
        chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

        with ThreadPoolExecutor() as executor:
            for chunk in chunks:
                executor.submit(insert_data_chunk, chunk)

        log_to_report("Data successfully loaded into MySQL database.")
    except Exception as e:
        log_to_report(f"Error during data loading: {e}")

# SQL Queries for Insights
def query_insights():
    log_to_report("Querying insights...")
    try:
        connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
        if connection.is_connected():
            log_to_report("Connected to MySQL database.")
        cursor = connection.cursor()

        # Total transactions per product category
        cursor.execute("SELECT product_category, COUNT(*) AS total_transactions FROM transactions GROUP BY product_category;")
        for row in cursor.fetchall():
            log_to_report(f"Product Category: {row[0]}, Total Transactions: {row[1]}")

        # Top 5 accounts by total transaction value
        cursor.execute("SELECT customer_id, SUM(transaction_amount) FROM transactions GROUP BY customer_id ORDER BY SUM(transaction_amount) DESC LIMIT 5;")
        for row in cursor.fetchall():
            log_to_report(f"Customer ID: {row[0]}, Total Value: {row[1]}")

        # Monthly spend trends over the past year
        cursor.execute(""" 
            SELECT DATE_FORMAT(transaction_date, '%Y-%m') AS month, SUM(transaction_amount) 
            FROM transactions WHERE transaction_date >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
            GROUP BY month ORDER BY month;
        """)
        for row in cursor.fetchall():
            log_to_report(f"Month: {row[0]}, Total Spend: {row[1]}")

    except Error as e:
        log_to_report(f"Error during querying: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main execution block
if __name__ == "__main__":
    with open(report_file, "w") as file:
        file.write("ETL Process Report\n" + "="*50 + "\n")

    api_key = 'GSJ3bdgMaoa8bHOXGs5YQHRYxmHMSx96WUmbVAjj'
    start_date = "2023-01-01"
    end_date = "2023-01-31"

    data = fetch_data(start_date, end_date, api_key)
    if data:
        transformed_data = process_data(data)
        if transformed_data is not None:
            load_data_to_mysql(transformed_data)
            query_insights()
