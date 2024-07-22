import mysql.connector
import os
import envvariables
import schedule
import time

# Establish a database connection
conn = mysql.connector.connect(
    host=envvariables.DB_HOST,
    user=envvariables.DB_USER,
    password=envvariables.DB_PASSWORD,
    database=envvariables.DB_NAME,
    auth_plugin=envvariables.DB_AUTH
)

cursor = conn.cursor()

def get_column_names(table_name):
    """Retrieve column names from a specified table."""
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = [row[0] for row in cursor.fetchall()]
    return columns

def load_csv_to_table(table_name, csv_file_path):
    """Load data from CSV file into the specified table."""
    # Retrieve column names
    columns = get_column_names(table_name)
    columns_str = ', '.join(columns)

    # Generate the LOAD DATA INFILE statement
    load_query = f"""
    LOAD DATA INFILE '{csv_file_path}'
    INTO TABLE {table_name}
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    ({columns_str});
    """

    # Execute the query
    cursor.execute(load_query)
    conn.commit()

def load_data():
    """Load CSV data into the database."""
    csv_directory = 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'
    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith('.csv'):
            table_name = os.path.splitext(csv_file)[0]  # Use filename (without .csv) as table name
            csv_file_path = os.path.join(csv_directory, csv_file)
            load_csv_to_table(table_name, csv_file_path)
            print(f"Loaded {csv_file} into {table_name}.")

# Schedule the data loading at 1 am every day
schedule.every().day.at("01:00").do(load_data)

# Run the scheduler in an infinite loop
while True:
    schedule.run_pending()
    time.sleep(1)
