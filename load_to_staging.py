import pandas as pd
from sqlalchemy import create_engine, text
import os

# File to save the database connection details
db_details_file = 'db_details.txt'

def get_db_details():
    # Check if the db_details_file exists
    if os.path.exists(db_details_file):
        choice = input("Choose an option:\n1. Enter new database details\n2. Use saved database details\nEnter 1 or 2: ")
    else:
        choice = '1'

    if choice == '1':
        db_username = input("Enter database username: ")
        db_password = input("Enter database password: ")
        db_host = input("Enter database host: ")
        db_name = input("Enter database name: ")
        csv_directory = input("Enter the directory containing CSV files: ")
        
        # Save the details to the file
        with open(db_details_file, 'w') as file:
            file.write(f"{db_username}\n{db_password}\n{db_host}\n{db_name}\n{csv_directory}")
    elif choice == '2':
        with open(db_details_file, 'r') as file:
            db_username, db_password, db_host, db_name, csv_directory = [line.strip() for line in file.readlines()]
    else:
        raise ValueError("Invalid choice. Please enter 1 or 2.")

    return db_username, db_password, db_host, db_name, csv_directory

def load_csv_to_db(csv_file, csv_directory, engine):
    # Construct the full path to the CSV file
    csv_path = os.path.join(csv_directory, csv_file)
    
    # Derive table name from the CSV file name
    table_name = os.path.splitext(csv_file)[0]
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_path)
    
    # Truncate the table before loading new data
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
    
    # Write the DataFrame to the SQL table
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Loaded data from {csv_file} into {table_name} table.")

def main():
    # Get database details from user input or file
    db_username, db_password, db_host, db_name, csv_directory = get_db_details()

    # Create a connection to the database
    engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_name}')

    # Iterate over the CSV files in the directory and load the data into the database
    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith('.csv'):
            load_csv_to_db(csv_file, csv_directory, engine)

    print("Data loading complete.")

if __name__ == "__main__":
    main()
