import os
import pandas as pd
import io

def parse_csv_file(csv_path):
    """
    Parses a CSV file that may contain multiple tables separated by a special delimiter.
    
    Parameters:
        csv_path (str): Path to the CSV file.
        
    Returns:
        List of DataFrames: Each DataFrame represents a table in the CSV file.
    """
    tables = []
    
    with open(csv_path, 'r') as file:
        lines = file.readlines()
        
    # Detect where tables start
    current_table_lines = []
    for line in lines:
        if line.strip() == "---":  # Assume '---' separates tables
            if current_table_lines:
                # Convert accumulated lines to DataFrame
                table_df = pd.read_csv(io.StringIO(''.join(current_table_lines)))
                tables.append(table_df)
                current_table_lines = []
        else:
            current_table_lines.append(line)
    
    # Add the last table if there are remaining lines
    if current_table_lines:
        table_df = pd.read_csv(io.StringIO(''.join(current_table_lines)))
        tables.append(table_df)
    
    return tables

def list_csv_tables_and_columns():
    """
    Lists all tables (CSV files) in the 'csvfiles' directory and their column names in a structured way.
    """
    # Get the current directory (where the script is located)
    directory = os.path.dirname(os.path.abspath(__file__))
    
    # Define the 'csvfiles' folder
    csv_folder = os.path.join(directory, 'csvfiles')
    
    # Check if the 'csvfiles' folder exists
    if not os.path.exists(csv_folder):
        print(f"The folder '{csv_folder}' does not exist.")
        return
    
    # Get list of all files in the 'csvfiles' folder
    files = os.listdir(csv_folder)
    
    # Filter out only .csv files
    csv_files = [file for file in files if file.endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in '{csv_folder}'.")
        return
    
    # Open the output file
    output_file_path = os.path.join(directory, 'csv_tables_and_columns.txt')
    with open(output_file_path, 'w') as output_file:
        # Iterate through each CSV file and list its columns
        for csv_file in csv_files:
            # Full path to the csv file
            csv_path = os.path.join(csv_folder, csv_file)
            
            try:
                # Parse the CSV file to handle multiple tables
                tables = parse_csv_file(csv_path)
                
                # Collect and write information about each table
                for index, df in enumerate(tables):
                    table_info = f"Table {index + 1} in {csv_file}\nColumns:\n"
                    for column in df.columns:
                        table_info += f"  - {column}\n"
                    table_info += "\n"
                    
                    # Print to console
                    print(table_info)
                    
                    # Write to file
                    output_file.write(table_info)
                
            except Exception as e:
                print(f"Error processing file {csv_file}: {e}")
    
    print(f"Details saved to {output_file_path}")

if __name__ == "__main__":
    list_csv_tables_and_columns()
