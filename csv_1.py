import os
import pandas as pd

def xlsx_to_csv_in_same_directory():
    """
    Converts all .xlsx files in the same directory as the script to .csv files
    and saves them in a folder named 'csvfiles'.
    """
    # Get the current directory (where the script is located)
    directory = os.path.dirname(os.path.abspath(__file__))
    
    # Create 'csvfiles' folder if it doesn't exist
    csv_folder = os.path.join(directory, 'csvfiles')
    os.makedirs(csv_folder, exist_ok=True)
    
    # Get list of all files in the current directory
    files = os.listdir(directory)
    
    # Filter out only .xlsx files
    xlsx_files = [file for file in files if file.endswith('.xlsx')]
    
    for xlsx_file in xlsx_files:
        # Full path to the xlsx file
        xlsx_path = os.path.join(directory, xlsx_file)
        
        # Read the Excel file
        df = pd.read_excel(xlsx_path)
        
        # Construct the output CSV file path in the 'csvfiles' folder
        csv_file = xlsx_file.replace('.xlsx', '.csv')
        csv_path = os.path.join(csv_folder, csv_file)
        
        # Write the dataframe to CSV
        df.to_csv(csv_path, index=False)
        print(f"Converted {xlsx_file} to {csv_file}")

if __name__ == "__main__":
    xlsx_to_csv_in_same_directory()
