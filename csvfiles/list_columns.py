import os
import csv

def list_csv_columns(directory):
    columns_dict = {}
    
    # Iterate through files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            filepath = os.path.join(directory, filename)
            with open(filepath, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                # Read the header row
                header = next(reader, None)
                if header:
                    columns_dict[filename] = header
    
    return columns_dict

def save_columns_to_txt(columns_dict, output_file):
    with open(output_file, 'w', encoding='utf-8') as txtfile:
        for filename, columns in columns_dict.items():
            txtfile.write(f"{filename}:\n")
            for column in columns:
                txtfile.write(f"  - {column}\n")
            txtfile.write("\n")

def main():
    directory = '.'  # Specify your directory containing the CSV files
    output_file = 'columns_list.txt'
    
    columns_dict = list_csv_columns(directory)
    save_columns_to_txt(columns_dict, output_file)
    print(f"Column names have been saved to {output_file}")

if __name__ == "__main__":
    main()
