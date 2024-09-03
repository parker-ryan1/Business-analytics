import os
import pandas as pd
import shutil

# Define folders (replace with your actual paths)
folder_raw_csv = 'Downloads/raw'  # Modify this to the path of your raw CSV folder on Windows
folder_clean_csv = 'Downloads/clean'  # Modify this to the path of your clean CSV folder on Windows

# Ensure the raw and clean CSV folders exist
os.makedirs(folder_raw_csv, exist_ok=True)
os.makedirs(folder_clean_csv, exist_ok=True)

# Function to clean data
def clean_data(csv_file):
    print(f'Starting to clean {csv_file}')
    # Load CSV file
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f'Error reading {csv_file}: {e}')
        return

    # Drop rows with any non-numeric values or empty cells
    df = df.apply(lambda x: pd.to_numeric(x, errors='coerce')).dropna()

    # Normalize all numerical values to range [0, 1]
    df = (df - df.min()) / (df.max() - df.min())

    # Clean up column names
    df.columns = df.columns.str.strip().str.capitalize().str.replace(' ', '_')

    # Save cleaned data to clean_csv folder
    cleaned_filename = os.path.splitext(os.path.basename(csv_file))[0] + '_cleaned.csv'
    cleaned_filepath = os.path.join(folder_clean_csv, cleaned_filename)
    try:
        df.to_csv(cleaned_filepath, index=False)
        print(f'Data cleaned and saved to: {cleaned_filepath}')
    except Exception as e:
        print(f'Error saving {cleaned_filepath}: {e}')

# Function to clean all CSV files in the raw folder
def clean_all_files_in_raw_folder():
    try:
        for filename in os.listdir(folder_raw_csv):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_raw_csv, filename)
                clean_data(file_path)
                try:
                    os.remove(file_path)
                    print(f'Original file {filename} deleted from raw folder.')
                except Exception as e:
                    print(f'Error deleting {file_path}: {e}')
    except Exception as e:
        print(f'Error processing files in {folder_raw_csv}: {e}')

if __name__ == "__main__":
    # Clean all existing CSV files in the raw folder
    clean_all_files_in_raw_folder()
