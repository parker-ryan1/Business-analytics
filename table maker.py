import os
import pandas as pd
import pyodbc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class CSVHandler(FileSystemEventHandler):
    def __init__(self, db_conn_str):
        self.db_conn_str = db_conn_str

    def on_created(self, event):
        if event.is_directory:
            return

        file_path, file_extension = os.path.splitext(event.src_path)
        if file_extension.lower() == '.csv':
            self.process_csv(event.src_path)

    def process_csv(self, file_path):
        # Read CSV file
        df = pd.read_csv(file_path)

        # Ensure the CSV has the required columns
        required_columns = ['Index', 'Country', 'Company']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"CSV file {file_path} is missing required columns: {missing_columns}")
            return

        # Create a DataFrame with only the required columns
        df = df[required_columns]

        table_name = os.path.splitext(os.path.basename(file_path))[0]
        table_name = table_name.replace('-', '_').replace(' ', '_')  # Sanitize table name

        # Connect to SQL Server database
        conn = pyodbc.connect(self.db_conn_str)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
        BEGIN
            CREATE TABLE {table_name} (
                [Index] INT,
                [Country] NVARCHAR(100),
                [Company] NVARCHAR(100)
            );
        END
        """)

        # Insert data into SQL table
        for _, row in df.iterrows():
            cursor.execute(
                f"INSERT INTO {table_name} ([Index], [Country], [Company]) VALUES (?, ?, ?)",
                row['Index'], row['Country'], row['Company']
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Table '{table_name}' created successfully with data from '{file_path}'")

def process_existing_files(folder_to_watch, db_conn_str):
    for filename in os.listdir(folder_to_watch):
        file_path = os.path.join(folder_to_watch, filename)
        if os.path.isfile(file_path) and filename.lower().endswith('.csv'):
            CSVHandler(db_conn_str).process_csv(file_path)

if __name__ == "__main__":
    folder_to_watch = r"C:\Users\ainsl\Downloads\clean"
    db_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=DESKTOP-I3BERSO;"
        "DATABASE=customers;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )

    # Process existing files in the folder
    process_existing_files(folder_to_watch, db_conn_str)

    event_handler = CSVHandler(db_conn_str)
    observer = Observer()
    observer.schedule(event_handler, path=folder_to_watch, recursive=False)
    
    observer.start()
    print(f"Watching folder: {folder_to_watch}")

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
