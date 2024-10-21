import os
import shutil
import sqlite3
import threading
import time
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)
CORS(app)

#limit the number of concurrent threads
executor = ThreadPoolExecutor(max_workers=5)

@app.route('/process', methods=['GET'])
def test_api():
    return jsonify({"message": "Test success!"}), 200

@app.route('/process', methods=['POST'])
def process_file_api():
    try:
        file = request.files['file']

        # Check if the uploaded file is a CSV
        if not file.filename.endswith('.csv'):
            error_path = f"error/{file.filename}"
            file.save(error_path)
            return jsonify({
                "data": [],
                "done": [],
                "error": [file.filename],
                "message": "Only CSV files are allowed!",
                "folder": "error"
            }), 400

        file_path = f"data/{file.filename}"
        file.save(file_path)

        process_file_async(file_path)

        return jsonify({"message": "File processing started."}), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return jsonify({"message": "Welcome to the CSV File Processor!"}), 200

@app.route('/folder-contents', methods=['GET'])
def get_folder_contents():
    try:
        data_files = os.listdir('data')
        done_files = os.listdir('done')
        error_files = os.listdir('error')

        print(f"Folder contents - Data: {data_files}, Done: {done_files}, Error: {error_files}")

        return jsonify({
            "data": data_files,
            "done": done_files,
            "error": error_files
        }), 200
    except Exception as e:
        print(f"Error fetching folder contents: {e}")
        return jsonify({"error": str(e)}), 500


class Watcher:
    DIRECTORY_TO_WATCH = "data"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

# creation of sqlite db
def create_table():
    conn = sqlite3.connect('file_processor.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS csv_data (
                        column1 TEXT,
                        column2 TEXT,
                        column3 TEXT,
                        file_type TEXT
                    )''')
    conn.commit()
    conn.close()

def validate_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        if not df.empty and len(df.columns) > 0:
            return True, df
        else:
            print(f"CSV file {file_path} is empty or has no valid columns")
            return False, None
    except Exception as e:
        print(f"Error reading file: {e}")
        return False, None

# Insert data into SQLite database
def insert_into_db(df):
    retries = 5
    for attempt in range(retries):
        try:
            conn = sqlite3.connect('file_processor.db')
            cursor = conn.cursor()

            columns = df.columns
            placeholders = ', '.join(['?'] * len(columns))
            columns_definition = ', '.join([f'{col} TEXT' for col in columns])

            cursor.execute("DROP TABLE IF EXISTS csv_data")
            create_table_query = f"CREATE TABLE csv_data ({columns_definition})"
            cursor.execute(create_table_query)

            insert_query = f"INSERT INTO csv_data ({', '.join(columns)}) VALUES ({placeholders})"
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))

            conn.commit()
            conn.close()
            break
        except sqlite3.OperationalError as e:
            print(f"Database is locked, retrying... (Attempt {attempt + 1})")
            time.sleep(1)

def process_file(file_path):
    print(f"Processing file: {file_path}") 
    retries = 3
    for attempt in range(retries):
        try:
            time.sleep(5)

            # Validate CSV file
            is_valid, df = validate_csv(file_path)
            if not is_valid:
                print(f"Invalid CSV: {file_path}. Moving to error folder.")
                shutil.move(file_path, 'error')
                return {"data": [], "done": [], "error": [os.path.basename(file_path)], "folder": "error"}

            # Insert data into the database
            print(f"Inserting data from {file_path} into the database.")
            insert_into_db(df)

            # Move to 'done' folder if successful
            shutil.move(file_path, 'done')
            print(f"File {os.path.basename(file_path)} processed successfully and moved to 'done'!")
            return {"data": [], "done": [os.path.basename(file_path)], "error": [], "folder": "done"}

        except PermissionError as e:
            print(f"PermissionError on attempt {attempt + 1}: {e}")
            time.sleep(10)

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            if os.path.exists(file_path):
                shutil.move(file_path, 'error')
                print(f"File {file_path} moved to error folder due to an error.")
            return {"data": [], "done": [], "error": [os.path.basename(file_path)], "folder": "error"}

# Handle file detection
class Handler(FileSystemEventHandler):
    def process(self, event):
        if event.is_directory:
            return None

        if event.event_type == 'created':
            print(f"New file detected: {event.src_path}")

            time.sleep(5)
            process_file_async(event.src_path)

    def on_created(self, event):
        self.process(event)

def process_file_async(file_path):
    executor.submit(process_file, file_path)

def run_flask_app():
    app.run(host='0.0.0.0', port=8000, debug=False, threaded=True)

if __name__ == '__main__':
    create_table()

    for directory in ['data', 'done', 'error']:
        if not os.path.exists(directory):
            os.makedirs(directory)

    w = Watcher()
    threading.Thread(target=w.run, daemon=True).start()
    run_flask_app()
