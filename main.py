import os
import re
import shutil
from datetime import datetime
import pytz
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from google.colab import drive
from google.api_core.exceptions import NotFound

# mount drive
def connect_to_drive():
    drive.mount('/content/drive')
    print("Google Drive connected.")

class Config:
    SERVICE_ACCOUNT_INFO = {
      #GCP service account with bigquery editor access
    }
    WORKING_DIRECTORY = "/content/drive/Shareddrives/WD"
    PROJECT_ID = "project_id"
    DATASET_ID = "project_dataset"
    table_a = "project_table_a"
    table_b = "project_table_b"
    FOLDER_PATH = "/content/drive/Shareddrives/PATH"
    SKIPROWS = 0  # Number of rows to skip when reading CSV/Google Sheets
    DATE_TODAY = datetime.now(pytz.timezone('US/Eastern')).date() # customize for your time zone

class BigQueryClass:
    def __init__(self):
        credentials = Credentials.from_service_account_info(Config.SERVICE_ACCOUNT_INFO)
        self.client = bigquery.Client(credentials=credentials, project=Config.PROJECT_ID)

    def format_column_name(self, name):
        if not isinstance(name, str):
            name = str(name)  # Ensure the column name is a string
        name = re.sub(r'[^a-zA-Z0-9 ]', '', name)  # Remove non-alphanumeric except spaces
        name = name.replace(' ', '_')  # Replace spaces with underscores
        name = name.lower()
        return name

    def retrieve_last_call_timestamp(self, table_id=None):
        project_id = Config.PROJECT_ID
        dataset_id = Config.DATASET_ID
        table_id = table_id or Config.table_a
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        client = self.client

        query = f"""
            SELECT FORMAT_DATETIME('%F %T', MAX(call_date)) AS last_call_timestamp
            FROM `{table_ref}`
        """

        try:
            query_job = client.query(query)
            result = query_job.result()
            last_call_timestamp = None
            for row in result:
                last_call_timestamp = row['last_call_timestamp']

            # If table is empty and no timestamp found, fallback to very old date
            if last_call_timestamp is None:
                last_call_timestamp = "1970-01-01 00:00:00"

            return last_call_timestamp

        except NotFound:
            print(f"Table {table_ref} not found. Assuming no previous data.")
            # Return a default very old timestamp so all data will be considered new
            return "1970-01-01 00:00:00"
        except Exception as e:
            print(f"Error retrieving last call timestamp from {table_ref}: {e}")
            # Optionally re-raise or return a default value
            return "1970-01-01 00:00:00"

    def upload_to_bigquery(self, dataframe, dataset_id=None, table_id=None):
        dataset_id = dataset_id or Config.DATASET_ID
        table_id = table_id or Config.table_a
        table_ref = f"{Config.PROJECT_ID}.{dataset_id}.{table_id}"

        # Format column names
        dataframe.columns = [self.format_column_name(col) for col in dataframe.columns]
        print("Formatted Column Names:", dataframe.columns.tolist())

        # Define schema explicitly
        schema = [
            bigquery.SchemaField("date_uploaded", "DATE"),
            bigquery.SchemaField("call_date", "TIMESTAMP"),
            bigquery.SchemaField("lead_creation", "TIMESTAMP"),
            bigquery.SchemaField("call_duration", "INT64"),
            # Add more schema fields if needed
        ]

        # Convert unspecified columns to STRING
        existing_fields = {field.name for field in schema}
        for column in dataframe.columns:
            if column not in existing_fields:
                print(f"Converting column {column} to STRING.")
                dataframe[column] = dataframe[column].astype(str)  # Convert to string
                schema.append(bigquery.SchemaField(column, "STRING"))

        # Configure BigQuery load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append table
            schema=schema,  # Explicitly set schema
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="call_date"
            )
        )

        # Load data to BigQuery
        job = self.client.load_table_from_dataframe(
            dataframe, table_ref, job_config=job_config
        )
        job.result()  # Wait for the job to complete
        print(f"Table {table_ref} loaded successfully with schema.")

class GoogleSheetsClass:
    def __init__(self):
        print("Google Drive already mounted.")

    def download_sheet_as_csv(self, file_path):
        # Read the file from the specified path
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path, skiprows=Config.SKIPROWS)
        elif file_path.endswith('.xlsx'):
            return pd.read_excel(file_path, skiprows=Config.SKIPROWS)
        else:
            raise ValueError("Unsupported file format")

class DataPipeline:
    def __init__(self):
        self.bigquery_client = BigQueryClass()
        self.sheets_client = GoogleSheetsClass()

    def move_excel_files_by_prefix(self):
        source_folder = Config.WORKING_DIRECTORY
        try:
            for filename in os.listdir(source_folder):
                if filename.endswith('.csv') or filename.endswith('.xlsx'):
                    print(f"Processing file: {filename}")
                    source_path = os.path.join(source_folder, filename)
                    suffix = datetime.strftime(Config.DATE_TODAY,'%Y%m%d')
                    print(f"Extracted date: {suffix}")
                    year, month, day = map(int, [suffix[:4], suffix[4:6], suffix[6:]])  # format required YYYYMMDD
                    # Create year, month directories
                    year_folder = os.path.join(source_folder, str(year))
                    os.makedirs(year_folder, exist_ok=True)
                    month_folder = os.path.join(year_folder, str(month))
                    os.makedirs(month_folder, exist_ok=True)

                    # Move file
                    destination_path = os.path.join(month_folder, filename)
                    shutil.move(source_path, destination_path)
                    print(f"File '{filename}' moved to '{month_folder}' successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def execute_pipeline(self):
        folder_path = Config.FOLDER_PATH
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)
                 if f.endswith('.csv') or f.endswith('.xlsx')]

        print(f"Found {len(files)} files in the folder. Processing...")

        for file_path in files:
            print(f"Processing file: {file_path}")

            dataframe = self.sheets_client.download_sheet_as_csv(file_path)

            if dataframe is None:
                print(f"Skipping file: {file_path}")
                continue

            filename = os.path.basename(file_path)
            dataframe['filename'] = filename
            dataframe['date uploaded'] = Config.DATE_TODAY

            # Parse date columns
            for col in dataframe.columns:
                if dataframe[col].dtype == 'object' and col in ['Call Date', 'Lead Creation']:
                    try:
                        dataframe[col] = pd.to_datetime(dataframe[col])
                    except Exception as e:
                        print(f"Failed to parse date column '{col}': {e}")

            # Determine table based on filename prefix
            if filename.upper().startswith("MA_"):
                target_table_id = Config.table_b
                print(f"File '{filename}' detected as MA prefix, uploading to table_b: {target_table_id}")
            else:
                target_table_id = Config.table_a
                print(f"File '{filename}' detected as KA prefix (or no prefix), uploading to table_a: {target_table_id}")

            # Retrieve last call timestamp from the relevant table
            last_call_timestamp = self.bigquery_client.retrieve_last_call_timestamp(table_id=target_table_id)

            # Filter dataframe
            filtered_dataframe = dataframe[dataframe['Call Date'] > pd.to_datetime(last_call_timestamp)]

            # Upload to correct BigQuery table
            self.bigquery_client.upload_to_bigquery(filtered_dataframe, table_id=target_table_id)


def main():
    connect_to_drive()  # Mount drive
    pipeline = DataPipeline()  # Instantiate pipeline
    pipeline.execute_pipeline()
    pipeline.move_excel_files_by_prefix()

if __name__ == "__main__":
    main()
