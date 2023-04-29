import time
import datetime
import os
import shutil
import tempfile
import csv
from google.cloud import storage
from google.cloud import bigquery
import requests
from google.api_core.exceptions import Forbidden

# Set the environment variable for Google Cloud service account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/dharmik/test/terraform-gcp/newdatakey.json"

# Create a Storage client object
storage_client = storage.Client()
bucket_name = "sep721-weather-data-bucket"
bucket = storage_client.get_bucket(bucket_name)


# Define the base URL for the data
base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=6901&Year={}&Month=1&Day=1&time=&timeframe=2&submit=Download+Data"

# Set the start and end years for the data
start_year = 1992
current_date = datetime.date.today()
end_year = int(current_date.strftime('%Y'))

# Define the indices of columns to remove from the input file
indices_to_remove = [0, 1, 2, 3, 5, 6, 7, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 27, 28, 29, 30]

# Download the data and clean it for each downloaded file
csv_folder = ""

# Set the environment variable for Google Cloud service account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/dharmik/test/terraform-gcp/newdatakey.json"

# Create BigQuery client object
client = bigquery.Client()

# Set the destination dataset ID and table ID
dataset_id = "weather_dataset"
table_id = "weather_data_table"

# Define the schema for the table
schema = [
    bigquery.SchemaField("Date_Time", "DATE"),
    bigquery.SchemaField("Max_Temp_C", "FLOAT64"),
    bigquery.SchemaField("Min_Temp_C", "FLOAT64"),
    bigquery.SchemaField("Mean_Temp_C", "FLOAT64"),
    bigquery.SchemaField("Heat_Deg_Days_C", "FLOAT64"),
    bigquery.SchemaField("Cool_Deg_Days_C", "FLOAT64"),
    bigquery.SchemaField("Total_Rain_mm", "FLOAT64"),
    bigquery.SchemaField("Total_Snow_cm", "FLOAT64"),
    bigquery.SchemaField("Total_Precip_mm", "FLOAT64"),
    bigquery.SchemaField("Snow_on_Grnd_cm", "FLOAT64")
]

# Create or get the destination table
table_ref = client.dataset(dataset_id).table(table_id)


try:
    table = client.get_table(table_ref)
    if table.schema != schema:
        client.delete_table(table_ref)
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
except:
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)


def download_data(base_url, year):
    
    url = base_url.format(year)
    print("URL: ", url)
    response = requests.get(url)
    filename = f"data_{year}.csv"
    with open(filename, "wb") as file:
        file.write(response.content)
        print(f"Downloaded data for year {year} to file {filename}")
    return (filename)

def clean_data(filename, output_filename, indices_to_remove):

    with open(filename , 'r') as input_file , open(output_filename , 'w' , newline='') as output_file:
        reader = csv.reader(input_file)
        writer = csv.writer(output_file)

        header = next(reader)
        header = [column for i , column in enumerate(header) if i not in indices_to_remove]

        writer.writerow(header)

        for row in reader:
            new_row = [row[i] for i in range(len(row)) if i not in indices_to_remove]
            if all(new_row):
                writer.writerow(new_row)

    print(f"Data cleaning for file {filename} done. Cleaned data saved to file {output_filename}")
    shutil.copy2(output_filename , filename)
    return(filename)

def Main_Data_Injest(csv_file , csv_folder):
    csv_file = os.path.join(csv_folder ,  csv_file)
    blob_name = os.path.basename(csv_file)  # Use only the filename
    blob = bucket.blob(blob_name)
    with tempfile.NamedTemporaryFile('w+') as fd:
        with open(csv_file , 'r') as f:
            fd.write(f.read())
            fd.flush()
        blob.upload_from_filename(fd.name , content_type="text/csv")
    return(csv_file)

def Main_BigQuery(year, csv_file,table_ref):
    csv_file = f"data_{year}.csv"
    blob_name = csv_file
    blob = bucket.blob(blob_name)

    # Load the CSV file from GCS to a BigQuery table
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = f"gs://{bucket_name}/{blob_name}"
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    # Add error handling to catch and retry rate limit errors
    while True:
        try:
            load_job = client.load_table_from_uri(uri , table_ref , job_config=job_config)
            # Wait for the load job to complete
            load_job.result()
            print(f"Loaded {uri} to {table_id}")
            break
        except Forbidden as e:
            print(f"Rate limit exceeded, retrying in 60 seconds: {e}")
            time.sleep(60)
    # Add a sleep time between each table update operation
    time.sleep(5)

for year in range(start_year, end_year+1):

    output_filename = "temp_file.csv"
    filename= download_data(base_url , year)
    print(filename)
    csv_file=clean_data(filename , output_filename , indices_to_remove)
    csv_file=Main_Data_Injest(csv_file , csv_folder)
    Main_BigQuery(year, csv_file, table_ref)


