import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import os
import logging
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed

client = storage.Client(project='liquid-kite-436018-c2')


# 1. Setup logging configuration
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    handlers=[
        logging.FileHandler("ticker_data_update.log"),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)


# 2. Function to read tickers from an Excel or CSV file
def read_tickers_from_spreadsheet(file_path, sheet_name=None):
    logging.info(f"Reading tickers from file: {file_path}")
    if file_path.endswith('.xlsx'):
        tickers_df = pd.read_excel(file_path, sheet_name=sheet_name)
    elif file_path.endswith('.csv'):
        tickers_df = pd.read_csv(file_path)
    else:
        logging.error(f"Unsupported file format: {file_path}")
        raise ValueError("Unsupported file format. Use .xlsx or .csv files.")
    
    tickers = tickers_df['Ticker'].tolist()
    logging.info(f"Found {len(tickers)} tickers in the spreadsheet: {tickers}")
    return tickers


# 3. Function to find the last partition by year and month for a ticker in GCS
def get_last_partition(bucket_name, parquet_dir, ticker):
    logging.info(f"Fetching last partition for {ticker} from GCS")
    last_year = None
    last_month = None
    ticker_prefix = f'{parquet_dir}/Year='  # Start by listing years
    
    # List the objects in the bucket starting from the base parquet directory
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=ticker_prefix)
    
    years = set()
    months = {}

    for blob in blobs:
        blob_path = blob.name
        parts = blob_path.split('/')
        
        # Extract Year, Month, and Ticker from the path
        if any(f'Ticker={ticker}' in part for part in parts):
            for part in parts:
                if part.startswith('Year='):
                    year = int(part.split('=')[-1])
                    years.add(year)
                    if year not in months:
                        months[year] = []
                if part.startswith('Month='):
                    month = int(part.split('=')[-1])
                    months[year].append(month)

    if years:
        last_year = max(years)
        last_month = max(months[last_year]) if months[last_year] else None
        logging.info(f"Last partition for {ticker} found in GCS: Year={last_year}, Month={last_month}")
    else:
        logging.info(f"No existing data found for ticker: {ticker}")

    return last_year, last_month


# 4. Function to get the last date from the most recent partition in GCS
def get_last_date_from_partition(bucket_name, parquet_dir, ticker, last_year, last_month):
    if last_year is None or last_month is None:
        return None
    
    # Path now structured as 'Year=YYYY/Month=MM/Ticker=<TICKER>'
    partition_path = f'{parquet_dir}/Year={last_year}/Month={last_month}/Ticker={ticker}/'
    logging.info(f"Checking last date from partition in GCS: {partition_path}")

    try:
        # List objects in the partition to find the parquet file
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=partition_path)

        for blob in blobs:
            if blob.name.endswith('.parquet'):
                blob_data = blob.download_as_bytes()
                partition_data = pd.read_parquet(blob_data)
                last_date = partition_data.index.max()
                logging.info(f"Last date for ticker {ticker} from partition in GCS: {last_date}")
                return last_date
    except Exception as e:
        logging.error(f"Error reading partition for {ticker} in GCS: {e}")
        return None

    return None


#5
def upload_to_gcs(bucket_name, local_file_path, gcs_file_path):
    """Uploads a file from local storage to a GCS bucket."""
    logging.info(f"Uploading {local_file_path} to GCS bucket {bucket_name} at {gcs_file_path}")
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)
    
    blob.upload_from_filename(local_file_path)
    logging.info(f"Successfully uploaded {local_file_path} to GCS at {gcs_file_path}")


#6
def fetch_and_append_data(bucket_name, ticker, parquet_dir, execution_date):
    logging.info(f"Starting data fetch for ticker: {ticker}")
    
    last_year, last_month = get_last_partition(bucket_name, parquet_dir, ticker)
    last_date = get_last_date_from_partition(bucket_name, parquet_dir, ticker, last_year, last_month)
    
    if last_date:
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        start_date = '1991-01-01'
    
    end_date = execution_date.strftime('%Y-%m-%d')
    logging.info(f"Fetching data for {ticker} from {start_date} to {end_date}")
    
    try:
        stock = yf.Ticker(ticker)
        new_data = stock.history(start=start_date, end=end_date)

        if not new_data.empty:
            new_data['Year'] = new_data.index.year
            new_data['Month'] = new_data.index.month
            new_data['Ticker'] = ticker
            logging.info(f"Retrieved {len(new_data)} rows for {ticker}")

            # Now group by Year and Month and store the data in GCS accordingly
            grouped = new_data.groupby(['Year', 'Month'])
            for (year, month), group in grouped:
                logging.info(f"Processing data for {ticker} for Year={year}, Month={month}")

                # Convert the group to a pyarrow table
                table = pa.Table.from_pandas(group)

                # Define local path for the parquet file
                local_parquet_path = f'/tmp/{ticker}_{year}_{month}.parquet'

                # Write the Parquet file locally first
                pq.write_table(table, local_parquet_path)

                # Define GCS path based on Year, Month, and Ticker
                gcs_parquet_path = f'{parquet_dir}/Year={year}/Month={month}/Ticker={ticker}/{ticker}_{year}_{month}.parquet'
                
                # Upload the file to GCS
                upload_to_gcs(bucket_name, local_parquet_path, gcs_parquet_path)

                logging.info(f"Uploaded data to GCS: {gcs_parquet_path}")

                # Optionally, delete the local file after upload
                os.remove(local_parquet_path)
        else:
            logging.info(f"No new data for {ticker} since {last_date}.")
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")


# 7. Main function to run the update process for all tickers
def update_all_tickers(bucket_name, parquet_dir, ticker_file, execution_date):
    logging.info(f"Starting update process for tickers from {ticker_file}")
    
    # Read the list of tickers from the spreadsheet
    tickers = read_tickers_from_spreadsheet(ticker_file)

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor() as executor:
        future_to_ticker = {
            executor.submit(fetch_and_append_data, bucket_name, ticker, parquet_dir, execution_date): ticker
            for ticker in tickers
        }

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                future.result()  # Check for exceptions during execution
                logging.info(f"Successfully processed ticker: {ticker}")
            except Exception as e:
                logging.error(f"Error processing ticker {ticker}: {e}")


bucket_name = 'trad-fi'
parquet_dir = 'raw'
ticker_file = '/home/tobi/de-projects/sp_tickers.csv'
execution_date = datetime.now()

# Update all tickers
update_all_tickers(bucket_name, parquet_dir, ticker_file, execution_date)
