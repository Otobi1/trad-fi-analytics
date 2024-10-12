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


# Logging Config (file and console)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ticker_data_update.log"),
        logging.StreamHandler()
    ]
)

def read_tickers_from_spreadsheet(file_path, sheet_name=None):
    '''
        Read tickers from an Excel or CSV file.
    '''
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


def get_last_partition(bucket_name, parquet_dir, ticker):
    '''
        Find the last partition by year and month for a ticker in GCS.
        List the years, then list the objects in the bucket from the base parquet directory.
    '''
    logging.info(f"Fetching last partition for {ticker} from GCS")
    last_year = None
    last_month = None
    ticker_prefix = f'{parquet_dir}/Year=' 
    
   
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

 
def get_last_date_from_partition(bucket_name, parquet_dir, ticker, last_year, last_month):
    '''
        Get the last date from the most recent partition in GCS.
    '''
    if last_year is None or last_month is None:
        return None
    
    # Partition path structured as - 'Year=YYYY/Month=MM/Ticker=<TICKER>'
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


def upload_to_gcs(bucket_name, local_file_path, gcs_file_path):
    """
    Uploads a file from local storage to a GCS bucket.
    """
    logging.info(f"Uploading {local_file_path} to GCS bucket {bucket_name} at {gcs_file_path}")
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)
    
    blob.upload_from_filename(local_file_path)
    logging.info(f"Successfully uploaded {local_file_path} to GCS at {gcs_file_path}")


def fetch_and_overwrite_data(bucket_name, ticker, parquet_dir, target_year, target_month):
    '''
        Fetch data for a specific year and month from yfinance.
        Overwrite existing data for that year and month in GCS.
    '''
    logging.info(f"Starting data fetch for ticker: {ticker} for Year={target_year}, Month={target_month}")
    
    # Define start and end dates for the specific month
    start_date = f'{target_year}-{target_month:02d}-01'
    end_date = (datetime(target_year, target_month, 1) + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')

    logging.info(f"Fetching data for {ticker} from {start_date} to {end_date}")
    
    try:
        stock = yf.Ticker(ticker)
        new_data = stock.history(start=start_date, end=end_date)

        if not new_data.empty:
            new_data['Year'] = new_data.index.year
            new_data['Month'] = new_data.index.month
            new_data['Ticker'] = ticker
            logging.info(f"Retrieved {len(new_data)} rows for {ticker}")

            # Group the data by Year and Month (should only be one group)
            grouped = new_data.groupby(['Year', 'Month'])

            for (year, month), group in grouped:
                logging.info(f"Processing data for {ticker} for Year={year}, Month={month}")

                table = pa.Table.from_pandas(group)

                local_parquet_path = f'/tmp/{ticker}_{year}_{month}.parquet'

                pq.write_table(table, local_parquet_path)

                gcs_parquet_path = f'{parquet_dir}/Year={year}/Month={month}/Ticker={ticker}/{ticker}_{year}_{month}.parquet'

                # Before uploading, delete any existing file for this year/month
                delete_existing_files(bucket_name, parquet_dir, ticker, year, month)

                upload_to_gcs(bucket_name, local_parquet_path, gcs_parquet_path)

                logging.info(f"Uploaded data to GCS: {gcs_parquet_path}")

                os.remove(local_parquet_path)
        else:
            logging.info(f"No new data for {ticker} in {target_year}-{target_month}.")
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")


def delete_existing_files(bucket_name, parquet_dir, ticker, year, month):
    """
    Delete existing files for a given ticker, year, and month in GCS.
    """
    logging.info(f"Deleting existing data for {ticker} for Year={year}, Month={month}")
    
    prefix = f'{parquet_dir}/Year={year}/Month={month}/Ticker={ticker}/'
    
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    for blob in blobs:
        logging.info(f"Deleting {blob.name}")
        blob.delete()


def update_all_tickers(bucket_name, parquet_dir, ticker_file, target_year, target_month):
    ''' 
        Update all tickers for a specific year and month, overwrite and upload to GCS.
        Use ThreadPoolExecutor for parallel processing.
    '''
    logging.info(f"Starting update process for tickers from {ticker_file} for Year={target_year}, Month={target_month}")
    
    tickers = read_tickers_from_spreadsheet(ticker_file)

    with ThreadPoolExecutor() as executor:
        future_to_ticker = {
            executor.submit(fetch_and_overwrite_data, bucket_name, ticker, parquet_dir, target_year, target_month): ticker
            for ticker in tickers
        }

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                future.result()
                logging.info(f"Successfully processed ticker: {ticker}")
            except Exception as e:
                logging.error(f"Error processing ticker {ticker}: {e}")


bucket_name = 'trad-fi'
parquet_dir = 'raw'
ticker_file = '/home/tobi/de-projects/sp_tickers.csv'
target_year = 2024
target_month = 10

# Update all tickers
update_all_tickers(bucket_name, parquet_dir, ticker_file, target_year, target_month)
