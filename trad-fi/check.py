import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import os
import logging

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
    logging.info(f"Found {len(tickers)} tickers in the spreadsheet.")
    return tickers

# 3. Function to find the last partition by year and month for a ticker
def get_last_partition(parquet_dir, ticker):
    last_year = None
    last_month = None
    ticker_path = os.path.join(parquet_dir, f'Ticker={ticker}')
    
    if not os.path.exists(ticker_path):
        logging.info(f"No existing data found for ticker: {ticker}")
        return None, None
    
    # Find the most recent year
    years = [int(year_dir.split('=')[-1]) for year_dir in os.listdir(ticker_path) if year_dir.startswith('Year=')]
    if years:
        last_year = max(years)
        year_path = os.path.join(ticker_path, f'Year={last_year}')
        months = [int(month_dir.split('=')[-1]) for month_dir in os.listdir(year_path) if month_dir.startswith('Month=')]
        if months:
            last_month = max(months)
            logging.info(f"Last partition for {ticker} found: Year={last_year}, Month={last_month}")
    
    return last_year, last_month

# 4. Function to get the last date from the most recent partition
def get_last_date_from_partition(parquet_dir, ticker, last_year, last_month):
    if last_year is None or last_month is None:
        return None
    
    partition_path = os.path.join(parquet_dir, f'Ticker={ticker}', f'Year={last_year}', f'Month={last_month}')
    logging.info(f"Checking last date from partition: {partition_path}")
    
    try:
        partition_data = pd.read_parquet(partition_path)
        last_date = partition_data.index.max()  # Get the last date in the partition
        logging.info(f"Last date for ticker {ticker} from partition: {last_date}")
        return last_date
    except Exception as e:
        logging.error(f"Error reading partition for {ticker}: {e}")
        return None

# 5. Function to fetch and append data based on the last partition or date
def fetch_and_append_data(ticker, parquet_dir, execution_date):
    # Get the last partition for the given ticker
    last_year, last_month = get_last_partition(parquet_dir, ticker)

    # Get the last available date from the most recent partition
    last_date = get_last_date_from_partition(parquet_dir, ticker, last_year, last_month)

    # If no existing data, fetch from the beginning of history
    if last_date:
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        start_date = '1900-01-01'  # Fetch full history if no data exists

    # Set the end date based on the execution date passed by the orchestrator
    end_date = execution_date.strftime('%Y-%m-%d')
    
    logging.info(f"Fetching data for {ticker} from {start_date} to {end_date}")

    # Fetch the data
    stock = yf.Ticker(ticker)
    new_data = stock.history(start=start_date, end=end_date)

    if not new_data.empty:
        # Add partitioning columns
        new_data['Year'] = new_data.index.year
        new_data['Month'] = new_data.index.month
        new_data['Day'] = new_data.index.day
        new_data['Ticker'] = ticker
        new_data['Sector'] = stock.info.get('sector', 'Unknown')

        logging.info(f"Retrieved {len(new_data)} rows for {ticker}")
        
        # Convert to pyarrow table
        table = pa.Table.from_pandas(new_data)
        logging.debug(f"Converted {ticker} data to PyArrow Table format")

        # Append the new data to the Parquet dataset (partitioned by Year, Month, Ticker)
        pq.write_to_dataset(
            table,
            root_path=parquet_dir,
            partition_cols=['Year', 'Month', 'Ticker']
        )
        
        # Log the partition that was appended
        logging.info(f"Appended new data to partition: Year={new_data['Year'].max()}, Month={new_data['Month'].max()}, Ticker={ticker}")
    else:
        logging.info(f"No new data for {ticker} since {last_date}.")

# 6. Main function to run the update process for all tickers
def update_all_tickers(parquet_dir, ticker_file, execution_date):
    logging.info(f"Starting update process for tickers from {ticker_file}")
    
    # Read the list of tickers from the spreadsheet
    tickers = read_tickers_from_spreadsheet(ticker_file)

    # Process each ticker
    for ticker in tickers:
        logging.info(f"Processing ticker: {ticker}")
        fetch_and_append_data(ticker, parquet_dir, execution_date)

# Example usage:

# Directory where partitioned Parquet files are stored
parquet_dir = 'historical_data'

# File path to the spreadsheet with tickers (Excel or CSV)
ticker_file = 'ticker.csv'  # Assumes the spreadsheet contains a "Ticker" column

# Airflow-like execution date (passed in from the orchestrator)
execution_date = datetime.now()

# Update all tickers based on the execution date
update_all_tickers(parquet_dir, ticker_file, execution_date)
