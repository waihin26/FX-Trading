import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def fetch_usdjpy_data(api_key):
    """
    Fetch USD/JPY historical data from Alpha Vantage
    
    Parameters:
    -----------
    api_key : str
        Alpha Vantage API key
    
    Returns:
    --------
    pd.DataFrame : OHLCV data with columns [Date, Open, High, Low, Close, Volume]
    """
    
    print(f"Fetching USD/JPY data from Alpha Vantage...")
    
    try:
        url = f'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol=JPY&outputsize=full&apikey={api_key}'
        
        print("Making API request...")
        response = requests.get(url)
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            print(f"API Error: {data['Error Message']}")
            return None
        
        if "Note" in data:
            print(f"API Note: {data['Note']}")
            print("You may have hit the API rate limit (5 calls/minute for free tier)")
            return None
        
        if "Time Series FX (Daily)" not in data:
            print(f"Unexpected API response: {data}")
            return None
        
        # Convert to DataFrame
        fx_data = data['Time Series FX (Daily)']
        df = pd.DataFrame.from_dict(fx_data, orient='index')
        df.reset_index(inplace=True)
        
        # Rename columns
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close']
        df['Volume'] = 0
        
        # Convert data types
        df['Date'] = pd.to_datetime(df['Date'])
        df['Open'] = pd.to_numeric(df['Open'])
        df['High'] = pd.to_numeric(df['High'])
        df['Low'] = pd.to_numeric(df['Low'])
        df['Close'] = pd.to_numeric(df['Close'])
        
        # Sort by date (oldest first)
        df = df.sort_values('Date').reset_index(drop=True)
        
        print(f"Successfully fetched {len(df)} daily candles")
        print(f"Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
        
        # Save to parquet
        os.makedirs('data', exist_ok=True)
        parquet_file = 'data/USDJPY_daily.parquet'
        df.to_parquet(parquet_file, index=False, compression='snappy')
        print(f"Data saved to: {parquet_file}")
        
        return df
        
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

def load_usdjpy_data(filename='data/USDJPY_daily.parquet'):
    """
    Load USD/JPY data from parquet file
    
    Parameters:
    -----------
    filename : str
        Path to parquet file
    
    Returns:
    --------
    pd.DataFrame : OHLCV data
    """
    try:
        df = pd.read_parquet(filename)
        print(f"Loaded {len(df)} candles from {filename}")
        return df
    except FileNotFoundError:
        print(f"File {filename} not found. Run fetch_usdjpy_data() first.")
        return None

if __name__ == "__main__":
    API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
    
    if API_KEY is None:
        print("ERROR: API key not found in .env file")
    else:
        df = fetch_usdjpy_data(api_key=API_KEY)
        
        if df is not None:
            print(f"\n--- Data Quality Check ---")
            print(f"Total rows: {len(df)}")
            print(f"Missing values: {df.isnull().sum().sum()}")
            print(f"Price range: {df['Close'].min():.2f} - {df['Close'].max():.2f}")
            
            years = (df['Date'].max() - df['Date'].min()).days / 365.25
            print(f"Years of data: {years:.1f}")