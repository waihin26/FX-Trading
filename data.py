import requests
import pandas as pd
from datetime import datetime
import os
import time

def fetch_usdjpy_data(api_key, save_to_csv=True):
    """
    Fetch USD/JPY historical data from Alpha Vantage
    
    Parameters:
    -----------
    api_key : str
        Your Alpha Vantage API key (get free key at https://www.alphavantage.co/support/#api-key)
    save_to_csv : bool
        Whether to save data to CSV file
    
    Returns:
    --------
    pd.DataFrame : OHLCV data with columns [Date, Open, High, Low, Close, Volume]
    """
    
    print(f"Fetching USD/JPY data from Alpha Vantage...")
    
    try:
        # Alpha Vantage FX_DAILY endpoint
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
        
        # Reset index to make Date a column
        df.reset_index(inplace=True)
        
        # Rename columns to match our standard format
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close']
        
        # Add Volume column (FX doesn't have real volume, set to 0)
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
        print(f"\nFirst few rows:")
        print(df.head())
        print(f"\nLast few rows:")
        print(df.tail())
        
        # Create data directory if it doesn't exist
        if save_to_csv:
            os.makedirs('data', exist_ok=True)
            filename = f'data/USDJPY_daily_alphavantage.csv'
            df.to_csv(filename, index=False)
            print(f"\nData saved to: {filename}")
        
        return df
        
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return None

def load_usdjpy_data(filename='data/USDJPY_daily_alphavantage.csv'):
    """
    Load USD/JPY data from CSV file
    
    Parameters:
    -----------
    filename : str
        Path to CSV file
    
    Returns:
    --------
    pd.DataFrame : OHLCV data
    """
    try:
        df = pd.read_csv(filename)
        df['Date'] = pd.to_datetime(df['Date'])
        print(f"Loaded {len(df)} candles from {filename}")
        return df
    except FileNotFoundError:
        print(f"File {filename} not found. Run fetch_usdjpy_data() first.")
        return None

if __name__ == "__main__":
    # IMPORTANT: Replace 'YOUR_API_KEY' with your actual Alpha Vantage API key
    # Get free API key at: https://www.alphavantage.co/support/#api-key
    
    API_KEY = 'YOUR_API_KEY'  # <-- PUT YOUR API KEY HERE
    
    if API_KEY == 'YOUR_API_KEY':
        print("=" * 60)
        print("ERROR: Please set your Alpha Vantage API key!")
        print("=" * 60)
        print("\nSteps:")
        print("1. Go to: https://www.alphavantage.co/support/#api-key")
        print("2. Get your free API key")
        print("3. Replace 'YOUR_API_KEY' in this file with your actual key")
        print("4. Run the script again")
    else:
        # Fetch and save USD/JPY data
        df = fetch_usdjpy_data(api_key=API_KEY, save_to_csv=True)
        
        # Check for missing data
        if df is not None:
            print(f"\n--- Data Quality Check ---")
            print(f"Total rows: {len(df)}")
            print(f"Missing values:\n{df.isnull().sum()}")
            print(f"\nPrice range:")
            print(f"Min Close: {df['Close'].min():.2f}")
            print(f"Max Close: {df['Close'].max():.2f}")
            
            # Calculate years of data
            years = (df['Date'].max() - df['Date'].min()).days / 365.25
            print(f"\nYears of data: {years:.1f}")