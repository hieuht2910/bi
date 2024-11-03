import yfinance as yf
from datetime import datetime


def download_stock_data(ticker="AMZN", start_date="2022-03-25", end_date=None):
    """
    Download stock data using yfinance and format dates

    Parameters:
    ticker (str): Stock ticker symbol
    start_date (str): Start date in YYYY-MM-DD format
    end_date (str): End date in YYYY-MM-DD format, defaults to today
    """
    try:
        # Handle end date
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')

        print(f"Downloading data for {ticker}")
        print(f"From: {start_date}")
        print(f"To: {end_date}")

        # Create a Ticker object
        stock = yf.Ticker(ticker)

        # Download the data
        df = stock.history(start=start_date, end=end_date)

        # Just format the date to YYYY-MM-DD while keeping original timezone
        df.index = df.index.strftime('%Y-%m-%d')

        # Reset index to make Date a column
        df = df.reset_index()

        # Rename the index column to 'Date'
        df = df.rename(columns={'index': 'Date'})

        # Create filename
        filename = f"{ticker}_stock_data_{start_date}_to_{end_date}.csv"

        # Save to CSV
        df.to_csv(filename, index=False)

        print(f"\nData successfully downloaded and saved to {filename}")
        print(f"Data shape: {df.shape}")
        print("\nFirst few rows of data:")
        print(df.head())

    except Exception as e:
        print(f"An error occurred: {str(e)}")


# Example usage
if __name__ == "__main__":
    download_stock_data(
        ticker="AMZN",
        start_date="2022-03-25",
        end_date="2024-03-03"
    )