import yfinance as yf
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import urllib3

def fetch_and_insert_amazon_stock():
    """
    Fetch Amazon stock data for today and insert it into InfluxDB.
    """
    try:
        # Fetch today's date
        today = datetime.today().strftime('%Y-%m-%d')

        # Download stock data for today
        print(f"Downloading data for AMZN")
        print(f"Date: {today}")

        stock = yf.Ticker("AMZN")
        df = stock.history(start=today, end=today)

        if df.empty:
            print(f"No data found for AMZN on {today}")
            return

        df['Adj Close'] = df['Close']

        # Just format the date to YYYY-MM-DD while keeping original timezone
        df.index = df.index.strftime('%Y-%m-%d')

        # Reset index to make Date a column
        df = df.reset_index()

        # Rename the index column to 'Date'
        df = df.rename(columns={'index': 'Date'})

        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')

        print(f"\nRecords: {records}")

        # Insert data into InfluxDB
        publish_to_influxdb(records)

    except Exception as e:
        print(f"An error occurred: {str(e)}")

def publish_to_influxdb(records):
    """
    Publish records to InfluxDB

    :param records: List of dictionaries containing stock data
    """
    url = "http://192.168.200.161:8086"
    token = ""
    org = "hust"
    bucket = "bi_amazon_stock"

    try:
        # Disable SSL warnings if needed
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Create InfluxDB client with increased timeout
        client = InfluxDBClient(
            url=url,
            token=token,
            org=org,
            timeout=30_000,  # Increased timeout to 30 seconds
            verify_ssl=False  # Disable SSL verification if needed
        )

        # Create write API with longer timeout
        write_api = client.write_api(
            write_options=SYNCHRONOUS,
            timeout=30_000  # Longer timeout for write operations
        )

        # Batch the points to reduce network overhead
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            points = []

            for record in batch:
                point = Point("stock_prices") \
                    .tag("symbol", "AMZN")  # Add tags as needed

                # Add fields
                point.field("open", float(record['Open']))
                point.field("high", float(record['High']))
                point.field("low", float(record['Low']))
                point.field("close", float(record['Close']))
                point.field("adj_close", float(record['Adj Close']))
                point.field("volume", float(record['Volume']))

                # Set timestamp
                point.time(datetime.strptime(str(record['Date']), "%Y-%m-%d"))

                points.append(point)

            # Write points to InfluxDB
            write_api.write(bucket=bucket, org=org, record=points)

            print(f"Successfully wrote {len(points)} points to InfluxDB")

    except Exception as e:
        print(f"Error publishing to InfluxDB: {str(e)}")
        # Print additional debug information
        import traceback
        traceback.print_exc()

    finally:
        # Close the client
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    fetch_and_insert_amazon_stock()