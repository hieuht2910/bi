from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, DateType
import pyspark.sql.functions as F
import os
import subprocess
from io import StringIO
import pandas as pd

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import urllib3


def create_spark_session():
    """Create Spark session with proper HDFS configurations."""
    os.environ['HADOOP_USER_NAME'] = 'root'

    return (SparkSession.builder
            .appName("HDFS CSV to InfluxDB")
            # Basic Spark Configuration
            .config("spark.sql.session.timeZone", "UTC")
            # HDFS Configuration
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
            # Namenode Configuration
            .config("spark.hadoop.dfs.namenode.http-address", "localhost:9870")
            .config("spark.hadoop.dfs.namenode.rpc-address", "localhost:9000")
            # Datanode Configuration
            .config("spark.hadoop.dfs.datanode.http.address", "localhost:9864")
            .config("spark.hadoop.dfs.datanode.address", "localhost:9866")
            # Connection Settings
            .config("spark.hadoop.dfs.client.socket-timeout", "120000")
            .config("spark.hadoop.ipc.client.connect.timeout", "60000")
            .config("spark.hadoop.ipc.client.connect.max.retries", "10")
            .getOrCreate())


def read_from_hdfs():
    """Read data directly from HDFS using docker exec."""
    try:
        cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", "/user/root/AMZN_yfinance.csv"]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            return result.stdout
        else:
            print(f"Error reading file: {result.stderr}")
            return None
    except Exception as e:
        print(f"Error executing command: {str(e)}")
        return None


def process_data(spark, data):
    """Process the CSV data and create a Spark DataFrame."""
    try:
        # Define schema
        schema = StructType([
            StructField("Date", DateType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Adj Close", DoubleType(), True),
            StructField("Volume", DoubleType(), True)
        ])

        # Read data into pandas first
        pdf = pd.read_csv(StringIO(data))
        pdf['Date'] = pd.to_datetime(pdf['Date']).dt.date

        # Convert to Spark DataFrame
        df = spark.createDataFrame(pdf)

        # Cache the DataFrame for better performance
        return df.cache()

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return None


def transform_data(df):
    """Transform the data by adding new columns."""
    try:
        df = df.withColumn("Open", F.col("Open"))
        df = df.withColumn("High", F.col("High"))
        df = df.withColumn("Low", F.col("Low"))
        df = df.withColumn("Close", F.col("Close"))
        df = df.withColumn("Adj Close", F.col("Adj Close"))
        df = df.withColumn("Return", (F.col("Close") - F.col("Open")) / F.col("Open"))
        df = df.withColumn("AVG Price", (F.col("Open") + F.col("Close")) / 2)
        df = df.withColumn("Close Pct Change", (F.col("Close") - F.col("Open")) / F.col("Open") * 100)

        return df

    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        return None


def publish_to_influxdb(records):
    """
    Publish records to InfluxDB

    :param records: List of dictionaries containing stock data
    """
    # InfluxDB connection parameters
    url = "http://192.168.200.161:8086"  # Default InfluxDB URL
    token = ""
    org = "hust"  # Replace with your organization name
    bucket = "bi_amazon_stock"  # Bucket name to store data

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
                point.field("return", float(record.get('Return', 0)))
                point.field("avg_price", float(record.get('AVG Price', 0)))
                point.field("close_pct_change", float(record.get('Close Pct Change', 0)))

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


def main():
    try:
        # Read data directly from HDFS
        print("Reading data from HDFS...")
        data = read_from_hdfs()

        if data is None:
            print("Failed to read data from HDFS")
            return

        # Create Spark session
        spark = create_spark_session()

        # Process data
        df = process_data(spark, data)

        if df is None:
            print("Failed to process data")
            return

        # Get total count
        total_rows = df.count()
        print(f"\nTotal number of records: {total_rows}")

        # Transform data
        df = transform_data(df)

        # Convert to Pandas DataFrame
        pandas_df = df.toPandas()

        # Convert datetime columns to strings
        for col in pandas_df.select_dtypes(include=['datetime64']).columns:
            pandas_df[col] = pandas_df[col].astype(str)

        # Convert DataFrame to list of dictionaries
        records = pandas_df.to_dict('records')

        # Publish to InfluxDB
        publish_to_influxdb(records)

    except Exception as e:
        print(f"Error in main: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()