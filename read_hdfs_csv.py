from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import pyspark.sql.functions as F
import subprocess
import pandas as pd
from io import StringIO
from datetime import datetime


def define_schema():
    """Define the schema for the stock data."""
    return StructType([
        StructField("Date", DateType(), False),
        StructField("Open", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("Adj_Close", DoubleType(), False),
        StructField("Volume", DoubleType(), False)
    ])


def read_from_hdfs():
    """Read data from HDFS using docker exec command."""
    try:
        cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", "/user/root/AMZN.csv"]
        print("Executing command:", " ".join(cmd))

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            # Verify we got data
            data = result.stdout.strip()
            if data:
                line_count = len(data.splitlines())
                print("Successfully read {} lines".format(line_count))
                return data
            else:
                print("No data returned from HDFS")
                return None
        else:
            print("Error reading file:", result.stderr)
            return None
    except Exception as e:
        print("Error executing command:", str(e))
        return None


def create_spark_session():
    """Create a basic Spark session."""
    return (SparkSession.builder
            .appName("Stock Data Processor")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def process_data(spark, data_str):
    """Process the data using Spark."""
    try:
        # Create DataFrame from pandas
        pdf = pd.read_csv(StringIO(data_str))

        # Convert date column
        pdf['Date'] = pd.to_datetime(pdf['Date'])

        # Create Spark DataFrame
        df = spark.createDataFrame(pdf)
        return df

    except Exception as e:
        print("Error processing data:", str(e))
        print("First few lines of data:")
        lines = data_str.splitlines()[:5]
        for line in lines:
            print(line)
        return None


def show_batch_data(df, start_idx, batch_size):
    """Show a batch of data with statistics."""
    try:
        batch_df = df.limit(batch_size).offset(start_idx)

        # Check if batch has data
        count = batch_df.count()
        if count > 0:
            print("\nBatch", (start_idx // batch_size + 1))
            print("-" * 100)

            # Show data
            batch_df.show(truncate=False)

            # Calculate batch statistics
            stats = batch_df.agg(
                F.min("Date").alias("Start_Date"),
                F.max("Date").alias("End_Date"),
                F.round(F.avg("Close"), 2).alias("Avg_Close"),
                F.round(F.sum("Volume"), 0).alias("Total_Volume")
            )

            print("\nBatch Statistics:")
            stats.show(truncate=False)
            print("-" * 100)
        else:
            print("\nNo data in batch", (start_idx // batch_size + 1))

    except Exception as e:
        print("Error showing batch data:", str(e))


def main():
    try:
        # Read data using docker exec
        print("Reading data from HDFS using docker exec...")
        data = read_from_hdfs()

        if data is None:
            print("Failed to read data from HDFS")
            return

        # Print first few lines for debugging
        print("\nFirst few lines of data:")
        lines = data.splitlines()[:5]
        for line in lines:
            print(line)

        # Create Spark session
        spark = create_spark_session()

        # Process data
        df = process_data(spark, data)

        if df is None:
            print("Failed to process data")
            return

        # Get total count
        total_rows = df.count()
        print("\nTotal number of records:", total_rows)

        if total_rows == 0:
            print("No records found in the DataFrame")
            return

        # Show data in batches
        batch_size = 5
        for batch_num in range(0, total_rows, batch_size):
            show_batch_data(df, batch_num, batch_size)

            # Ask user if they want to continue
            if batch_num + batch_size < total_rows:
                response = input("\nPress Enter to see next batch (or 'q' to quit): ")
                if response.lower() == 'q':
                    break

        # Show overall statistics
        print("\nOverall Statistics:")
        df.select("Open", "High", "Low", "Close", "Volume").summary().show()

    except Exception as e:
        print("Error in main:", str(e))
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()