from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import pyspark.sql.functions as F
import os
import subprocess
from io import StringIO
import pandas as pd

from import_es import publish_to_elasticsearch


def create_spark_session():
    """Create Spark session with proper HDFS configurations."""
    os.environ['HADOOP_USER_NAME'] = 'root'

    return (SparkSession.builder
            .appName("HDFS CSV Reader")
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
        # divide column by 20
        df = df.withColumn("Open", F.col("Open"))
        df = df.withColumn("High", F.col("High"))
        df = df.withColumn("Low", F.col("Low"))
        df = df.withColumn("Close", F.col("Close"))
        df = df.withColumn("Adj Close", F.col("Adj Close"))
        df = df.withColumn("Return", (F.col("Close") - F.col("Open")) / F.col("Open"))
        df = df.withColumn("AVG Price", (F.col("Open") + F.col("Close")) / 2)
        df = df.withColumn("Close Pct Change", (F.col("Close") - F.col("Open")) / F.col("Open") * 100)

        # df.show(truncate=False)
        return df

    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        return None

def show_batch(df, batch_num, batch_size):
    """Display a batch of data with statistics."""
    try:
        start_idx = batch_num * batch_size
        batch_df = df.limit(batch_size).offset(start_idx)

        print(f"\nBatch {batch_num + 1}:")
        print("-" * 100)

        # Show batch data
        batch_df.show(truncate=False)

        # Calculate statistics
        stats = batch_df.agg(
            F.min("Date").alias("Start_Date"),
            F.max("Date").alias("End_Date"),
            F.round(F.avg("Close"), 2).alias("Avg_Close"),
            F.round(F.sum("Volume"), 0).alias("Total_Volume")
        )

        print("\nBatch Statistics:")
        stats.show(truncate=False)
        print("-" * 100)

        return batch_df.count() > 0

    except Exception as e:
        print(f"Error showing batch: {str(e)}")
        return False


def analyze_data(df):
    """Perform various analyses on the data."""
    print("\nOverall Statistics:")
    df.select("Open", "High", "Low", "Close", "Volume").summary().show()

    print("\nMonthly Analysis:")
    (df.withColumn("Month", F.date_format("Date", "yyyy-MM"))
     .groupBy("Month")
     .agg(
        F.round(F.avg("Close"), 2).alias("Avg_Close"),
        F.round(F.sum("Volume"), 0).alias("Total_Volume")
    )
     .orderBy("Month")
     .show(truncate=False))

    print("\nPrice Movement Analysis:")
    df = df.withColumn(
        "Daily_Return",
        F.round(((F.col("Close") - F.col("Open")) / F.col("Open") * 100), 2)
    )

    movement_stats = df.agg(
        F.round(F.avg("Daily_Return"), 2).alias("Avg_Daily_Return_%"),
        F.round(F.min("Daily_Return"), 2).alias("Max_Daily_Loss_%"),
        F.round(F.max("Daily_Return"), 2).alias("Max_Daily_Gain_%")
    )

    print("Daily Returns Statistics:")
    movement_stats.show()


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

        df = transform_data(df)

        pandas_df = df.toPandas()
        for col in pandas_df.select_dtypes(include=['datetime64']).columns:
            pandas_df[col] = pandas_df[col].astype(str)

            # Convert DataFrame to list of dictionaries
        records = pandas_df.to_dict('records')
        print(records)
        publish_to_elasticsearch(records)
        # Perform analysis
        # analyze_data(df)

    except Exception as e:
        print(f"Error in main: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()