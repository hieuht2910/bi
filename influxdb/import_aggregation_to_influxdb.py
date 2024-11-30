from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, DoubleType, DateType, IntegerType, StringType
import pyspark.sql.functions as F
import os
import subprocess
from io import StringIO
import pandas as pd
from datetime import datetime

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import urllib3


def create_spark_session():
    """Create Spark session with proper HDFS configurations."""
    os.environ['HADOOP_USER_NAME'] = 'root'

    return (SparkSession.builder
            .appName("HDFS Stock Data to InfluxDB")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
            .config("spark.hadoop.dfs.namenode.http-address", "localhost:9870")
            .config("spark.hadoop.dfs.namenode.rpc-address", "localhost:9000")
            .config("spark.hadoop.dfs.datanode.http.address", "localhost:9864")
            .config("spark.hadoop.dfs.datanode.address", "localhost:9866")
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


def calculate_time_statistics(df):
    """
    Calculate statistics for different time periods (monthly, quarterly, yearly)
    """
    try:
        # Convert Date to proper format if needed
        df = df.withColumn("Date", F.to_date("Date", "yyyy-MM-dd"))

        # Extract time components
        df = df.withColumn("Year", F.year("Date"))
        df = df.withColumn("Month", F.date_format("Date", "yyyy-MM"))
        df = df.withColumn("Quarter", F.concat(
            F.year("Date").cast("string"),
            F.lit("-Q"),
            F.quarter("Date").cast("string")
        ))

        # Calculate Monthly Statistics
        monthly_stats = df.groupBy("Month").agg(
            F.avg("Close").alias("Avg_Close"),
            F.sum("Volume").alias("Total_Volume"),
            F.avg("High").alias("Avg_High"),
            F.avg("Low").alias("Avg_Low"),
            F.avg("Open").alias("Avg_Open"),
            F.stddev("Close").alias("Std_Close"),
            F.count("*").alias("Trading_Days")
        ).withColumn("Period_Type", F.lit("Monthly"))

        # Calculate Quarterly Statistics
        quarterly_stats = df.groupBy("Quarter").agg(
            F.avg("Close").alias("Avg_Close"),
            F.sum("Volume").alias("Total_Volume"),
            F.avg("High").alias("Avg_High"),
            F.avg("Low").alias("Avg_Low"),
            F.avg("Open").alias("Avg_Open"),
            F.stddev("Close").alias("Std_Close"),
            F.count("*").alias("Trading_Days")
        ).withColumn("Period_Type", F.lit("Quarterly"))

        # Calculate Yearly Statistics with Year-over-Year growth
        window_spec = Window.orderBy("Year")
        yearly_stats = df.groupBy("Year").agg(
            F.avg("Close").alias("Avg_Close"),
            F.sum("Volume").alias("Total_Volume"),
            F.avg("High").alias("Avg_High"),
            F.avg("Low").alias("Avg_Low"),
            F.avg("Open").alias("Avg_Open"),
            F.stddev("Close").alias("Std_Close"),
            F.count("*").alias("Trading_Days")
        ).withColumn(
            "YoY_Growth",
            ((F.col("Avg_Close") - F.lag("Avg_Close").over(window_spec)) /
             F.lag("Avg_Close").over(window_spec) * 100)
        ).withColumn("Period_Type", F.lit("Yearly"))

        return {
            'monthly': monthly_stats,
            'quarterly': quarterly_stats,
            'yearly': yearly_stats
        }

    except Exception as e:
        print(f"Error calculating statistics: {str(e)}")
        return None


def publish_to_influxdb(monthly_data, quarterly_data, yearly_data):
    """
    Publish aggregated statistics to InfluxDB

    :param monthly_data: Monthly statistics DataFrame
    :param quarterly_data: Quarterly statistics DataFrame
    :param yearly_data: Yearly statistics DataFrame
    """
    # InfluxDB connection parameters
    url = "http://192.168.200.161:8086"  # Default InfluxDB URL
    token = ""
    org = "hust"  # Replace with your organization name

    try:
        # Disable SSL warnings if needed
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Create InfluxDB client
        client = InfluxDBClient(
            url=url,
            token=token,
            org=org,
            timeout=30_000,
            verify_ssl=False
        )

        # Create write API
        write_api = client.write_api(
            write_options=SYNCHRONOUS,
            timeout=30_000
        )

        # Buckets for different aggregation levels
        buckets = {
            'bi_amazon_monthly_stats': monthly_data,
            'bi_amazon_quarterly_stats': quarterly_data,
            'bi_amazon_yearly_stats': yearly_data
        }

        # Process each bucket
        for bucket_name, df in buckets.items():
            # Convert to Pandas for easier iteration
            pandas_df = df.toPandas()

            # Batch points to reduce network overhead
            batch_size = 1000
            for i in range(0, len(pandas_df), batch_size):
                batch = pandas_df.iloc[i:i + batch_size]
                points = []

                for _, record in batch.iterrows():
                    # Create a point for each record
                    point = Point("stock_aggregated_stats") \
                        .tag("symbol", "AMZN") \
                        .tag("period_type", record['Period_Type'])

                    # Add period identifier based on bucket type
                    if bucket_name == 'bi_amazon_monthly_stats':
                        point.tag("month", record['Month'])
                    elif bucket_name == 'bi_amazon_quarterly_stats':
                        point.tag("quarter", record['Quarter'])
                    elif bucket_name == 'bi_amazon_yearly_stats':
                        point.tag("year", str(record['Year']))

                    # Add numeric fields
                    point.field("avg_close", float(record['Avg_Close']))
                    point.field("total_volume", float(record['Total_Volume']))
                    point.field("avg_high", float(record['Avg_High']))
                    point.field("avg_low", float(record['Avg_Low']))
                    point.field("avg_open", float(record['Avg_Open']))
                    point.field("std_close", float(record['Std_Close']))
                    point.field("trading_days", int(record['Trading_Days']))

                    # Add YoY Growth for yearly stats if available
                    if bucket_name == 'yearly_stats' and not pd.isna(record.get('YoY_Growth')):
                        point.field("yoy_growth", float(record['YoY_Growth']))

                    # Set timestamp (use the first day of the period)
                    if bucket_name == 'monthly_stats':
                        point.time(datetime.strptime(record['Month'] + "-01", "%Y-%m-%d"))
                    elif bucket_name == 'quarterly_stats':
                        # Parse quarter and use first day of first month
                        year, quarter = record['Quarter'].split('-Q')
                        month = {'1': '01', '2': '04', '3': '07', '4': '10'}[quarter]
                        point.time(datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d"))
                    elif bucket_name == 'yearly_stats':
                        point.time(datetime.strptime(f"{record['Year']}-01-01", "%Y-%m-%d"))

                    points.append(point)

                # Write points to InfluxDB
                write_api.write(bucket=bucket_name, org=org, record=points)
                print(f"Successfully wrote {len(points)} points to {bucket_name}")

    except Exception as e:
        print(f"Error publishing to InfluxDB: {str(e)}")
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

        # Calculate statistics
        stats = calculate_time_statistics(df)
        if not stats:
            print("Failed to calculate statistics")
            return

        # Publish to InfluxDB
        publish_to_influxdb(
            stats['monthly'],
            stats['quarterly'],
            stats['yearly']
        )

    except Exception as e:
        print(f"Error in main: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()