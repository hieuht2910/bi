from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import pyspark.sql.functions as F
import subprocess
import pandas as pd
from io import StringIO


def read_hdfs_file():
    """Read file directly using hdfs command."""
    cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", "/user/root/AMZN.csv"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else None


def create_spark_session():
    """Create basic Spark session."""
    return (SparkSession.builder
            .appName("HDFS CSV Reader")
            .getOrCreate())


def main():
    try:
        # Read data using hdfs command
        print("Reading data from HDFS...")
        data = read_hdfs_file()

        if data is None:
            print("Failed to read file from HDFS")
            return

        # Create Spark session
        spark = create_spark_session()

        # Convert to pandas DataFrame first
        pdf = pd.read_csv(StringIO(data))

        # Convert to Spark DataFrame
        df = spark.createDataFrame(pdf)

        # Process in batches
        batch_size = 5
        total_rows = df.count()

        print(f"\nTotal records: {total_rows}")

        for batch_num in range(0, total_rows, batch_size):
            print(f"\nBatch {batch_num // batch_size + 1}:")
            print("-" * 80)

            batch_df = df.limit(batch_size).offset(batch_num)
            batch_df.show(truncate=False)

            stats = batch_df.agg(
                F.min("Date").alias("Start_Date"),
                F.max("Date").alias("End_Date"),
                F.round(F.avg("Close"), 2).alias("Avg_Close"),
                F.round(F.avg("Volume"), 2).alias("Avg_Volume")
            )

            print("\nBatch Statistics:")
            stats.show(truncate=False)

        # Overall statistics
        print("\nOverall Statistics:")
        df.describe().show()

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()