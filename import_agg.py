from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
import subprocess
from io import StringIO
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import pandas as pd
from datetime import datetime, timedelta
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


def query_opensearch_data(index_name="amazon_stock_2", host='localhost', port=9200, size=10000):
    """
    Query data from OpenSearch and convert to DataFrame
    """
    try:
        # Initialize OpenSearch client
        es = Elasticsearch(
            ['http://192.168.200.161:9200'],
            basic_auth=('elastic', 'hust@2024')
        )

        # Query to get all data sorted by date
        query = {
            "query": {
                "match_all": {}
            },
            "sort": [
                {"Date": {"order": "desc"}}
            ],
            "size": size  # Adjust based on your data size
        }

        # Execute query
        response = es.search(index=index_name, body=query)

        # Extract data from response
        hits = response['hits']['hits']

        # Convert to list of dictionaries
        data = []
        for hit in hits:
            source = hit['_source']
            data.append({
                'Date': source.get('Date'),
                'Open': float(source.get('Open', 0)),
                'High': float(source.get('High', 0)),
                'Low': float(source.get('Low', 0)),
                'Close': float(source.get('Close', 0)),
                'Adj Close': float(source.get('Adj Close', 0)),
                'Volume': int(source.get('Volume', 0)),
                'AVG Price': float(source.get('AVG Price', 0)),
                'Return': float(source.get('Return', 0)),
                'Close Pct Change': float(source.get('Close Pct Change', 0))
            })

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Convert Date to datetime
        df['Date'] = pd.to_datetime(df['Date'])

        # Sort by date
        df = df.sort_values('Date')

        print(f"Retrieved {len(df)} records")
        # print("\nDataFrame head:")
        # print(df.head())
        # print("\nDataFrame info:")
        # print(df.info())

        return df

    except Exception as e:
        print(f"Error querying OpenSearch: {str(e)}")
        return None


def query_opensearch_date_range(start_date, end_date, index_name="amazon_stock_2", host='localhost', port=9200):
    """
    Query data from OpenSearch for a specific date range
    """
    try:
        es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])

        query = {
            "query": {
                "range": {
                    "Date": {
                        "gte": start_date,
                        "lte": end_date,
                        "format": "yyyy-MM-dd"
                    }
                }
            },
            "sort": [
                {"Date": {"order": "asc"}}
            ],
            "size": 10000  # Adjust based on your needs
        }

        response = es.search(index=index_name, body=query)
        hits = response['hits']['hits']

        data = []
        for hit in hits:
            source = hit['_source']
            data.append({
                'Date': source.get('Date'),
                'Open': float(source.get('Open', 0)),
                'High': float(source.get('High', 0)),
                'Low': float(source.get('Low', 0)),
                'Close': float(source.get('Close', 0)),
                'Adj Close': float(source.get('Adj Close', 0)),
                'Volume': int(source.get('Volume', 0)),
                'AVG Price': float(source.get('AVG Price', 0)),
                'Close Pct Change': float(source.get('Close Pct Change', 0))
            })

        df = pd.DataFrame(data)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')

        return df

    except Exception as e:
        print(f"Error querying OpenSearch: {str(e)}")
        return None


# Example usage with date range query
def analyze_stock_data():
    """
    Query and analyze stock data
    """
    try:
        # Get last 30 days of data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        print(f"Querying data from {start_date} to {end_date}")

        # Get data from OpenSearch
        df = query_opensearch_date_range(start_date, end_date)

        if df is None or df.empty:
            print("No data retrieved")
            return

        # Basic analysis
        print("\nBasic Statistics:")
        print(df.describe())

        # Calculate daily returns
        df['Daily Return'] = df['Close'].pct_change()

        # Calculate moving averages
        df['MA5'] = df['Close'].rolling(window=5).mean()
        df['MA20'] = df['Close'].rolling(window=20).mean()

        # Calculate volatility
        df['Volatility'] = df['Daily Return'].rolling(window=20).std() * (252 ** 0.5)

        print("\nAnalysis Results:")
        print(f"Average Daily Return: {df['Daily Return'].mean():.4f}")
        print(f"Volatility: {df['Volatility'].mean():.4f}")
        print(f"Max Close: {df['Close'].max():.2f}")
        print(f"Min Close: {df['Close'].min():.2f}")

        return df

    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        return None
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

def prepare_elasticsearch_data(stats_dict):
    """
    Prepare statistics data for Elasticsearch insertion
    """
    try:
        result = {
            'monthly_stats': [],
            'quarterly_stats': [],
            'yearly_stats': []
        }

        # Process each time period
        for period, df in stats_dict.items():
            # Convert to pandas and round numeric columns
            pandas_df = df.toPandas()

            # Handle NULL values and convert to proper types
            for record in pandas_df.to_dict('records'):
                # Convert NaN and NULL to None for JSON serialization
                processed_record = {}
                for key, value in record.items():
                    if pd.isna(value) or value == 'NULL':
                        processed_record[key] = None

                # Add period type
                processed_record['period_type'] = period
                result[f'{period}_stats'].append(processed_record)

        return result

    except Exception as e:
        print(f"Error preparing data: {str(e)}")
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
        ).orderBy("Month")

        # Calculate Quarterly Statistics
        quarterly_stats = df.groupBy("Quarter").agg(
            F.avg("Close").alias("Avg_Close"),
            F.sum("Volume").alias("Total_Volume"),
            F.avg("High").alias("Avg_High"),
            F.avg("Low").alias("Avg_Low"),
            F.avg("Open").alias("Avg_Open"),
            F.stddev("Close").alias("Std_Close"),
            F.count("*").alias("Trading_Days")
        ).orderBy("Quarter")

        # Calculate Yearly Statistics
        yearly_stats = df.groupBy("Year").agg(
            F.avg("Close").alias("Avg_Close"),
            F.sum("Volume").alias("Total_Volume"),
            F.avg("High").alias("Avg_High"),
            F.avg("Low").alias("Avg_Low"),
            F.avg("Open").alias("Avg_Open"),
            F.stddev("Close").alias("Std_Close"),
            F.count("*").alias("Trading_Days")
        ).orderBy("Year")

        # Calculate Year-over-Year growth
        window_spec = Window.orderBy("Year")
        yearly_stats = yearly_stats.withColumn(
            "YoY_Growth",
            ((F.col("Avg_Close") - F.lag("Avg_Close").over(window_spec)) /
             F.lag("Avg_Close").over(window_spec) * 100)
        )

        return {
            'monthly': monthly_stats,
            'quarterly': quarterly_stats,
            'yearly': yearly_stats
        }

    except Exception as e:
        print(f"Error calculating statistics: {str(e)}")
        return None

def analyze_data(df):
    """Perform various analyses on the data."""
    # print("\nOverall Statistics:")
    # df.select("Open", "High", "Low", "Close", "Volume").summary().show()

    print("\nMonthly Analysis:")
    df_monthly_analysis = (df.withColumn("Month", F.date_format("Date", "yyyy-MM"))
     .groupBy("Month")
     .agg(
        F.round(F.avg("Close"), 2).alias("Avg_Close"),
        F.round(F.sum("Volume"), 0).alias("Total_Volume")
    )
     .orderBy("Month"))
    df_monthly_analysis.show(truncate=False)

    # print("\nPrice Movement Analysis:")
    # df = df.withColumn(
    #     "Daily_Return",
    #     F.round(((F.col("Close") - F.col("Open")) / F.col("Open") * 100), 2)
    # )
    #
    # movement_stats = df.agg(
    #     F.round(F.avg("Daily_Return"), 2).alias("Avg_Daily_Return_%"),
    #     F.round(F.min("Daily_Return"), 2).alias("Max_Daily_Loss_%"),
    #     F.round(F.max("Daily_Return"), 2).alias("Max_Daily_Gain_%")
    # )
    #
    # print("Daily Returns Statistics:")
    # movement_stats.show()

def insert_statistics_to_elastic(stats_data):
    """
    Insert statistics into different Elasticsearch indices
    """
    try:
        # Initialize Elasticsearch client

        es = Elasticsearch(
            ['http://192.168.200.161:9200'],
            basic_auth=('elastic', 'hust@2024')
        )

        # es = Elasticsearch(
        #     ['http://localhost:9200'],
        #     basic_auth=('elastic', 'changeme')
        # )

        # Index mappings for different statistics
        mappings = {
            'amazon_stock_monthly_stats': {
                'properties': {
                    'Month': {'type': 'date', 'format': 'yyyy-MM'},
                    'Avg_Close': {'type': 'float'},
                    'Total_Volume': {'type': 'long'},
                    'Avg_High': {'type': 'float'},
                    'Avg_Low': {'type': 'float'},
                    'Avg_Open': {'type': 'float'},
                    'Std_Close': {'type': 'float'},
                    'Trading_Days': {'type': 'integer'}
                }
            },
            'amazon_stock_quarterly_stats': {
                'properties': {
                    'Quarter': {'type': 'keyword'},
                    'Avg_Close': {'type': 'float'},
                    'Total_Volume': {'type': 'long'},
                    'Avg_High': {'type': 'float'},
                    'Avg_Low': {'type': 'float'},
                    'Avg_Open': {'type': 'float'},
                    'Std_Close': {'type': 'float'},
                    'Trading_Days': {'type': 'integer'}
                }
            },
            'amazon_stock_yearly_stats': {
                'properties': {
                    'Year': {'type': 'integer'},
                    'Avg_Close': {'type': 'float'},
                    'Total_Volume': {'type': 'long'},
                    'Avg_High': {'type': 'float'},
                    'Avg_Low': {'type': 'float'},
                    'Avg_Open': {'type': 'float'},
                    'Std_Close': {'type': 'float'},
                    'Trading_Days': {'type': 'integer'},
                    'YoY_Growth': {'type': 'float'}
                }
            }
        }

        # Create indices with mappings
        for index_name, mapping in mappings.items():
            if not es.indices.exists(index=index_name):
                es.indices.create(index=index_name, body={'mappings': mapping})

        # Insert data for each zperiod
        for period, records in stats_data.items():
            index_name = f'amazon_stock_{period}'
            actions = [
                {
                    '_index': index_name,
                    '_source': record
                }
                for record in records
            ]

            # Bulk insert with progress tracking
            success, failed = 0, 0
            for i in range(0, len(actions), 1000):
                batch = actions[i:i + 1000]
                try:
                    response = helpers.bulk(es, batch)
                    success += response[0]
                    if response[1]:
                        failed += len(response[1])
                    print(f"{index_name} progress: {i + len(batch)}/{len(actions)}")
                except Exception as e:
                    print(f"Error in batch: {str(e)}")
                    failed += len(batch)

            print(f"\n{index_name} insertion complete:")
            print(f"Successfully inserted: {success} records")
            print(f"Failed to insert: {failed} records")

    except Exception as e:
        print(f"Error inserting to Elasticsearch: {str(e)}")


def main():
    try:
        # Create Spark session
        spark = create_spark_session()

        # Process data

        pandas_df = query_opensearch_data()
        if pandas_df is not None:
            print("\nAll Data:")
            print(f"\nTotal number of records: {pandas_df.count()}")
            # print(df_all.describe())

        if pandas_df is not None:
            print("\nTotal records in Pandas DataFrame:", len(pandas_df))

            # Define the schema for Spark DataFrame
            schema = StructType([
                StructField("Date", DateType(), True),
                StructField("Open", DoubleType(), True),
                StructField("High", DoubleType(), True),
                StructField("Low", DoubleType(), True),
                StructField("Close", DoubleType(), True),
                StructField("Adj Close", DoubleType(), True),
                StructField("Volume", LongType(), True),
                StructField("AVG Price", DoubleType(), True),
                StructField("Return", DoubleType(), True),
                StructField("Close Pct Change", DoubleType(), True)
            ])

            # Convert pandas DataFrame to Spark DataFrame
            spark_df = spark.createDataFrame(pandas_df, schema=schema)
            print(f"\nTotal number of records: {spark_df.count()}")

            # Calculate statistics
            stats = calculate_time_statistics(spark_df)
            if not stats:
                print("Failed to calculate statistics")
                return

            # Show sample of statistics
            print("\nSample Statistics:")
            for period, stat_df in stats.items():
                print(f"\n{period.capitalize()} Statistics:")
                stat_df.show(5)

            # Prepare data for Elasticsearch
            es_data = prepare_elasticsearch_data(stats)
            if not es_data:
                print("Failed to prepare data for Elasticsearch")
                return

            # Insert into Elasticsearch
            insert_statistics_to_elastic(es_data)

    except Exception as e:
        print(f"Error in main: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()