import logging
import os
import requests
from typing import Optional, List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from hdfs import InsecureClient


class HDFSSparkReader:
    def __init__(
            self,
            namenode_host: str = "localhost",
            namenode_webhdfs_port: int = 9870,
            datanode_host: str = "localhost",
            datanode_port: int = 9864,
            hdfs_user: str = "root"
    ):
        """
        Initialize Spark Reader for HDFS with host port mappings.
        """
        self.namenode_host = namenode_host
        self.namenode_webhdfs_port = namenode_webhdfs_port
        self.datanode_host = datanode_host
        self.datanode_port = datanode_port
        self.hdfs_user = hdfs_user
        self.spark = None
        self.hdfs_client = None
        self._setup_logging()
        self._setup_hdfs_client()

    def _setup_logging(self):
        """Configure logging for the reader."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('HDFSSparkReader')

    def _setup_hdfs_client(self):
        """Setup HDFS client for metadata operations."""
        try:
            hdfs_url = f"http://{self.namenode_host}:{self.namenode_webhdfs_port}"
            self.hdfs_client = InsecureClient(
                url=hdfs_url,
                user=self.hdfs_user,
                root='/'
            )
            self.logger.info("HDFS client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize HDFS client: {str(e)}")

    def _verify_file_exists(self, hdfs_path: str) -> bool:
        """Verify if file exists in HDFS using WebHDFS."""
        try:
            status = self.hdfs_client.status(hdfs_path)
            return status is not None and status['type'] == 'FILE'
        except Exception as e:
            self.logger.error(f"Error verifying file existence: {str(e)}")
            return False

    def _create_spark_session(self) -> bool:
        """
        Create and configure Spark session with HDFS configurations.
        """
        try:
            self.logger.info("Creating Spark session...")

            # Basic Hadoop configuration
            hadoop_conf = {
                'fs.defaultFS': f'hdfs://{self.namenode_host}:8020',  # Use default HDFS port
                'dfs.client.use.datanode.hostname': 'false',
                'dfs.datanode.use.datanode.hostname': 'false',
                'dfs.client.read.shortcircuit': 'false',
                'dfs.client.read.shortcircuit.skip.checksum': 'false',
                'dfs.replication': '1',
                # Increase timeout and buffer sizes
                'dfs.client.socket-timeout': '120000',
                'dfs.client.socket.send.buffer.size': '4194304',
                'dfs.client.socket.receive.buffer.size': '4194304',
                'ipc.maximum.data.length': '134217728'  # 128MB
            }

            # Create SparkSession
            self.spark = (SparkSession.builder
                          .appName("HDFSSparkReader")
                          .master("local[*]")
                          # Memory configuration
                          .config("spark.driver.memory", "4g")
                          .config("spark.executor.memory", "4g")
                          .config("spark.memory.offHeap.enabled", "true")
                          .config("spark.memory.offHeap.size", "2g")
                          # Performance tuning
                          .config("spark.sql.files.maxPartitionBytes", "134217728")
                          .config("spark.sql.adaptive.enabled", "true")
                          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                          .config("spark.sql.shuffle.partitions", "10"))

            # Add all Hadoop configurations
            for key, value in hadoop_conf.items():
                self.spark = self.spark.config(f"spark.hadoop.{key}", value)

            self.spark = self.spark.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")

            self.logger.info("Spark session created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            return False

    def read_csv(
            self,
            hdfs_path: str,
            schema: Optional[StructType] = None,
            options: Optional[Dict[str, str]] = None
    ) -> Optional[DataFrame]:
        """
        Read CSV file from HDFS into Spark DataFrame.
        """
        try:
            if not self.spark:
                if not self._create_spark_session():
                    return None

            # Verify file exists
            if not self._verify_file_exists(hdfs_path):
                self.logger.error(f"File does not exist: {hdfs_path}")
                return None

            # Use native HDFS protocol
            file_path = f"hdfs://{self.namenode_host}:8020{hdfs_path}"
            self.logger.info(f"Reading CSV from HDFS path: {file_path}")

            # Default CSV options with optimizations
            default_options = {
                "header": "true",
                "inferSchema": "true" if not schema else "false",
                "mode": "PERMISSIVE",
                "maxCharsPerColumn": "-1",
                "maxColumns": "20000",
                "multiLine": "true",
                "escape": '"',
                "quote": '"',
                "columnNameOfCorruptRecord": "_corrupt_record",
                # Performance options
                "parserLib": "univocity",
                "recursiveFileLookup": "true",
                "cleanFields": "true"
            }

            # Merge with user-provided options
            if options:
                default_options.update(options)

            # Create DataFrame reader
            reader = self.spark.read.format("csv")

            # Apply options
            for key, value in default_options.items():
                reader = reader.option(key, value)

            # Apply schema if provided
            if schema:
                reader = reader.schema(schema)

            # Load the DataFrame with repartitioning
            df = reader.load(file_path).repartition(10)

            self.logger.info(f"Successfully read CSV file. Schema: {df.schema}")
            return df

        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            return None

    def read_multiple_csv(
            self,
            hdfs_paths: List[str],
            schema: Optional[StructType] = None,
            options: Optional[Dict[str, str]] = None
    ) -> Optional[DataFrame]:
        """
        Read multiple CSV files from HDFS and union them into a single DataFrame.
        """
        try:
            dataframes = []
            for path in hdfs_paths:
                df = self.read_csv(path, schema, options)
                if df is not None:
                    dataframes.append(df)
                else:
                    self.logger.error(f"Failed to read file: {path}")

            if not dataframes:
                return None

            # Union all DataFrames
            result_df = dataframes[0]
            for df in dataframes[1:]:
                result_df = result_df.unionByName(df, allowMissingColumns=True)

            # Cache the result
            result_df.cache()

            self.logger.info(f"Successfully combined {len(dataframes)} CSV files")
            return result_df

        except Exception as e:
            self.logger.error(f"Error combining CSV files: {str(e)}")
            return None

    def close(self):
        """
        Stop Spark session and clean up resources.
        """
        if self.spark:
            try:
                self.spark.stop()
                self.logger.info("Spark session stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Spark session: {str(e)}")


def main():
    # Use the same port mappings as the uploader
    reader = HDFSSparkReader(
        namenode_host="localhost",
        namenode_webhdfs_port=9870,
        datanode_host="localhost",
        datanode_port=9864
    )

    # Read CSV files
    files = [
        # "/user/root/AMZN_kaggle.csv",
        "/user/root/AMZN_yfinance.csv"
    ]

    try:
        # Read and combine CSV files with optimized options
        options = {
            "maxCharsPerColumn": "-1",
            "maxColumns": "20000",
            "multiLine": "true",
            "parserLib": "univocity"
        }

        df = reader.read_multiple_csv(files, options=options)

        if df is not None:
            print("Successfully read CSV files")
            print(f"Total rows: {df.count()}")
            print("Schema:")
            df.printSchema()

            # Show sample data
            print("\nSample data:")
            df.show(5)
        else:
            print("Failed to read CSV files")

    finally:
        # Clean up
        reader.close()


if __name__ == "__main__":
    main()