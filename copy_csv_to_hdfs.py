import os
import logging
import requests
from hdfs import InsecureClient
from typing import Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode


class HDFSUploader:
    def __init__(
            self,
            namenode_host: str = "localhost",
            namenode_webhdfs_port: int = 9870,
            datanode_host: str = "localhost",
            datanode_port: int = 9864,
            hdfs_user: str = "root"
    ):
        """
        Initialize HDFS Uploader with host port mappings.
        """
        self.namenode_host = namenode_host
        self.namenode_webhdfs_port = namenode_webhdfs_port
        self.datanode_host = datanode_host
        self.datanode_port = datanode_port
        self.hdfs_user = hdfs_user
        self.client = None
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging for the uploader."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('HDFSUploader')

    def _modify_redirect_url(self, redirect_url: str) -> str:
        """
        Modify the redirect URL to use localhost and mapped ports.
        """
        parsed = urlparse(redirect_url)
        # Get the original query parameters
        query_params = parse_qs(parsed.query)

        # Ensure each parameter has only one value
        for key in query_params:
            if isinstance(query_params[key], list):
                query_params[key] = query_params[key][0]

        # Build new URL with localhost
        new_parsed = parsed._replace(
            scheme='http',
            netloc=f'{self.datanode_host}:{self.datanode_port}',
            query=urlencode(query_params, doseq=False)
        )

        final_url = urlunparse(new_parsed)
        self.logger.info(f"Modified redirect URL from {redirect_url} to {final_url}")
        return final_url

    def connect(self) -> bool:
        """
        Establish connection to HDFS.
        """
        try:
            hdfs_url = f"http://{self.namenode_host}:{self.namenode_webhdfs_port}"
            self.logger.info(f"Attempting to connect to {hdfs_url}")

            self.client = InsecureClient(
                url=hdfs_url,
                user=self.hdfs_user,
                root='/'
            )

            # Test connection
            self.client.status('/')
            self.logger.info("Successfully connected to HDFS")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to HDFS: {str(e)}")
            return False

    def _upload_file(self, local_path: str, hdfs_path: str, overwrite: bool = False) -> bool:
        """
        Upload a file to HDFS using direct write operation.
        """
        try:
            with open(local_path, 'rb') as file_data:
                content = file_data.read()

            # Create initial write request
            params = {
                'op': 'CREATE',
                'overwrite': str(overwrite).lower(),
                'user.name': self.hdfs_user,
                'createparent': 'true'
            }

            init_url = f"http://{self.namenode_host}:{self.namenode_webhdfs_port}/webhdfs/v1{hdfs_path}"
            self.logger.info(f"Initiating upload request to {init_url}")

            # Get redirect URL
            response = requests.put(init_url, params=params, allow_redirects=False)
            self.logger.info(f"Initial response status code: {response.status_code}")

            if response.status_code == 307:
                redirect_url = response.headers['Location']
                self.logger.info(f"Received redirect URL: {redirect_url}")

                modified_url = self._modify_redirect_url(redirect_url)
                self.logger.info(f"Uploading data to modified URL: {modified_url}")

                # Create a session with retry capability
                session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    max_retries=3,
                    pool_connections=1,
                    pool_maxsize=1
                )
                session.mount('http://', adapter)

                # Upload the file with extended timeout
                upload_response = session.put(
                    modified_url,
                    data=content,
                    timeout=60,
                    verify=False  # Disable SSL verification for local connections
                )

                if upload_response.status_code == 201:
                    self.logger.info("Upload completed successfully")
                    return True
                else:
                    self.logger.error(f"Upload failed with status code: {upload_response.status_code}")
                    self.logger.error(f"Response content: {upload_response.text}")
                    return False
            else:
                self.logger.error(f"Failed to get redirect URL. Status code: {response.status_code}")
                self.logger.error(f"Response content: {response.text}")
                return False

        except Exception as e:
            self.logger.error(f"Error in upload operation: {str(e)}")
            return False

    def copy_to_hdfs(
            self,
            local_path: str,
            hdfs_path: str,
            overwrite: bool = False,
            create_parent_dirs: bool = True
    ) -> Optional[str]:
        """
        Copy a local file to HDFS.
        """
        if not self.client:
            if not self.connect():
                return None

        self.logger.info(f"Copying {local_path} to HDFS path {hdfs_path}")

        # Validate local file
        if not os.path.exists(local_path):
            self.logger.error(f"Local file {local_path} does not exist")
            return None

        if not os.path.isfile(local_path):
            self.logger.error(f"{local_path} is not a file")
            return None

        try:
            # Create parent directories if needed
            if create_parent_dirs:
                parent_dir = os.path.dirname(hdfs_path)
                if parent_dir:
                    self.client.makedirs(parent_dir)

            # Upload file using direct method
            if self._upload_file(local_path, hdfs_path, overwrite):
                self.logger.info(f"Successfully copied file to {hdfs_path}")
                return hdfs_path
            else:
                return None

        except Exception as e:
            self.logger.error(f"Error copying file: {str(e)}")
            return None


def main():
    # Use localhost for all connections since ports are mapped
    uploader = HDFSUploader(
        namenode_host="localhost",
        namenode_webhdfs_port=9870,
        datanode_host="localhost",  # Ensure we use localhost
        datanode_port=9864  # Use the mapped port
    )

    files = [
        "AMZN_kaggle.csv",
        "AMZN_yfinance.csv"
    ]
    # local_file = "AMZN_kaggle.csv""
    hdfs_path = f"/user/root/"
    for file in files:
        result = uploader.copy_to_hdfs(
            local_path=file,
            hdfs_path=f"{hdfs_path}{file}",
            overwrite=True,
            create_parent_dirs=True
        )
        if result:
            print(f"Upload {file} successful!")
        else:
            print(f"Upload {file} failed. Check logs for details.")




if __name__ == "__main__":
    main()