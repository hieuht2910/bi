import logging
from hdfs import InsecureClient
from typing import Optional, Dict, Any
from datetime import datetime

class HDFSVerifier:
    def __init__(
        self,
        namenode_host: str = "localhost",
        namenode_webhdfs_port: int = 9870,
        hdfs_user: str = "root"
    ):
        """
        Initialize HDFS Verifier.

        Args:
            namenode_host: Host for namenode
            namenode_webhdfs_port: WebHDFS port
            hdfs_user: HDFS username
        """
        self.namenode_host = namenode_host
        self.namenode_webhdfs_port = namenode_webhdfs_port
        self.hdfs_user = hdfs_user
        self.client = None
        self._setup_logging()
        self.connect()

    def _setup_logging(self):
        """Configure logging for the verifier."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('HDFSVerifier')

    def connect(self) -> bool:
        """Establish connection to HDFS."""
        try:
            hdfs_url = f"http://{self.namenode_host}:{self.namenode_webhdfs_port}"
            self.client = InsecureClient(url=hdfs_url, user=self.hdfs_user)
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to HDFS: {str(e)}")
            return False

    def get_file_info(self, hdfs_path: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a file in HDFS.

        Args:
            hdfs_path: Path to the file in HDFS

        Returns:
            Dict containing file information or None if file doesn't exist
        """
        try:
            status = self.client.status(hdfs_path)
            if status:
                # Convert timestamps to readable format
                if 'modificationTime' in status:
                    status['modificationTime'] = datetime.fromtimestamp(
                        status['modificationTime'] / 1000
                    ).strftime('%Y-%m-%d %H:%M:%S')
                if 'accessTime' in status:
                    status['accessTime'] = datetime.fromtimestamp(
                        status['accessTime'] / 1000
                    ).strftime('%Y-%m-%d %H:%M:%S')

                # Convert size to human-readable format
                if 'length' in status:
                    size_bytes = status['length']
                    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                        if size_bytes < 1024:
                            status['humanSize'] = f"{size_bytes:.2f} {unit}"
                            break
                        size_bytes /= 1024

                return status
        except Exception as e:
            self.logger.error(f"Error getting file info: {str(e)}")
        return None

    def preview_file(self, hdfs_path: str, lines: int = 5) -> Optional[str]:
        """
        Preview the content of a file in HDFS.

        Args:
            hdfs_path: Path to the file in HDFS
            lines: Number of lines to preview

        Returns:
            String containing the preview content or None if error
        """
        try:
            with self.client.read(hdfs_path) as reader:
                content = reader.read().decode('utf-8')
                preview_lines = content.split('\n')[:lines]
                return '\n'.join(preview_lines)
        except Exception as e:
            self.logger.error(f"Error previewing file: {str(e)}")
            return None

    def verify_file(self, hdfs_path: str) -> None:
        """
        Perform comprehensive file verification and print results.

        Args:
            hdfs_path: Path to the file in HDFS
        """
        # Check if file exists
        file_info = self.get_file_info(hdfs_path)
        if not file_info:
            self.logger.error(f"File {hdfs_path} not found in HDFS")
            return

        # Print file information
        print("\n=== HDFS File Information ===")
        print(f"Path: {hdfs_path}")
        print(f"Size: {file_info.get('humanSize', 'Unknown')}")
        print(f"Owner: {file_info.get('owner', 'Unknown')}")
        print(f"Group: {file_info.get('group', 'Unknown')}")
        print(f"Permission: {file_info.get('permission', 'Unknown')}")
        print(f"Replication: {file_info.get('replication', 'Unknown')}")
        print(f"Block Size: {file_info.get('blockSize', 'Unknown')}")
        print(f"Last Modified: {file_info.get('modificationTime', 'Unknown')}")
        print(f"Last Access: {file_info.get('accessTime', 'Unknown')}")

        # Preview file content
        print("\n=== File Preview (first 5 lines) ===")
        preview = self.preview_file(hdfs_path)
        if preview:
            print(preview)
        else:
            print("Unable to preview file content")

def main():
    verifier = HDFSVerifier(
        namenode_host="localhost",
        namenode_webhdfs_port=9870
    )

    hdfs_path = "/user/root/AMZN.csv"
    verifier.verify_file(hdfs_path)

if __name__ == "__main__":
    main()