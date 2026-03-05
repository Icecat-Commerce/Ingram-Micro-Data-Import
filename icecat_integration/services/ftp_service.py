"""FTP service for downloading Icecat data files."""

import ftplib
import logging
import zipfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class IcecatFTPService:
    """
    Service for connecting to Icecat FTP and downloading data files.

    Files available:
    - DatasheetSKUCoverage_Global 3.zip: Product assortment (Brand + MPN)
    - Daily index files
    - Category/Brand master data
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        timeout: int = 30,
    ):
        """
        Initialize FTP service.

        Args:
            host: FTP server hostname
            username: FTP username
            password: FTP password
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.username = username
        self.password = password
        self.timeout = timeout
        self._ftp: Optional[ftplib.FTP] = None

    def connect(self) -> bool:
        """
        Connect to FTP server.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            self._ftp = ftplib.FTP(self.host, timeout=self.timeout)
            self._ftp.login(self.username, self.password)
            logger.info(f"Connected to FTP: {self.host}")
            return True
        except ftplib.all_errors as e:
            logger.error(f"FTP connection failed: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from FTP server."""
        if self._ftp:
            try:
                self._ftp.quit()
            except ftplib.all_errors:
                pass
            self._ftp = None
            logger.info("Disconnected from FTP")

    def list_files(self, directory: str = "/") -> list[str]:
        """
        List files in a directory.

        Args:
            directory: Directory path to list

        Returns:
            List of file listing lines
        """
        if not self._ftp:
            raise RuntimeError("Not connected to FTP")

        files = []
        try:
            self._ftp.cwd(directory)
            self._ftp.retrlines("LIST", lambda x: files.append(x))
        except ftplib.all_errors as e:
            logger.error(f"Failed to list directory {directory}: {e}")

        return files

    def list_filenames(self, directory: str = "/") -> list[str]:
        """
        List just the filenames in a directory.

        Args:
            directory: Directory path to list

        Returns:
            List of filenames
        """
        if not self._ftp:
            raise RuntimeError("Not connected to FTP")

        files = []
        try:
            self._ftp.cwd(directory)
            self._ftp.retrlines("NLST", lambda x: files.append(x))
        except ftplib.all_errors as e:
            logger.error(f"Failed to list directory {directory}: {e}")

        return files

    def get_file_size(self, remote_path: str) -> int | None:
        """
        Get the size of a file on the FTP server.

        Args:
            remote_path: Path to the file

        Returns:
            File size in bytes, or None if unable to get size
        """
        if not self._ftp:
            raise RuntimeError("Not connected to FTP")

        try:
            return self._ftp.size(remote_path)
        except ftplib.all_errors as e:
            logger.warning(f"Could not get file size for {remote_path}: {e}")
            return None

    def download_file(
        self,
        remote_path: str,
        local_path: Path,
        progress_callback: Optional[callable] = None,
    ) -> bool:
        """
        Download a file from FTP.

        Args:
            remote_path: Path to file on FTP server
            local_path: Local path to save file
            progress_callback: Optional callback(bytes_downloaded) for progress

        Returns:
            True if download successful, False otherwise
        """
        if not self._ftp:
            raise RuntimeError("Not connected to FTP")

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)

            total_bytes = 0

            def write_callback(data: bytes) -> None:
                nonlocal total_bytes
                f.write(data)
                total_bytes += len(data)
                if progress_callback:
                    progress_callback(total_bytes)

            with open(local_path, "wb") as f:
                self._ftp.retrbinary(f"RETR {remote_path}", write_callback)

            logger.info(f"Downloaded: {remote_path} -> {local_path} ({total_bytes} bytes)")
            return True
        except ftplib.all_errors as e:
            logger.error(f"Download failed for {remote_path}: {e}")
            return False

    def download_and_extract(
        self,
        remote_path: str,
        extract_dir: Path,
        keep_zip: bool = False,
    ) -> list[Path]:
        """
        Download a ZIP file and extract it.

        Args:
            remote_path: Path to ZIP file on FTP server
            extract_dir: Directory to extract files to
            keep_zip: If True, keep the downloaded ZIP file

        Returns:
            List of extracted file paths
        """
        # Download to extract directory
        zip_path = extract_dir / Path(remote_path).name
        if not self.download_file(remote_path, zip_path):
            return []

        # Extract ZIP
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
                extracted_files = [extract_dir / name for name in zf.namelist()]
            logger.info(f"Extracted {len(extracted_files)} files to {extract_dir}")
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to extract ZIP {zip_path}: {e}")
            return []

        # Optionally remove the ZIP file
        if not keep_zip and zip_path.exists():
            zip_path.unlink()
            logger.debug(f"Removed ZIP file: {zip_path}")

        return extracted_files

    def pwd(self) -> str:
        """
        Get current working directory on FTP server.

        Returns:
            Current directory path
        """
        if not self._ftp:
            raise RuntimeError("Not connected to FTP")
        return self._ftp.pwd()

    def cwd(self, directory: str) -> bool:
        """
        Change working directory on FTP server.

        Args:
            directory: Directory to change to

        Returns:
            True if successful, False otherwise
        """
        if not self._ftp:
            raise RuntimeError("Not connected to FTP")

        try:
            self._ftp.cwd(directory)
            return True
        except ftplib.all_errors as e:
            logger.error(f"Failed to change to directory {directory}: {e}")
            return False

    def __enter__(self) -> "IcecatFTPService":
        """Context manager entry - connects to FTP."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - disconnects from FTP."""
        self.disconnect()
