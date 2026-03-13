"""FTP/SFTP service for downloading Icecat data files."""

import ftplib
import logging
import socket
import stat
import zipfile
from pathlib import Path
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)

# Default ports per protocol
_DEFAULT_PORTS = {"ftp": 21, "sftp": 22}


class IcecatFTPService:
    """
    Service for connecting to Icecat FTP/SFTP and downloading data files.

    Supports both FTP (ftplib) and SFTP (paramiko) backends, controlled by
    the ``protocol`` parameter.

    Files available:
    - /Ingram_m/DatasheetSKUGlobal_Coverage_1.zip: Product assortment (Brand + MPN)
    - Daily index files
    - Category/Brand master data
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        timeout: int = 30,
        protocol: str = "ftp",
        port: int = 0,
    ):
        """
        Initialize FTP/SFTP service.

        Args:
            host: Server hostname
            username: Login username
            password: Login password
            timeout: Connection timeout in seconds
            protocol: "ftp" or "sftp"
            port: Server port (0 = auto: 21 for FTP, 22 for SFTP)
        """
        self.host = host
        self.username = username
        self.password = password
        self.timeout = timeout
        self.protocol = protocol.lower()
        self.port = port if port else _DEFAULT_PORTS.get(self.protocol, 21)

        # FTP backend
        self._ftp: Optional[ftplib.FTP] = None
        # SFTP backend
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    @property
    def _is_sftp(self) -> bool:
        return self.protocol == "sftp"

    @property
    def _connected(self) -> bool:
        if self._is_sftp:
            return self._sftp is not None
        return self._ftp is not None

    def _check_connected(self) -> None:
        if not self._connected:
            raise RuntimeError(f"Not connected to {self.protocol.upper()}")

    def connect(self) -> bool:
        """
        Connect to FTP/SFTP server.

        Returns:
            True if connection successful, False otherwise
        """
        if self._is_sftp:
            return self._connect_sftp()
        return self._connect_ftp()

    def _connect_ftp(self) -> bool:
        try:
            self._ftp = ftplib.FTP()
            self._ftp.connect(self.host, self.port, timeout=self.timeout)
            self._ftp.login(self.username, self.password)
            logger.info(f"Connected to FTP: {self.host}:{self.port}")
            return True
        except ftplib.all_errors as e:
            logger.error(f"FTP connection failed: {e}")
            self._ftp = None
            return False

    def _connect_sftp(self) -> bool:
        try:
            sock = socket.create_connection(
                (self.host, self.port), timeout=self.timeout
            )
            self._transport = paramiko.Transport(sock)
            self._transport.connect(username=self.username, password=self.password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            # Apply timeout to SFTP channel for read/write operations
            self._sftp.get_channel().settimeout(self.timeout)
            logger.info(f"Connected to SFTP: {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"SFTP connection failed: {e}")
            self._transport = None
            self._sftp = None
            return False

    def disconnect(self) -> None:
        """Disconnect from FTP/SFTP server."""
        if self._is_sftp:
            self._disconnect_sftp()
        else:
            self._disconnect_ftp()

    def _disconnect_ftp(self) -> None:
        if self._ftp:
            try:
                self._ftp.quit()
            except ftplib.all_errors:
                pass
            self._ftp = None
            logger.info("Disconnected from FTP")

    def _disconnect_sftp(self) -> None:
        if self._sftp:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
            self._transport = None
        logger.info("Disconnected from SFTP")

    def list_files(self, directory: str = "/") -> list[str]:
        """
        List files in a directory (long format).

        Args:
            directory: Directory path to list

        Returns:
            List of file listing lines
        """
        self._check_connected()

        if self._is_sftp:
            return self._list_files_sftp(directory)
        return self._list_files_ftp(directory)

    def _list_files_ftp(self, directory: str) -> list[str]:
        files = []
        try:
            self._ftp.cwd(directory)
            self._ftp.retrlines("LIST", lambda x: files.append(x))
        except ftplib.all_errors as e:
            logger.error(f"Failed to list directory {directory}: {e}")
        return files

    def _list_files_sftp(self, directory: str) -> list[str]:
        files = []
        try:
            self._sftp.chdir(directory)
            for attr in self._sftp.listdir_attr():
                # Format similar to FTP LIST output
                perms = _format_permissions(attr.st_mode)
                size = attr.st_size
                name = attr.filename
                files.append(f"{perms}  1 owner group {size:>12} {name}")
        except Exception as e:
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
        self._check_connected()

        if self._is_sftp:
            return self._list_filenames_sftp(directory)
        return self._list_filenames_ftp(directory)

    def _list_filenames_ftp(self, directory: str) -> list[str]:
        files = []
        try:
            self._ftp.cwd(directory)
            self._ftp.retrlines("NLST", lambda x: files.append(x))
        except ftplib.all_errors as e:
            logger.error(f"Failed to list directory {directory}: {e}")
        return files

    def _list_filenames_sftp(self, directory: str) -> list[str]:
        files = []
        try:
            self._sftp.chdir(directory)
            files = self._sftp.listdir()
        except Exception as e:
            logger.error(f"Failed to list directory {directory}: {e}")
        return files

    def get_file_size(self, remote_path: str) -> int | None:
        """
        Get the size of a file on the server.

        Args:
            remote_path: Path to the file

        Returns:
            File size in bytes, or None if unable to get size
        """
        self._check_connected()

        if self._is_sftp:
            return self._get_file_size_sftp(remote_path)
        return self._get_file_size_ftp(remote_path)

    def _get_file_size_ftp(self, remote_path: str) -> int | None:
        try:
            return self._ftp.size(remote_path)
        except ftplib.all_errors as e:
            logger.warning(f"Could not get file size for {remote_path}: {e}")
            return None

    def _get_file_size_sftp(self, remote_path: str) -> int | None:
        try:
            return self._sftp.stat(remote_path).st_size
        except Exception as e:
            logger.warning(f"Could not get file size for {remote_path}: {e}")
            return None

    def download_file(
        self,
        remote_path: str,
        local_path: Path,
        progress_callback: Optional[callable] = None,
    ) -> bool:
        """
        Download a file from FTP/SFTP.

        Args:
            remote_path: Path to file on server
            local_path: Local path to save file
            progress_callback: Optional callback(bytes_downloaded) for progress

        Returns:
            True if download successful, False otherwise
        """
        self._check_connected()

        if self._is_sftp:
            return self._download_file_sftp(remote_path, local_path, progress_callback)
        return self._download_file_ftp(remote_path, local_path, progress_callback)

    def _download_file_ftp(
        self, remote_path: str, local_path: Path, progress_callback
    ) -> bool:
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

    def _download_file_sftp(
        self, remote_path: str, local_path: Path, progress_callback
    ) -> bool:
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)

            total_bytes = 0

            def _sftp_progress(bytes_so_far: int, _total: int) -> None:
                nonlocal total_bytes
                total_bytes = bytes_so_far
                if progress_callback:
                    progress_callback(bytes_so_far)

            with open(local_path, "wb") as f:
                self._sftp.getfo(remote_path, f, callback=_sftp_progress)

            logger.info(f"Downloaded: {remote_path} -> {local_path} ({total_bytes} bytes)")
            return True
        except Exception as e:
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
            remote_path: Path to ZIP file on server
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
        Get current working directory on server.

        Returns:
            Current directory path
        """
        self._check_connected()

        if self._is_sftp:
            cwd = self._sftp.getcwd()
            return cwd if cwd else "/"
        return self._ftp.pwd()

    def cwd(self, directory: str) -> bool:
        """
        Change working directory on server.

        Args:
            directory: Directory to change to

        Returns:
            True if successful, False otherwise
        """
        self._check_connected()

        if self._is_sftp:
            return self._cwd_sftp(directory)
        return self._cwd_ftp(directory)

    def _cwd_ftp(self, directory: str) -> bool:
        try:
            self._ftp.cwd(directory)
            return True
        except ftplib.all_errors as e:
            logger.error(f"Failed to change to directory {directory}: {e}")
            return False

    def _cwd_sftp(self, directory: str) -> bool:
        try:
            self._sftp.chdir(directory)
            return True
        except Exception as e:
            logger.error(f"Failed to change to directory {directory}: {e}")
            return False

    def __enter__(self) -> "IcecatFTPService":
        """Context manager entry - connects to FTP/SFTP."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - disconnects from FTP/SFTP."""
        self.disconnect()


def _format_permissions(mode: int | None) -> str:
    """Format a file mode as a Unix-style permission string (e.g. drwxr-xr-x)."""
    if mode is None:
        return "----------"
    is_dir = "d" if stat.S_ISDIR(mode) else "-"
    perms = ""
    for who in ("USR", "GRP", "OTH"):
        r = "r" if mode & getattr(stat, f"S_IR{who}") else "-"
        w = "w" if mode & getattr(stat, f"S_IW{who}") else "-"
        x = "x" if mode & getattr(stat, f"S_IX{who}") else "-"
        perms += r + w + x
    return is_dir + perms
