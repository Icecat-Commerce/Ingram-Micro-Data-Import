"""
Configuration management for Icecat Integration.

Supports:
- YAML config file loading
- Environment variables
- CLI argument overrides (CLI takes precedence)
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    driver: str = "mysql+pymysql"
    host: str = "localhost"
    port: int = 3306
    database: str = "icecat_integration"
    username: str = "root"
    password: str = ""
    pool_size: int = 20
    max_overflow: int = 10
    ssl: bool = False

    @property
    def connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        if self.password:
            return f"{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        return f"{self.driver}://{self.username}@{self.host}:{self.port}/{self.database}"

    @property
    def connection_string_masked(self) -> str:
        """Build connection string with masked password for logging."""
        pwd = "***" if self.password else ""
        return f"{self.driver}://{self.username}:{pwd}@{self.host}:{self.port}/{self.database}"


@dataclass
class IcecatConfig:
    """Icecat FrontOffice API credentials configuration."""

    # FrontOffice credentials (for Live API access)
    front_office_username: str = ""
    front_office_password: str = ""
    front_office_api_key: str = ""

    # API access token (bypasses IP whitelisting — required for cloud environments)
    api_token: str = ""

    # FTP/SFTP credentials (for data file downloads)
    ftp_host: str = "data.icecat.biz"
    ftp_protocol: str = "ftp"  # "ftp" or "sftp"
    ftp_port: int = 0  # 0 = auto (21 for FTP, 22 for SFTP)
    ftp_username: str = ""
    ftp_password: str = ""
    ftp_timeout: int = 30

    def validate_api_credentials(self) -> None:
        """Raise SystemExit if FrontOffice API credentials are missing."""
        missing = []
        if not self.front_office_username:
            missing.append("front_office_username")
        if not self.front_office_password:
            missing.append("front_office_password")
        if not self.front_office_api_key:
            missing.append("front_office_api_key")
        if missing:
            raise SystemExit(
                f"Missing Icecat API credentials: {', '.join(missing)}. "
                f"Set them in the config file or via environment variables "
                f"(ICECAT_FO_USERNAME, ICECAT_FO_PASSWORD, ICECAT_FO_API_KEY)."
            )

    def validate_ftp_credentials(self) -> None:
        """Raise SystemExit if FTP/SFTP credentials are missing or invalid."""
        if self.ftp_protocol not in ("ftp", "sftp"):
            raise SystemExit(
                f"Invalid ftp_protocol: '{self.ftp_protocol}'. Must be 'ftp' or 'sftp'. "
                f"Set via config file or ICECAT_FTP_PROTOCOL env var."
            )
        missing = []
        if not self.ftp_username:
            missing.append("ftp_username")
        if not self.ftp_password:
            missing.append("ftp_password")
        if missing:
            raise SystemExit(
                f"Missing Icecat FTP credentials: {', '.join(missing)}. "
                f"Set them in the config file or via environment variables "
                f"(ICECAT_FTP_USERNAME, ICECAT_FTP_PASSWORD)."
            )


@dataclass
class SyncConfig:
    """Sync processing configuration."""

    batch_size: int = 100
    request_timeout: int = 300  # 5 minutes in seconds
    concurrency: int = 40  # Max concurrent API requests


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    max_file_size: int = 10 * 1024 * 1024  # 10 MB
    backup_count: int = 5


@dataclass
class AppConfig:
    """Main application configuration."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    icecat: IcecatConfig = field(default_factory=IcecatConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_yaml(cls, config_path: Path) -> "AppConfig":
        """Load configuration from YAML file."""
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}
        return cls._from_dict(data)

    @classmethod
    def _from_dict(cls, data: dict) -> "AppConfig":
        """Create config from dictionary."""
        return cls(
            database=DatabaseConfig(**data.get("database", {})),
            icecat=IcecatConfig(**data.get("icecat", {})),
            sync=SyncConfig(**data.get("sync", data.get("scheduling", {}))),
            logging=LoggingConfig(**data.get("logging", {})),
        )

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load configuration from environment variables."""
        return cls(
            database=DatabaseConfig(
                driver=os.getenv("DB_DRIVER", "mysql+pymysql"),
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", "3306")),
                database=os.getenv("DB_NAME", "icecat_integration"),
                username=os.getenv("DB_USER", "root"),
                password=os.getenv("DB_PASSWORD", ""),
                pool_size=int(os.getenv("DB_POOL_SIZE", "20")),
                max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
                ssl=os.getenv("DB_SSL", "false").lower() in ("true", "1", "yes"),
            ),
            icecat=IcecatConfig(
                front_office_username=os.getenv("ICECAT_FO_USERNAME", ""),
                front_office_password=os.getenv("ICECAT_FO_PASSWORD", ""),
                front_office_api_key=os.getenv("ICECAT_FO_API_KEY", ""),
                api_token=os.getenv("ICECAT_API_TOKEN", ""),
                ftp_host=os.getenv("ICECAT_FTP_HOST", "data.icecat.biz"),
                ftp_protocol=os.getenv("ICECAT_FTP_PROTOCOL", "ftp"),
                ftp_port=int(os.getenv("ICECAT_FTP_PORT", "0")),
                ftp_username=os.getenv("ICECAT_FTP_USERNAME", ""),
                ftp_password=os.getenv("ICECAT_FTP_PASSWORD", ""),
                ftp_timeout=int(os.getenv("ICECAT_FTP_TIMEOUT", "30")),
            ),
            sync=SyncConfig(
                batch_size=int(os.getenv("BATCH_SIZE", "100")),
                request_timeout=int(os.getenv("REQUEST_TIMEOUT", "300")),
                concurrency=int(os.getenv("SYNC_CONCURRENCY", "40")),
            ),
            logging=LoggingConfig(
                level=os.getenv("LOG_LEVEL", "INFO"),
                file_path=os.getenv("LOG_FILE_PATH"),
            ),
        )

    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> "AppConfig":
        """
        Load configuration with priority:
        1. Config file (if provided)
        2. Environment variables (fallback for missing values)
        """
        if config_path and config_path.exists():
            config = cls.from_yaml(config_path)
        else:
            config = cls.from_env()

        # Override with environment variables if set
        config = cls._apply_env_overrides(config)
        return config

    @classmethod
    def _apply_env_overrides(cls, config: "AppConfig") -> "AppConfig":
        """Apply environment variable overrides to existing config."""
        # Database overrides
        if os.getenv("DB_HOST"):
            config.database.host = os.getenv("DB_HOST", config.database.host)
        if os.getenv("DB_PORT"):
            config.database.port = int(os.getenv("DB_PORT", str(config.database.port)))
        if os.getenv("DB_NAME"):
            config.database.database = os.getenv("DB_NAME", config.database.database)
        if os.getenv("DB_USER"):
            config.database.username = os.getenv("DB_USER", config.database.username)
        if os.getenv("DB_PASSWORD"):
            config.database.password = os.getenv("DB_PASSWORD", config.database.password)
        if os.getenv("DB_POOL_SIZE"):
            config.database.pool_size = int(os.getenv("DB_POOL_SIZE", str(config.database.pool_size)))
        if os.getenv("DB_MAX_OVERFLOW"):
            config.database.max_overflow = int(os.getenv("DB_MAX_OVERFLOW", str(config.database.max_overflow)))
        if os.getenv("DB_SSL"):
            config.database.ssl = os.getenv("DB_SSL", "false").lower() in ("true", "1", "yes")

        # Icecat FrontOffice overrides
        if os.getenv("ICECAT_FO_USERNAME"):
            config.icecat.front_office_username = os.getenv(
                "ICECAT_FO_USERNAME", config.icecat.front_office_username
            )
        if os.getenv("ICECAT_FO_PASSWORD"):
            config.icecat.front_office_password = os.getenv(
                "ICECAT_FO_PASSWORD", config.icecat.front_office_password
            )
        if os.getenv("ICECAT_FO_API_KEY"):
            config.icecat.front_office_api_key = os.getenv(
                "ICECAT_FO_API_KEY", config.icecat.front_office_api_key
            )

        if os.getenv("ICECAT_API_TOKEN"):
            config.icecat.api_token = os.getenv(
                "ICECAT_API_TOKEN", config.icecat.api_token
            )

        # Icecat FTP overrides
        if os.getenv("ICECAT_FTP_HOST"):
            config.icecat.ftp_host = os.getenv(
                "ICECAT_FTP_HOST", config.icecat.ftp_host
            )
        if os.getenv("ICECAT_FTP_PROTOCOL"):
            config.icecat.ftp_protocol = os.getenv(
                "ICECAT_FTP_PROTOCOL", config.icecat.ftp_protocol
            )
        if os.getenv("ICECAT_FTP_PORT"):
            config.icecat.ftp_port = int(
                os.getenv("ICECAT_FTP_PORT", str(config.icecat.ftp_port))
            )
        if os.getenv("ICECAT_FTP_USERNAME"):
            config.icecat.ftp_username = os.getenv(
                "ICECAT_FTP_USERNAME", config.icecat.ftp_username
            )
        if os.getenv("ICECAT_FTP_PASSWORD"):
            config.icecat.ftp_password = os.getenv(
                "ICECAT_FTP_PASSWORD", config.icecat.ftp_password
            )
        if os.getenv("ICECAT_FTP_TIMEOUT"):
            config.icecat.ftp_timeout = int(
                os.getenv("ICECAT_FTP_TIMEOUT", str(config.icecat.ftp_timeout))
            )

        # Sync overrides
        if os.getenv("BATCH_SIZE"):
            config.sync.batch_size = int(os.getenv("BATCH_SIZE", str(config.sync.batch_size)))
        if os.getenv("REQUEST_TIMEOUT"):
            config.sync.request_timeout = int(os.getenv("REQUEST_TIMEOUT", str(config.sync.request_timeout)))
        if os.getenv("SYNC_CONCURRENCY"):
            config.sync.concurrency = int(os.getenv("SYNC_CONCURRENCY", str(config.sync.concurrency)))
        # Logging overrides
        if os.getenv("LOG_LEVEL"):
            config.logging.level = os.getenv("LOG_LEVEL", config.logging.level)
        if os.getenv("LOG_FILE_PATH"):
            config.logging.file_path = os.getenv("LOG_FILE_PATH", config.logging.file_path)

        return config
