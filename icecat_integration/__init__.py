"""
Icecat Integration - Python application for fetching product data from Icecat.

This package provides:
- XML data fetching (daily index, feature groups, features list, categories)
- FrontOffice JSON API fetching (complete product data in single request)
- Database persistence with SQLAlchemy ORM
- CLI interface for command-line usage
"""

__version__ = "1.0.0"
__author__ = "Icecat Commerce"

from .config import AppConfig, DatabaseConfig, IcecatConfig
from .api import IcecatXmlDataService, IcecatJsonDataFetchService
from .mappers import IcecatLanguageMapper

__all__ = [
    "AppConfig",
    "DatabaseConfig",
    "IcecatConfig",
    "IcecatXmlDataService",
    "IcecatJsonDataFetchService",
    "IcecatLanguageMapper",
]
