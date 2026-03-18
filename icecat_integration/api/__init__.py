"""API clients for Icecat integration."""

from .base_client import BaseHttpClient
from .xml_data_service import IcecatXmlDataService
from .json_data_fetch_service import IcecatJsonDataFetchService, FetchResult
from .xml_product_fetch_service import IcecatXmlProductFetchService, XmlFetchResult

__all__ = [
    "BaseHttpClient",
    "IcecatXmlDataService",
    "IcecatJsonDataFetchService",
    "FetchResult",
    "IcecatXmlProductFetchService",
    "XmlFetchResult",
]
