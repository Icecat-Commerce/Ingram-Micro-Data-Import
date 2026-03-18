"""Icecat XML Data Service for fetching XML-based data files."""

import gzip
import logging
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
from lxml import etree

from ..config import IcecatConfig
from .base_client import BaseHttpClient

if TYPE_CHECKING:
    from pydantic_xml import BaseXmlModel

logger = logging.getLogger(__name__)

T = TypeVar("T")


class IcecatXmlDataService(BaseHttpClient):
    """
    Service for fetching XML data from Icecat.

    Uses Basic Authentication with Front Office credentials.
    Handles gzip-compressed files and XML parsing.
    """

    BASE_URL = "https://data.icecat.biz/export/level4"

    def __init__(self, config: IcecatConfig, timeout: float = 300.0):
        super().__init__(timeout=timeout, api_token=config.api_token)
        self._config = config

    @property
    def _auth(self) -> tuple[str, str]:
        """Get basic auth credentials."""
        return (
            self._config.front_office_username,
            self._config.front_office_password,
        )

    async def download_daily_index_file_async(
        self,
        culture_id: str = "EN",
    ) -> etree._Element | None:
        """
        Download the Icecat daily index file.

        The daily index contains a list of products updated in the last 24 hours.

        Args:
            culture_id: Language/culture code (e.g., "EN", "NL", "FR")

        Returns:
            Parsed XML Element or None if download fails
        """
        url = f"{self.BASE_URL}/{culture_id}/daily.index.xml"
        logger.info(f"Downloading daily index file: {url}")

        try:
            content = await self._get_with_basic_auth(
                url,
                self._auth[0],
                self._auth[1],
                decompress_gzip=False,
            )
            # Parse XML with DTD processing support
            parser = etree.XMLParser(dtd_validation=False, load_dtd=True)
            root = etree.fromstring(content, parser=parser)
            logger.info(f"Successfully downloaded daily index for culture: {culture_id}")
            return root

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error downloading daily index: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Failed to download daily index: {e}")
            return None

    async def download_feature_groups_list_async(self) -> etree._Element | None:
        """
        Download the Icecat feature groups list.

        Contains all feature groups used to categorize product features.

        Returns:
            Parsed XML Element or None if download fails
        """
        url = f"{self.BASE_URL}/refs/FeatureGroupsList.xml.gz"
        logger.info(f"Downloading feature groups list: {url}")

        try:
            content = await self._get_with_basic_auth(
                url,
                self._auth[0],
                self._auth[1],
                decompress_gzip=True,
            )
            parser = etree.XMLParser(dtd_validation=False, load_dtd=True)
            root = etree.fromstring(content, parser=parser)
            logger.info("Successfully downloaded feature groups list")
            return root

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error downloading feature groups: {e.response.status_code}"
            )
            return None
        except Exception as e:
            logger.error(f"Failed to download feature groups: {e}")
            return None

    async def download_features_list_async(self) -> etree._Element | None:
        """
        Download the Icecat features list.

        Contains all features (specifications) that can be assigned to products.

        Returns:
            Parsed XML Element or None if download fails
        """
        url = f"{self.BASE_URL}/refs/FeaturesList.xml.gz"
        logger.info(f"Downloading features list: {url}")

        try:
            content = await self._get_with_basic_auth(
                url,
                self._auth[0],
                self._auth[1],
                decompress_gzip=True,
            )
            parser = etree.XMLParser(dtd_validation=False, load_dtd=True)
            root = etree.fromstring(content, parser=parser)
            logger.info("Successfully downloaded features list")
            return root

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error downloading features list: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Failed to download features list: {e}")
            return None

    async def download_categories_list_async(self) -> etree._Element | None:
        """
        Download the Icecat categories list.

        Contains all product categories in the Icecat taxonomy.

        Returns:
            Parsed XML Element or None if download fails
        """
        url = f"{self.BASE_URL}/refs/CategoriesList.xml.gz"
        logger.info(f"Downloading categories list: {url}")

        try:
            content = await self._get_with_basic_auth(
                url,
                self._auth[0],
                self._auth[1],
                decompress_gzip=True,
            )
            parser = etree.XMLParser(dtd_validation=False, load_dtd=True)
            root = etree.fromstring(content, parser=parser)
            logger.info("Successfully downloaded categories list")
            return root

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error downloading categories list: {e.response.status_code}"
            )
            return None
        except Exception as e:
            logger.error(f"Failed to download categories list: {e}")
            return None

    async def download_daily_index_raw_async(
        self,
        culture_id: str = "EN",
    ) -> bytes | None:
        """
        Download the raw daily index file content.

        Useful when you need to process the XML with custom parsing logic.

        Args:
            culture_id: Language/culture code

        Returns:
            Raw XML bytes or None if download fails
        """
        url = f"{self.BASE_URL}/{culture_id}/daily.index.xml"

        try:
            return await self._get_with_basic_auth(
                url,
                self._auth[0],
                self._auth[1],
                decompress_gzip=False,
            )
        except Exception as e:
            logger.error(f"Failed to download raw daily index: {e}")
            return None
