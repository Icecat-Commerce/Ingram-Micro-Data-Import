"""Icecat XML Product Fetch Service for xml_server3.cgi endpoint.

Fetches complete product data with all locales in a single request using
lang=INT. Uses Basic Auth with the same FrontOffice credentials as the
taxonomy download service.
"""

import asyncio
import logging
from dataclasses import dataclass
from urllib.parse import quote

import httpx
from lxml import etree

from ..config import IcecatConfig
from .base_client import BaseHttpClient

logger = logging.getLogger(__name__)


@dataclass
class XmlFetchResult:
    """Result of an XML product fetch."""

    xml_root: etree._Element | None = None
    raw_bytes: bytes | None = None
    error_message: str = ""
    status_code: int = 0

    @property
    def success(self) -> bool:
        return self.xml_root is not None and not self.error_message


class IcecatXmlProductFetchService(BaseHttpClient):
    """Fetch product data from Icecat xml_server3.cgi with lang=INT (all locales)."""

    XML_ENDPOINT = "https://data.icecat.biz/xml_s3/xml_server3.cgi"

    def __init__(self, config: IcecatConfig, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self._config = config

    @property
    def _auth(self) -> tuple[str, str]:
        return (
            self._config.front_office_username,
            self._config.front_office_password,
        )

    async def fetch_product_xml(self, brand: str, mpn: str) -> XmlFetchResult:
        """Fetch product XML with all locales by brand + MPN."""
        url = (
            f"{self.XML_ENDPOINT}"
            f"?lang=INT"
            f";prod_id={quote(mpn, safe='')}"
            f";vendor={quote(brand, safe='')}"
            f";output=productxml"
        )
        return await self._fetch_xml(url, f"{brand}/{mpn}")

    async def fetch_product_xml_by_icecat_id(self, icecat_id: int) -> XmlFetchResult:
        """Fetch product XML with all locales by Icecat product ID."""
        url = (
            f"{self.XML_ENDPOINT}"
            f"?lang=INT"
            f";product_id={icecat_id}"
            f";output=productxml"
        )
        return await self._fetch_xml(url, f"icecat_id={icecat_id}")

    async def fetch_product_xml_by_ean(self, ean: str) -> XmlFetchResult:
        """Fetch product XML with all locales by EAN/UPC."""
        url = (
            f"{self.XML_ENDPOINT}"
            f"?lang=INT"
            f";ean_upc={quote(ean, safe='')}"
            f";output=productxml"
        )
        return await self._fetch_xml(url, f"ean={ean}")

    async def _fetch_xml(self, url: str, identifier: str) -> XmlFetchResult:
        """Fetch and parse XML from the given URL. Retries on 429 rate-limit."""
        for attempt in range(3):
            try:
                result = await self._fetch_xml_once(url, identifier)
                if result.status_code == 429:
                    wait = 5 * (attempt + 1)  # 5s, 10s, 15s
                    logger.warning(f"Rate limited (429) for {identifier}, retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                return result
            except Exception as e:
                logger.error(f"Failed to fetch XML for {identifier}: {e}")
                return XmlFetchResult(error_message=str(e))
        return XmlFetchResult(error_message="Rate limited after 3 retries", status_code=429)

    async def _fetch_xml_once(self, url: str, identifier: str) -> XmlFetchResult:
        """Single fetch attempt."""
        try:
            content = await self._get_with_basic_auth(
                url, self._auth[0], self._auth[1],
            )

            parser = etree.XMLParser(
                dtd_validation=False,
                load_dtd=False,
                resolve_entities=False,
            )
            root = etree.fromstring(content, parser=parser)

            product = root.find(".//Product")
            if product is None:
                logger.warning(f"No <Product> element for {identifier}, response[:200]={content[:200]}")
                return XmlFetchResult(error_message="No <Product> element in response")

            error_msg = product.get("ErrorMessage")
            if error_msg:
                code_attr = product.get("Code", "")
                logger.debug(f"ErrorMessage for {identifier}: Code={code_attr}, Msg={error_msg}")
                return XmlFetchResult(
                    error_message=error_msg,
                    status_code=404,
                )

            code = product.get("Code", "")
            if code == "-1":
                return XmlFetchResult(
                    error_message="Product not found in Icecat",
                    status_code=404,
                )

            return XmlFetchResult(
                xml_root=root,
                raw_bytes=content,
                status_code=200,
            )

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP {e.response.status_code} fetching XML for {identifier}")
            return XmlFetchResult(
                error_message=f"HTTP {e.response.status_code}",
                status_code=e.response.status_code,
            )
        except etree.XMLSyntaxError as e:
            logger.error(f"XML parse error for {identifier}: {e}")
            return XmlFetchResult(error_message=f"XML parse error: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch XML for {identifier}: {e}")
            return XmlFetchResult(error_message=str(e))
