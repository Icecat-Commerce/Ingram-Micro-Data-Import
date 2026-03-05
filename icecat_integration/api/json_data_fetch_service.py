"""Icecat FrontOffice JSON Data Fetch Service.

This service uses the Icecat Live API (FrontOffice) to fetch complete product data
in a single request. All product information including features, gallery, multimedia,
bullet points, and descriptions are returned in one JSON response.

API Endpoint: https://live.icecat.biz/api/
Authentication: API key (app_key) + username
"""

import logging
from http import HTTPStatus
from typing import Any

import orjson

from ..config import IcecatConfig
from ..models.json.product_response import ProductResponse
from .base_client import BaseHttpClient

logger = logging.getLogger(__name__)


class FetchResult:
    """Result container for fetch operations."""

    def __init__(
        self,
        data: dict[str, Any] | None = None,
        product_response: ProductResponse | None = None,
        logs: str = "",
        status_code: HTTPStatus = HTTPStatus.OK,
        error_message: str = "",
    ):
        self.data = data
        self.product_response = product_response
        self.logs = logs
        self.status_code = status_code
        self.error_message = error_message

    @property
    def success(self) -> bool:
        """Check if the fetch was successful."""
        return self.data is not None and not self.error_message


class IcecatJsonDataFetchService(BaseHttpClient):
    """
    Service for fetching product data from Icecat FrontOffice Live API.

    The Live API returns complete product data in a single request, including:
    - GeneralInfo (title, brand, category, descriptions, bullet points, GTINs)
    - Image (all resolutions)
    - Gallery (all product images)
    - Multimedia (videos, PDFs, EU energy labels)
    - FeaturesGroups (all specifications)
    - ProductStory, ReasonsToBuy, etc.

    Authentication uses API key (app_key) combined with username.
    """

    LIVE_API_URL = "https://live.icecat.biz/api/"

    def __init__(
        self,
        config: IcecatConfig,
        timeout: float = 300.0,
    ):
        """
        Initialize the FrontOffice data fetch service.

        Args:
            config: Icecat configuration with FrontOffice credentials
            timeout: Request timeout in seconds (default: 300)
        """
        super().__init__(timeout=timeout)
        self._config = config

    async def fetch_product_data_by_ean_async(
        self,
        ean: str,
        language_short_code: str = "EN",
        username: str | None = None,
        app_key: str | None = None,
    ) -> FetchResult:
        """
        Fetch complete product data by EAN/UPC code.

        Args:
            ean: The EAN/UPC barcode
            language_short_code: Language code (e.g., "EN", "NL", "DE")
            username: Optional custom username (uses config if not provided)
            app_key: Optional custom API key (uses config if not provided)

        Returns:
            FetchResult with complete product data and parsed ProductResponse
        """
        parameters = {"ean_upc": ean}
        return await self._fetch_product_data(
            parameters,
            f"EAN: {ean}",
            language_short_code,
            username,
            app_key,
        )

    async def fetch_product_data_by_product_code_async(
        self,
        product_code: str,
        brand: str,
        language_short_code: str = "EN",
        username: str | None = None,
        app_key: str | None = None,
    ) -> FetchResult:
        """
        Fetch complete product data by product code (MPN) and brand.

        Args:
            product_code: The manufacturer's product code (MPN/VPN)
            brand: The brand name
            language_short_code: Language code
            username: Optional custom username
            app_key: Optional custom API key

        Returns:
            FetchResult with complete product data and parsed ProductResponse
        """
        parameters = {"ProductCode": product_code, "Brand": brand}
        return await self._fetch_product_data(
            parameters,
            f"Product Code: {product_code}, Brand: {brand}",
            language_short_code,
            username,
            app_key,
        )

    async def fetch_product_data_by_icecat_id_async(
        self,
        icecat_id: int,
        language_short_code: str = "EN",
        username: str | None = None,
        app_key: str | None = None,
    ) -> FetchResult:
        """
        Fetch complete product data by Icecat product ID.

        Args:
            icecat_id: The Icecat internal product ID
            language_short_code: Language code
            username: Optional custom username
            app_key: Optional custom API key

        Returns:
            FetchResult with complete product data and parsed ProductResponse
        """
        parameters = {"icecat_id": str(icecat_id)}
        return await self._fetch_product_data(
            parameters,
            f"Icecat ID: {icecat_id}",
            language_short_code,
            username,
            app_key,
        )

    async def _fetch_product_data(
        self,
        parameters: dict[str, str],
        identifier: str,
        language_short_code: str,
        username: str | None = None,
        app_key: str | None = None,
    ) -> FetchResult:
        """
        Internal method to fetch product data from Live API.

        Args:
            parameters: Query parameters for the API (ean_upc, ProductCode+Brand, or icecat_id)
            identifier: Human-readable identifier for logging
            language_short_code: Language code (EN, NL, DE, etc.)
            username: Optional custom username
            app_key: Optional custom API key

        Returns:
            FetchResult with raw data dict and parsed ProductResponse
        """
        user = username or self._config.front_office_username
        key = app_key or self._config.front_office_api_key

        query_params = {
            "UserName": user,
            "Language": language_short_code,
            "app_key": key,
            **parameters,
        }

        try:
            client = self._get_client()
            response = await client.get(self.LIVE_API_URL, params=query_params)

            if response.is_success:
                data = orjson.loads(response.content)

                # Parse into Pydantic model
                product_response = None
                try:
                    product_response = ProductResponse.model_validate(data)
                except Exception as parse_error:
                    logger.warning(
                        f"Failed to parse response into ProductResponse: {parse_error}"
                    )

                return FetchResult(
                    data=data,
                    product_response=product_response,
                    logs=f"Successfully fetched data for {identifier}.",
                    status_code=HTTPStatus(response.status_code),
                )
            else:
                error = response.text
                return FetchResult(
                    logs=f"HTTP request failed: {response.status_code}. Response: {error}.",
                    status_code=HTTPStatus(response.status_code),
                    error_message=error,
                )

        except Exception as e:
            logger.exception(f"Failed to fetch product data for {identifier}")
            return FetchResult(
                logs=f"Failed to fetch product data for {identifier}. Exception: {e}.",
                status_code=HTTPStatus.BAD_REQUEST,
                error_message=str(e),
            )
