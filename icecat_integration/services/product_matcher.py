"""Product matcher service for matching Brand+MPN to Icecat products."""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from ..api import IcecatJsonDataFetchService
from ..config import IcecatConfig
from ..utils.retry import retry
from ..utils.logging_utils import SyncLogger

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of attempting to match a product to Icecat."""

    brand: str
    mpn: str
    found: bool
    icecat_id: int | None = None
    icecat_data: dict[str, Any] | None = None
    error_message: str | None = None
    api_response_code: int | None = None
    api_response_body: str | None = None
    duration_ms: int = 0

    @property
    def is_success(self) -> bool:
        return self.found and self.icecat_id is not None


@dataclass
class BatchMatchResult:
    """Result of batch matching operation."""

    total: int
    matched: int
    not_found: int
    errors: int
    results: list[MatchResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.matched / self.total) * 100


class ProductMatcher:
    """
    Match Brand+MPN to Icecat products using the Icecat FrontOffice API.

    Features:
    - Single product matching
    - Batch matching with concurrency control
    - Full API response logging
    - Retry on transient failures
    """

    def __init__(
        self,
        icecat_config: IcecatConfig,
        max_concurrent: int = 10,
        sync_logger: SyncLogger | None = None,
    ):
        """
        Initialize the matcher.

        Args:
            icecat_config: Icecat API configuration
            max_concurrent: Maximum concurrent API calls
            sync_logger: Optional logger for sync operations
        """
        self.config = icecat_config
        self.fetch_service = IcecatJsonDataFetchService(icecat_config)
        self.max_concurrent = max_concurrent
        self.sync_logger = sync_logger
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def match_product(
        self,
        brand: str,
        mpn: str,
        language: str = "EN",
    ) -> MatchResult:
        """
        Match a single product by Brand + MPN.

        Args:
            brand: Brand name
            mpn: Manufacturer part number
            language: Language code for the API call

        Returns:
            MatchResult with match details
        """
        start_time = time.perf_counter()
        result = MatchResult(brand=brand, mpn=mpn, found=False)

        try:
            # Call Icecat API
            api_result = await self._fetch_with_retry(mpn, brand, language)

            duration_ms = int((time.perf_counter() - start_time) * 1000)
            result.duration_ms = duration_ms

            # Omit successful response bodies to minimize storage; keep error responses for troubleshooting
            if api_result.data:
                result.api_response_body = None
                result.api_response_code = 200
            else:
                result.api_response_code = 404 if not api_result.success else 200

            # Check if product was found
            if api_result.success and api_result.data:
                data = api_result.data
                general_info = data.get("data", {}).get("GeneralInfo", {})
                icecat_id = general_info.get("IcecatId")

                if icecat_id:
                    result.found = True
                    result.icecat_id = icecat_id
                    result.icecat_data = data
                else:
                    result.found = False
                    result.error_message = "Product found but missing IcecatId"
            else:
                result.found = False
                result.error_message = api_result.error_message or "Product not found"
                result.api_response_body = api_result.error_message

            # Log to sync logger if available
            if self.sync_logger:
                self.sync_logger.log_api_call(
                    endpoint=f"https://live.icecat.biz/api/?ProductCode={mpn}&Brand={brand}",
                    response_code=result.api_response_code or 0,
                    response_body=result.api_response_body or "",
                    duration_ms=duration_ms,
                    brand=brand,
                    mpn=mpn,
                    icecat_id=result.icecat_id,
                )

        except Exception as e:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            result.duration_ms = duration_ms
            result.found = False
            result.error_message = str(e)

            logger.error(f"Error matching {brand}/{mpn}: {e}")

            if self.sync_logger:
                self.sync_logger.log_error(
                    f"API error matching product: {e}",
                    brand=brand,
                    mpn=mpn,
                )

        return result

    @retry(max_attempts=3, backoff_factor=2)
    async def _fetch_with_retry(self, mpn: str, brand: str, language: str):
        """Fetch product with retry logic."""
        return await self.fetch_service.fetch_product_data_by_product_code_async(
            mpn, brand, language
        )

    async def batch_match(
        self,
        items: list[tuple[str, str]],
        language: str = "EN",
    ) -> BatchMatchResult:
        """
        Match multiple products with controlled concurrency.

        Args:
            items: List of (brand, mpn) tuples
            language: Language code for API calls

        Returns:
            BatchMatchResult with all match results
        """
        batch_result = BatchMatchResult(
            total=len(items),
            matched=0,
            not_found=0,
            errors=0,
        )

        async def match_with_semaphore(brand: str, mpn: str) -> MatchResult:
            async with self._semaphore:
                return await self.match_product(brand, mpn, language)

        # Create tasks for all items
        tasks = [match_with_semaphore(brand, mpn) for brand, mpn in items]

        # Execute with gathering results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Handle exceptions from gather
                brand, mpn = items[i]
                match_result = MatchResult(
                    brand=brand,
                    mpn=mpn,
                    found=False,
                    error_message=str(result),
                )
                batch_result.results.append(match_result)
                batch_result.errors += 1
            else:
                batch_result.results.append(result)
                if result.found:
                    batch_result.matched += 1
                elif result.error_message:
                    batch_result.errors += 1
                else:
                    batch_result.not_found += 1

        return batch_result

    async def match_by_ean(
        self,
        ean: str,
        language: str = "EN",
    ) -> MatchResult:
        """
        Match a product by EAN/UPC.

        Args:
            ean: EAN/UPC barcode
            language: Language code

        Returns:
            MatchResult with match details
        """
        start_time = time.perf_counter()
        result = MatchResult(brand="", mpn="", found=False)

        try:
            api_result = await self.fetch_service.fetch_product_data_by_ean_async(
                ean, language
            )

            duration_ms = int((time.perf_counter() - start_time) * 1000)
            result.duration_ms = duration_ms

            if api_result.success and api_result.data:
                data = api_result.data
                general_info = data.get("data", {}).get("GeneralInfo", {})
                icecat_id = general_info.get("IcecatId")

                if icecat_id:
                    result.found = True
                    result.icecat_id = icecat_id
                    result.icecat_data = data
                    result.brand = general_info.get("Brand", "")
                    result.mpn = general_info.get("ProductCode", "")
                    result.api_response_code = 200
                    result.api_response_body = json.dumps(data, default=str)
                else:
                    result.error_message = "Product found but missing IcecatId"
            else:
                result.error_message = api_result.error_message or "Product not found"

        except Exception as e:
            result.duration_ms = int((time.perf_counter() - start_time) * 1000)
            result.error_message = str(e)
            logger.error(f"Error matching EAN {ean}: {e}")

        return result

    async def match_by_icecat_id(
        self,
        icecat_id: int,
        language: str = "EN",
    ) -> MatchResult:
        """
        Fetch product by Icecat ID.

        Args:
            icecat_id: Icecat product ID
            language: Language code

        Returns:
            MatchResult with product details
        """
        start_time = time.perf_counter()
        result = MatchResult(brand="", mpn="", found=False)

        try:
            api_result = await self.fetch_service.fetch_product_data_by_icecat_id_async(
                icecat_id, language
            )

            duration_ms = int((time.perf_counter() - start_time) * 1000)
            result.duration_ms = duration_ms

            if api_result.success and api_result.data:
                data = api_result.data
                general_info = data.get("data", {}).get("GeneralInfo", {})

                result.found = True
                result.icecat_id = icecat_id
                result.icecat_data = data
                result.brand = general_info.get("Brand", "")
                result.mpn = general_info.get("ProductCode", "")
                result.api_response_code = 200
                result.api_response_body = json.dumps(data, default=str)
            else:
                result.error_message = api_result.error_message or "Product not found"

        except Exception as e:
            result.duration_ms = int((time.perf_counter() - start_time) * 1000)
            result.error_message = str(e)
            logger.error(f"Error fetching Icecat ID {icecat_id}: {e}")

        return result
