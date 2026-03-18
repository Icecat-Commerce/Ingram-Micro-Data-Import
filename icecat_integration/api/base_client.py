"""Base HTTP client for Icecat API integration."""

import gzip
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class BaseHttpClient:
    """Base HTTP client with connection pooling."""

    DEFAULT_TIMEOUT = 300.0  # 5 minutes

    def __init__(self, timeout: float = DEFAULT_TIMEOUT, api_token: str = ""):
        self._timeout = timeout
        self._api_token = api_token
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        """Get or create a shared async HTTP client with connection pooling."""
        if self._client is None or self._client.is_closed:
            headers: dict[str, str] = {"Accept": "application/json"}
            if self._api_token:
                headers["Api-Token"] = self._api_token
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout),
                headers=headers,
                limits=httpx.Limits(
                    max_connections=100,
                    max_keepalive_connections=40,
                    keepalive_expiry=30,
                ),
                http2=True,
            )
        return self._client

    async def close(self):
        """Close the underlying HTTP client and release connections."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _get_with_basic_auth(
        self,
        url: str,
        username: str,
        password: str,
        decompress_gzip: bool = False,
    ) -> bytes:
        """
        Perform a GET request with basic authentication.

        Args:
            url: The URL to fetch
            username: Basic auth username
            password: Basic auth password
            decompress_gzip: Whether to decompress gzip response

        Returns:
            Response content as bytes
        """
        client = self._get_client()
        response = await client.get(
            url,
            auth=(username, password),
        )
        response.raise_for_status()

        content = response.content
        if decompress_gzip:
            content = gzip.decompress(content)

        return content

    async def _get_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        """
        Perform a GET request and return JSON response.

        Args:
            url: The URL to fetch
            params: Query parameters

        Returns:
            Parsed JSON response
        """
        client = self._get_client()
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def _post_json(
        self,
        url: str,
        json_data: dict[str, Any],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        """
        Perform a POST request with JSON body.

        Args:
            url: The URL to post to
            json_data: JSON body data
            params: Query parameters

        Returns:
            Parsed JSON response
        """
        client = self._get_client()
        response = await client.post(
            url,
            json=json_data,
            params=params,
        )
        response.raise_for_status()
        return response.json()
