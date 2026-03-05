"""Retry decorator with exponential backoff for reliability."""

import asyncio
import functools
import logging
import random
from typing import Any, Callable, TypeVar

import httpx

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Default exceptions to retry on
DEFAULT_RETRY_EXCEPTIONS = (
    httpx.HTTPError,
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    ConnectionError,
    TimeoutError,
)


def retry(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    exceptions: tuple = DEFAULT_RETRY_EXCEPTIONS,
    on_retry: Callable[[Exception, int], None] | None = None,
) -> Callable:
    """
    Decorator for retrying async functions with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        backoff_factor: Multiplier for delay between retries
        initial_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay between retries
        jitter: Add random jitter to prevent thundering herd
        exceptions: Tuple of exception types to retry on
        on_retry: Optional callback called on each retry (exception, attempt_number)

    Example:
        @retry(max_attempts=3, backoff_factor=2)
        async def fetch_data():
            ...
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            delay = initial_delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt >= max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise

                    # Calculate delay with optional jitter
                    current_delay = min(delay, max_delay)
                    if jitter:
                        current_delay = current_delay * (0.5 + random.random())

                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} for {func.__name__} failed: {e}. "
                        f"Retrying in {current_delay:.2f}s..."
                    )

                    # Call retry callback if provided
                    if on_retry:
                        on_retry(e, attempt)

                    await asyncio.sleep(current_delay)
                    delay *= backoff_factor

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
            raise RuntimeError(f"Unexpected state in retry for {func.__name__}")

        return wrapper

    return decorator


def retry_sync(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    exceptions: tuple = DEFAULT_RETRY_EXCEPTIONS,
    on_retry: Callable[[Exception, int], None] | None = None,
) -> Callable:
    """
    Decorator for retrying synchronous functions with exponential backoff.

    Same parameters as retry() but for sync functions.
    """
    import time

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            delay = initial_delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt >= max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise

                    current_delay = min(delay, max_delay)
                    if jitter:
                        current_delay = current_delay * (0.5 + random.random())

                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} for {func.__name__} failed: {e}. "
                        f"Retrying in {current_delay:.2f}s..."
                    )

                    if on_retry:
                        on_retry(e, attempt)

                    time.sleep(current_delay)
                    delay *= backoff_factor

            if last_exception:
                raise last_exception
            raise RuntimeError(f"Unexpected state in retry for {func.__name__}")

        return wrapper

    return decorator


class RetryConfig:
    """Configuration class for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_factor: float = 2.0,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.jitter = jitter

    def as_decorator(
        self,
        exceptions: tuple = DEFAULT_RETRY_EXCEPTIONS,
        on_retry: Callable[[Exception, int], None] | None = None,
    ) -> Callable:
        """Create a retry decorator from this config."""
        return retry(
            max_attempts=self.max_attempts,
            backoff_factor=self.backoff_factor,
            initial_delay=self.initial_delay,
            max_delay=self.max_delay,
            jitter=self.jitter,
            exceptions=exceptions,
            on_retry=on_retry,
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "RetryConfig":
        """Create from configuration dictionary."""
        return cls(
            max_attempts=config.get("max_retries", 3),
            backoff_factor=config.get("retry_backoff", 2.0),
            initial_delay=config.get("initial_delay", 1.0),
            max_delay=config.get("max_delay", 60.0),
            jitter=config.get("jitter", True),
        )


async def retry_operation(
    operation: Callable[[], T],
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    exceptions: tuple = DEFAULT_RETRY_EXCEPTIONS,
) -> T:
    """
    Retry an async operation without using decorator.

    Useful for one-off retries or dynamic retry configurations.

    Args:
        operation: Async callable to retry
        max_attempts: Maximum retry attempts
        backoff_factor: Delay multiplier
        initial_delay: Initial delay in seconds
        exceptions: Exception types to retry on

    Returns:
        Result of the operation

    Example:
        result = await retry_operation(
            lambda: fetch_product(product_id),
            max_attempts=3
        )
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await operation()
        except exceptions as e:
            last_exception = e
            if attempt >= max_attempts:
                raise

            current_delay = delay * (0.5 + random.random())
            logger.warning(
                f"Attempt {attempt}/{max_attempts} failed: {e}. "
                f"Retrying in {current_delay:.2f}s..."
            )
            await asyncio.sleep(current_delay)
            delay *= backoff_factor

    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected state in retry_operation")
