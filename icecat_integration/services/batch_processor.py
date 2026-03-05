"""Batch processor for large-scale product synchronization."""

import asyncio
import logging
import signal
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class BatchResult:
    """Result of processing a single batch."""

    batch_number: int
    total_in_batch: int
    succeeded: int
    failed: int
    skipped: int
    duration_ms: int
    errors: list[str] = field(default_factory=list)


@dataclass
class ProcessingStats:
    """Overall processing statistics."""

    total_items: int
    total_batches: int
    processed: int
    succeeded: int
    failed: int
    skipped: int
    start_time: float = field(default_factory=time.perf_counter)
    end_time: float | None = None

    @property
    def elapsed_seconds(self) -> float:
        end = self.end_time or time.perf_counter()
        return end - self.start_time

    @property
    def items_per_second(self) -> float:
        if self.elapsed_seconds == 0:
            return 0.0
        return self.processed / self.elapsed_seconds

    @property
    def success_rate(self) -> float:
        if self.processed == 0:
            return 0.0
        return (self.succeeded / self.processed) * 100

    @property
    def eta_seconds(self) -> float:
        if self.items_per_second == 0:
            return 0.0
        remaining = self.total_items - self.processed
        return remaining / self.items_per_second

    def get_summary(self) -> dict[str, Any]:
        return {
            "total_items": self.total_items,
            "total_batches": self.total_batches,
            "processed": self.processed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped": self.skipped,
            "elapsed_seconds": self.elapsed_seconds,
            "items_per_second": self.items_per_second,
            "success_rate": f"{self.success_rate:.1f}%",
            "eta_seconds": self.eta_seconds,
        }


class BatchProcessor:
    """
    Process items in batches with controlled concurrency.

    Features:
    - Configurable batch size and concurrency
    - Graceful shutdown handling
    - Progress callbacks
    - Error isolation (failed items don't stop batch)
    """

    def __init__(
        self,
        batch_size: int = 100,
        max_concurrent: int = 10,
        on_progress: Callable[[ProcessingStats], None] | None = None,
        on_batch_complete: Callable[[BatchResult], None] | None = None,
    ):
        """
        Initialize batch processor.

        Args:
            batch_size: Number of items per batch
            max_concurrent: Maximum concurrent operations within a batch
            on_progress: Callback for progress updates
            on_batch_complete: Callback after each batch completes
        """
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.on_progress = on_progress
        self.on_batch_complete = on_batch_complete

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._shutdown_requested = False
        self._current_stats: ProcessingStats | None = None

    def request_shutdown(self) -> None:
        """Request graceful shutdown after current batch."""
        logger.info("Shutdown requested - will stop after current batch")
        self._shutdown_requested = True

    async def process_all(
        self,
        items: list[T],
        processor: Callable[[T], Coroutine[Any, Any, bool]],
    ) -> ProcessingStats:
        """
        Process all items in batches.

        Args:
            items: List of items to process
            processor: Async function that processes a single item
                      Returns True for success, False for failure

        Returns:
            ProcessingStats with overall results
        """
        total_items = len(items)
        total_batches = (total_items + self.batch_size - 1) // self.batch_size

        stats = ProcessingStats(
            total_items=total_items,
            total_batches=total_batches,
            processed=0,
            succeeded=0,
            failed=0,
            skipped=0,
        )
        self._current_stats = stats
        self._shutdown_requested = False

        logger.info(
            f"Starting batch processing: {total_items} items in {total_batches} batches"
        )

        # Process in batches
        for batch_num, batch_start in enumerate(range(0, total_items, self.batch_size), 1):
            if self._shutdown_requested:
                logger.info(f"Shutdown requested - stopping at batch {batch_num}")
                break

            batch_end = min(batch_start + self.batch_size, total_items)
            batch_items = items[batch_start:batch_end]

            batch_result = await self._process_batch(batch_num, batch_items, processor)

            # Update stats
            stats.processed += batch_result.total_in_batch
            stats.succeeded += batch_result.succeeded
            stats.failed += batch_result.failed
            stats.skipped += batch_result.skipped

            # Callbacks
            if self.on_batch_complete:
                self.on_batch_complete(batch_result)

            if self.on_progress:
                self.on_progress(stats)

            # Log progress
            logger.info(
                f"Batch {batch_num}/{total_batches} complete: "
                f"{batch_result.succeeded} succeeded, {batch_result.failed} failed "
                f"({stats.success_rate:.1f}% overall)"
            )

        stats.end_time = time.perf_counter()
        self._current_stats = None

        logger.info(
            f"Batch processing complete: {stats.processed}/{total_items} items, "
            f"{stats.succeeded} succeeded, {stats.failed} failed in {stats.elapsed_seconds:.1f}s"
        )

        return stats

    async def _process_batch(
        self,
        batch_number: int,
        items: list[T],
        processor: Callable[[T], Coroutine[Any, Any, bool]],
    ) -> BatchResult:
        """Process a single batch of items."""
        start_time = time.perf_counter()
        result = BatchResult(
            batch_number=batch_number,
            total_in_batch=len(items),
            succeeded=0,
            failed=0,
            skipped=0,
            duration_ms=0,
        )

        async def process_with_semaphore(item: T) -> bool:
            async with self._semaphore:
                try:
                    return await processor(item)
                except Exception as e:
                    logger.error(f"Error processing item: {e}")
                    result.errors.append(str(e))
                    return False

        # Process all items in batch concurrently
        tasks = [process_with_semaphore(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                result.failed += 1
                result.errors.append(str(r))
            elif r is True:
                result.succeeded += 1
            elif r is False:
                result.failed += 1
            else:
                result.skipped += 1

        result.duration_ms = int((time.perf_counter() - start_time) * 1000)
        return result


class GracefulShutdownHandler:
    """
    Handle graceful shutdown for long-running sync operations.

    Registers signal handlers for SIGINT and SIGTERM to allow
    clean interruption of batch processing.
    """

    def __init__(self, batch_processor: BatchProcessor):
        self.batch_processor = batch_processor
        self._original_sigint = None
        self._original_sigterm = None

    def __enter__(self):
        """Set up signal handlers."""
        self._original_sigint = signal.signal(signal.SIGINT, self._handle_signal)
        self._original_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original signal handlers."""
        if self._original_sigint:
            signal.signal(signal.SIGINT, self._original_sigint)
        if self._original_sigterm:
            signal.signal(signal.SIGTERM, self._original_sigterm)

    def _handle_signal(self, signum, frame):
        """Handle interrupt signal."""
        signal_name = signal.Signals(signum).name
        logger.warning(f"Received {signal_name} - requesting graceful shutdown")
        self.batch_processor.request_shutdown()
