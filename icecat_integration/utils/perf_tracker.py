"""Lightweight performance diagnostics for the async sync pipeline.

When enabled via ``--diagnostics``, records per-stage timing (API fetch,
XML parse, DB write, commit, queue wait) and emits a periodic report
every *report_interval* products.  Zero external dependencies — uses
only ``time.perf_counter`` and ``threading.Lock``.
"""

import logging
import threading
import time
from dataclasses import dataclass

logger = logging.getLogger("icecat_sync")

# Actionable hints shown next to the dominant bottleneck stage.
_HINTS: dict[str, str] = {
    "API fetch": "check network RTT to Icecat, or increase --fetch-workers",
    "XML parse": "CPU-bound; check container vCPU allocation",
    "DB write": "check innodb_flush_log_at_trx_commit, IOPS ceiling, or DB connection pool",
    "Queue wait": "write workers starved — fetchers are the bottleneck, increase --fetch-workers",
}


def _fmt(ms: float) -> str:
    """Format milliseconds for display."""
    if ms == float("inf") or ms < 0:
        return "n/a"
    if ms >= 10_000:
        return f"{ms / 1000:.1f}s"
    if ms >= 1000:
        return f"{ms / 1000:.2f}s"
    return f"{ms:.0f}ms"


@dataclass
class _Bucket:
    """Running accumulator for a single timing category."""

    count: int = 0
    total_ms: float = 0.0
    min_ms: float = float("inf")
    max_ms: float = 0.0

    def record(self, ms: float) -> None:
        self.count += 1
        self.total_ms += ms
        if ms < self.min_ms:
            self.min_ms = ms
        if ms > self.max_ms:
            self.max_ms = ms

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.count if self.count else 0.0

    def reset(self) -> "_Bucket":
        """Snapshot current state and reset in-place.  Caller holds lock."""
        snap = _Bucket(self.count, self.total_ms, self.min_ms, self.max_ms)
        self.count = 0
        self.total_ms = 0.0
        self.min_ms = float("inf")
        self.max_ms = 0.0
        return snap


class PerfTracker:
    """Thread-safe aggregator for pipeline-stage timings.

    All ``record_*`` methods are no-ops when *enabled* is ``False``
    (a single attribute check, no lock acquired).
    """

    def __init__(
        self,
        enabled: bool = False,
        report_interval: int = 1000,
        fetch_workers: int = 1,
        write_workers: int = 1,
    ) -> None:
        self._enabled = enabled
        self._report_interval = report_interval
        self._fetch_workers = fetch_workers
        self._write_workers = write_workers
        self._lock = threading.Lock()

        # Per-window accumulators (reset after each report)
        self._api_fetch = _Bucket()
        self._xml_parse = _Bucket()
        self._db_write = _Bucket()
        self._db_commit = _Bucket()
        self._queue_wait = _Bucket()

        # Buffer queue depth tracking
        self._queue_depth = _Bucket()

        # Batch size tracking (actual products per bulk write)
        self._batch_size = _Bucket()

        # Per-table DB write breakdown
        self._db_tables: dict[str, _Bucket] = {}

        # Response size tracking (bytes)
        self._resp_size = _Bucket()

        # Retry / deadlock counters for the current window
        self._api_retries = 0
        self._api_retry_wait_ms = 0.0
        self._deadlocks = 0

        # Product counters
        self._products_since_report = 0
        self._total_products = 0
        self._window_start = time.perf_counter()

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ── Recording methods (called from hot paths) ────────────────

    def record_fetch(self, ms: float, response_bytes: int = 0) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._api_fetch.record(ms)
            if response_bytes > 0:
                self._resp_size.record(response_bytes)

    def record_parse(self, ms: float) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._xml_parse.record(ms)

    def record_api_retry(self, wait_ms: float) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._api_retries += 1
            self._api_retry_wait_ms += wait_ms

    def record_db_write(
        self,
        write_ms: float,
        commit_ms: float,
        batch_size: int = 0,
        table_timings: dict[str, float] | None = None,
    ) -> None:
        """Record a batch write.  Called from the thread-pool."""
        if not self._enabled:
            return
        with self._lock:
            self._db_write.record(write_ms)
            self._db_commit.record(commit_ms)
            if batch_size > 0:
                self._batch_size.record(batch_size)
            if table_timings:
                for table_name, ms in table_timings.items():
                    if table_name not in self._db_tables:
                        self._db_tables[table_name] = _Bucket()
                    self._db_tables[table_name].record(ms)

    def record_queue_wait(self, ms: float) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._queue_wait.record(ms)

    def record_queue_depth(self, depth: int) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._queue_depth.record(float(depth))

    def record_deadlock(self) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._deadlocks += 1

    def record_products_committed(self, count: int) -> None:
        """Called after each batch.  Triggers a report when the window fills."""
        if not self._enabled:
            return
        with self._lock:
            self._products_since_report += count
            self._total_products += count
            if self._products_since_report >= self._report_interval:
                self._emit_report()

    def flush(self) -> None:
        """Emit a final report for any remaining products in the window."""
        if not self._enabled:
            return
        with self._lock:
            if self._products_since_report > 0:
                self._emit_report()

    # ── Report generation (called with lock held) ────────────────

    def _emit_report(self) -> None:
        window_elapsed = time.perf_counter() - self._window_start
        prod_count = self._products_since_report

        # Snapshot and reset buckets
        api = self._api_fetch.reset()
        parse = self._xml_parse.reset()
        db_w = self._db_write.reset()
        db_c = self._db_commit.reset()
        q_wait = self._queue_wait.reset()
        q_depth = self._queue_depth.reset()
        bsize = self._batch_size.reset()
        rsize = self._resp_size.reset()
        retries = self._api_retries
        retry_wait = self._api_retry_wait_ms
        deadlocks = self._deadlocks
        total = self._total_products
        window_start_total = total - prod_count

        # Snapshot per-table buckets
        table_snaps: dict[str, _Bucket] = {}
        for name, bucket in self._db_tables.items():
            table_snaps[name] = bucket.reset()

        # Reset window
        self._products_since_report = 0
        self._api_retries = 0
        self._api_retry_wait_ms = 0.0
        self._deadlocks = 0
        self._window_start = time.perf_counter()

        # Compute effective per-product cost (normalised for parallelism)
        avg_fetch_eff = api.avg_ms / max(self._fetch_workers, 1)
        avg_parse_eff = parse.avg_ms / max(self._fetch_workers, 1)
        batch_count = max(db_w.count, 1)
        avg_write_per_prod = db_w.avg_ms / max(prod_count / batch_count, 1)
        avg_write_eff = avg_write_per_prod / max(self._write_workers, 1)
        avg_qwait_eff = q_wait.avg_ms

        stages: dict[str, float] = {
            "API fetch": avg_fetch_eff,
            "XML parse": avg_parse_eff,
            "DB write": avg_write_eff,
            "Queue wait": avg_qwait_eff,
        }
        total_stage = sum(stages.values()) or 1.0
        bottleneck_name = max(stages, key=stages.get)  # type: ignore[arg-type]
        bottleneck_pct = stages[bottleneck_name] / total_stage * 100

        rate = prod_count / window_elapsed if window_elapsed > 0 else 0

        # Throughput: how many products fetched per second across all workers
        fetch_throughput = api.count / (api.total_ms / 1000) if api.total_ms > 0 else 0
        # Effective fetch throughput (wall-clock, accounting for parallelism)
        effective_fetch_per_sec = api.count / window_elapsed if window_elapsed > 0 else 0

        # Response size formatting
        avg_kb = rsize.avg_ms / 1024 if rsize.count else 0  # _Bucket reused for bytes
        max_kb = rsize.max_ms / 1024 if rsize.count else 0

        lines = [
            f"[DIAGNOSTICS] Performance report (products {window_start_total:,}-{total:,}):",
            f"  API fetch:      avg={_fmt(api.avg_ms)}  min={_fmt(api.min_ms)}  max={_fmt(api.max_ms)}  "
            f"({api.count} fetches)",
            f"    -> throughput: {effective_fetch_per_sec:.1f} fetches/s actual  "
            f"({fetch_throughput:.1f}/s per worker x {self._fetch_workers} workers)  "
            f"(retries: {retries}, retry_wait: {retry_wait / 1000:.1f}s)",
            f"  XML parse:      avg={_fmt(parse.avg_ms)}  min={_fmt(parse.min_ms)}  max={_fmt(parse.max_ms)}",
            f"  DB write:       avg={_fmt(db_w.avg_ms)}  min={_fmt(db_w.min_ms)}  max={_fmt(db_w.max_ms)}  "
            f"({db_w.count} batches)",
            f"  DB commit:      avg={_fmt(db_c.avg_ms)}  min={_fmt(db_c.min_ms)}  max={_fmt(db_c.max_ms)}",
            f"  Queue wait:     avg={_fmt(q_wait.avg_ms)}  min={_fmt(q_wait.min_ms)}  max={_fmt(q_wait.max_ms)}  "
            f"(write worker idle time)",
            f"  Deadlocks:      {deadlocks}",
        ]

        # Buffer queue depth
        if q_depth.count:
            lines.append(
                f"  Buffer depth:   avg={q_depth.avg_ms:.0f}  min={q_depth.min_ms:.0f}  "
                f"max={q_depth.max_ms:.0f}  (of buffer capacity)"
            )

        # Batch size
        if bsize.count:
            lines.append(
                f"  Batch size:     avg={bsize.avg_ms:.0f}  min={bsize.min_ms:.0f}  "
                f"max={bsize.max_ms:.0f}  products per bulk write"
            )

        # Response size
        if rsize.count:
            lines.append(
                f"  Response size:  avg={avg_kb:.0f}KB  max={max_kb:.0f}KB  "
                f"({rsize.count} responses)"
            )

        # Per-table DB breakdown
        if table_snaps:
            lines.append("  DB per-table breakdown:")
            # Sort by total time descending (slowest tables first)
            sorted_tables = sorted(
                table_snaps.items(),
                key=lambda x: x[1].total_ms,
                reverse=True,
            )
            for name, snap in sorted_tables:
                pct = (snap.total_ms / db_w.total_ms * 100) if db_w.total_ms > 0 else 0
                lines.append(
                    f"    {name:<20s} avg={_fmt(snap.avg_ms)}  "
                    f"total={_fmt(snap.total_ms)}  ({pct:.0f}%)"
                )

        # Write throughput
        write_throughput = prod_count / (db_w.total_ms / 1000) if db_w.total_ms > 0 else 0
        lines.append(
            f"  Write throughput: {write_throughput:.1f} products written/s  "
            f"(across {self._write_workers} writer(s))"
        )

        # Batch fill efficiency
        if bsize.count:
            fill_pct = min(bsize.avg_ms, 100) / 100 * 100
            lines.append(
                f"  Batch fill:     {fill_pct:.0f}% efficiency  "
                f"(avg {bsize.avg_ms:.0f} of 100 — small batches waste commit overhead)"
                if fill_pct < 70 else
                f"  Batch fill:     {fill_pct:.0f}% efficiency  "
                f"(avg {bsize.avg_ms:.0f} of 100 — good)"
            )

        lines.append("  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
        lines.append(
            f"  Bottleneck: {bottleneck_name} ({bottleneck_pct:.0f}%) "
            f"\u2192 {_HINTS.get(bottleneck_name, '')}"
        )
        lines.append(f"  Sustained rate: {rate:.1f} prod/s")

        logger.info("\n".join(lines))
