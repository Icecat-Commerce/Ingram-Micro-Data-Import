"""Read-only diagnostic for the Phase 3.5 index prefilter.

Loads the same brand_map and parses the same cached `files.index.csv.gz`
that `_prefilter_against_index` uses, then reports what the prefilter
*would* match against the local sync_product table — without writing
anything to the database.

Two views are reported:

  View A — current behaviour: prefilter operates on rows WHERE status='PENDING'.
           This is what the orchestrator does today.

  View B — full-refresh behaviour: prefilter operates on rows
           WHERE status NOT IN ('DELETED'). This simulates what would
           happen after the new Phase 3a reset (5.1 in the plan): every
           non-DELETED row is treated as PENDING, then prefiltered.

Run from the project root:

    python3 -m benchmark.prefilter_diagnostic

or:

    python3 benchmark/prefilter_diagnostic.py
"""

from __future__ import annotations

import gzip
import sys
import time
from pathlib import Path

from sqlalchemy import text as sa_text

# Allow running as a script (`python3 benchmark/prefilter_diagnostic.py`)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from icecat_integration.config import AppConfig  # noqa: E402
from icecat_integration.database.connection import init_db  # noqa: E402
from icecat_integration.repositories.supplier_mapping_repository import (  # noqa: E402
    SupplierMappingRepository,
)


CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
INDEX_PATH = PROJECT_ROOT / "data" / "downloads" / "files.index.csv.gz"


def fmt_int(n: int) -> str:
    return f"{n:,}"


def section(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def main() -> int:
    if not CONFIG_PATH.exists():
        print(f"ERROR: config not found at {CONFIG_PATH}")
        return 1
    if not INDEX_PATH.exists():
        print(f"ERROR: cached index not found at {INDEX_PATH}")
        print("Run a sync first to populate it, or copy from another env.")
        return 1

    cfg = AppConfig.from_yaml(CONFIG_PATH)
    db = init_db(cfg.database)

    with db.session() as session:
        # ── 1. Status distribution in sync_product ──
        section("sync_product status distribution")
        rows = session.execute(sa_text(
            "SELECT status, COUNT(*) FROM sync_product GROUP BY status"
        )).all()
        total = 0
        for status, count in rows:
            print(f"  {status:<12} {fmt_int(count)}")
            total += count
        print(f"  {'TOTAL':<12} {fmt_int(total)}")
        if total == 0:
            print("  (table is empty — load assortment first)")
            return 0

        # ── 2. Load brand_map ──
        section("Brand mapping")
        t0 = time.perf_counter()
        mapping_repo = SupplierMappingRepository(session)
        brand_map = mapping_repo.load_all_mappings()
        print(f"  Loaded {fmt_int(len(brand_map))} brand aliases "
              f"({time.perf_counter() - t0:.1f}s)")

        # ── 3. Parse the cached index (mirror _prefilter_against_index) ──
        section(f"Parsing index file: {INDEX_PATH.name}")
        size_mb = INDEX_PATH.stat().st_size / 1e6
        print(f"  File size: {size_mb:.0f} MB")

        # Build vendor_id → vendor.name (lower) — same query as orchestrator
        vendor_id_to_name: dict[int, str] = {}
        for vid, name in session.execute(sa_text(
            "SELECT vendorid, LOWER(name) FROM vendor"
        )):
            vendor_id_to_name[int(vid)] = name
        print(f"  Vendor table: {fmt_int(len(vendor_id_to_name))} rows")

        t0 = time.perf_counter()
        icecat_pairs: set[tuple[str, str]] = set()
        line_count = 0
        with gzip.open(INDEX_PATH, "rt", errors="replace") as f:
            next(f)  # skip header
            for line in f:
                line_count += 1
                fields = line.split("\t")
                if len(fields) < 9:
                    continue
                supplier_id = int(fields[4]) if fields[4].isdigit() else 0
                prod_id = fields[5].strip()
                m_prod_id = fields[7].strip()

                vendor_name = vendor_id_to_name.get(supplier_id, "")
                if vendor_name and prod_id:
                    icecat_pairs.add((vendor_name, prod_id.lower()))
                if vendor_name and m_prod_id and m_prod_id != prod_id:
                    icecat_pairs.add((vendor_name, m_prod_id.lower()))

                if line_count % 5_000_000 == 0:
                    print(f"  ... parsed {fmt_int(line_count)} lines so far")
        parse_dur = time.perf_counter() - t0
        print(f"  Parsed {fmt_int(line_count)} index rows, "
              f"built {fmt_int(len(icecat_pairs))} unique (vendor, mpn) pairs "
              f"({parse_dur:.1f}s)")

        # ── 4. Match sync_product against the index ──
        # We do two views in a single pass to keep the DB pull cheap:
        #   View A: only rows WHERE status='PENDING'
        #   View B: all rows WHERE status<>'DELETED'  (simulates Phase 3a reset)
        section("Matching sync_product against the index")
        t0 = time.perf_counter()
        view_a_total = 0
        view_a_matched = 0
        view_b_total = 0
        view_b_matched = 0

        # Single pull of (status, brand, mpn) — minimal columns, raw rows.
        result = session.execute(sa_text(
            "SELECT status, brand, mpn FROM sync_product WHERE status <> 'DELETED'"
        ))
        for status, brand, mpn in result:
            mapped = brand_map.get(brand.lower(), brand).lower()
            key = (mapped, mpn.lower())
            in_index = key in icecat_pairs

            view_b_total += 1
            if in_index:
                view_b_matched += 1

            # View A excludes anything that isn't PENDING today
            status_str = (
                status.value if hasattr(status, "value") else str(status)
            ).upper()
            if status_str == "PENDING":
                view_a_total += 1
                if in_index:
                    view_a_matched += 1

        match_dur = time.perf_counter() - t0
        print(f"  Compared {fmt_int(view_b_total)} non-DELETED rows "
              f"({match_dur:.1f}s)")

        # ── 5. Report ──
        section("Results")
        print()
        print("  View A — current prefilter (WHERE status='PENDING')")
        print(f"    Considered:  {fmt_int(view_a_total)}")
        print(f"    Matched:     {fmt_int(view_a_matched)}")
        print(f"    Not in idx:  {fmt_int(view_a_total - view_a_matched)}")
        if view_a_total:
            print(f"    Hit rate:    "
                  f"{100 * view_a_matched / view_a_total:.1f}%")

        print()
        print("  View B — after Phase 3a reset (WHERE status<>'DELETED')")
        print(f"    Considered:  {fmt_int(view_b_total)}")
        print(f"    Matched:     {fmt_int(view_b_matched)}")
        print(f"    Not in idx:  {fmt_int(view_b_total - view_b_matched)}")
        if view_b_total:
            print(f"    Hit rate:    "
                  f"{100 * view_b_matched / view_b_total:.1f}%")

        print()
        delta = view_b_matched - view_a_matched
        print(f"  Δ matched (B − A): {fmt_int(delta)} additional rows would be "
              f"fetched after the reset")
        print()
        print("  No database changes were made.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
