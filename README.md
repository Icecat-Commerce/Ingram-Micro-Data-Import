# Icecat Integration

A Python CLI application that syncs product data from the [Icecat](https://icecat.biz) catalog into a database. It fetches product specifications, descriptions, media, attributes, and relationships for a given product assortment (Brand + MPN list) across multiple languages.

## Features

| Feature | Description |
| :------ | :---------- |
| **Product Sync** | Fetches full product data from Icecat by Brand + MPN via JSON API or XML endpoint |
| **Multi-language** | Supports 10 languages: EN, NL, FR, DE, IT, ES, PT, ZH, HU, TH. XML mode (`lang=INT`) fetches all in one call |
| **Index Prefilter** | Downloads the Icecat full product index and matches against assortment before making any API calls — skips products that don't exist in Icecat |
| **Bulk DB Writes** | Writes up to 100 products per transaction using bulk SQL INSERT for maximum throughput |
| **Taxonomy Import** | Downloads and imports Icecat category hierarchy, feature groups, and attribute names (~6.8K categories, ~290K attributes) |
| **Supplier Import** | Downloads and imports brand/vendor mapping (~42K vendors, ~34K brand aliases) |
| **Assortment Download** | Downloads product assortment file from FTP/SFTP |
| **Daily Index** | Downloads daily index to detect changed products for delta sync |
| **Sync Tracking** | Tracks every product's sync status, API responses, and errors |
| **Addon Type Derivation** | Derives product relationship types (Upsell/Cross-sell) from category comparison |

## Quick Start

### Prerequisites

- Python 3.10+
- MySQL 8.0+
- Icecat API credentials
- Assortment File FTP/SFTP credentials

### Installation

```bash
cd icecat-integration
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Initialize Database

`init-db` creates tables that don't already exist (idempotent).

```bash
python -m icecat_integration -c config/config.yaml init-db
python -m icecat_integration -c config/config.yaml seed-locales
```

### Required Schema Patches (apply on every new environment)

> **IMPORTANT** — these statements **must** be applied on every fresh database (UAT, STG, PROD, DR, any new replica) **before** running the first sync. Without them, the `product_addons` and `productfeatures` writes are unindexed / wrongly-typed and DB write time per batch goes from ~5-8 s up to ~25-30 s. Skipping this step is the single biggest source of "the sync is slow" reports.

```sql
-- product_addons: productId/relatedProductId must be INT (not VARCHAR), and productId must be indexed
ALTER TABLE product_addons MODIFY productId INT NOT NULL;
ALTER TABLE product_addons MODIFY relatedProductId INT NOT NULL;
ALTER TABLE product_addons ADD INDEX idx_productId (productId);

-- deleted_addons: same column types as product_addons
ALTER TABLE deleted_addons MODIFY product_id INT NOT NULL;
ALTER TABLE deleted_addons MODIFY relatedProductId INT NOT NULL;

-- productfeatures: productfeatureid must be BIGINT (not DECIMAL)
ALTER TABLE productfeatures MODIFY productfeatureid BIGINT NOT NULL;
```

**Verify the patches are in place** before running the sync:

```sql
SHOW CREATE TABLE product_addons;    -- productId, relatedProductId → INT; idx_productId must exist
SHOW CREATE TABLE deleted_addons;    -- product_id, relatedProductId → INT
SHOW CREATE TABLE productfeatures;   -- productfeatureid → BIGINT
```

If any column still shows `VARCHAR` or `DECIMAL`, re-apply the corresponding `ALTER` and re-check. Only proceed once all three tables match the spec above.

### Import Reference Data

```bash
# Download and import taxonomy (categories, feature groups, attribute names)
# Downloads ~1.5 GB XML, takes ~8 minutes
python -m icecat_integration -c config/config.yaml update-taxonomy

# Download supplier reference files
python -m icecat_integration -c config/config.yaml ftp-download-suppliers

# Import suppliers and brand mapping into DB
python -m icecat_integration -c config/config.yaml import-suppliers
```

### Download Assortment & Sync

```bash
# Download the product assortment file from FTP/SFTP
python -m icecat_integration -c config/config.yaml ftp-download-assortment

# Delta mode - only processes new/unsynced products (daily use)
python -m icecat_integration -c config/config.yaml sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode delta --source xml

# Full mode - re-processes entire assortment (weekly refresh)
python -m icecat_integration -c config/config.yaml sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full --source xml
```

## Configuration

The app can be configured via **YAML file** or **environment variables**. Environment variables take precedence.

### YAML Configuration

See `config/config.example.yaml` for the full template. Copy it and fill in your values:

```bash
cp config/config.example.yaml config/config.yaml
```

### Environment Variables

When deploying to containers or CI/CD, use environment variables instead of a config file. The app auto-detects env vars when no `-c` flag is passed.

| Variable | Description |
| :------- | :---------- |
| DB_HOST | Database hostname |
| DB_PORT | Database port |
| DB_NAME | Database name |
| DB_USER | Database username |
| DB_PASSWORD | Database password |
| DB_POOL_SIZE | Connection pool size |
| DB_MAX_OVERFLOW | Max overflow connections |
| DB_SSL | Enable SSL (true/false) |
| SYNC_CONCURRENCY | API calls concurrency (default: 1, sequential) |
| ICECAT_FO_USERNAME | FrontOffice API username |
| ICECAT_FO_PASSWORD | FrontOffice API password |
| ICECAT_FO_API_KEY | FrontOffice API key |
| ICECAT_API_TOKEN | API access token (bypasses IP whitelisting — required for cloud) |
| ICECAT_FTP_HOST | FTP/SFTP server hostname |
| ICECAT_FTP_PROTOCOL | Protocol: "ftp" or "sftp" (default: ftp) |
| ICECAT_FTP_PORT | Server port (0 = auto: 21 for FTP, 22 for SFTP) |
| ICECAT_FTP_USERNAME | FTP/SFTP username |
| ICECAT_FTP_PASSWORD | FTP/SFTP password |
| LOG_LEVEL | Logging level |

### Time zones

All timestamps in this project are **UTC**, end-to-end. There is no local-time anywhere in the application path.

- **Application code** uses `datetime.now(timezone.utc)` exclusively. No naive `datetime.now()` calls.
- **Database connections** issue `SET time_zone='+00:00'` on connect, so MySQL's `CURRENT_TIMESTAMP` / `NOW()` (used by every TIMESTAMP column's `server_default` and `onupdate`) returns UTC regardless of the MySQL server's system timezone. This works on Cloud SQL, Azure MySQL, and any local MySQL without changing server config.
- **Logs** are formatted with an explicit `Z` suffix — e.g. `2026-04-08 11:09:34,023Z` — so a log line is unambiguous regardless of the deployment region or developer machine.

If you ever see a log timestamp without a `Z`, the line came from a third-party library that bypasses Python's `logging.Formatter` (rare). Those will still be UTC because we set `logging.Formatter.converter = time.gmtime` globally; they just lack the suffix marker.

## CLI Commands

Base invocation: `python -m icecat_integration [-c config.yaml] <command>`

| Flag | Description |
| :--- | :---------- |
| -c, --config PATH | Path to YAML config file. If omitted, env vars are used |
| -v, --verbose | Enable debug logging |

### Database Management

**init-db** -- Create tables (only creates missing tables, idempotent). No options.

**drop-db** -- Drop ALL tables (requires confirmation).

| Option | Description |
| :----- | :---------- |
| --yes | Skip confirmation prompt |

**clean-products** -- Reset all product and sync data to start a fresh full sync without re-importing taxonomy. Unlike `drop-db` which deletes everything, this keeps categories, attribute names, supplier mappings, and locales intact.

| Option | Description |
| :----- | :---------- |
| --yes | Skip confirmation prompt |

**seed-locales** -- Insert the 10 supported languages (idempotent). No options.

### Data Downloads

**ftp-download-assortment** -- Download product assortment ZIP from FTP/SFTP and extract it.

| Option | Description |
| :----- | :---------- |
| -o, --output DIR | Output directory (default: `data/assortment`) |

**ftp-download-suppliers** -- Download supplier XML files (SuppliersList.xml + supplier_mapping.xml).

| Option | Description |
| :----- | :---------- |
| -o, --output-dir DIR | Output directory (default: `data/refs`) |

**download-icecat-index** -- Download the Icecat full product index (`files.index.csv.gz`, ~947 MB) into `data/downloads/`. Streams the file using FrontOffice basic auth. Same source the Phase 3.5 prefilter inside `sync` uses, but as a standalone step that can be chained with other commands.

| Option | Description |
| :----- | :---------- |
| -o, --output PATH | Output path (default: `data/downloads/files.index.csv.gz`) |
| --url URL | Override the index URL (default: level4 EN index) |

**ftp-test** -- Test FTP/SFTP connection, optionally list or download files.

| Option | Description |
| :----- | :---------- |
| -l, --list | List files on server |
| -d, --download FILE | Download a specific file |
| -o, --output DIR | Output directory (default: `data/downloads`) |
| --keep-zip | Keep ZIP file after extraction |

### Reference Data Import

**update-taxonomy** -- Download + import category hierarchy, feature groups, and attribute names. Schedule weekly or biweekly.

| Option | Description |
| :----- | :---------- |
| --skip-download | Skip download, use existing file in download dir |
| -f, --file PATH | Path to an existing CategoryFeaturesList.xml.gz file |
| -b, --batch-size N | Batch size for bulk inserts (default: 5000) |
| --download-dir DIR | Directory for downloaded files (default: `data/downloads`) |

**import-suppliers** -- Import vendor + brand alias mapping from XML files.

| Option | Description |
| :----- | :---------- |
| --suppliers-xml PATH | Path to SuppliersList.xml (default: `data/refs/SuppliersList.xml`) |
| --mapping-xml PATH | Path to supplier_mapping.xml (default: `data/refs/supplier_mapping.xml`) |

### Product Sync

**prepare-sync** -- Load assortment into the `sync_product` table without syncing (Phases 1-3 only). Run this once before starting parallel sync jobs with `--skip-assortment`.

| Option | Description |
| :----- | :---------- |
| -f, --file PATH | **(required)** Path to assortment file |
| -m, --mode full\|delta | Sync mode used to load assortment (default: `full`) |
| --delimiter DELIM | File delimiter (default: auto-detect) |
| --brand-column NAME | Override brand column name |
| --mpn-column NAME | Override MPN column name |

**sync** -- Sync products from an assortment file (Brand + MPN).

| Option | Description |
| :----- | :---------- |
| -f, --file PATH | Path to assortment file (required unless `--skip-assortment` is set) |
| -m, --mode delta\|full | Sync mode: `delta` (default) or `full` (see Sync Modes below) |
| -s, --source json\|xml | Data source: `json` (default) or `xml` (see Data Sources below) |
| --all-languages | Fetch all 10 supported languages per product (automatic with `--source xml`) |
| -b, --batch-size N | Products per DB commit batch (default: 100) |
| -c, --concurrency N | API calls concurrency (default: 1, sequential recommended) |
| --max-products N | Max products to process from start-index. Omit to process all remaining |
| --start-index N | Skip first N products in the queue (default: 0). Use with `--max-products` to split work across parallel jobs |
| --skip-assortment | Skip FTP download and assortment loading (Phases 1-3). Use when `prepare-sync` already loaded the data |
| --skip-icecat-index-download | Skip ONLY the Icecat index download in Phase 3.5; the prefilter / matching step still runs against the cached `data/downloads/files.index.csv.gz` from a prior `download-icecat-index` call. Errors out if the cached file is missing |
| --fetch-workers N | Parallel XML fetch workers (default: **6**). See "Tuning parallel workers" below |
| --write-workers K | Parallel DB-write workers (default: **2**). See "Tuning parallel workers" below |
| --buffer-size B | Bounded buffer between fetch and write workers (default: 100) |
| --diagnostics | Enable periodic performance diagnostics (every 1000 products). Logs per-stage timing breakdown to identify bottlenecks |
| --resume RUN_ID | Resume an interrupted sync run by UUID |

**sync-product** -- Sync a single product by Brand + MPN.

| Option | Description |
| :----- | :---------- |
| -b, --brand NAME | **(required)** Brand name |
| -m, --mpn CODE | **(required)** Manufacturer part number |
| -s, --source json\|xml | Data source: `json` (default) or `xml` |
| --all-languages | Fetch all 10 supported languages (automatic with `--source xml`) |
| -l, --language CODE | Single language code (default: EN) |

**update-daily-index** -- Download the daily index and mark updated products as PENDING for re-sync.

| Option | Description |
| :----- | :---------- |
| -l, --culture-id CODE | Culture/language ID (default: EN) |

### Monitoring

**prefilter-report** -- Read-only diagnostic that loads `brand_map`, parses the cached Icecat index file, and reports how many rows in `sync_product` exist in the current Icecat catalog. Does NOT modify the database. Useful to validate the prefilter without running a full sync — pair with `download-icecat-index` for a fresh index. Reports two views: `WHERE status='PENDING'` (current prefilter scope) and `WHERE status<>'DELETED'` (post-`Phase 3a` reset scope).

| Option | Description |
| :----- | :---------- |
| --index-path PATH | Cached index file (default: `data/downloads/files.index.csv.gz`) |

**sync-status** -- Check sync run progress and statistics.

| Option | Description |
| :----- | :---------- |
| --run-id ID | Specific run ID to check (default: latest) |

**sync-logs** -- View sync logs for a specific run.

| Option | Description |
| :----- | :---------- |
| --run-id ID | **(required)** Sync run ID |
| --errors-only | Show only error logs |
| --level LEVEL | Filter by log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| -n, --limit N | Number of logs to show (default: 50) |

**sync-cleanup** -- Clean up old sync logs to free disk space.

| Option | Description |
| :----- | :---------- |
| --older-than DAYS | Delete logs older than N days (default: 30) |
| --yes | Skip confirmation prompt |

**assortment-stats** -- Show statistics about an assortment file.

| Option | Description |
| :----- | :---------- |
| -f, --file PATH | **(required)** Path to assortment file |
| --delimiter DELIM | File delimiter (default: auto-detect) |
| --brand-column NAME | Override brand column name |
| --mpn-column NAME | Override MPN column name |

### API Testing

**test-api** -- Test Icecat API connectivity with a sample EAN.

| Option | Description |
| :----- | :---------- |
| --ean EAN | EAN to test with (default: Samsung product) |
| -l, --language CODE | Language code (default: EN) |

**fetch-product** -- Fetch a single product's raw data from the API.

| Option | Description |
| :----- | :---------- |
| --ean EAN | Fetch product by EAN/UPC |
| --icecat-id ID | Fetch product by Icecat ID |
| --product-code CODE | Fetch product by product code (requires --brand) |
| --brand NAME | Brand name (required with --product-code) |
| -l, --language CODE | Language code (default: EN) |

## Sync Modes

Both modes always load the full assortment file into the `sync_product` tracking table. The difference is in what happens after the load.

### `--mode full`

A "fresh list" run. Every non-DELETED row in `sync_product` is re-evaluated against the current Icecat catalog and re-fetched from the API. Use this as the daily / weekly refresh — it answers the question "what does Icecat have for our assortment **right now**". Existing rows in the `product` table are refreshed (not skipped, as in older versions of this code).

The 5 steps:

1. **Load assortment** → upsert into `sync_product` (Phases 1-2). New rows start as `PENDING`; existing rows have only `updated_at` touched.
2. **Detect deletions** → products no longer in the assortment file are marked `DELETED` (Phase 3).
3. **Reset to PENDING** → every non-DELETED row is reset to `PENDING` (Phase 3a, **new in this version**). This clears any stale `SYNCED` / `NOT_FOUND` / `ERROR` / `MATCHED` state from previous runs so the rest of the pipeline starts from a clean slate.
4. **Prefilter against Icecat index** → download `files.index.csv.gz` (~947 MB), build a `(vendor, mpn)` set, mark every PENDING row that is NOT in the index as `NOT_FOUND` (Phase 3.5). Avoids wasting API calls on products Icecat doesn't have.
5. **Fetch from API and write to DB** → every remaining PENDING row is fetched from the Icecat XML API and bulk-written to the `product` and child tables in 100-row transactions (Phase 5).

```bash
python -m icecat_integration sync -f assortment.txt --mode full --source xml
```

> **Note (parallel jobs):** the Phase 3a reset is intentionally **skipped** when `--skip-assortment` is used (parallel-job pattern), so worker jobs don't race-update each other. In that pattern the reset belongs in `prepare-sync` and will be added in a follow-up. For now, parallel `--mode full` jobs behave like delta with respect to existing-row refreshing.

### `--mode delta` (default)

1. Load assortment file → upsert `sync_product` table
2. Select only rows with status `PENDING`, `MATCHED`, or `ERROR` (retry < 3) → only products that need updating

```bash
python -m icecat_integration sync -f assortment.txt --mode delta --all-languages
```

### `update-daily-index` (separate command, run before delta)

- Downloads Icecat's daily XML (products updated in the last 24 hours)
- Cross-references with the `sync_product` table
- Marks matching products as `PENDING` so the next delta sync picks them up

**Typical daily workflow:**

```bash
python -m icecat_integration update-daily-index          # mark Icecat-changed products as PENDING
python -m icecat_integration sync -f assortment.txt      # delta: only syncs PENDING products
```

### `--resume`

Not a mode — resumes an interrupted run from where it left off. Works with both delta and full.

```bash
python -m icecat_integration sync -f assortment.txt --resume <UUID>
```

## Data Sources

The `--source` flag controls how product data is fetched from Icecat.

### `--source json` (default)

Uses the Icecat FrontOffice Live JSON API (`live.icecat.biz/api`). Makes **one API call per language per product** — for 10 languages, that's 10 calls per product. Each call returns one language's data, and the results are merged locally before writing to the database.

- Auth: API key header
- Endpoint: `live.icecat.biz/api`
- Throughput: ~3-5 products/sec (single job, Azure)

### `--source xml` (recommended)

Uses the Icecat XML endpoint (`data.icecat.biz/xml_s3/xml_server3.cgi`) with `lang=INT`, which returns **all locales in a single response**. This eliminates 9 out of 10 API calls per product.

- Auth: HTTP Basic Auth (same FrontOffice credentials)
- Endpoint: `data.icecat.biz/xml_s3/xml_server3.cgi?lang=INT`
- Throughput depends on API rate limits and DB performance
- Automatically sets `--all-languages` (the response contains all locales)
- API calls are made sequentially to ensure 100% data accuracy

Both sources produce the same database output — descriptions, attributes, media, etc. in all 10 languages.

## Sync Pipeline

A `--mode full` run executes the following 5 phases (in order):

1. **Load assortment** (Phases 1-2) — reads the Brand + MPN file into the `sync_product` tracking table via bulk `ON DUPLICATE KEY UPDATE`. New rows are inserted as `PENDING`; existing rows have only their `updated_at` touched.
2. **Detect deletions** (Phase 3) — products that were in `sync_product` but no longer in the assortment file are marked `DELETED`. These are deactivated in the `product` table later (Phase 6).
3. **Reset to PENDING** (Phase 3a) — every non-`DELETED` row in `sync_product` is reset to `PENDING`, with `retry_count=0` and `error_message=NULL`. This makes `--mode full` a true daily refresh: any state from prior runs is cleared so steps 4-5 re-evaluate every product against today's Icecat catalog. Skipped when `--skip-assortment` is used (parallel-job pattern).
4. **Prefilter against Icecat index** (Phase 3.5) — downloads the full Icecat product index (~27M products, ~947 MB) and builds a `(vendor, mpn)` set. Every `PENDING` row that is NOT in the index is marked `NOT_FOUND` so we avoid wasting API calls. On a typical IM assortment of ~3.4M products, ~32% match the index (~1.09M products); the remaining ~68% are short-circuited here. Pass `--skip-icecat-index-download` to skip ONLY the download step and reuse a cached `data/downloads/files.index.csv.gz` (the matching itself still runs).
5. **Fetch and write** (Phase 5) — for each remaining `PENDING` row, fetches the full product data from the Icecat XML API and writes it to the `product` and child tables using bulk SQL (100 rows per transaction). Successful rows transition to `SYNCED`; XML 404s transition to `NOT_FOUND`.

Two cleanup phases run after Phase 5:

- **Phase 6 — Deactivate stale** — products marked `DELETED` in step 2 get `isactive=0` set on the `product` row.
- **Phase 7 — Retry** — currently a no-op; reserved for retrying transient `ERROR` rows.

API calls are made sequentially to ensure 100% data accuracy. Products are written to the database in bulk (100 per transaction).

### Single-job chained run (Cloud Run / single container)

The recommended way to run a full sync from one container is to chain three commands so the SFTP and HTTP downloads happen exactly **once**:

```bash
python -m icecat_integration -c config/config.yaml ftp-download-assortment && \
python -m icecat_integration -c config/config.yaml download-icecat-index && \
python -m icecat_integration -c config/config.yaml sync \
    -f data/assortment/DatasheetSKUGlobal_Coverage.txt \
    --mode full --source xml --skip-icecat-index-download
```

Without `--skip-icecat-index-download`, Phase 3.5 would re-download the ~947 MB Icecat index even though `download-icecat-index` just put a fresh copy on disk. The flag tells `sync` to skip ONLY the download — the prefilter/matching against the cached file still runs as normal. If the cached file is missing the command errors out cleanly.

### Parallel Jobs

For large assortments (1M+ products), split the workload across multiple parallel sync jobs using `--start-index` and `--max-products`:

```bash
# Step 1: Load assortment and run the index prefilter once
python -m icecat_integration prepare-sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full

# Step 2: Run 3 parallel sync jobs (each on its own compute node)
# Job 1: products 0–299,999
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt \
  --mode full --source xml --skip-assortment --start-index 0 --max-products 300000

# Job 2: products 300,000–599,999
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt \
  --mode full --source xml --skip-assortment --start-index 300000 --max-products 300000

# Job 3: products 600,000+
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt \
  --mode full --source xml --skip-assortment --start-index 600000 --max-products 400000
```

Each parallel job **must run on its own compute node** (separate container instance). Running multiple jobs on a shared node provides minimal benefit because they compete for CPU and DB connections.

### Performance

When `--fetch-workers > 1` (default: 6), the sync uses a **producer/consumer parallel pipeline**:

- **M fetch workers** call the Icecat XML API concurrently and parse the responses
- A **bounded buffer** (default: 100 items) sits between fetchers and writers
- **K write workers** drain the buffer and bulk-commit batches of `--batch-size` products to the database, each with its own DB session

This architecture was benchmarked on a US-based Azure setup (container in `centralus`, MySQL in `centralus`, Icecat API in NL) against Ingram Micro's full 1M+ product assortment.

**Proven defaults (shipped as the CLI defaults):**

| Parameter | Default | Why |
| :-------- | :------ | :-- |
| `--fetch-workers` | **6** | 6 concurrent XML calls = ~28 req/s to Icecat. Stays safely under Icecat's sustained rate limit. Higher values (10+) trigger HTTP 429 throttling over long runs, which paradoxically *lowers* throughput. |
| `--write-workers` | **2** | 2 DB writers give enough parallelism for the batched commits. Higher values (3+) cause MySQL InnoDB deadlocks on `productfeatures` / `media_data` / `product_addons` because concurrent transactions lock overlapping row ranges. Each deadlock adds 50-800ms of retry overhead. |
| `--batch-size` | **100** | Each DB commit writes 100 products in one transaction. Larger batches (500+) increase deadlock blast radius AND memory usage (risk of OOM). Smaller batches (10-50) increase commit overhead. 100 is the tested sweet spot. |
| `--buffer-size` | **100** | Matches batch-size so one full batch is always ready for the writer. |
| `DB_POOL_SIZE` | **20** | Connection pool. 6 fetchers + 2 writers + orchestrator overhead = ~10 active connections. Pool of 20 gives comfortable headroom. |

**Benchmark results (US centralus → Icecat NL):**

| Config | Sustained rate | 429s | Deadlocks | DB Errors |
| :----- | :------------ | :--- | :-------- | :-------- |
| `1/1` (sequential) | ~3.5 prod/s | 0 | 0 | 0 |
| **`6/2` (default)** | **~18 prod/s** | **0** | **0** | **0** |
| `10/5` | ~9 prod/s | 5 | some | 73 |
| `15/8` | ~5 prod/s | 4 | many | 113 |
| `30/15` | ~5 prod/s | hundreds | hundreds | 1,121 |

The counterintuitive result: **less concurrency = higher sustained throughput** because avoiding Icecat 429s and MySQL deadlocks saves more time than the extra parallelism gains. The 6/2 default is the tested optimum.

### Tuning parallel workers

**When to lower `--fetch-workers`:**
- You see repeated `HTTP 429 ... retrying in 5s` lines in the logs. Each 429 wastes 5 seconds. Drop to 4 or 3 and re-check.
- Icecat tightens their per-IP rate limit (they can change this without notice).

**When to lower `--write-workers`:**
- You see `[Parallel] batch of N failed ... Deadlock found` in the logs. The deadlock retry succeeds most of the time, but if `ERROR` rows accumulate in `sync_product`, drop to 1 writer (zero deadlocks guaranteed).

**When to raise `--fetch-workers`:**
- You are running from a container in the **same region as Icecat** (EU / NL). The cross-Atlantic round-trip (~215 ms) is the main per-call cost. From EU, each call is ~95 ms, so more workers can be useful. Test with 10, then 15, and watch for 429s.
- Icecat confirms a higher rate limit for your IP/account.

**When to raise `--write-workers`:**
- You are using a high-vCore DB (8+) with plenty of IOPS. More writers can help if the DB is not the bottleneck. Test with 3, watch for deadlocks in the logs.

**Setting `--fetch-workers 1`** disables the parallel path entirely and uses the original sequential pipeline (single-threaded fetch → DB write with one-batch overlap). Useful for debugging or when you need deterministic row ordering.

### Performance diagnostics (`--diagnostics`)

Add `--diagnostics` to any sync command to enable periodic performance reports (every 1000 products). The report breaks down where time is spent across the pipeline and automatically identifies the bottleneck:

```
[DIAGNOSTICS] Performance report (products 0-1,025):
  API fetch:      avg=826ms  min=38ms  max=3.76s  (1068 fetches)
    -> throughput: 12.0 fetches/s actual  (1.2/s per worker x 10 workers)
  XML parse:      avg=4ms  min=0ms  max=269ms
  DB write:       avg=2.57s  min=56ms  max=5.60s  (60 batches)
  DB commit:      avg=28ms  min=1ms  max=447ms
  Queue wait:     avg=39ms  min=0ms  max=699ms  (write worker idle time)
  Deadlocks:      1
  Buffer depth:   avg=14  min=0  max=36  (of buffer capacity)
  Batch size:     avg=17  min=1  max=39  products per bulk write
  Response size:  avg=143KB  max=872KB  (1068 responses)
  DB per-table breakdown:
    attributes           avg=1.22s  total=73.4s  (48%)
    media                avg=507ms  total=30.4s  (20%)
    search_attrs         avg=288ms  total=17.3s  (11%)
    features             avg=221ms  total=13.3s  (9%)
    ...
  Write throughput: 6.6 products written/s  (across 2 writer(s))
  Batch fill:     17% efficiency  (avg 17 of 100)
  ─────────────
  Bottleneck: API fetch (42%) → check network RTT to Icecat, or increase --fetch-workers
  Sustained rate: 11.4 prod/s
```

**How to read the report:**

| Metric | What it tells you |
| :----- | :---------------- |
| **API fetch avg** | Network round-trip time to Icecat per product. High = increase `--fetch-workers` |
| **Fetch throughput** | Actual fetches/second across all workers — the pipeline input rate |
| **Queue wait** | Time write workers spend idle waiting for data. High = fetchers can't keep up |
| **Buffer depth** | How full the buffer is (0 = starved, 100 = full). Low = fetch-bound, high = write-bound |
| **Batch size** | Products per bulk DB write. Low = wasted commit overhead. Target: close to 100 |
| **DB per-table** | Which tables consume the most write time. `attributes` is typically dominant |
| **Write throughput** | Products written per second — the pipeline output rate |
| **Batch fill** | Efficiency of batching. Below 50% = fetchers are too slow to fill batches |
| **Bottleneck** | Auto-detected dominant stage with actionable hint |

When `--diagnostics` is not set, zero overhead (all recording methods short-circuit on a single boolean check).

## Cloud Deployment

### Docker

The app ships with a multi-stage Dockerfile (Python 3.12-slim).

```bash
docker build -t icecat-integration:latest .
```

```bash
docker run --rm -e DB_HOST=your-db-host -e DB_PORT=3306 -e DB_NAME=icecat_integration -e DB_USER=admin -e DB_PASSWORD=secret -e DB_SSL=true -e DB_POOL_SIZE=20 -e ICECAT_FO_USERNAME=your-fo-user -e ICECAT_FO_PASSWORD=your-fo-pass -e ICECAT_FO_API_KEY=your-api-key -e ICECAT_FTP_USERNAME=your-ftp-user -e ICECAT_FTP_PASSWORD=your-ftp-pass -e LOG_LEVEL=INFO icecat-integration:latest "python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode delta --all-languages"
```

### Container Jobs

Deploy as container jobs on your cloud provider (Azure Container App Jobs, Google Cloud Run Jobs, AWS ECS Tasks, etc.).

#### Infrastructure Requirements

1. **Container Registry** -- to host the Docker image
2. **Managed database** -- with SSL enabled
3. **Container orchestration platform** -- shared environment for all jobs

#### Required Jobs

**Reference data** (run first, once per week):

| Job Name | Command | Schedule | CPU | Memory |
| :------- | :------ | :------- | :-- | :----- |
| icecat-taxonomy | update-taxonomy | Weekly | 2 | 4 Gi |
| icecat-suppliers | ftp-download-suppliers && import-suppliers | Weekly | 2 | 4 Gi |

**Full sync** (weekly — 3 parallel jobs for maximum throughput):

First, load the assortment once with a single preparation job:

| Job Name | Command | CPU | Memory |
| :------- | :------ | :-- | :----- |
| icecat-prepare | ftp-download-assortment && prepare-sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full | 4 | 16 Gi |

Then run 3 parallel sync jobs (each on its own compute node):

| Job Name | Command | CPU | Memory |
| :------- | :------ | :-- | :----- |
| icecat-sync-1 | sync -f ... --mode full --source xml --skip-assortment --start-index 0 --max-products 300000 | 4 | 8 Gi |
| icecat-sync-2 | sync -f ... --mode full --source xml --skip-assortment --start-index 300000 --max-products 300000 | 4 | 8 Gi |
| icecat-sync-3 | sync -f ... --mode full --source xml --skip-assortment --start-index 600000 --max-products 400000 | 4 | 8 Gi |

> **Important:** The prepare job needs 16 Gi because the Icecat index prefilter loads 27M+ products into memory (~6 GB). The sync jobs use `--skip-assortment` which skips the prefilter, so 8 Gi is enough. Each sync job must run on its own dedicated compute node (not shared) for best throughput.

**Delta sync** (daily, single job is sufficient):

| Job Name | Command | Schedule | CPU | Memory |
| :------- | :------ | :------- | :-- | :----- |
| icecat-sync-delta | ftp-download-assortment && update-daily-index && sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode delta --source xml | Daily | 4 | 16 Gi |

#### Authentication

Icecat validates API requests by IP whitelist. Cloud containers (Azure, GCP, AWS) use dynamic outbound IPs that change on every execution, which breaks IP whitelisting. Set `ICECAT_API_TOKEN` to bypass IP validation entirely. Get the token from the Icecat portal: **My Profile → Access Tokens → API Access Token**.

#### Key Settings

- **ICECAT_API_TOKEN**: Required for cloud deployments (bypasses IP whitelist).
- **Timeout**: Set maximum runtime to 86400 seconds (24 hours) for full syncs of large assortments (1M+ products).
- **CPU / Memory**: 4 CPU minimum per sync job. The prepare job (index prefilter) needs 16 Gi; sync jobs with `--skip-assortment` need 8 Gi.
- **DB_POOL_SIZE=20**: Optimal for bulk writes.
- **DB proximity**: Keep the database in the same region as the containers. Cross-region DB latency is the main performance bottleneck.

## Initial Setup Sequence

When deploying for the first time, run these steps in order:

```bash
# 1. Create tables
python -m icecat_integration init-db

# 2. Seed languages
python -m icecat_integration seed-locales

# 3. Import taxonomy (~8 min, downloads 1.5 GB)
python -m icecat_integration update-taxonomy

# 4. Import suppliers
python -m icecat_integration ftp-download-suppliers
python -m icecat_integration import-suppliers

# 5. Download assortment
python -m icecat_integration ftp-download-assortment

# 6. Run initial full sync
# The sync automatically downloads the Icecat index, prefilters,
# and syncs only products that exist in Icecat.
# Single job: ~12 hours for 1M products (EU same-region DB)
# 3 parallel jobs on 3 nodes: ~7-8 hours
# See "Parallel Jobs" section above for splitting across multiple jobs
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full --source xml
```

To add additional locales later, insert directly into the `locales` table in the database.

### Expected Data Volumes

| Table | Approximate Rows |
| :---- | :--------------- |
| locales | 10 |
| vendor | ~42,000 |
| supplier_mapping | ~34,000 |
| categoryMapping | ~6,800 |
| category | ~68,000 (6.8K x 10 locales) |
| attributenames | ~290,000 |
| categoryheader | ~85,000 |
| categorydisplayattributes | ~7,000,000 |

Product table volumes depend on assortment size and Icecat hit rate.

## Ongoing Operations

### Daily Delta Sync

```bash
python -m icecat_integration ftp-download-assortment
python -m icecat_integration update-daily-index
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode delta --source xml
```

### Weekly Full Refresh

```bash
python -m icecat_integration update-taxonomy
python -m icecat_integration ftp-download-suppliers && python -m icecat_integration import-suppliers
python -m icecat_integration ftp-download-assortment
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full --source xml
```

### Monitoring a Running Sync

```bash
# Check latest run status
python -m icecat_integration sync-status

# Check specific run
python -m icecat_integration sync-status --run-id <UUID>

# View error logs
python -m icecat_integration sync-logs --run-id <UUID> --errors-only
```
