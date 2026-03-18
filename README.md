# Icecat Integration

A Python CLI application that syncs product data from the [Icecat](https://icecat.biz) catalog into a database. It fetches product specifications, descriptions, media, attributes, and relationships for a given product assortment (Brand + MPN list) across multiple languages.

## Features

| Feature | Description |
| :------ | :---------- |
| **Product Sync** | Fetches full product data from Icecat by Brand + MPN via JSON API or XML endpoint |
| **Multi-language** | Supports 10 languages: EN, NL, FR, DE, IT, ES, PT, ZH, HU, TH. XML mode (`lang=INT`) fetches all in one call |
| **Parallel Sync** | Split large assortments across multiple jobs using `prepare-sync` + `--skip-assortment --start-index` |
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
| SYNC_CONCURRENCY | Max concurrent API calls |
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
| -c, --concurrency N | Max concurrent API calls (default: 10) |
| --max-products N | Max products to process from start-index. Omit to process all remaining |
| --start-index N | Skip first N products in the queue (default: 0). Use with `--max-products` to split work across parallel jobs |
| --skip-assortment | Skip FTP download and assortment loading (Phases 1-3). Use when `prepare-sync` already loaded the data |
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

Both modes always load the full assortment file into the `sync_product` tracking table. The difference is in what gets fetched from the Icecat API.

### `--mode full`

1. Load assortment file → upsert `sync_product` table
2. Select **ALL** rows from `sync_product` → re-fetch everything from the Icecat API regardless of status

```bash
python -m icecat_integration sync -f assortment.txt --mode full --all-languages
```

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

### `--source xml`

Uses the Icecat XML endpoint (`data.icecat.biz/xml_s3/xml_server3.cgi`) with `lang=INT`, which returns **all locales in a single response**. This eliminates 9 out of 10 API calls per product.

- Auth: HTTP Basic Auth (same FrontOffice credentials)
- Endpoint: `data.icecat.biz/xml_s3/xml_server3.cgi?lang=INT`
- Throughput: ~10-40 products/sec per job (depending on parallelism)
- Automatically sets `--all-languages` (the response contains all locales)

Both sources produce the same database output — descriptions, attributes, media, etc. in all 10 languages.

## Parallel Sync (Large Assortments)

For assortments with 100K+ products, use parallel jobs to speed up the sync. The workflow splits the sync_product table into non-overlapping slices using SQL `OFFSET`/`LIMIT`.

### Step 1: Load Assortment (once)

```bash
python -m icecat_integration prepare-sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full
```

This loads the assortment into the `sync_product` table (~6 min for 1.5M products).

### Step 2: Run Parallel Sync Jobs

Split the work across N jobs using `--start-index` and `--max-products`. Divide the total product count by N to determine each job's slice:

```
Total products: T
Number of jobs: N
Slice size:     S = T / N

Job 1: --start-index 0   --max-products S
Job 2: --start-index S   --max-products S
Job 3: --start-index 2*S --max-products S
...
Job N: --start-index (N-1)*S   (no --max-products = process all remaining)
```

Example with 4 jobs:

```bash
python -m icecat_integration sync --skip-assortment --source xml --start-index 0      --max-products 385000 --mode full --batch-size 100 --concurrency 3
python -m icecat_integration sync --skip-assortment --source xml --start-index 385000  --max-products 385000 --mode full --batch-size 100 --concurrency 3
python -m icecat_integration sync --skip-assortment --source xml --start-index 770000  --max-products 385000 --mode full --batch-size 100 --concurrency 3
python -m icecat_integration sync --skip-assortment --source xml --start-index 1155000 --mode full --batch-size 100 --concurrency 3
```

Each job operates on a deterministic slice (ordered by `sync_product.id`) with no overlap.

> **Rate limit**: Icecat enforces a limit of ~100 requests/second per account. When running N parallel jobs, set `--concurrency` so that the total across all jobs stays under this limit (e.g. 4 jobs × 25 concurrency = 100). Exceeding this triggers HTTP 429 responses — the client retries automatically with backoff, but sustained overload slows down all jobs.

### Performance Tuning

| Parameter | Description | Recommended |
| :-------- | :---------- | :---------- |
| --batch-size | Products per DB commit batch | 100 |
| --concurrency | Parallel API calls | 40 |
| DB_POOL_SIZE | Connection pool size | 20 |

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

**Full sync** (weekly, parallel XML approach — see "Parallel Sync" section for how to calculate ranges):

| Job Name | Command | Schedule | CPU | Memory |
| :------- | :------ | :------- | :-- | :----- |
| icecat-prepare-sync | ftp-download-assortment && prepare-sync -f ... --mode full | Weekly | 1 | 2 Gi |
| icecat-sync-N (×N jobs) | sync --skip-assortment --source xml --start-index ... --max-products ... --mode full | After prepare | 1 | 2 Gi |

**Delta sync** (daily, single job is sufficient):

| Job Name | Command | Schedule | CPU | Memory |
| :------- | :------ | :------- | :-- | :----- |
| icecat-sync-delta | ftp-download-assortment && update-daily-index && sync --mode delta --source xml | Daily | 2 | 4 Gi |

#### Authentication

Icecat validates API requests by IP whitelist. Cloud containers (Azure, GCP, AWS) use dynamic outbound IPs that change on every execution, which breaks IP whitelisting. Set `ICECAT_API_TOKEN` to bypass IP validation entirely. Get the token from the Icecat portal: **My Profile → Access Tokens → API Access Token**.

#### Key Settings

- **ICECAT_API_TOKEN**: Required for cloud deployments (bypasses IP whitelist).
- **Timeout**: Set maximum runtime to 86400 seconds (24 hours) for full syncs of large assortments (1M+ products).
- **CPU / Memory**: 1 CPU / 2 Gi per parallel sync job, 2 CPU / 4 Gi for single jobs.
- **DB_POOL_SIZE=20**: Optimal for concurrency=40.
- **Concurrency**: 3-10 per job when running 4 parallel jobs (to avoid 429 rate limiting), 40 for a single job.

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

# 6. Run initial full sync (hours, depending on assortment size)
# Option A: Single job with XML (recommended for <100K products)
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full --source xml --batch-size 100 --concurrency 40

# Option B: Parallel jobs with XML (recommended for 100K+ products)
# See "Parallel Sync" section above
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
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode delta --source xml --batch-size 100 --concurrency 40
```

### Weekly Full Refresh

```bash
python -m icecat_integration update-taxonomy
python -m icecat_integration ftp-download-suppliers && python -m icecat_integration import-suppliers
python -m icecat_integration ftp-download-assortment

# Single job (small assortments)
python -m icecat_integration sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full --source xml --batch-size 100 --concurrency 40

# Or parallel jobs (large assortments, see "Parallel Sync" section)
python -m icecat_integration prepare-sync -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full
# Then run 4 parallel sync jobs with --skip-assortment --start-index ...
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
