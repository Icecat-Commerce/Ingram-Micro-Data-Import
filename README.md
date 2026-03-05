# Icecat Integration

A Python CLI application that syncs product data from the [Icecat](https://icecat.biz) catalog into a database. It fetches product specifications, descriptions, media, attributes, and relationships for a given product assortment (Brand + MPN list) across multiple languages.

## Features

| Feature | Description |
| :------ | :---------- |
| **Product Sync** | Fetches full product data from the Icecat API by Brand + MPN |
| **Multi-language** | Supports 9 languages (configurable): EN, NL, FR, DE, IT, ES, PT, ZH, TH |
| **Taxonomy Import** | Downloads and imports Icecat category hierarchy, feature groups, and attribute names (~6.8K categories, ~290K attributes) |
| **Supplier Import** | Downloads and imports brand/vendor mapping (~42K vendors, ~34K brand aliases) |
| **Assortment Download** | Downloads product assortment file from FTP |
| **Daily Index** | Downloads daily index to detect changed products for delta sync |
| **Sync Tracking** | Tracks every product's sync status, API responses, and errors |
| **Addon Type Derivation** | Derives product relationship types (Upsell/Cross-sell) from category comparison |

## Quick Start

### Prerequisites

- Python 3.10+
- MySQL 8.0+
- Icecat API credentials
- Assortment File FTP credentials

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
# Download the product assortment file from FTP
python -m icecat_integration -c config/config.yaml ftp-download-assortment

# Delta mode - only processes new/unsynced products (daily use)
python -m icecat_integration -c config/config.yaml sync -f data/assortment/DatasheetSKUCoverage_Global.txt --mode delta --all-languages

# Full mode - re-processes entire assortment (weekly refresh)
python -m icecat_integration -c config/config.yaml sync -f data/assortment/DatasheetSKUCoverage_Global.txt --mode full --all-languages
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
| ICECAT_FTP_HOST | FTP server hostname |
| ICECAT_FTP_USERNAME | FTP username |
| ICECAT_FTP_PASSWORD | FTP password |
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

**seed-locales** -- Insert the 9 supported languages (idempotent). No options.

### Data Downloads

**ftp-download-assortment** -- Download product assortment ZIP from FTP and extract it.

| Option | Description |
| :----- | :---------- |
| -o, --output DIR | Output directory (default: `data/assortment`) |

**ftp-download-suppliers** -- Download supplier XML files (SuppliersList.xml + supplier_mapping.xml).

| Option | Description |
| :----- | :---------- |
| -o, --output-dir DIR | Output directory (default: `data/refs`) |

**ftp-test** -- Test FTP connection, optionally list or download files.

| Option | Description |
| :----- | :---------- |
| -l, --list | List files on FTP server |
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

**sync** -- Sync products from an assortment file (Brand + MPN).

| Option | Description |
| :----- | :---------- |
| -f, --file PATH | **(required)** Path to assortment file |
| -m, --mode delta\|full | Sync mode: `delta` (default) or `full` (see Sync Modes below) |
| --all-languages | Fetch all 9 supported languages per product |
| -b, --batch-size N | Products per DB commit batch (default: 100) |
| -c, --concurrency N | Max concurrent API calls (default: 10) |
| --resume RUN_ID | Resume an interrupted sync run by UUID |

**sync-product** -- Sync a single product by Brand + MPN.

| Option | Description |
| :----- | :---------- |
| -b, --brand NAME | **(required)** Brand name |
| -m, --mpn CODE | **(required)** Manufacturer part number |
| --all-languages | Fetch all 9 supported languages |
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
docker run --rm -e DB_HOST=your-db-host -e DB_PORT=3306 -e DB_NAME=icecat_integration -e DB_USER=admin -e DB_PASSWORD=secret -e DB_SSL=true -e DB_POOL_SIZE=20 -e ICECAT_FO_USERNAME=your-fo-user -e ICECAT_FO_PASSWORD=your-fo-pass -e ICECAT_FO_API_KEY=your-api-key -e ICECAT_FTP_USERNAME=your-ftp-user -e ICECAT_FTP_PASSWORD=your-ftp-pass -e LOG_LEVEL=INFO icecat-integration:latest sync -f data/assortment/DatasheetSKUCoverage_Global.txt --mode delta --all-languages
```

### Container Jobs

Deploy as container jobs on your cloud provider (Azure Container App Jobs, Google Cloud Run Jobs, AWS ECS Tasks, etc.).

#### Infrastructure Requirements

1. **Container Registry** -- to host the Docker image
2. **Managed database** -- with SSL enabled
3. **Container orchestration platform** -- shared environment for all jobs

#### Required Jobs

| Job Name | Command | Schedule | CPU | Memory |
| :------- | :------ | :------- | :-- | :----- |
| icecat-sync | ftp-download-assortment && sync --mode full --all-languages | Weekly | 2 | 4 Gi |
| icecat-sync-delta | ftp-download-assortment && sync --mode delta --all-languages | Daily | 2 | 4 Gi |
| icecat-taxonomy | update-taxonomy | Weekly | 2 | 4 Gi |
| icecat-suppliers | ftp-download-suppliers && import-suppliers | Weekly | 2 | 4 Gi |

#### Key Settings

- **Timeout**: Set maximum runtime to 86400 seconds (24 hours) for full syncs of large assortments (200K+ products).
- **CPU / Memory**: 2 CPU / 4 Gi minimum.
- **DB_POOL_SIZE=20**: Optimal for concurrency=40.

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
python -m icecat_integration sync -f data/assortment/DatasheetSKUCoverage_Global.txt --mode full --all-languages --batch-size 100 --concurrency 40
```

To add additional locales later, insert directly into the `locales` table in the database.

### Expected Data Volumes

| Table | Approximate Rows |
| :---- | :--------------- |
| locales | 9 |
| vendor | ~42,000 |
| supplier_mapping | ~34,000 |
| categoryMapping | ~6,800 |
| category | ~60,000 (6.8K x 9 locales) |
| attributenames | ~290,000 |
| categoryheader | ~85,000 |
| categorydisplayattributes | ~7,000,000 |

Product table volumes depend on assortment size and Icecat hit rate.

## Ongoing Operations

### Daily Delta Sync

```bash
python -m icecat_integration ftp-download-assortment
python -m icecat_integration sync -f data/assortment/DatasheetSKUCoverage_Global.txt --mode delta --all-languages --batch-size 100 --concurrency 40
```

### Weekly Full Refresh

```bash
python -m icecat_integration update-taxonomy
python -m icecat_integration ftp-download-suppliers && python -m icecat_integration import-suppliers
python -m icecat_integration ftp-download-assortment
python -m icecat_integration sync -f data/assortment/DatasheetSKUCoverage_Global.txt --mode full --all-languages --batch-size 100 --concurrency 40
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
