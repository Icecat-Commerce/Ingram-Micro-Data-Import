"""CLI entry point for Icecat Integration."""

import asyncio
import logging
import sys
from pathlib import Path

import click

from sqlalchemy import text

from .config import AppConfig
from .api import IcecatXmlDataService, IcecatJsonDataFetchService
from .database.connection import init_db


def setup_logging(verbose: bool = False, log_file: str | None = None) -> None:
    """Configure logging based on settings."""
    level = logging.DEBUG if verbose else logging.INFO
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=level, format=format_str, handlers=handlers)

    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


@click.group()
@click.option(
    "--config",
    "-c",
    "config_file",
    type=click.Path(exists=True),
    help="Path to config YAML file",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose/debug logging")
@click.pass_context
def cli(ctx: click.Context, config_file: str | None, verbose: bool) -> None:
    """Icecat Integration CLI - Fetch product data from Icecat."""
    ctx.ensure_object(dict)

    # Load configuration
    if config_file:
        config = AppConfig.from_yaml(config_file)
    else:
        config = AppConfig.load()

    ctx.obj["config"] = config
    ctx.obj["verbose"] = verbose

    # Setup logging
    setup_logging(verbose, config.logging.file_path if config.logging else None)


# =============================================================================
# Database Commands
# =============================================================================


@cli.command("init-db")
@click.pass_context
def init_database(ctx: click.Context) -> None:
    """Create any missing tables (never alters or drops existing ones)."""
    config: AppConfig = ctx.obj["config"]

    click.echo("Initializing database...")
    db = init_db(config.database)
    created = db.create_tables()
    if created:
        click.echo(f"Created {len(created)} table(s): {', '.join(created)}")
    else:
        click.echo("All tables already exist — nothing to do.")
    click.echo(f"Database ready at {config.database.host}")


@cli.command("drop-db")
@click.option("--yes", is_flag=True, help="Confirm deletion without prompt")
@click.pass_context
def drop_database(ctx: click.Context, yes: bool) -> None:
    """Drop all tables from the database."""
    if not yes:
        click.confirm("This will delete all data. Are you sure?", abort=True)

    config: AppConfig = ctx.obj["config"]
    db = init_db(config.database)
    db.drop_tables()
    click.echo("Database tables dropped successfully.")


@cli.command("clean-products")
@click.option("--yes", is_flag=True, help="Confirm deletion without prompt")
@click.pass_context
def clean_products(ctx: click.Context, yes: bool) -> None:
    """Truncate all product/sync data while keeping taxonomy tables intact."""
    if not yes:
        click.confirm(
            "This will delete ALL product and sync data (taxonomy is kept). Continue?",
            abort=True,
        )

    config: AppConfig = ctx.obj["config"]
    db = init_db(config.database)

    tables_to_truncate = [
        # Child tables (FK dependents of product)
        "productdescriptions",
        "productmarketingInfo",
        "productfeatures",
        "productattribute",
        "search_attribute",
        "media_data",
        "icecat_media_thumbnails",
        "product_addons",
        # Delete logs
        "deleted_media",
        "deleted_attributes",
        "deleted_features",
        "deleted_addons",
        # Sync state
        "sync_errors",
        "sync_log",
        "sync_product",
        "sync_run",
        # Delta tables
        "Delta_SYS_sequence",
        "Delta_SYS_product_sequence",
        "Delta_SYS_deletion_prodlocids",
        "Delta_SYS_prodlocaleids_full",
        # Core (after children are cleared)
        "product",
        "vendor",
    ]

    with db.engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        for table in tables_to_truncate:
            conn.execute(text(f"TRUNCATE TABLE `{table}`"))
            click.echo(f"  Truncated {table}")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()

    click.echo(f"Cleaned {len(tables_to_truncate)} product/sync tables. Taxonomy intact.")


# =============================================================================
# XML Data Commands
# =============================================================================


@cli.command("fetch-daily-index")
@click.option("--culture-id", "-l", default="EN", help="Culture/language ID (default: EN)")
@click.pass_context
def fetch_daily_index(ctx: click.Context, culture_id: str) -> None:
    """Fetch the daily index file from Icecat."""
    config: AppConfig = ctx.obj["config"]

    async def _fetch():
        service = IcecatXmlDataService(config.icecat)
        result = await service.download_daily_index_file_async(culture_id)
        if result is not None:
            # Count files
            files_index = result.find(".//files.index") or result
            files = files_index.findall(".//file") if hasattr(files_index, 'findall') else []
            click.echo(f"Successfully fetched daily index for culture: {culture_id}")
            click.echo(f"Found {len(files)} products in index")
        else:
            click.echo("Failed to fetch daily index", err=True)

    asyncio.run(_fetch())


@cli.command("update-daily-index")
@click.option("--culture-id", "-l", default="EN", help="Culture/language ID (default: EN)")
@click.pass_context
def update_daily_index(ctx: click.Context, culture_id: str) -> None:
    """Download daily index and mark updated products for re-sync.

    Downloads the Icecat daily index XML file (lists all products updated
    in the last 24 hours), cross-references with the sync_product table,
    and marks products needing re-sync as PENDING.

    Run this before 'sync --mode delta' to only re-fetch changed products.

    Example:
        icecat update-daily-index        # Process EN daily index
        icecat sync -f assortment.csv    # Delta sync picks up pending products
    """
    from .database.connection import init_db
    from .services.daily_index_service import DailyIndexService

    config: AppConfig = ctx.obj["config"]
    db_manager = init_db(config.database)

    click.echo(f"Downloading daily index for culture: {culture_id}...")

    async def _update():
        with db_manager.session() as session:
            service = DailyIndexService(config.icecat, session)
            result = await service.update_from_daily_index(culture_id)

            click.echo()
            click.secho("Daily Index Update Results", bold=True)
            click.echo("=" * 40)
            click.echo(f"  Products in index:     {result.total_in_index:,}")
            click.echo(f"  In our assortment:     {result.products_in_assortment:,}")
            click.echo(f"  Marked for re-sync:    {result.products_marked_pending:,}")
            click.echo(f"  Already pending:       {result.products_already_pending:,}")
            if result.parse_errors:
                click.echo(f"  Parse errors:          {result.parse_errors:,}")

            if result.products_marked_pending > 0:
                click.secho(
                    f"\n{result.products_marked_pending:,} products will be re-synced on next delta run.",
                    fg="green",
                )
            else:
                click.echo("\nNo products need re-syncing.")

    asyncio.run(_update())


@cli.command("fetch-feature-groups")
@click.pass_context
def fetch_feature_groups(ctx: click.Context) -> None:
    """Fetch the feature groups list from Icecat."""
    config: AppConfig = ctx.obj["config"]

    async def _fetch():
        service = IcecatXmlDataService(config.icecat)
        result = await service.download_feature_groups_list_async()
        if result is not None:
            click.echo("Successfully fetched feature groups list")
        else:
            click.echo("Failed to fetch feature groups list", err=True)

    asyncio.run(_fetch())


@cli.command("fetch-features-list")
@click.pass_context
def fetch_features_list(ctx: click.Context) -> None:
    """Fetch the features list from Icecat."""
    config: AppConfig = ctx.obj["config"]

    async def _fetch():
        service = IcecatXmlDataService(config.icecat)
        result = await service.download_features_list_async()
        if result is not None:
            click.echo("Successfully fetched features list")
        else:
            click.echo("Failed to fetch features list", err=True)

    asyncio.run(_fetch())


@cli.command("fetch-categories")
@click.pass_context
def fetch_categories(ctx: click.Context) -> None:
    """Fetch the categories list from Icecat."""
    config: AppConfig = ctx.obj["config"]

    async def _fetch():
        service = IcecatXmlDataService(config.icecat)
        result = await service.download_categories_list_async()
        if result is not None:
            click.echo("Successfully fetched categories list")
        else:
            click.echo("Failed to fetch categories list", err=True)

    asyncio.run(_fetch())


# =============================================================================
# FTP Commands
# =============================================================================


@cli.command("ftp-test")
@click.option("--list", "-l", "list_files", is_flag=True, help="List files on server")
@click.option("--download", "-d", "download_file", help="File to download (e.g., '/Ingram_m/DatasheetSKUGlobal_Coverage_1.zip')")
@click.option("--output", "-o", "output_dir", type=click.Path(), default="data/downloads",
              help="Output directory for downloads (default: data/downloads)")
@click.option("--keep-zip", is_flag=True, help="Keep ZIP file after extraction")
@click.pass_context
def ftp_test(
    ctx: click.Context,
    list_files: bool,
    download_file: str | None,
    output_dir: str,
    keep_zip: bool,
) -> None:
    """Test FTP/SFTP connection and optionally list/download files.

    Examples:
        icecat ftp-test                         # Test connection
        icecat ftp-test --list                  # List files
        icecat ftp-test -d "file.zip" -o ./data # Download file
    """
    from .services.ftp_service import IcecatFTPService

    config: AppConfig = ctx.obj["config"]

    if not config.icecat.ftp_username or not config.icecat.ftp_password:
        raise click.ClickException("FTP/SFTP credentials not configured. Set ICECAT_FTP_USERNAME and ICECAT_FTP_PASSWORD or configure in config.yaml")

    proto = config.icecat.ftp_protocol.upper()
    click.echo(f"Connecting to {proto}: {config.icecat.ftp_host}")
    click.echo(f"Username: {config.icecat.ftp_username[:3]}***")

    ftp = IcecatFTPService(
        host=config.icecat.ftp_host,
        username=config.icecat.ftp_username,
        password=config.icecat.ftp_password,
        timeout=config.icecat.ftp_timeout,
        protocol=config.icecat.ftp_protocol,
        port=config.icecat.ftp_port,
    )

    try:
        with ftp:
            click.secho(f"✓ Connected to {config.icecat.ftp_host} ({proto})", fg="green")
            click.echo(f"Current directory: {ftp.pwd()}")

            if list_files:
                click.echo(f"\nFiles on {proto} server:")
                click.echo("-" * 60)
                for line in ftp.list_files("/"):
                    click.echo(f"  {line}")

            if download_file:
                output_path = Path(output_dir)
                click.echo(f"\nDownloading: {download_file}")
                click.echo(f"Output dir: {output_path.absolute()}")

                if download_file.endswith(".zip"):
                    files = ftp.download_and_extract(download_file, output_path, keep_zip=keep_zip)
                    if files:
                        click.secho(f"✓ Extracted {len(files)} files to {output_path}", fg="green")
                        for f in files[:10]:  # Show first 10 files
                            size_kb = f.stat().st_size / 1024 if f.exists() else 0
                            click.echo(f"  - {f.name} ({size_kb:.1f} KB)")
                        if len(files) > 10:
                            click.echo(f"  ... and {len(files) - 10} more files")
                    else:
                        click.secho("✗ Download or extraction failed", fg="red", err=True)
                else:
                    local_path = output_path / Path(download_file).name
                    if ftp.download_file(download_file, local_path):
                        size_kb = local_path.stat().st_size / 1024
                        click.secho(f"✓ Downloaded to: {local_path} ({size_kb:.1f} KB)", fg="green")
                    else:
                        click.secho("✗ Download failed", fg="red", err=True)

    except Exception as e:
        click.secho(f"✗ {proto} Error: {e}", fg="red", err=True)


@cli.command("ftp-download-assortment")
@click.option("--output", "-o", "output_dir", type=click.Path(), default="data/assortment",
              help="Output directory (default: data/assortment)")
@click.pass_context
def ftp_download_assortment(ctx: click.Context, output_dir: str) -> None:
    """Download the product assortment file from Icecat FTP/SFTP.

    Downloads DatasheetSKUGlobal_Coverage_1.zip from /Ingram_m/ and extracts it.
    """
    from .services.ftp_service import IcecatFTPService

    config: AppConfig = ctx.obj["config"]

    if not config.icecat.ftp_username or not config.icecat.ftp_password:
        raise click.ClickException("FTP/SFTP credentials not configured")

    ftp = IcecatFTPService(
        host=config.icecat.ftp_host,
        username=config.icecat.ftp_username,
        password=config.icecat.ftp_password,
        timeout=config.icecat.ftp_timeout,
        protocol=config.icecat.ftp_protocol,
        port=config.icecat.ftp_port,
    )

    output_path = Path(output_dir)
    assortment_file = "/Ingram_m/DatasheetSKUGlobal_Coverage_1.zip"

    click.echo(f"Downloading assortment file from {config.icecat.ftp_host}...")

    try:
        with ftp:
            files = ftp.download_and_extract(assortment_file, output_path)
            if files:
                click.secho(f"✓ Downloaded and extracted assortment data", fg="green")
                click.echo(f"\nExtracted files:")
                for f in files:
                    size_kb = f.stat().st_size / 1024 if f.exists() else 0
                    click.echo(f"  - {f.name} ({size_kb:.1f} KB)")
            else:
                click.secho("✗ Failed to download assortment file", fg="red", err=True)
    except Exception as e:
        raise click.ClickException(str(e))


@cli.command("ftp-download-suppliers")
@click.option("--output-dir", "-o", type=click.Path(), default="data/refs",
              help="Output directory (default: data/refs)")
@click.pass_context
def ftp_download_suppliers(ctx: click.Context, output_dir: str) -> None:
    """Download supplier reference files from Icecat HTTP.

    Downloads two files using FrontOffice credentials:
    - SuppliersList.xml.gz → decompressed to SuppliersList.xml (~1.2 MB gzipped)
    - supplier_mapping.xml (~295 KB plain XML)

    Run before 'import-suppliers' to ensure fresh data.
    """
    import gzip
    import httpx

    config: AppConfig = ctx.obj["config"]
    username = config.icecat.front_office_username
    password = config.icecat.front_office_password

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    SUPPLIERS_URL = "https://data.icecat.biz/export/freexml/refs/SuppliersList.xml.gz"
    MAPPING_URL = "https://data.icecat.biz/export/freexml.int/INT/supplier_mapping.xml"

    # ── 1. SuppliersList.xml.gz ──
    gz_path = out / "SuppliersList.xml.gz"
    xml_path = out / "SuppliersList.xml"
    click.echo(f"[1/2] Downloading {SUPPLIERS_URL} ...")

    with httpx.stream(
        "GET", SUPPLIERS_URL,
        auth=(username, password),
        timeout=httpx.Timeout(120.0, connect=30.0),
        follow_redirects=True,
    ) as resp:
        resp.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=256 * 1024):
                f.write(chunk)

    # Decompress .gz → .xml
    with gzip.open(gz_path, "rb") as gz_in, open(xml_path, "wb") as xml_out:
        while True:
            block = gz_in.read(1024 * 1024)
            if not block:
                break
            xml_out.write(block)

    gz_path.unlink()  # Remove .gz after decompression
    size_kb = xml_path.stat().st_size / 1024
    click.secho(f"  ✓ {xml_path} ({size_kb:.0f} KB)", fg="green")

    # ── 2. supplier_mapping.xml ──
    mapping_path = out / "supplier_mapping.xml"
    click.echo(f"[2/2] Downloading {MAPPING_URL} ...")

    with httpx.stream(
        "GET", MAPPING_URL,
        auth=(username, password),
        timeout=httpx.Timeout(120.0, connect=30.0),
        follow_redirects=True,
    ) as resp:
        resp.raise_for_status()
        with open(mapping_path, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=256 * 1024):
                f.write(chunk)

    size_kb = mapping_path.stat().st_size / 1024
    click.secho(f"  ✓ {mapping_path} ({size_kb:.0f} KB)", fg="green")

    click.secho("\nSupplier files downloaded successfully.", fg="green")


# =============================================================================
# FrontOffice JSON API Commands
# =============================================================================


@cli.command("fetch-product")
@click.option("--ean", help="Fetch product by EAN/UPC")
@click.option("--product-code", help="Fetch product by product code (requires --brand)")
@click.option("--brand", help="Brand name (required with --product-code)")
@click.option("--icecat-id", type=int, help="Fetch product by Icecat ID")
@click.option("--language", "-l", default="EN", help="Language code (default: EN)")
@click.pass_context
def fetch_product(
    ctx: click.Context,
    ean: str | None,
    product_code: str | None,
    brand: str | None,
    icecat_id: int | None,
    language: str,
) -> None:
    """Fetch complete product data from Icecat FrontOffice Live API."""
    config: AppConfig = ctx.obj["config"]

    if not any([ean, product_code, icecat_id]):
        raise click.UsageError("Specify one of: --ean, --product-code, or --icecat-id")

    if product_code and not brand:
        raise click.UsageError("--brand is required when using --product-code")

    async def _fetch():
        service = IcecatJsonDataFetchService(config.icecat)

        if ean:
            result = await service.fetch_product_data_by_ean_async(ean, language)
            identifier = f"EAN {ean}"
        elif product_code and brand:
            result = await service.fetch_product_data_by_product_code_async(
                product_code, brand, language
            )
            identifier = f"{brand} / {product_code}"
        elif icecat_id:
            result = await service.fetch_product_data_by_icecat_id_async(icecat_id, language)
            identifier = f"Icecat ID {icecat_id}"
        else:
            return

        if result.success:
            click.echo(f"Successfully fetched product data for {identifier}")
            click.echo(result.logs)

            # Show summary from parsed response
            if result.product_response and result.product_response.data:
                data = result.product_response.data
                if data.general_info:
                    gi = data.general_info
                    click.echo(f"\nProduct: {gi.title or 'N/A'}")
                    click.echo(f"Icecat ID: {gi.icecat_id}")
                    click.echo(f"Brand: {gi.brand_info.brand_name if gi.brand_info else gi.brand}")
                    click.echo(f"Category: {gi.category_name or 'N/A'}")

                click.echo(f"\nGallery: {len(data.gallery)} images")
                click.echo(f"Multimedia: {len(data.multimedia)} items")
                click.echo(f"Feature Groups: {len(data.features_groups)}")

                # Show bullet points
                if data.general_info and data.general_info.bullet_points:
                    bp = data.general_info.bullet_points
                    if bp.values:
                        click.echo(f"\nBullet Points ({len(bp.values)}):")
                        for i, point in enumerate(bp.values[:5], 1):
                            click.echo(f"  {i}. {point}")
                        if len(bp.values) > 5:
                            click.echo(f"  ... and {len(bp.values) - 5} more")

            if ctx.obj["verbose"] and result.data:
                import json
                click.echo("\n--- Full JSON Response ---")
                click.echo(json.dumps(result.data, indent=2, ensure_ascii=False))
        else:
            click.echo(f"Failed to fetch product: {result.error_message}", err=True)

    asyncio.run(_fetch())


@cli.command("test-api")
@click.option("--ean", default="8806095582443", help="EAN to test with (default: Samsung product)")
@click.option("--language", "-l", default="EN", help="Language code (default: EN)")
@click.pass_context
def test_api(ctx: click.Context, ean: str, language: str) -> None:
    """Test FrontOffice API connection with a sample EAN."""
    config: AppConfig = ctx.obj["config"]

    click.echo("Testing Icecat FrontOffice API connection...")
    click.echo(f"Username: {config.icecat.front_office_username[:3]}***" if config.icecat.front_office_username else "Username: NOT SET")
    click.echo(f"API Key: ***CONFIGURED***" if config.icecat.front_office_api_key else "API Key: NOT SET")
    click.echo(f"Test EAN: {ean}")
    click.echo(f"Language: {language}")
    click.echo()

    async def _test():
        service = IcecatJsonDataFetchService(config.icecat)
        result = await service.fetch_product_data_by_ean_async(ean, language)

        if result.success:
            click.echo("API connection successful!")
            if result.product_response and result.product_response.data:
                data = result.product_response.data
                if data.general_info:
                    click.echo(f"Product found: {data.general_info.title}")
                    click.echo(f"Icecat ID: {data.general_info.icecat_id}")
        else:
            click.echo(f"API connection failed: {result.error_message}", err=True)

    asyncio.run(_test())


# =============================================================================
# Sync Commands
# =============================================================================


@cli.command("prepare-sync")
@click.option("--file", "-f", "assortment_file", required=True, type=click.Path(exists=True),
              help="Path to assortment file (Brand + MPN)")
@click.option("--mode", "-m", type=click.Choice(["full", "delta"]), default="full",
              help="Sync mode used to load assortment: 'full' (default) or 'delta'")
@click.option("--delimiter", default=None, help="File delimiter (default: auto-detect)")
@click.option("--brand-column", default=None, help="Override brand column name")
@click.option("--mpn-column", default=None, help="Override MPN column name")
@click.pass_context
def prepare_sync(
    ctx: click.Context,
    assortment_file: str,
    mode: str,
    delimiter: str | None,
    brand_column: str | None,
    mpn_column: str | None,
) -> None:
    """Download assortment and load into sync_product table (Phases 1-3 only).

    Run this ONCE before starting parallel sync jobs with --skip-assortment.
    This avoids each sync job spending ~5 min loading the assortment independently.
    """
    from .services import SyncOrchestrator
    from .database.connection import init_db

    config: AppConfig = ctx.obj["config"]

    click.echo()
    click.secho("=" * 60, fg="cyan")
    click.secho("  PREPARE SYNC (assortment load only)", fg="cyan", bold=True)
    click.secho("=" * 60, fg="cyan")
    click.echo(f"  File: {assortment_file}")
    click.echo(f"  Mode: {mode.upper()}")
    click.secho("-" * 60, fg="cyan")
    click.echo()

    async def _prepare():
        db_manager = init_db(config.database)
        orchestrator = SyncOrchestrator(
            config,
            db_manager,
            delimiter=delimiter,
            brand_column=brand_column,
            mpn_column=mpn_column,
        )
        await orchestrator.prepare_assortment(
            assortment_file=assortment_file,
            mode=mode,
        )

    asyncio.run(_prepare())


@cli.command("sync")
@click.option("--file", "-f", "assortment_file", required=False, default=None, type=click.Path(),
              help="Path to assortment file (Brand + MPN). Not required when --skip-assortment is set.")
@click.option("--mode", "-m", type=click.Choice(["full", "delta"]), default="delta",
              help="Sync mode: 'full' (weekend full run) or 'delta' (daily delta, default)")
@click.option("--batch-size", "-b", default=100, type=int, help="Products per batch (default: 100)")
@click.option("--concurrency", "-c", default=10, type=int, help="Max concurrent API calls (default: 10)")
@click.option("--resume", "resume_run_id", help="Resume an interrupted sync run by ID")
@click.option("--language", "-l", default="EN", help="Language code (default: EN)")
@click.option("--all-languages", is_flag=True,
              help="Fetch all 10 supported languages per product (EN,NL,FR,DE,IT,ES,PT,ZH,HU,TH)")
@click.option("--delimiter", default=None,
              help="File delimiter (default: auto-detect). Use '~~' for Ingram Micro format")
@click.option("--brand-column", default=None, help="Override brand column name")
@click.option("--mpn-column", default=None, help="Override MPN column name")
@click.option("--max-products", default=None, type=int,
              help="Max products to process (from start-index). Omit to process all remaining.")
@click.option("--start-index", default=0, type=int,
              help="Skip first N products in the queue (default: 0). Use with --max-products to split work across parallel jobs.")
@click.option("--source", "-s", type=click.Choice(["json", "xml"]), default="json",
              help="Data source: 'json' (9 API calls/product) or 'xml' (1 call with lang=INT, all locales)")
@click.option("--skip-assortment", is_flag=True, default=False,
              help="Skip FTP download and assortment loading (Phases 1-3). Use when a prepare-sync job already loaded the data.")
@click.pass_context
def sync_command(
    ctx: click.Context,
    assortment_file: str,
    mode: str,
    batch_size: int,
    concurrency: int,
    resume_run_id: str | None,
    language: str,
    all_languages: bool,
    delimiter: str | None,
    brand_column: str | None,
    mpn_column: str | None,
    max_products: int | None,
    start_index: int,
    source: str,
    skip_assortment: bool,
) -> None:
    """Run full product sync from assortment file.

    Sync Modes:
      - delta (default): Process only changed/new products from the assortment file.
                         Used for daily delta runs.
      - full: Compare entire assortment file against database, process all products.
              Used for weekend full runs to ensure consistency.

    Data Source:
      - json (default): Fetch via JSON Live API (10 calls per product for all languages).
      - xml: Fetch via XML xml_server3.cgi with lang=INT (1 call per product, all locales).

    The file delimiter is auto-detected (supports ~~, tab, comma, etc.).
    """
    from .services import SyncOrchestrator
    from .database.connection import init_db
    from .mappers.icecat_language_mapper import IcecatLanguageMapper

    config: AppConfig = ctx.obj["config"]
    config.icecat.validate_api_credentials()

    if not skip_assortment and not assortment_file:
        raise click.UsageError("--file is required unless --skip-assortment is set.")
    # Use a placeholder path when skipping assortment (file not read)
    if skip_assortment and not assortment_file:
        assortment_file = "skipped"

    # XML source implies all languages (lang=INT returns all locales)
    if source == "xml":
        all_languages = True

    # Build language list
    if all_languages:
        sync_languages = [m.short_code for m in IcecatLanguageMapper.get_supported_languages()]
    else:
        sync_languages = [language]

    source_label = "XML (lang=INT)" if source == "xml" else f"JSON ({len(sync_languages)} languages)"

    click.echo()
    click.secho("=" * 60, fg="cyan")
    click.secho("  ICECAT PRODUCT SYNC", fg="cyan", bold=True)
    click.secho("=" * 60, fg="cyan")
    click.echo(f"  File:        {assortment_file}")
    click.echo(f"  Mode:        {mode.upper()}")
    click.echo(f"  Source:      {source_label}")
    click.echo(f"  Batch size:  {batch_size}")
    click.echo(f"  Concurrency: {concurrency}")
    if source != "xml":
        click.echo(f"  Languages:   {','.join(sync_languages)} ({len(sync_languages)} total)")
    if start_index > 0:
        click.echo(f"  Start index: {start_index:,}")
    if max_products:
        click.echo(f"  Max products:{max_products:,}")
    if skip_assortment:
        click.echo(f"  Assortment:  SKIPPED (using existing sync_product data)")
    if delimiter:
        click.echo(f"  Delimiter:   {repr(delimiter)}")
    if resume_run_id:
        click.echo(f"  Resuming:    {resume_run_id}")
    click.secho("-" * 60, fg="cyan")
    click.echo()

    async def _sync():
        db_manager = init_db(config.database)
        orchestrator = SyncOrchestrator(
            config,
            db_manager,
            delimiter=delimiter,
            brand_column=brand_column,
            mpn_column=mpn_column,
        )
        orchestrator.batch_size = batch_size
        orchestrator.max_concurrent = concurrency

        result = await orchestrator.run_sync(
            assortment_file=assortment_file,
            languages=sync_languages,
            resume_run_id=resume_run_id,
            mode=mode,
            max_products=max_products,
            start_index=start_index,
            source=source,
            skip_assortment=skip_assortment,
        )

        click.echo()
        click.secho("=" * 60, fg="green" if result.status == "completed" else "red")
        click.secho("  SYNC COMPLETED", fg="green" if result.status == "completed" else "red", bold=True)
        click.secho("=" * 60, fg="green" if result.status == "completed" else "red")
        click.echo(f"  Run ID:      {result.run_id}")
        click.echo(f"  Status:      {result.status}")
        click.echo(f"  Duration:    {result.duration_seconds:.1f}s")
        click.echo(f"  Total:       {result.total_products:,}")
        click.echo(f"  Matched:     {result.products_matched:,}")
        click.echo(f"  Not found:   {result.products_not_found:,}")
        click.echo(f"  Created:     {result.products_created:,}")
        click.echo(f"  Updated:     {result.products_updated:,}")
        click.echo(f"  Deleted:     {result.products_deleted:,}")
        click.echo(f"  Errors:      {result.products_errored:,}")
        click.echo(f"  Success:     {result.success_rate:.1f}%")

    asyncio.run(_sync())


@cli.command("sync-product")
@click.option("--brand", "-b", required=True, help="Brand name")
@click.option("--mpn", "-m", required=True, help="Manufacturer part number (MPN)")
@click.option("--language", "-l", default="EN", help="Language code (default: EN)")
@click.option("--all-languages", is_flag=True,
              help="Fetch all 10 supported languages")
@click.option("--source", "-s", type=click.Choice(["json", "xml"]), default="json",
              help="Data source: 'json' (default) or 'xml' (1 call with lang=INT)")
@click.pass_context
def sync_single_product(
    ctx: click.Context,
    brand: str,
    mpn: str,
    language: str,
    all_languages: bool,
    source: str,
) -> None:
    """Sync a single product by Brand + MPN."""
    from .services import SyncOrchestrator
    from .database.connection import init_db
    from .mappers.icecat_language_mapper import IcecatLanguageMapper

    config: AppConfig = ctx.obj["config"]
    config.icecat.validate_api_credentials()

    # XML source implies all languages
    if source == "xml":
        all_languages = True

    if all_languages:
        sync_languages = [m.short_code for m in IcecatLanguageMapper.get_supported_languages()]
        source_label = "XML (lang=INT)" if source == "xml" else f"all {len(sync_languages)} languages"
        click.echo(f"Syncing product: {brand} / {mpn} ({source_label})")
    else:
        sync_languages = [language]
        click.echo(f"Syncing product: {brand} / {mpn} (language: {language})")

    async def _sync():
        db_manager = init_db(config.database)
        orchestrator = SyncOrchestrator(config, db_manager)

        result = await orchestrator.sync_single_product(
            brand, mpn, languages=sync_languages, source=source,
        )

        if result.status == "completed":
            click.echo(f"Successfully synced product!")
            click.echo(f"  Run ID: {result.run_id}")
            if result.products_created:
                click.echo(f"  Action: Created")
            elif result.products_updated:
                click.echo(f"  Action: Updated")
        else:
            click.echo(f"Sync failed: {result.status}", err=True)
            if result.products_not_found:
                click.echo("  Product not found in Icecat", err=True)

    asyncio.run(_sync())


@cli.command("sync-status")
@click.option("--run-id", help="Specific run ID to check (default: latest)")
@click.pass_context
def sync_status(ctx: click.Context, run_id: str | None) -> None:
    """Check sync status and statistics."""
    from .database.connection import init_db
    from .repositories.sync_repository import SyncRepository, SyncRunRepository

    config: AppConfig = ctx.obj["config"]
    db_manager = init_db(config.database)

    with db_manager.session() as session:
        run_repo = SyncRunRepository(session)
        sync_repo = SyncRepository(session)

        if run_id:
            sync_run = run_repo.get_by_id(run_id)
        else:
            sync_run = run_repo.get_latest_run()

        if sync_run:
            click.echo("\nSync Run Status")
            click.echo("=" * 40)
            click.echo(f"Run ID: {sync_run.id}")
            click.echo(f"Status: {sync_run.status.value}")
            click.echo(f"Started: {sync_run.started_at}")
            click.echo(f"Ended: {sync_run.ended_at or 'In progress'}")
            click.echo(f"Total: {sync_run.total_products}")
            click.echo(f"Matched: {sync_run.products_matched}")
            click.echo(f"Not found: {sync_run.products_not_found}")
            click.echo(f"Created: {sync_run.products_created}")
            click.echo(f"Updated: {sync_run.products_updated}")
            click.echo(f"Deleted: {sync_run.products_deleted}")
            click.echo(f"Errors: {sync_run.products_errored}")
            click.echo(f"Progress: {sync_run.progress_percentage:.1f}%")
            click.echo(f"Success rate: {sync_run.success_rate:.1f}%")
        else:
            click.echo("No sync runs found")

        # Show overall sync product stats
        click.echo("\nSync Product Status Summary")
        click.echo("=" * 40)
        status_counts = sync_repo.get_status_counts()
        for status, count in status_counts.items():
            click.echo(f"  {status}: {count}")


@cli.command("sync-logs")
@click.option("--run-id", required=True, help="Sync run ID")
@click.option("--level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
              help="Filter by log level")
@click.option("--limit", "-n", default=50, type=int, help="Number of logs to show (default: 50)")
@click.option("--errors-only", is_flag=True, help="Show only error logs")
@click.pass_context
def sync_logs(
    ctx: click.Context,
    run_id: str,
    level: str | None,
    limit: int,
    errors_only: bool,
) -> None:
    """View sync logs for a specific run."""
    from .database.connection import init_db
    from .repositories.log_repository import LogRepository
    from .models.db.sync_log import LogLevel

    config: AppConfig = ctx.obj["config"]
    db_manager = init_db(config.database)

    with db_manager.session() as session:
        log_repo = LogRepository(session)

        log_level = LogLevel[level] if level else None

        if errors_only:
            logs = log_repo.get_error_logs_by_run(run_id, limit)
        else:
            logs = log_repo.get_logs_by_run(run_id, level=log_level, limit=limit)

        click.echo(f"\nLogs for run {run_id[:8]}...")
        click.echo("=" * 60)

        for log in logs:
            timestamp = log.created_at.strftime("%H:%M:%S") if log.created_at else ""
            level_str = log.log_level.value if log.log_level else "INFO"
            type_str = log.log_type.value if log.log_type else ""

            # Color code by level
            if level_str == "ERROR" or level_str == "CRITICAL":
                msg_color = "red"
            elif level_str == "WARNING":
                msg_color = "yellow"
            else:
                msg_color = None

            click.secho(
                f"[{timestamp}] [{level_str}] [{type_str}] {log.message}",
                fg=msg_color,
            )

            # Show product context if available
            if log.brand or log.mpn:
                click.echo(f"    Product: {log.brand}/{log.mpn}")

            # Show duration if available
            if log.duration_ms:
                click.echo(f"    Duration: {log.duration_ms}ms")

        click.echo(f"\nShowing {len(logs)} logs (limit: {limit})")

        # Show summary counts
        counts = log_repo.get_log_counts_by_level(run_id)
        click.echo("\nLog counts by level:")
        for lvl, count in counts.items():
            click.echo(f"  {lvl}: {count}")


@cli.command("sync-cleanup")
@click.option("--older-than", default=30, type=int, help="Delete logs older than N days (default: 30)")
@click.option("--yes", is_flag=True, help="Confirm deletion without prompt")
@click.pass_context
def sync_cleanup(ctx: click.Context, older_than: int, yes: bool) -> None:
    """Cleanup old sync logs to free disk space."""
    from .database.connection import init_db
    from .repositories.log_repository import LogRepository

    config: AppConfig = ctx.obj["config"]

    if not yes:
        click.confirm(f"Delete sync logs older than {older_than} days?", abort=True)

    db_manager = init_db(config.database)

    with db_manager.session() as session:
        log_repo = LogRepository(session)
        deleted = log_repo.cleanup_old_logs(older_than)
        session.commit()
        click.echo(f"Deleted {deleted} log entries older than {older_than} days")


@cli.command("seed-locales")
@click.pass_context
def seed_locales(ctx: click.Context) -> None:
    """Seed the locales table with supported languages.

    Populates 10 supported languages: EN, NL, FR, DE, IT, ES, PT, ZH, HU, TH.
    Safe to run multiple times (uses INSERT IGNORE).
    """
    from .database.connection import init_db
    from .mappers.icecat_language_mapper import IcecatLanguageMapper
    from .models.db.locales import Locales

    config: AppConfig = ctx.obj["config"]
    db_manager = init_db(config.database)

    languages = IcecatLanguageMapper.get_supported_languages()

    with db_manager.session() as session:
        inserted = 0
        for lang in languages:
            existing = session.get(Locales, lang.lang_id)
            if existing:
                click.echo(f"  Already exists: {lang.short_code} (id={lang.lang_id})")
                continue

            locale = Locales(
                localeid=lang.lang_id,
                isactive=True,
                languagecode=lang.short_code,
                name=lang.code.title(),
            )
            session.add(locale)
            inserted += 1
            click.echo(f"  Inserted: {lang.short_code} (id={lang.lang_id}, name={lang.code.title()})")

        session.commit()

    click.echo(f"\nLocales seeded: {inserted} new, {len(languages) - inserted} already existed.")


@cli.command("import-suppliers")
@click.option(
    "--suppliers-xml",
    default="data/refs/SuppliersList.xml",
    type=click.Path(),
    help="Path to SuppliersList.xml (supplier data + logos)",
)
@click.option(
    "--mapping-xml",
    default="data/refs/supplier_mapping.xml",
    type=click.Path(),
    help="Path to supplier_mapping.xml (brand alias mapping)",
)
@click.pass_context
def import_suppliers(ctx: click.Context, suppliers_xml: str, mapping_xml: str) -> None:
    """Import Icecat supplier data and brand mapping into the database.

    Imports two XML files:
    - SuppliersList.xml → vendor table (supplier IDs, names, logo URLs)
    - supplier_mapping.xml → supplier_mapping table (34K+ brand aliases)

    After import, the sync pipeline uses the mapping table to resolve
    distributor brand names to Icecat canonical names before API calls.
    """
    import time
    from .database.connection import init_db
    from .mappers.icecat_supplier_mapper import IcecatSupplierMapper
    from .repositories.product_repository import VendorRepository
    from .repositories.supplier_mapping_repository import SupplierMappingRepository

    config: AppConfig = ctx.obj["config"]
    db_manager = init_db(config.database)

    # Ensure tables exist
    db_manager.create_tables()

    mapper = IcecatSupplierMapper()

    with db_manager.session() as session:
        vendor_repo = VendorRepository(session)
        mapping_repo = SupplierMappingRepository(session)

        # ── Step 1: Import SuppliersList.xml → vendor table ──
        suppliers_path = Path(suppliers_xml)
        if suppliers_path.exists():
            click.echo(f"\n[1/2] Importing suppliers from {suppliers_path}...")
            start = time.perf_counter()

            count = mapper.load_from_xml(suppliers_path)

            # Bulk upsert into vendor table
            vendor_records = list(mapper.iter_suppliers_for_vendor_table())
            BATCH = 5000
            vendor_count = 0
            for i in range(0, len(vendor_records), BATCH):
                batch = vendor_records[i:i + BATCH]
                vendor_repo.bulk_upsert(batch, update_columns=["name", "logourl"])
                vendor_count += len(batch)
            session.commit()

            dur = time.perf_counter() - start
            click.echo(
                f"  Loaded {count:,} suppliers from XML, "
                f"upserted {vendor_count:,} vendors into DB ({dur:.1f}s)"
            )
        else:
            click.echo(f"\n[1/2] SKIPPED: {suppliers_path} not found")

        # ── Step 2: Import supplier_mapping.xml → supplier_mapping table ──
        mapping_path = Path(mapping_xml)
        if mapping_path.exists():
            click.echo(f"\n[2/2] Importing brand mapping from {mapping_path}...")
            start = time.perf_counter()

            # Parse all mapping records
            records = list(IcecatSupplierMapper.parse_supplier_mapping_xml(mapping_path))
            click.echo(f"  Parsed {len(records):,} mapping symbols from XML")

            # Deduplicate: keep first occurrence per (symbol_lower, distributor_id)
            seen = set()
            unique_records = []
            for r in records:
                key = (r["symbol_lower"], r["distributor_id"])
                if key not in seen:
                    seen.add(key)
                    unique_records.append(r)

            click.echo(f"  Unique mappings: {len(unique_records):,} (deduped from {len(records):,})")

            # Truncate + bulk insert
            total = mapping_repo.bulk_import(unique_records)
            session.commit()

            dur = time.perf_counter() - start
            click.echo(f"  Imported {total:,} mapping records into DB ({dur:.1f}s)")
        else:
            click.echo(f"\n[2/2] SKIPPED: {mapping_path} not found")

        # ── Summary ──
        vendor_total = vendor_repo.count()
        mapping_total = mapping_repo.get_mapping_count()
        click.echo(f"\nDone! vendor table: {vendor_total:,} rows, supplier_mapping table: {mapping_total:,} rows")


@cli.command("assortment-stats")
@click.option("--file", "-f", "assortment_file", required=True, type=click.Path(exists=True),
              help="Path to assortment file")
@click.option("--delimiter", default=None,
              help="File delimiter (default: auto-detect)")
@click.option("--brand-column", default=None, help="Override brand column name")
@click.option("--mpn-column", default=None, help="Override MPN column name")
@click.pass_context
def assortment_stats(
    ctx: click.Context,
    assortment_file: str,
    delimiter: str | None,
    brand_column: str | None,
    mpn_column: str | None,
) -> None:
    """Show statistics about an assortment file."""
    from .services import AssortmentReader

    reader = AssortmentReader(
        delimiter=delimiter,
        brand_column=brand_column,
        mpn_column=mpn_column,
    )
    stats = reader.get_stats(assortment_file)

    click.echo(f"\nAssortment File Statistics: {assortment_file}")
    click.echo("=" * 50)
    click.echo(f"Total rows:     {stats.total_rows:,}")
    click.echo(f"Valid rows:     {stats.valid_rows:,}")
    click.echo(f"Invalid rows:   {stats.invalid_rows:,}")
    click.echo(f"Duplicate rows: {stats.duplicate_rows:,}")
    click.echo(f"Unique items:   {stats.unique_items:,}")


# =============================================================================
# EAN Batch Sync Commands
# =============================================================================


@cli.command("sync-eans")
@click.option("--file", "-f", "ean_file", required=True, type=click.Path(exists=True),
              help="Path to EAN list file (one EAN per line)")
@click.option("--language", "-l", default="EN", help="Language code (default: EN)")
@click.option("--phase", type=click.Choice(["create", "update", "both"]), default="both",
              help="Sync phase: create (new only), update (all), or both (default: both)")
@click.option("--concurrency", "-c", default=10, type=int, help="Max concurrent API calls (default: 10)")
@click.option("--report", "-r", type=click.Path(), help="Save summary report to file")
@click.pass_context
def sync_eans(
    ctx: click.Context,
    ean_file: str,
    language: str,
    phase: str,
    concurrency: int,
    report: str | None,
) -> None:
    """Sync products from an EAN list file.

    Two-phase workflow:
    - CREATE phase: Insert new products only (skip existing)
    - UPDATE phase: Update all products with fresh data

    Example:
        icecat sync-eans -f eans.txt -l EN --phase both
    """
    from .scripts.batch_sync_eans import EANBatchSyncer, generate_summary_report
    from .database.connection import init_db

    config: AppConfig = ctx.obj["config"]
    config.icecat.validate_api_credentials()

    click.echo(f"Reading EANs from: {ean_file}")

    async def _sync():
        db_manager = init_db(config.database)
        syncer = EANBatchSyncer(config, db_manager, concurrency=concurrency)

        # Read EANs
        eans = syncer.read_eans(ean_file)
        click.echo(f"Found {len(eans)} EANs to process")
        click.echo(f"Language: {language}, Concurrency: {concurrency}")
        click.echo()

        if phase == "create":
            click.echo("Running CREATE phase only...")
            create_stats = await syncer.run_create_phase(eans, language)
            syncer._log_phase_summary("CREATE", create_stats)

            if report:
                from .scripts.batch_sync_eans import SyncStats
                empty_stats = SyncStats()
                generate_summary_report(create_stats, empty_stats, report)

        elif phase == "update":
            click.echo("Running UPDATE phase only...")
            update_stats = await syncer.run_update_phase(eans, language)
            syncer._log_phase_summary("UPDATE", update_stats)

            if report:
                from .scripts.batch_sync_eans import SyncStats
                empty_stats = SyncStats()
                generate_summary_report(empty_stats, update_stats, report)

        else:  # both
            click.echo("Running full sync (CREATE then UPDATE)...")
            create_stats, update_stats = await syncer.run_full_sync(eans, language)

            if report:
                generate_summary_report(create_stats, update_stats, report)

        click.echo("\nSync completed!")

    asyncio.run(_sync())


# =============================================================================
# XML vs JSON Comparison Commands
# =============================================================================


@cli.command("compare-xml-json")
@click.option("--brand", "-b", default=None, help="Single product brand")
@click.option("--mpn", "-m", default=None, help="Single product MPN")
@click.option("--file", "-f", "assortment_file", default=None, type=click.Path(exists=True),
              help="Assortment file (Brand + MPN)")
@click.option("--count", "-n", default=10, type=int,
              help="Number of products to compare from file (default: 10)")
@click.option("--delimiter", default=None, help="File delimiter (default: auto-detect)")
@click.option("--brand-column", default=None, help="Override brand column name")
@click.option("--mpn-column", default=None, help="Override MPN column name")
@click.pass_context
def compare_xml_json(
    ctx: click.Context,
    brand: str | None,
    mpn: str | None,
    assortment_file: str | None,
    count: int,
    delimiter: str | None,
    brand_column: str | None,
    mpn_column: str | None,
) -> None:
    """Compare XML (lang=INT) vs JSON (10 language calls) output for data parity.

    Fetches the same product(s) via both the JSON Live API (10 calls per product)
    and the XML xml_server3.cgi endpoint (1 call with lang=INT), then compares
    the merged output field by field.

    Examples:
        icecat compare-xml-json -b Lenovo -m 30JQ009XUK
        icecat compare-xml-json -f assortment.txt -n 10
    """
    from .api import IcecatXmlProductFetchService
    from .parsers import XmlProductParser
    from .services import ComparisonService, ComparisonResult
    from .services.product_matcher import ProductMatcher
    from .mappers.product_mapper import ProductMapper, MultiLanguageProductMapper
    from .mappers.icecat_language_mapper import IcecatLanguageMapper
    from .database.connection import init_db
    from .repositories.supplier_mapping_repository import SupplierMappingRepository

    config: AppConfig = ctx.obj["config"]
    config.icecat.validate_api_credentials()

    if not brand and not assortment_file:
        click.secho("Provide --brand/--mpn or --file", fg="red", err=True)
        raise SystemExit(1)

    # Build product list
    products: list[tuple[str, str]] = []
    if brand and mpn:
        products.append((brand, mpn))
    elif assortment_file:
        from .services import AssortmentReader
        reader = AssortmentReader(
            delimiter=delimiter,
            brand_column=brand_column,
            mpn_column=mpn_column,
        )
        items = reader.read_csv_to_list(assortment_file)
        products = [(item.brand, item.mpn) for item in items[:count]]

    click.echo()
    click.secho("=" * 60, fg="cyan")
    click.secho("  XML vs JSON COMPARISON", fg="cyan", bold=True)
    click.secho("=" * 60, fg="cyan")
    click.echo(f"  Products: {len(products)}")
    click.secho("-" * 60, fg="cyan")
    click.echo()

    supported_langs = IcecatLanguageMapper.get_supported_languages()
    lang_pairs = [(m.short_code, m.lang_id) for m in supported_langs]

    async def _compare():
        # Load brand mapping
        db_manager = init_db(config.database)
        brand_map: dict[str, str] = {}
        with db_manager.session() as session:
            mapping_repo = SupplierMappingRepository(session)
            brand_map = mapping_repo.load_all_mappings() or {}

        if brand_map:
            click.echo(f"  Brand mapping: {len(brand_map):,} aliases loaded")

        matcher = ProductMatcher(config.icecat)
        xml_fetch = IcecatXmlProductFetchService(config.icecat)
        xml_parser = XmlProductParser()
        comparator = ComparisonService()

        results: list[ComparisonResult] = []

        for idx, (prod_brand, prod_mpn) in enumerate(products, 1):
            mapped_brand = brand_map.get(prod_brand.lower(), prod_brand)
            click.echo(f"\n[{idx}/{len(products)}] {mapped_brand} / {prod_mpn}")

            result = ComparisonResult(brand=mapped_brand, mpn=prod_mpn)

            # ── JSON path: 10 language calls → MultiLanguageProductMapper ──
            click.echo("  JSON: fetching 10 languages...", nl=False)
            ml_mapper = MultiLanguageProductMapper()
            json_ok = False

            for short_code, lang_id in lang_pairs:
                try:
                    match = await matcher.match_product(mapped_brand, prod_mpn, short_code)
                    if match.found and match.icecat_data:
                        ml_mapper.add_language_response(match.icecat_data, lang_id)
                except Exception as e:
                    pass  # Some languages may fail for certain products

            json_merged = ml_mapper.get_merged_data()
            if json_merged:
                json_ok = True
                click.echo(f" OK ({len(ml_mapper._descriptions)} descs)")
            else:
                result.json_ok = False
                result.json_error = "No data from JSON API"
                click.secho(" FAILED", fg="red")

            # ── XML path: 1 call with lang=INT → XmlProductParser ──
            click.echo("  XML:  fetching lang=INT...", nl=False)
            xml_merged = None

            try:
                xml_result = await xml_fetch.fetch_product_xml(mapped_brand, prod_mpn)
                if xml_result.success and xml_result.xml_root is not None:
                    xml_merged = xml_parser.parse(xml_result.xml_root)
                    if xml_merged:
                        click.echo(f" OK ({len(xml_merged.get('descriptions', []))} descs)")
                    else:
                        result.xml_ok = False
                        result.xml_error = "Parser returned None"
                        click.secho(" PARSE FAILED", fg="red")
                else:
                    result.xml_ok = False
                    result.xml_error = xml_result.error_message
                    click.secho(f" FAILED: {xml_result.error_message}", fg="red")
            except Exception as e:
                result.xml_ok = False
                result.xml_error = str(e)
                click.secho(f" ERROR: {e}", fg="red")

            # ── Compare ──
            if json_ok and xml_merged:
                diffs = comparator.compare(json_merged, xml_merged)
                result.differences = diffs

                if diffs:
                    click.secho(f"  DIFFS: {len(diffs)} differences found", fg="yellow")
                    for diff in diffs[:20]:
                        click.echo(f"    {diff}")
                    if len(diffs) > 20:
                        click.echo(f"    ... and {len(diffs) - 20} more")
                else:
                    click.secho("  MATCH: identical output", fg="green")
            elif not json_ok:
                click.secho("  SKIP: JSON path failed", fg="yellow")
            elif not xml_merged:
                click.secho("  SKIP: XML path failed", fg="yellow")

            results.append(result)

        # ── Summary ──
        click.echo()
        click.secho("=" * 60, fg="cyan")
        click.secho("  COMPARISON SUMMARY", fg="cyan", bold=True)
        click.secho("=" * 60, fg="cyan")

        total = len(results)
        matches = sum(1 for r in results if r.match)
        json_failures = sum(1 for r in results if not r.json_ok)
        xml_failures = sum(1 for r in results if not r.xml_ok)
        with_diffs = sum(1 for r in results if r.json_ok and r.xml_ok and r.diff_count > 0)

        click.echo(f"  Total products:  {total}")
        click.secho(f"  Exact matches:   {matches}", fg="green" if matches == total else None)
        if with_diffs:
            click.secho(f"  With diffs:      {with_diffs}", fg="yellow")
        if json_failures:
            click.secho(f"  JSON failures:   {json_failures}", fg="red")
        if xml_failures:
            click.secho(f"  XML failures:    {xml_failures}", fg="red")

        # Per-section diff summary
        if with_diffs:
            section_counts: dict[str, int] = {}
            for r in results:
                for d in r.differences:
                    section_counts[d.section] = section_counts.get(d.section, 0) + 1

            click.echo("\n  Differences by section:")
            for section, cnt in sorted(section_counts.items(), key=lambda x: -x[1]):
                click.echo(f"    {section}: {cnt}")

        click.echo()

    asyncio.run(_compare())


# =============================================================================
# Taxonomy Commands
# =============================================================================


@cli.command("update-taxonomy")
@click.option(
    "--skip-download",
    is_flag=True,
    help="Skip download, use existing file in data/downloads/",
)
@click.option(
    "--file",
    "-f",
    "file_path",
    type=click.Path(exists=True),
    help="Path to existing CategoryFeaturesList.xml.gz file",
)
@click.option(
    "--batch-size",
    "-b",
    default=5000,
    type=int,
    help="Batch size for bulk inserts (default: 5000)",
)
@click.option(
    "--download-dir",
    type=click.Path(),
    default="data/downloads",
    help="Directory for downloaded files (default: data/downloads)",
)
@click.pass_context
def update_taxonomy(
    ctx: click.Context,
    skip_download: bool,
    file_path: str | None,
    batch_size: int,
    download_dir: str,
) -> None:
    """Update taxonomy from Icecat CategoryFeaturesList.xml.

    Downloads CategoryFeaturesList.xml.gz (1.5 GB) from Icecat,
    stream-parses the XML, and populates: category, categoryheader,
    categorydisplayattributes, and attributenames tables.

    Uses UPSERT strategy: existing rows are updated, new rows are inserted.
    Tables are never truncated. Safe to run daily/weekly.

    Examples:
        icecat update-taxonomy                    # Full download + update
        icecat update-taxonomy --skip-download    # Re-parse existing file
        icecat update-taxonomy -f /path/to/file   # Use specific file
    """
    from .services.taxonomy_update_service import TaxonomyUpdateService

    config: AppConfig = ctx.obj["config"]

    # FTP/SFTP credentials required only when downloading
    if not file_path and not skip_download:
        config.icecat.validate_ftp_credentials()

    click.echo("Starting taxonomy update...")
    click.echo(f"Batch size: {batch_size}")

    if file_path:
        click.echo(f"Using file: {file_path}")
    elif skip_download:
        click.echo(f"Skipping download, using existing file in {download_dir}/")
    else:
        click.echo("Will download CategoryFeaturesList.xml.gz from Icecat")

    db_manager = init_db(config.database)

    service = TaxonomyUpdateService(
        config=config,
        db_manager=db_manager,
        batch_size=batch_size,
        download_dir=download_dir,
    )

    try:
        stats = service.run(
            skip_download=skip_download,
            file_path=file_path,
        )

        click.echo("\n" + "=" * 60)
        click.secho("TAXONOMY UPDATE COMPLETE", fg="green")
        click.echo("=" * 60)
        click.echo(f"Categories processed: {stats.categories_processed}")
        click.echo(f"Feature groups processed: {stats.feature_groups_processed}")
        click.echo(f"Features processed: {stats.features_processed}")
        click.echo()
        click.echo("Rows upserted:")
        click.echo(f"  category:                    {stats.categories_upserted:>10,}")
        click.echo(f"  categoryheader:              {stats.headers_upserted:>10,}")
        click.echo(
            f"  categorydisplayattributes:   {stats.display_attrs_upserted:>10,}"
        )
        click.echo(
            f"  attributenames:              {stats.attribute_names_upserted:>10,}"
        )
        if stats.stale_rows_reported > 0:
            click.secho(f"  stale rows (log only):       {stats.stale_rows_reported:>10,}", fg="yellow")
        click.echo()
        click.echo(f"Download time: {stats.download_seconds:.1f}s")
        click.echo(f"Parse + upsert time: {stats.parse_seconds:.1f}s")
        click.echo(f"Total time: {stats.total_seconds:.1f}s")

    except FileNotFoundError as e:
        click.secho(f"File not found: {e}", fg="red", err=True)
        raise SystemExit(1)
    except Exception as e:
        click.secho(f"Taxonomy update failed: {e}", fg="red", err=True)
        raise SystemExit(1)


def main() -> None:
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
