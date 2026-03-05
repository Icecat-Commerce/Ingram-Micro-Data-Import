"""Assortment CSV file reader for sync operations."""

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from ..utils.validators import validate_brand, validate_mpn

logger = logging.getLogger(__name__)


@dataclass
class AssortmentItem:
    """Represents a single item from the assortment file."""

    brand: str
    mpn: str
    row_number: int
    extra_data: dict | None = None

    def __hash__(self):
        return hash((self.brand.lower(), self.mpn.lower()))

    def __eq__(self, other):
        if not isinstance(other, AssortmentItem):
            return False
        return (
            self.brand.lower() == other.brand.lower()
            and self.mpn.lower() == other.mpn.lower()
        )


@dataclass
class AssortmentStats:
    """Statistics from reading an assortment file."""

    total_rows: int
    valid_rows: int
    invalid_rows: int
    duplicate_rows: int
    unique_items: int


class AssortmentReader:
    """
    Read Brand + MPN pairs from assortment files.

    Supports:
    - Multiple column name conventions (brand, Brand, MasterVendNam, etc.)
    - Single-char delimiters (comma, tab) and multi-char delimiters (~~)
    - Auto-detection of delimiter from file header
    - Large file streaming (memory efficient)
    - Validation and deduplication
    """

    # Common column name variations for brand
    BRAND_COLUMNS = [
        "brand",
        "Brand",
        "BRAND",
        "manufacturer",
        "Manufacturer",
        "MANUFACTURER",
        "vendor",
        "Vendor",
        "VENDOR",
        # Ingram Micro format
        "MasterVendNam",
        "mastervendnam",
        "MASTERVENDNAM",
        "vendor_name",
        "VendorName",
        "VENDOR_NAME",
    ]

    # Common column name variations for MPN
    MPN_COLUMNS = [
        "mpn",
        "MPN",
        "part_number",
        "PartNumber",
        "PART_NUMBER",
        "vpn",
        "VPN",
        "mfgpartno",
        "MfgPartNo",
        "MFGPARTNO",
        "product_code",
        "ProductCode",
        "PRODUCT_CODE",
        "sku",
        "SKU",
        "article_number",
        "ArticleNumber",
        # Ingram Micro format
        "VendorPartNbr",
        "vendorpartnbr",
        "VENDORPARTNBR",
        "vendor_part_number",
        "VendorPartNumber",
        "VENDOR_PART_NUMBER",
    ]

    def __init__(
        self,
        brand_column: str | None = None,
        mpn_column: str | None = None,
        delimiter: str | None = None,
        encoding: str = "utf-8",
    ):
        """
        Initialize the reader.

        Args:
            brand_column: Override column name for brand (auto-detect if None)
            mpn_column: Override column name for MPN (auto-detect if None)
            delimiter: Delimiter string (auto-detect from file if None)
            encoding: File encoding
        """
        self.brand_column = brand_column
        self.mpn_column = mpn_column
        self.delimiter = delimiter
        self.encoding = encoding

    @staticmethod
    def detect_delimiter(file_path: str | Path, encoding: str = "utf-8") -> str:
        """
        Auto-detect delimiter from the file header line.

        Checks multi-char delimiters first (~~, ||), then common single-char
        delimiters (tab, pipe, semicolon, comma).

        Args:
            file_path: Path to the file

        Returns:
            Detected delimiter string
        """
        with open(file_path, "r", encoding=encoding) as f:
            header = f.readline()

        # Multi-char delimiters (check first since they contain single chars)
        for delim in ["~~", "||"]:
            if delim in header:
                return delim

        # Single-char delimiters
        for delim in ["\t", "|", ";", ","]:
            if delim in header:
                return delim

        return ","

    def _resolve_delimiter(self, file_path: str | Path) -> str:
        """Get delimiter, auto-detecting if not explicitly set."""
        if self.delimiter is not None:
            return self.delimiter
        detected = self.detect_delimiter(file_path, self.encoding)
        logger.info(f"Auto-detected delimiter: {repr(detected)}")
        return detected

    def _iter_rows(
        self, file_path: Path, delimiter: str
    ) -> Iterator[tuple[dict[str, str], list[str]]]:
        """
        Iterate rows from a delimited file, yielding (row_dict, fieldnames).

        Handles both single-char delimiters (via csv.DictReader) and
        multi-char delimiters (via manual line splitting).
        """
        with open(file_path, "r", encoding=self.encoding, newline="") as f:
            if len(delimiter) == 1:
                reader = csv.DictReader(f, delimiter=delimiter)
                fieldnames = list(reader.fieldnames or [])
                for row in reader:
                    yield row, fieldnames
            else:
                # Multi-char delimiter: split manually
                header_line = f.readline().rstrip("\n\r")
                fieldnames = header_line.split(delimiter)
                for line in f:
                    values = line.rstrip("\n\r").split(delimiter)
                    row = dict(zip(fieldnames, values))
                    yield row, fieldnames

    def read_csv(
        self,
        file_path: str | Path,
        deduplicate: bool = True,
    ) -> Iterator[AssortmentItem]:
        """
        Read and yield items from an assortment file.

        Args:
            file_path: Path to the file
            deduplicate: Skip duplicate brand+mpn combinations

        Yields:
            AssortmentItem for each valid row
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Assortment file not found: {file_path}")

        delimiter = self._resolve_delimiter(file_path)
        seen = set()
        row_num = 0
        brand_col = None
        mpn_col = None

        for row, fieldnames in self._iter_rows(file_path, delimiter):
            # Detect columns from first row
            if brand_col is None:
                brand_col = self._find_column(fieldnames, self.BRAND_COLUMNS, self.brand_column)
                mpn_col = self._find_column(fieldnames, self.MPN_COLUMNS, self.mpn_column)

                if not brand_col:
                    raise ValueError(
                        f"Could not find brand column. "
                        f"Available columns: {fieldnames}"
                    )
                if not mpn_col:
                    raise ValueError(
                        f"Could not find MPN column. "
                        f"Available columns: {fieldnames}"
                    )
                logger.info(
                    f"Using columns: brand='{brand_col}', mpn='{mpn_col}', "
                    f"delimiter={repr(delimiter)}"
                )

            row_num += 1

            # Extract and validate brand
            brand_raw = row.get(brand_col, "").strip()
            valid, brand = validate_brand(brand_raw)
            if not valid:
                continue

            # Extract and validate MPN
            mpn_raw = row.get(mpn_col, "").strip()
            valid, mpn = validate_mpn(mpn_raw)
            if not valid:
                continue

            # Deduplicate
            if deduplicate:
                key = (brand.lower(), mpn.lower())
                if key in seen:
                    continue
                seen.add(key)

            # Collect extra data from other columns
            extra = {
                k: v for k, v in row.items()
                if k not in (brand_col, mpn_col) and v
            }

            yield AssortmentItem(
                brand=brand,
                mpn=mpn,
                row_number=row_num,
                extra_data=extra if extra else None,
            )

    def read_csv_to_list(
        self,
        file_path: str | Path,
        deduplicate: bool = True,
    ) -> list[AssortmentItem]:
        """Read entire file into a list."""
        return list(self.read_csv(file_path, deduplicate))

    def get_row_count(self, file_path: str | Path) -> int:
        """
        Count total data rows in the file (excluding header).

        Fast: just counts lines without parsing.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            return 0

        count = 0
        with open(file_path, "r", encoding=self.encoding) as f:
            next(f, None)  # Skip header
            for _ in f:
                count += 1
        return count

    def get_stats(
        self,
        file_path: str | Path,
    ) -> AssortmentStats:
        """
        Get detailed statistics about the assortment file.

        Reads and parses the entire file to count valid, invalid,
        and duplicate rows.
        """
        file_path = Path(file_path)
        delimiter = self._resolve_delimiter(file_path)

        total = 0
        valid = 0
        invalid = 0
        duplicate = 0
        seen = set()

        brand_col = None
        mpn_col = None

        for row, fieldnames in self._iter_rows(file_path, delimiter):
            if brand_col is None:
                brand_col = self._find_column(fieldnames, self.BRAND_COLUMNS, self.brand_column)
                mpn_col = self._find_column(fieldnames, self.MPN_COLUMNS, self.mpn_column)

                if not brand_col or not mpn_col:
                    return AssortmentStats(0, 0, 0, 0, 0)

            total += 1

            brand_raw = row.get(brand_col, "").strip()
            mpn_raw = row.get(mpn_col, "").strip()

            brand_valid, brand = validate_brand(brand_raw)
            mpn_valid, mpn = validate_mpn(mpn_raw)

            if not brand_valid or not mpn_valid:
                invalid += 1
                continue

            key = (brand.lower(), mpn.lower())
            if key in seen:
                duplicate += 1
            else:
                seen.add(key)
                valid += 1

        return AssortmentStats(
            total_rows=total,
            valid_rows=valid,
            invalid_rows=invalid,
            duplicate_rows=duplicate,
            unique_items=len(seen),
        )

    def _find_column(
        self,
        fieldnames: list[str] | None,
        candidates: list[str],
        override: str | None = None,
    ) -> str | None:
        """Find a column name from a list of candidates."""
        if override:
            if fieldnames and override in fieldnames:
                return override
            return None

        if not fieldnames:
            return None

        for candidate in candidates:
            if candidate in fieldnames:
                return candidate
        return None

    def read_csv_batched(
        self,
        file_path: str | Path,
        batch_size: int = 100,
        deduplicate: bool = True,
    ) -> Iterator[list[AssortmentItem]]:
        """
        Read file in batches for processing.

        Yields:
            Lists of AssortmentItem of up to batch_size
        """
        batch = []
        for item in self.read_csv(file_path, deduplicate):
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch
