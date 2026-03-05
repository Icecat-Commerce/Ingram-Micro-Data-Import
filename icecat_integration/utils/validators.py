"""Data validation utilities for sync operations."""

import re
from typing import Any


def validate_ean(ean: str | None) -> bool:
    """
    Validate EAN/UPC barcode.

    Supports:
    - EAN-13 (13 digits)
    - EAN-8 (8 digits)
    - UPC-A (12 digits)
    - GTIN-14 (14 digits)

    Args:
        ean: The EAN/UPC to validate

    Returns:
        True if valid, False otherwise
    """
    if not ean:
        return False

    # Remove any spaces or hyphens
    ean = ean.replace(" ", "").replace("-", "")

    # Must be all digits
    if not ean.isdigit():
        return False

    # Check length
    if len(ean) not in (8, 12, 13, 14):
        return False

    # Verify check digit
    return _verify_ean_check_digit(ean)


def _verify_ean_check_digit(ean: str) -> bool:
    """Verify EAN/UPC check digit using modulo 10 algorithm."""
    digits = [int(d) for d in ean]

    # For EAN-8, EAN-13, GTIN-14
    if len(ean) in (8, 13, 14):
        odd_sum = sum(digits[i] for i in range(0, len(digits) - 1, 2))
        even_sum = sum(digits[i] for i in range(1, len(digits) - 1, 2))
        checksum = (10 - ((odd_sum + even_sum * 3) % 10)) % 10
    # For UPC-A (12 digits)
    elif len(ean) == 12:
        odd_sum = sum(digits[i] for i in range(0, len(digits) - 1, 2))
        even_sum = sum(digits[i] for i in range(1, len(digits) - 1, 2))
        checksum = (10 - ((odd_sum * 3 + even_sum) % 10)) % 10
    else:
        return False

    return checksum == digits[-1]


def normalize_ean(ean: str | None) -> str | None:
    """
    Normalize EAN/UPC to standard format.

    - Removes spaces and hyphens
    - Pads with leading zeros if needed (for EAN-8 or UPC conversion)

    Args:
        ean: The EAN/UPC to normalize

    Returns:
        Normalized EAN or None if invalid
    """
    if not ean:
        return None

    # Clean up
    ean = ean.replace(" ", "").replace("-", "").strip()

    # Remove leading zeros if too long
    ean = ean.lstrip("0") or "0"

    # Pad to valid length
    if len(ean) <= 8:
        ean = ean.zfill(8)
    elif len(ean) <= 12:
        ean = ean.zfill(12)
    elif len(ean) <= 13:
        ean = ean.zfill(13)
    elif len(ean) <= 14:
        ean = ean.zfill(14)

    return ean if validate_ean(ean) else None


def sanitize_string(value: str | None, max_length: int | None = None) -> str | None:
    """
    Sanitize a string value for database storage.

    - Trims whitespace
    - Replaces control characters
    - Truncates to max length if specified

    Args:
        value: The string to sanitize
        max_length: Optional maximum length

    Returns:
        Sanitized string or None
    """
    if value is None:
        return None

    # Convert to string if needed
    value = str(value)

    # Trim whitespace
    value = value.strip()

    # Replace control characters (except newlines and tabs)
    value = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", value)

    # Replace multiple spaces with single space
    value = re.sub(r" +", " ", value)

    # Truncate if needed
    if max_length and len(value) > max_length:
        value = value[:max_length]

    return value if value else None


def sanitize_html(value: str | None) -> str | None:
    """
    Basic HTML sanitization for product descriptions.

    Allows common formatting tags but removes scripts and styles.

    Args:
        value: The HTML string to sanitize

    Returns:
        Sanitized HTML or None
    """
    if not value:
        return None

    # Remove script tags and content
    value = re.sub(r"<script[^>]*>.*?</script>", "", value, flags=re.DOTALL | re.IGNORECASE)

    # Remove style tags and content
    value = re.sub(r"<style[^>]*>.*?</style>", "", value, flags=re.DOTALL | re.IGNORECASE)

    # Remove onclick, onerror, and other event handlers
    value = re.sub(r'\s+on\w+="[^"]*"', "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+on\w+='[^']*'", "", value, flags=re.IGNORECASE)

    # Remove javascript: URLs
    value = re.sub(r'javascript:', '', value, flags=re.IGNORECASE)

    return sanitize_string(value)


def validate_brand(brand: str | None) -> tuple[bool, str | None]:
    """
    Validate and normalize a brand name.

    Args:
        brand: The brand name to validate

    Returns:
        Tuple of (is_valid, normalized_brand)
    """
    if not brand:
        return False, None

    # Sanitize
    brand = sanitize_string(brand, max_length=255)
    if not brand:
        return False, None

    # Must have at least 1 non-whitespace character
    if len(brand.strip()) == 0:
        return False, None

    return True, brand


def validate_mpn(mpn: str | None) -> tuple[bool, str | None]:
    """
    Validate and normalize a manufacturer part number.

    Args:
        mpn: The MPN to validate

    Returns:
        Tuple of (is_valid, normalized_mpn)
    """
    if not mpn:
        return False, None

    # Sanitize
    mpn = sanitize_string(mpn, max_length=255)
    if not mpn:
        return False, None

    # Must have at least 1 non-whitespace character
    if len(mpn.strip()) == 0:
        return False, None

    return True, mpn


def validate_language_code(code: str | None) -> bool:
    """
    Validate a language code (ISO 639-1 or locale).

    Accepts:
    - Two-letter codes (en, de, fr)
    - Locale codes (en-US, de-DE)
    - Icecat short codes (EN_US, DE_AT)

    Args:
        code: The language code to validate

    Returns:
        True if valid format
    """
    if not code:
        return False

    # Pattern for ISO 639-1, locales, and Icecat short codes
    pattern = re.compile(
        r"^[a-zA-Z]{2}(?:[-_][a-zA-Z]{2})?$"
    )
    return bool(pattern.match(code))


def validate_assortment_row(row: dict[str, Any]) -> tuple[bool, str | None, str | None, str | None]:
    """
    Validate a row from the assortment CSV.

    Args:
        row: Dictionary with row data

    Returns:
        Tuple of (is_valid, brand, mpn, error_message)
    """
    # Get brand (try various column names)
    brand = None
    for key in ["brand", "Brand", "BRAND", "manufacturer", "Manufacturer"]:
        if key in row and row[key]:
            brand = row[key]
            break

    # Get MPN (try various column names)
    mpn = None
    for key in ["mpn", "MPN", "part_number", "PartNumber", "VPN", "vpn", "mfgpartno"]:
        if key in row and row[key]:
            mpn = row[key]
            break

    # Validate brand
    brand_valid, brand = validate_brand(brand)
    if not brand_valid:
        return False, None, None, "Missing or invalid brand"

    # Validate MPN
    mpn_valid, mpn = validate_mpn(mpn)
    if not mpn_valid:
        return False, brand, None, "Missing or invalid MPN"

    return True, brand, mpn, None
