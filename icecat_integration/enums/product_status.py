"""Product status enumeration."""

from enum import Enum


class ProductStatus(str, Enum):
    """Product status values."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DISCONTINUED = "discontinued"
