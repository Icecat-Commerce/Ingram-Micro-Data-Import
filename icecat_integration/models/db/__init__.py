"""SQLAlchemy ORM models for database tables."""

from .base import Base, TimestampMixin

# Master data models
from .category import Category, CategoryMapping
from .vendor import Vendor
from .locales import Locales

# Product data models
from .product import Product
from .product_descriptions import ProductDescriptions
from .product_marketing_info import ProductMarketingInfo
from .product_features import ProductFeatures
from .icecat_media_data import MediaData
from .icecat_media_thumbnails import IcecatMediaThumbnails
from .attribute_names import AttributeNames
from .product_attribute import ProductAttribute
from .search_attribute import SearchAttribute
from .product_addons import ProductAddons

# Category display models
from .category_display import CategoryHeader, CategoryDisplayAttributes

# Sync tracking models
from .sync_product import SyncProduct, SyncStatus
from .sync_log import SyncLog, LogLevel, LogType
from .sync_run import SyncRun, RunStatus

# Delta sync tracking models
from .delta_sys_models import (
    DeltaSysSequence,
    DeltaSysProductSequence,
    DeltaSysDeletionProdLocIds,
    DeltaSysProdLocaleIdsFull,
)

# Error tracking models
from .sync_errors import SyncErrors

# Supplier mapping models
from .supplier_mapping import SupplierMapping

# Deleted items audit log models
from .deleted_items_log import (
    DeletedMedia,
    DeletedAttributes,
    DeletedFeatures,
    DeletedAddons,
)

__all__ = [
    # Base
    "Base",
    "TimestampMixin",
    # Master data models
    "Category",
    "CategoryMapping",
    "Vendor",
    "Locales",
    # Product data models
    "Product",
    "ProductDescriptions",
    "ProductMarketingInfo",
    "ProductFeatures",
    "MediaData",
    "IcecatMediaThumbnails",
    "AttributeNames",
    "ProductAttribute",
    "SearchAttribute",
    "ProductAddons",
    # Category display models
    "CategoryHeader",
    "CategoryDisplayAttributes",
    # Sync tracking models
    "SyncProduct",
    "SyncStatus",
    "SyncLog",
    "LogLevel",
    "LogType",
    "SyncRun",
    "RunStatus",
    # Delta sync tracking models
    "DeltaSysSequence",
    "DeltaSysProductSequence",
    "DeltaSysDeletionProdLocIds",
    "DeltaSysProdLocaleIdsFull",
    # Error tracking models
    "SyncErrors",
    # Supplier mapping models
    "SupplierMapping",
    # Deleted items audit log models
    "DeletedMedia",
    "DeletedAttributes",
    "DeletedFeatures",
    "DeletedAddons",
]
