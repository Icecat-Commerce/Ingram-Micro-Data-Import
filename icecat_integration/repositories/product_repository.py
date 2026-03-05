"""Repository for product and related data operations.

- When item deleted from any table, copy to log table first
- Tables: deleted_media, deleted_attributes, deleted_features, deleted_addons
"""

from typing import Any

from sqlalchemy import select, delete, and_
from sqlalchemy.orm import Session

from .base_repository import BaseRepository
from ..models.db.product import Product
from ..models.db.product_descriptions import ProductDescriptions
from ..models.db.product_marketing_info import ProductMarketingInfo
from ..models.db.product_features import ProductFeatures
from ..models.db.icecat_media_data import MediaData
from ..models.db.product_attribute import ProductAttribute
from ..models.db.search_attribute import SearchAttribute
from ..models.db.icecat_media_thumbnails import IcecatMediaThumbnails
from ..models.db.product_addons import ProductAddons
from ..models.db.vendor import Vendor
from ..models.db.category import Category
from ..models.db.deleted_items_log import (
    DeletedMedia,
    DeletedAttributes,
    DeletedFeatures,
    DeletedAddons,
)


class ProductRepository(BaseRepository[Product]):
    """
    Repository for product and related data operations.

    Handles:
    - Product CRUD with related data
    - Delete & recreate patterns for child records
    - Bulk operations for sync performance
    """

    def __init__(self, session: Session):
        super().__init__(session, Product)

    def get_by_product_id(self, product_id: int) -> Product | None:
        """Get product by Icecat product ID."""
        return self.get_one_by_filter(productid=product_id)

    def get_by_mpn_vendor(self, mpn: str, vendor_id: int) -> Product | None:
        """Get product by MPN and vendor ID."""
        stmt = select(Product).where(
            and_(Product.mfgpartno == mpn, Product.vendorid == vendor_id)
        )
        return self.session.scalars(stmt).first()

    def upsert_product(self, product_data: dict[str, Any]) -> tuple[Product, bool]:
        """
        Insert or update a product.

        Args:
            product_data: Dictionary with product fields

        Returns:
            Tuple of (Product, is_new)
        """
        product_id = product_data.get("productid")
        if not product_id:
            raise ValueError("productid is required")

        existing = self.get_by_product_id(product_id)
        if existing:
            # Update existing product
            for key, value in product_data.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
            self.session.flush()
            return existing, False

        # Create new product
        product = Product(**product_data)
        self.create(product)
        return product, True

    # =========================================================================
    # Delete & Recreate Operations for Child Records
    # =========================================================================

    def delete_descriptions(self, product_id: int) -> int:
        """Delete all descriptions for a product."""
        stmt = delete(ProductDescriptions).where(
            ProductDescriptions.productid == product_id
        )
        result = self.session.execute(stmt)
        return result.rowcount

    def create_descriptions(
        self, product_id: int, descriptions: list[dict[str, Any]]
    ) -> list[ProductDescriptions]:
        """Create descriptions for a product."""
        entities = [
            ProductDescriptions(productid=product_id, **desc) for desc in descriptions
        ]
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_descriptions(
        self, product_id: int, descriptions: list[dict[str, Any]]
    ) -> list[ProductDescriptions]:
        """Delete existing and create new descriptions (delete & recreate pattern)."""
        self.delete_descriptions(product_id)
        return self.create_descriptions(product_id, descriptions)

    def delete_marketing_info(self, product_id: int) -> int:
        """Delete all marketing info for a product."""
        stmt = delete(ProductMarketingInfo).where(
            ProductMarketingInfo.productid == product_id
        )
        result = self.session.execute(stmt)
        return result.rowcount

    def create_marketing_info(
        self, product_id: int, marketing_data: list[dict[str, Any]]
    ) -> list[ProductMarketingInfo]:
        """Create marketing info for a product."""
        entities = [
            ProductMarketingInfo(productid=product_id, **data) for data in marketing_data
        ]
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_marketing_info(
        self, product_id: int, marketing_data: list[dict[str, Any]]
    ) -> list[ProductMarketingInfo]:
        """Delete existing and create new marketing info."""
        self.delete_marketing_info(product_id)
        return self.create_marketing_info(product_id, marketing_data)

    def delete_features(
        self,
        product_id: int,
        run_id: str | None = None,
        reason: str | None = None,
    ) -> int:
        """
        Delete all features for a product.

        Copy to deleted_features table before deleting.
        """
        # First, copy existing records to audit table
        existing = self.session.scalars(
            select(ProductFeatures).where(ProductFeatures.productid == product_id)
        ).all()

        for feat in existing:
            deleted_record = DeletedFeatures(
                product_id=feat.productid,
                productfeatureid=feat.productfeatureid,
                localeid=feat.localeid,
                ordernumber=feat.ordernumber,
                text=feat.text,
                isactive=feat.isactive,
                deleted_by_run_id=run_id,
                deletion_reason=reason or "sync_update",
            )
            self.session.add(deleted_record)

        # Then delete the records
        stmt = delete(ProductFeatures).where(ProductFeatures.productid == product_id)
        result = self.session.execute(stmt)
        return result.rowcount

    def create_features(
        self, product_id: int, features: list[dict[str, Any]]
    ) -> list[ProductFeatures]:
        """
        Create features for a product.

        productfeatureid = productid * 1000000 + localeid * 1000 + ordernumber
        """
        entities = []
        for feat in features:
            # Generate unique productfeatureid from product + feature group + locale
            localeid = feat.get("localeid", 0)
            ordernumber = feat.get("ordernumber", 0)
            productfeatureid = product_id * 1000000 + localeid * 1000 + ordernumber

            entity = ProductFeatures(
                productfeatureid=productfeatureid,
                productid=product_id,
                localeid=localeid,
                ordernumber=ordernumber,
                text=feat.get("text", ""),
                isactive=feat.get("isactive", True),
            )
            entities.append(entity)

        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_features(
        self,
        product_id: int,
        features: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> list[ProductFeatures]:
        """Delete existing and create new features."""
        self.delete_features(product_id, run_id=run_id)
        return self.create_features(product_id, features)

    def delete_media(
        self,
        product_id: int,
        run_id: str | None = None,
        reason: str | None = None,
    ) -> int:
        """
        Delete all media for a product.

        Copy to deleted_media table before deleting.
        """
        # First, copy existing records to audit table
        existing = self.session.scalars(
            select(MediaData).where(MediaData.product_id == product_id)
        ).all()

        for media in existing:
            deleted_record = DeletedMedia(
                original_media_id=media.id,
                product_id=media.product_id,
                original=media.original,
                original_media_type=media.original_media_type,
                imageType=media.imageType,
                localeid=media.localeid,
                image500=media.image500,
                high=media.high,
                medium=media.medium,
                low=media.low,
                image_max_size=media.image_max_size,
                deleted_by_run_id=run_id,
                deletion_reason=reason or "sync_update",
            )
            self.session.add(deleted_record)

        # Then delete the records
        stmt = delete(MediaData).where(MediaData.product_id == product_id)
        result = self.session.execute(stmt)
        return result.rowcount

    def create_media(
        self, product_id: int, media_data: list[dict[str, Any]]
    ) -> list[MediaData]:
        """Create media for a product."""
        entities = [
            MediaData(product_id=product_id, **media) for media in media_data
        ]
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_media(
        self,
        product_id: int,
        media_data: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> list[MediaData]:
        """Delete existing and create new media."""
        self.delete_media(product_id, run_id=run_id)
        return self.create_media(product_id, media_data)

    def delete_attributes(
        self,
        product_id: int,
        run_id: str | None = None,
        reason: str | None = None,
    ) -> int:
        """
        Delete all attributes for a product.

        Copy to deleted_attributes table before deleting.
        """
        # First, copy existing records to audit table
        existing = self.session.scalars(
            select(ProductAttribute).where(ProductAttribute.productid == product_id)
        ).all()

        for attr in existing:
            deleted_record = DeletedAttributes(
                product_id=attr.productid,
                attributeid=attr.attributeid,
                setnumber=attr.setnumber,
                displayvalue=attr.displayvalue,
                absolutevalue=attr.absolutevalue,
                unitid=attr.unitid,
                isabsolute=attr.isabsolute,
                isactive=attr.isactive,
                localeid=attr.localeid,
                attribute_type=attr.type,
                deleted_by_run_id=run_id,
                deletion_reason=reason or "sync_update",
            )
            self.session.add(deleted_record)

        # Then delete the records
        stmt = delete(ProductAttribute).where(ProductAttribute.productid == product_id)
        result = self.session.execute(stmt)
        return result.rowcount

    def create_attributes(
        self, product_id: int, attributes: list[dict[str, Any]]
    ) -> list[ProductAttribute]:
        """Create attributes for a product."""
        entities = [
            ProductAttribute(productid=product_id, **attr) for attr in attributes
        ]
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_attributes(
        self,
        product_id: int,
        attributes: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> list[ProductAttribute]:
        """Delete existing and create new attributes."""
        self.delete_attributes(product_id, run_id=run_id)
        return self.create_attributes(product_id, attributes)

    # =========================================================================
    # Search Attributes (searchable specifications)
    # =========================================================================

    def delete_search_attributes(self, product_id: int) -> int:
        """Delete all search attributes for a product."""
        stmt = delete(SearchAttribute).where(SearchAttribute.productid == product_id)
        result = self.session.execute(stmt)
        return result.rowcount

    def create_search_attributes(
        self, product_id: int, attributes: list[dict[str, Any]]
    ) -> list[SearchAttribute]:
        """Create search attributes for a product."""
        entities = [
            SearchAttribute(productid=product_id, **attr) for attr in attributes
        ]
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_search_attributes(
        self,
        product_id: int,
        search_attributes: list[dict[str, Any]],
    ) -> list[SearchAttribute]:
        """Delete existing and create new search attributes."""
        self.delete_search_attributes(product_id)
        return self.create_search_attributes(product_id, search_attributes)

    # =========================================================================
    # Media Thumbnails (all image size variants)
    # =========================================================================

    def delete_thumbnails(self, product_id: int) -> int:
        """Delete all thumbnails for a product."""
        stmt = delete(IcecatMediaThumbnails).where(
            IcecatMediaThumbnails.productid == product_id
        )
        result = self.session.execute(stmt)
        return result.rowcount

    def create_thumbnails(
        self, product_id: int, thumbnails: list[dict[str, Any]]
    ) -> list[IcecatMediaThumbnails]:
        """Create thumbnails for a product."""
        entities = [
            IcecatMediaThumbnails(productid=product_id, **thumb) for thumb in thumbnails
        ]
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_thumbnails(
        self,
        product_id: int,
        thumbnails: list[dict[str, Any]],
    ) -> list[IcecatMediaThumbnails]:
        """Delete existing and create new thumbnails."""
        self.delete_thumbnails(product_id)
        return self.create_thumbnails(product_id, thumbnails)

    def delete_addons(
        self,
        product_id: int,
        run_id: str | None = None,
        reason: str | None = None,
    ) -> int:
        """
        Delete all addons/related products for a product.

        Copy to deleted_addons table before deleting.
        """
        # First, copy existing records to audit table
        existing = self.session.scalars(
            select(ProductAddons).where(ProductAddons.productId == product_id)
        ).all()

        for addon in existing:
            deleted_record = DeletedAddons(
                product_id=str(addon.productId),
                relatedProductId=str(addon.relatedProductId),
                type=addon.type,
                order=addon.order,
                available=addon.available,
                isactive=addon.isactive,
                deleted_by_run_id=run_id,
                deletion_reason=reason or "sync_update",
            )
            self.session.add(deleted_record)

        # Then delete the records
        stmt = delete(ProductAddons).where(ProductAddons.productId == product_id)
        result = self.session.execute(stmt)
        return result.rowcount

    def create_addons(
        self, product_id, addons: list[dict[str, Any]]
    ) -> list[ProductAddons]:
        """Create addons for a product."""
        entities = [
            ProductAddons(productId=str(product_id), **addon) for addon in addons
        ]
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def sync_addons(
        self,
        product_id: int,
        addons: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> list[ProductAddons]:
        """Delete existing and create new addons."""
        self.delete_addons(product_id, run_id=run_id)
        return self.create_addons(product_id, addons)

    # =========================================================================
    # Full Product Sync (All Related Data)
    # =========================================================================

    def sync_product_full(
        self,
        product_data: dict[str, Any],
        descriptions: list[dict[str, Any]] | None = None,
        marketing_info: list[dict[str, Any]] | None = None,
        features: list[dict[str, Any]] | None = None,
        media: list[dict[str, Any]] | None = None,
        attributes: list[dict[str, Any]] | None = None,
        search_attributes: list[dict[str, Any]] | None = None,
        thumbnails: list[dict[str, Any]] | None = None,
        addons: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
    ) -> tuple[Product, bool]:
        """
        Sync a product with all related data.

        Uses delete & recreate pattern for all child records.
        Before deleting, copies records to audit tables.

        Args:
            product_data: Core product data
            descriptions: Product descriptions by language
            marketing_info: Marketing info by language
            features: Product features/bullet points
            media: Product media (images, videos)
            attributes: Non-searchable product attributes
            search_attributes: Searchable product attributes
            thumbnails: Media thumbnails (all image size variants)
            addons: Related products/addons
            run_id: Sync run ID for audit trail

        Returns:
            Tuple of (Product, is_new)
        """
        # Upsert the main product
        product, is_new = self.upsert_product(product_data)

        # Sync all related data using delete & recreate pattern
        # Child tables use productid (int) as reference
        if descriptions is not None:
            self.sync_descriptions(product.productid, descriptions)

        if marketing_info is not None:
            self.sync_marketing_info(product.productid, marketing_info)

        if features is not None:
            self.sync_features(product.productid, features, run_id=run_id)

        if media is not None:
            self.sync_media(product.productid, media, run_id=run_id)

        if attributes is not None:
            self.sync_attributes(product.productid, attributes, run_id=run_id)

        if search_attributes is not None:
            self.sync_search_attributes(product.productid, search_attributes)

        if thumbnails is not None:
            self.sync_thumbnails(product.productid, thumbnails)

        if addons is not None:
            self.sync_addons(product.productid, addons, run_id=run_id)

        return product, is_new

    def deactivate_product(
        self,
        product_id: int,
        run_id: str | None = None,
        reason: str | None = None,
    ) -> bool:
        """
        Mark a product as inactive (soft delete).

        Products no longer in assortment should be marked as inactive
        (isactive=0), NOT hard deleted. Child records are left intact.

        Args:
            product_id: Icecat product ID
            run_id: Sync run ID for audit trail
            reason: Reason for deactivation (e.g., 'not_in_assortment')

        Returns:
            True if deactivated successfully
        """
        from datetime import datetime

        product = self.get_by_product_id(product_id)
        if not product:
            return False

        product.isactive = False
        product.lastupdated = datetime.now()
        self.session.flush()
        return True


class VendorRepository(BaseRepository[Vendor]):
    """Repository for vendor operations."""

    def __init__(self, session: Session):
        super().__init__(session, Vendor)

    def get_by_vendor_id(self, vendor_id: int) -> Vendor | None:
        """Get vendor by vendor ID."""
        return self.get_one_by_filter(vendorid=vendor_id)

    def get_by_name(self, name: str) -> Vendor | None:
        """Get vendor by name (case-insensitive)."""
        stmt = select(Vendor).where(Vendor.name.ilike(name))
        return self.session.scalars(stmt).first()

    def get_or_create(
        self,
        vendor_id: int,
        vendor_name: str,
        logo_url: str | None = None,
    ) -> tuple[Vendor, bool]:
        """
        Get existing vendor or create new one.

        If vendor exists and logo_url is provided, update the logo if it's empty.
        """
        existing = self.get_by_vendor_id(vendor_id)
        if existing:
            # Update logo if provided and currently empty
            if logo_url and not existing.logourl:
                existing.logourl = logo_url
                self.update(existing)
            return existing, False

        vendor = Vendor(vendorid=vendor_id, name=vendor_name, logourl=logo_url)
        self.create(vendor)
        return vendor, True


class CategoryRepository(BaseRepository[Category]):
    """Repository for category operations."""

    def __init__(self, session: Session):
        super().__init__(session, Category)

    def get_by_category_id(self, category_id: int, locale_id: int = 1) -> Category | None:
        """Get category by category ID and locale."""
        stmt = select(Category).where(
            and_(Category.categoryid == category_id, Category.localeid == locale_id)
        )
        return self.session.scalars(stmt).first()

    def get_or_create(
        self,
        category_id: int,
        category_name: str,
        locale_id: int = 1,
    ) -> tuple[Category, bool]:
        """Get existing category or create new one."""
        existing = self.get_by_category_id(category_id, locale_id)
        if existing:
            return existing, False

        category = Category(
            categoryid=category_id,
            categoryname=category_name,
            localeid=locale_id,
        )
        self.create(category)
        return category, True
