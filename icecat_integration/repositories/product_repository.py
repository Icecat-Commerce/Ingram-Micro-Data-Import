"""Repository for product and related data operations.

- When item deleted from any table, copy to log table first
- Tables: deleted_media, deleted_attributes, deleted_features, deleted_addons

Performance: Uses raw SQL bulk INSERT and INSERT...SELECT for audit copies
instead of per-row ORM operations. Flushes are deferred to commit time.
"""

from typing import Any

from sqlalchemy import select, delete, and_, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
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
    #
    # All methods deduplicate records before INSERT to prevent violations
    # on composite PRIMARY KEYs and UNIQUE constraints. Icecat XML
    # sometimes returns duplicate entries for the same key combination.
    # =========================================================================

    @staticmethod
    def _dedup(rows: list[dict], key_fields: list[str]) -> list[dict]:
        """Remove duplicate rows by key fields, keeping the first occurrence."""
        seen: set[tuple] = set()
        result = []
        for row in rows:
            key = tuple(row.get(f, "") for f in key_fields)
            if key not in seen:
                seen.add(key)
                result.append(row)
        return result

    def sync_descriptions(
        self, product_id: int, descriptions: list[dict[str, Any]]
    ) -> int:
        """Delete existing and bulk insert new descriptions."""
        self.session.execute(
            delete(ProductDescriptions).where(ProductDescriptions.productid == product_id)
        )
        if not descriptions:
            return 0
        records = [{"productid": product_id, **desc} for desc in descriptions]
        records = self._dedup(records, ["productid", "localeid"])
        if records:
            self.session.execute(mysql_insert(ProductDescriptions).values(records))
        return len(records)

    def sync_marketing_info(
        self, product_id: int, marketing_data: list[dict[str, Any]]
    ) -> int:
        """Delete existing and bulk insert new marketing info."""
        self.session.execute(
            delete(ProductMarketingInfo).where(ProductMarketingInfo.productid == product_id)
        )
        if not marketing_data:
            return 0
        records = [{"productid": product_id, **data} for data in marketing_data]
        records = self._dedup(records, ["productid", "localeid"])
        if records:
            self.session.execute(mysql_insert(ProductMarketingInfo).values(records))
        return len(records)

    def sync_features(
        self,
        product_id: int,
        features: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> int:
        """Audit-copy, delete, and bulk insert features."""
        self.session.execute(text(
            "INSERT INTO deleted_features "
            "(product_id, productfeatureid, localeid, ordernumber, text, isactive, deleted_by_run_id, deletion_reason) "
            "SELECT productid, productfeatureid, localeid, ordernumber, text, isactive, :run_id, 'sync_update' "
            "FROM productfeatures WHERE productid = :pid"
        ), {"pid": product_id, "run_id": run_id})
        self.session.execute(
            delete(ProductFeatures).where(ProductFeatures.productid == product_id)
        )
        if not features:
            return 0
        seen: set[tuple] = set()
        records = []
        for feat in features:
            localeid = feat.get("localeid", 0)
            ordernumber = feat.get("ordernumber", 0)
            key = (localeid, ordernumber)
            if key in seen:
                continue
            seen.add(key)
            records.append({
                "productfeatureid": product_id * 1000000 + localeid * 1000 + ordernumber,
                "productid": product_id,
                "localeid": localeid,
                "ordernumber": ordernumber,
                "text": feat.get("text", ""),
                "isactive": feat.get("isactive", True),
            })
        if records:
            self.session.execute(mysql_insert(ProductFeatures).values(records))
        return len(records)

    def sync_media(
        self,
        product_id: int,
        media_data: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> int:
        """Audit-copy, delete, and bulk insert media."""
        self.session.execute(text(
            "INSERT INTO deleted_media "
            "(original_media_id, product_id, `original`, original_media_type, imageType, localeid, image500, high, medium, low, image_max_size, deleted_by_run_id, deletion_reason) "
            "SELECT id, product_id, `original`, original_media_type, imageType, localeid, image500, high, medium, low, image_max_size, :run_id, 'sync_update' "
            "FROM media_data WHERE product_id = :pid"
        ), {"pid": product_id, "run_id": run_id})
        self.session.execute(
            delete(MediaData).where(MediaData.product_id == product_id)
        )
        if not media_data:
            return 0
        records = [{"product_id": product_id, **media} for media in media_data]
        records = self._dedup(records, ["product_id", "original", "original_media_type", "localeid"])
        if records:
            self.session.execute(mysql_insert(MediaData).values(records))
        return len(records)

    def sync_attributes(
        self,
        product_id: int,
        attributes: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> int:
        """Audit-copy, delete, and bulk insert attributes."""
        self.session.execute(text(
            "INSERT INTO deleted_attributes "
            "(product_id, attributeid, setnumber, displayvalue, absolutevalue, unitid, isabsolute, isactive, localeid, attribute_type, deleted_by_run_id, deletion_reason) "
            "SELECT productid, attributeid, setnumber, displayvalue, absolutevalue, unitid, isabsolute, isactive, localeid, type, :run_id, 'sync_update' "
            "FROM productattribute WHERE productid = :pid"
        ), {"pid": product_id, "run_id": run_id})
        self.session.execute(
            delete(ProductAttribute).where(ProductAttribute.productid == product_id)
        )
        if not attributes:
            return 0
        records = [{"productid": product_id, **attr} for attr in attributes]
        records = self._dedup(records, ["productid", "localeid", "attributeid", "setnumber"])
        if records:
            self.session.execute(mysql_insert(ProductAttribute).values(records))
        return len(records)

    # =========================================================================
    # Search Attributes (searchable specifications)
    # =========================================================================

    def sync_search_attributes(
        self,
        product_id: int,
        search_attributes: list[dict[str, Any]],
    ) -> int:
        """Delete existing and bulk insert search attributes."""
        self.session.execute(
            delete(SearchAttribute).where(SearchAttribute.productid == product_id)
        )
        if not search_attributes:
            return 0
        records = [{"productid": product_id, **attr} for attr in search_attributes]
        records = self._dedup(records, ["productid", "localeid", "attributeid", "setnumber"])
        if records:
            self.session.execute(mysql_insert(SearchAttribute).values(records))
        return len(records)

    # =========================================================================
    # Media Thumbnails (all image size variants)
    # =========================================================================

    def sync_thumbnails(
        self,
        product_id: int,
        thumbnails: list[dict[str, Any]],
    ) -> int:
        """Delete existing and bulk insert thumbnails."""
        self.session.execute(
            delete(IcecatMediaThumbnails).where(IcecatMediaThumbnails.productid == product_id)
        )
        if not thumbnails:
            return 0
        records = [{"productid": product_id, **thumb} for thumb in thumbnails]
        self.session.execute(mysql_insert(IcecatMediaThumbnails).values(records))
        return len(records)

    def sync_addons(
        self,
        product_id: int,
        addons: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> int:
        """Audit-copy, delete, and bulk insert addons."""
        self.session.execute(text(
            "INSERT INTO deleted_addons "
            "(product_id, relatedProductId, type, `order`, available, isactive, deleted_by_run_id, deletion_reason) "
            "SELECT productId, relatedProductId, type, `order`, available, isactive, :run_id, 'sync_update' "
            "FROM product_addons WHERE productId = :pid"
        ), {"pid": str(product_id), "run_id": run_id})
        self.session.execute(
            delete(ProductAddons).where(ProductAddons.productId == product_id)
        )
        if not addons:
            return 0
        records = [{"productId": str(product_id), **addon} for addon in addons]
        records = self._dedup(records, ["productId", "relatedProductId"])
        if not records:
            return 0
        self.session.execute(mysql_insert(ProductAddons).values(records))
        return len(records)

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
        """Sync a product with all related data using bulk operations."""
        product, is_new = self.upsert_product(product_data)
        pid = product.productid

        if descriptions is not None:
            self.sync_descriptions(pid, descriptions)
        if marketing_info is not None:
            self.sync_marketing_info(pid, marketing_info)
        if features is not None:
            self.sync_features(pid, features, run_id=run_id)
        if media is not None:
            self.sync_media(pid, media, run_id=run_id)
        if attributes is not None:
            self.sync_attributes(pid, attributes, run_id=run_id)
        if search_attributes is not None:
            self.sync_search_attributes(pid, search_attributes)
        if thumbnails is not None:
            self.sync_thumbnails(pid, thumbnails)
        if addons is not None:
            self.sync_addons(pid, addons, run_id=run_id)

        return product, is_new

    def bulk_sync_many(
        self,
        products: list[dict[str, Any]],
        run_id: str | None = None,
    ) -> int:
        """
        Sync N products in a single transaction using bulk SQL.

        Instead of N commits with ~20 queries each, this does ~20 queries total + 1 commit.
        Each product dict has: product, descriptions, marketing_info, features, media,
        attributes, search_attributes, thumbnails, addons.
        """
        if not products:
            return 0

        pids = []
        product_records = []
        for merged in products:
            pd = merged["product"]
            pids.append(pd["productid"])
            product_records.append(pd)

        # 1. Bulk upsert all product rows
        stmt = mysql_insert(Product).values(product_records)
        data_keys = set()
        for rec in product_records:
            data_keys.update(rec.keys())
        update_cols = {
            col.name: stmt.inserted[col.name]
            for col in Product.__table__.columns
            if not col.primary_key and col.name in data_keys
        }
        if update_cols:
            self.session.execute(stmt.on_duplicate_key_update(**update_cols))
        else:
            self.session.execute(stmt.prefix_with("IGNORE"))

        pid_list = ",".join(str(p) for p in pids)

        # 2. For each child table: audit copy → delete → bulk insert
        # Features (audited)
        self.session.execute(text(
            "INSERT INTO deleted_features "
            "(product_id, productfeatureid, localeid, ordernumber, text, isactive, deleted_by_run_id, deletion_reason) "
            "SELECT productid, productfeatureid, localeid, ordernumber, text, isactive, :run_id, 'sync_update' "
            f"FROM productfeatures WHERE productid IN ({pid_list})"
        ), {"run_id": run_id})
        self.session.execute(delete(ProductFeatures).where(ProductFeatures.productid.in_(pids)))

        feat_rows = []
        seen_feat = set()
        for merged in products:
            pid = merged["product"]["productid"]
            for feat in merged.get("features") or []:
                lid = feat.get("localeid", 0)
                orn = feat.get("ordernumber", 0)
                key = (pid, lid, orn)
                if key in seen_feat:
                    continue
                seen_feat.add(key)
                feat_rows.append({
                    "productfeatureid": pid * 1000000 + lid * 1000 + orn,
                    "productid": pid, "localeid": lid, "ordernumber": orn,
                    "text": feat.get("text", ""), "isactive": feat.get("isactive", True),
                })
        if feat_rows:
            self.session.execute(mysql_insert(ProductFeatures).values(feat_rows))

        # Media (audited)
        self.session.execute(text(
            "INSERT INTO deleted_media "
            "(original_media_id, product_id, `original`, original_media_type, imageType, localeid, image500, high, medium, low, image_max_size, deleted_by_run_id, deletion_reason) "
            "SELECT id, product_id, `original`, original_media_type, imageType, localeid, image500, high, medium, low, image_max_size, :run_id, 'sync_update' "
            f"FROM media_data WHERE product_id IN ({pid_list})"
        ), {"run_id": run_id})
        self.session.execute(delete(MediaData).where(MediaData.product_id.in_(pids)))

        media_rows = []
        seen_media = set()
        for merged in products:
            pid = merged["product"]["productid"]
            for row in merged.get("media") or []:
                key = (pid, row.get("original", ""), row.get("original_media_type", ""), row.get("localeid", 0))
                if key not in seen_media:
                    seen_media.add(key)
                    media_rows.append({"product_id": pid, **row})
        if media_rows:
            self.session.execute(mysql_insert(MediaData).values(media_rows))

        # Attributes (audited)
        self.session.execute(text(
            "INSERT INTO deleted_attributes "
            "(product_id, attributeid, setnumber, displayvalue, absolutevalue, unitid, isabsolute, isactive, localeid, attribute_type, deleted_by_run_id, deletion_reason) "
            "SELECT productid, attributeid, setnumber, displayvalue, absolutevalue, unitid, isabsolute, isactive, localeid, type, :run_id, 'sync_update' "
            f"FROM productattribute WHERE productid IN ({pid_list})"
        ), {"run_id": run_id})
        self.session.execute(delete(ProductAttribute).where(ProductAttribute.productid.in_(pids)))

        attr_rows = []
        for merged in products:
            pid = merged["product"]["productid"]
            for row in merged.get("attributes") or []:
                attr_rows.append({"productid": pid, **row})
        attr_rows = self._dedup(attr_rows, ["productid", "localeid", "attributeid", "setnumber"])
        if attr_rows:
            self.session.execute(mysql_insert(ProductAttribute).values(attr_rows))

        # Addons (audited)
        str_pids = [str(p) for p in pids]
        self.session.execute(text(
            "INSERT INTO deleted_addons "
            "(product_id, relatedProductId, type, `order`, available, isactive, deleted_by_run_id, deletion_reason) "
            "SELECT productId, relatedProductId, type, `order`, available, isactive, :run_id, 'sync_update' "
            f"FROM product_addons WHERE productId IN ({pid_list})"
        ), {"run_id": run_id})
        self.session.execute(delete(ProductAddons).where(ProductAddons.productId.in_(str_pids)))

        addon_rows = []
        for merged in products:
            pid = merged["product"]["productid"]
            for row in merged.get("addons") or []:
                addon_rows.append({"productId": str(pid), **row})
        addon_rows = self._dedup(addon_rows, ["productId", "relatedProductId"])
        if addon_rows:
            self.session.execute(mysql_insert(ProductAddons).values(addon_rows))

        # Descriptions (no audit)
        self.session.execute(delete(ProductDescriptions).where(ProductDescriptions.productid.in_(pids)))
        desc_rows = []
        for merged in products:
            pid = merged["product"]["productid"]
            for row in merged.get("descriptions") or []:
                desc_rows.append({"productid": pid, **row})
        desc_rows = self._dedup(desc_rows, ["productid", "localeid"])
        if desc_rows:
            self.session.execute(mysql_insert(ProductDescriptions).values(desc_rows))

        # Marketing info (no audit)
        self.session.execute(delete(ProductMarketingInfo).where(ProductMarketingInfo.productid.in_(pids)))
        mkt_rows = []
        for merged in products:
            pid = merged["product"]["productid"]
            for row in merged.get("marketing_info") or []:
                mkt_rows.append({"productid": pid, **row})
        mkt_rows = self._dedup(mkt_rows, ["productid", "localeid"])
        if mkt_rows:
            self.session.execute(mysql_insert(ProductMarketingInfo).values(mkt_rows))

        # Search attributes (no audit)
        self.session.execute(delete(SearchAttribute).where(SearchAttribute.productid.in_(pids)))
        sa_rows = []
        for merged in products:
            pid = merged["product"]["productid"]
            for row in merged.get("search_attributes") or []:
                sa_rows.append({"productid": pid, **row})
        sa_rows = self._dedup(sa_rows, ["productid", "localeid", "attributeid", "setnumber"])
        if sa_rows:
            self.session.execute(mysql_insert(SearchAttribute).values(sa_rows))

        # Thumbnails (no audit)
        self.session.execute(delete(IcecatMediaThumbnails).where(IcecatMediaThumbnails.productid.in_(pids)))
        thumb_rows = []
        for merged in products:
            pid = merged["product"]["productid"]
            for row in merged.get("thumbnails") or []:
                thumb_rows.append({"productid": pid, **row})
        if thumb_rows:
            self.session.execute(mysql_insert(IcecatMediaThumbnails).values(thumb_rows))

        return len(products)

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
