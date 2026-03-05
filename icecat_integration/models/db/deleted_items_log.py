"""Deleted items audit log database models.

- When item deleted from any table, copy to log table first
- Tables: deleted_media, deleted_attributes, deleted_features, deleted_addons
- Log tables persist across runs (not recreated each time)
"""

from sqlalchemy import BigInteger, Boolean, Column, Index, Integer, String, Text, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class DeletedMedia(Base):
    """
    Audit log for deleted media items.
    Copy of media_data row before deletion.
    """

    __tablename__ = "deleted_media"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Original media_data fields
    original_media_id = Column(Integer, nullable=True, comment="Original ID from media_data")
    product_id = Column(BigInteger, nullable=False, comment="Icecat Product ID")
    original = Column(String(500), nullable=False, comment="Original image URL")
    original_media_type = Column(String(250), nullable=True, comment="Media type description")
    imageType = Column(String(70), nullable=True, comment="Image type (Image/Rich Media)")
    localeid = Column(Integer, nullable=True, comment="Locale/Language ID")
    image500 = Column(String(500), nullable=True, comment="500px thumbnail URL")
    high = Column(String(500), nullable=True, comment="High resolution URL")
    medium = Column(String(500), nullable=True, comment="Medium resolution URL")
    low = Column(String(500), nullable=True, comment="Low resolution URL")
    image_max_size = Column(String(25), nullable=True, comment="Maximum image size")
    # Audit fields
    deleted_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=func.current_timestamp(),
        comment="When the record was deleted",
    )
    deleted_by_run_id = Column(String(36), nullable=True, comment="Sync run ID that deleted this")
    deletion_reason = Column(String(100), nullable=True, comment="Reason for deletion")

    __table_args__ = (
        Index("idx_del_media_product", "product_id"),
        Index("idx_del_media_date", "deleted_at"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeletedMedia(product_id={self.product_id}, deleted_at={self.deleted_at})>"


class DeletedAttributes(Base):
    """
    Audit log for deleted product attributes.
    Copy of productattribute row before deletion.
    """

    __tablename__ = "deleted_attributes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Original productattribute fields
    product_id = Column(BigInteger, nullable=False, comment="Icecat Product ID")
    attributeid = Column(BigInteger, nullable=False, comment="Attribute ID")
    setnumber = Column(Integer, nullable=True, default=1, comment="Set number")
    displayvalue = Column(Text, nullable=True, comment="Display value")
    absolutevalue = Column(String(250), nullable=True, comment="Absolute/numeric value")
    unitid = Column(Integer, nullable=True, comment="Unit ID")
    isabsolute = Column(Boolean, nullable=True, comment="Is absolute value")
    isactive = Column(Boolean, nullable=True, default=True, comment="Is active flag")
    localeid = Column(Integer, nullable=True, comment="Locale/Language ID")
    attribute_type = Column(String(50), nullable=True, comment="Attribute type")
    # Audit fields
    deleted_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=func.current_timestamp(),
        comment="When the record was deleted",
    )
    deleted_by_run_id = Column(String(36), nullable=True, comment="Sync run ID that deleted this")
    deletion_reason = Column(String(100), nullable=True, comment="Reason for deletion")

    __table_args__ = (
        Index("idx_del_attr_product", "product_id"),
        Index("idx_del_attr_date", "deleted_at"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeletedAttributes(product_id={self.product_id}, attr={self.attributeid})>"


class DeletedFeatures(Base):
    """
    Audit log for deleted product features (bullet points).
    Copy of productfeatures row before deletion.
    """

    __tablename__ = "deleted_features"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Original productfeatures fields
    product_id = Column(BigInteger, nullable=False, comment="Icecat Product ID")
    productfeatureid = Column(BigInteger, nullable=True, comment="Feature ID")
    localeid = Column(Integer, nullable=True, comment="Locale/Language ID")
    ordernumber = Column(Integer, nullable=True, comment="Display order")
    text = Column(Text, nullable=True, comment="Feature/bullet point text")
    isactive = Column(Boolean, nullable=True, default=True, comment="Is active flag")
    # Audit fields
    deleted_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=func.current_timestamp(),
        comment="When the record was deleted",
    )
    deleted_by_run_id = Column(String(36), nullable=True, comment="Sync run ID that deleted this")
    deletion_reason = Column(String(100), nullable=True, comment="Reason for deletion")

    __table_args__ = (
        Index("idx_del_feat_product", "product_id"),
        Index("idx_del_feat_date", "deleted_at"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeletedFeatures(product_id={self.product_id}, order={self.ordernumber})>"


class DeletedAddons(Base):
    """
    Audit log for deleted product addons (relations).
    Copy of product_addons row before deletion.
    """

    __tablename__ = "deleted_addons"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Original product_addons fields (varchar(30) to match product_addons table)
    product_id = Column(String(30), nullable=False, comment="Base Product ID")
    relatedProductId = Column(String(30), nullable=False, comment="Related Product ID")
    type = Column(String(2), nullable=True, comment="Addon type: C/U/D/W/Z")
    order = Column(Integer, nullable=True, comment="Display order")
    available = Column(Boolean, nullable=True, default=True, comment="Is available")
    isactive = Column(Boolean, nullable=True, default=True, comment="Was active")
    # Audit fields
    deleted_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=func.current_timestamp(),
        comment="When the record was deleted",
    )
    deleted_by_run_id = Column(String(36), nullable=True, comment="Sync run ID that deleted this")
    deletion_reason = Column(String(100), nullable=True, comment="Reason for deletion")

    __table_args__ = (
        Index("idx_del_addon_product", "product_id"),
        Index("idx_del_addon_date", "deleted_at"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeletedAddons(product_id={self.product_id}, related={self.relatedProductId})>"
