"""Product database model - aligned with schema v2.1."""

from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class Product(Base):
    """
    Core product master data.
    FK: vendorid → vendor.vendorid, categoryid → categoryMapping.categoryid
    """

    __tablename__ = "product"

    productid = Column(Integer, primary_key=True, default=0, comment="Icecat Product ID")
    vendorid = Column(
        Integer,
        ForeignKey("vendor.vendorid", ondelete="RESTRICT", onupdate="CASCADE"),
        nullable=False,
        default=0,
        comment="Vendor/Brand ID",
    )
    isactive = Column(Boolean, nullable=False, default=True, comment="Product active status")
    mfgpartno = Column(String(70), nullable=False, default="", comment="Manufacturer Part Number")
    categoryid = Column(
        Integer,
        ForeignKey("categoryMapping.categoryid", ondelete="RESTRICT", onupdate="CASCADE"),
        nullable=False,
        default=0,
        comment="Category ID",
    )
    isaccessory = Column(Boolean, nullable=False, default=False, comment="Is accessory product")
    creationdate = Column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp(),
        comment="Product creation date",
    )
    modifieddate = Column(
        TIMESTAMP, nullable=True, server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(), comment="Last modified timestamp",
    )
    lastupdated = Column(TIMESTAMP, nullable=True, comment="Last sync update timestamp")

    __table_args__ = (
        Index("product_vendorID", "vendorid"),
        Index("product_categoryID", "categoryid"),
        Index("product_mfgPartNo", "mfgpartno"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<Product(productid={self.productid}, mfgpartno={self.mfgpartno})>"
