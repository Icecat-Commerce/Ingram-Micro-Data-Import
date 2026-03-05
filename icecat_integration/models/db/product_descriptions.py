"""Product descriptions database model."""

from sqlalchemy import Boolean, Column, Integer, PrimaryKeyConstraint, TIMESTAMP
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.sql import func

from .base import Base


class ProductDescriptions(Base):
    """
    Language-specific product descriptions.
    """

    __tablename__ = "productdescriptions"

    productid = Column(Integer, nullable=False, default=0, comment="Icecat Product ID")
    description = Column(MEDIUMTEXT, nullable=False, comment="Product description")
    isdefault = Column(Boolean, nullable=False, default=False, comment="Is default description")
    localeid = Column(Integer, nullable=False, default=0, comment="Locale/Language ID")
    isactive = Column(Boolean, nullable=False, default=True, comment="Active status")
    created_date = Column(
        TIMESTAMP, nullable=True, server_default=func.current_timestamp(), comment="Creation date"
    )
    modified_date = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        comment="Last modified date",
    )

    __table_args__ = (
        PrimaryKeyConstraint("productid", "localeid", name="pk_productdescriptions"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<ProductDescriptions(productid={self.productid}, localeid={self.localeid})>"
