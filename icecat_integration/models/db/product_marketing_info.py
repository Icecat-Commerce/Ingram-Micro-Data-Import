"""Product marketing info database model."""

from sqlalchemy import Boolean, Column, Integer, PrimaryKeyConstraint, TIMESTAMP
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.sql import func

from .base import Base


class ProductMarketingInfo(Base):
    """
    Language-specific marketing information.
    """

    __tablename__ = "productmarketingInfo"

    productid = Column(Integer, nullable=False, default=0, comment="Icecat Product ID")
    marketing = Column(MEDIUMTEXT, nullable=False, comment="Marketing text/description")
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
        PrimaryKeyConstraint("productid", "localeid", name="pk_productmarketinginfo"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<ProductMarketingInfo(productid={self.productid}, localeid={self.localeid})>"
