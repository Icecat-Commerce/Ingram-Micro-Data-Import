"""Product features database model."""

from sqlalchemy import Boolean, Column, Index, Integer, Numeric, SmallInteger, String, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class ProductFeatures(Base):
    """
    Product feature bullet points.
    """

    __tablename__ = "productfeatures"

    productfeatureid = Column(
        Numeric(24, 0), primary_key=True, default=0, comment="Unique feature ID"
    )
    productid = Column(Integer, nullable=False, default=0, comment="Icecat Product ID")
    localeid = Column(Integer, nullable=False, default=0, comment="Locale/Language ID")
    ordernumber = Column(SmallInteger, nullable=False, default=0, comment="Display order")
    text = Column(String(1000), nullable=False, default="", comment="Bullet point text")
    modifieddate = Column(
        TIMESTAMP,
        nullable=False,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        comment="Last modified date",
    )
    isactive = Column(Boolean, nullable=False, default=True, comment="Active status")

    __table_args__ = (
        Index("productfeatures_productID", "productid", "localeid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<ProductFeatures(productfeatureid={self.productfeatureid}, productid={self.productid})>"
