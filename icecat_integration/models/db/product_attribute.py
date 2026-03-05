"""Product attribute database model."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Double,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SmallInteger,
    TIMESTAMP,
)
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.sql import func

from .base import Base


class ProductAttribute(Base):
    """
    Product specifications (non-searchable attributes).
    """

    __tablename__ = "productattribute"

    productid = Column(Integer, nullable=False, default=0, comment="Icecat Product ID")
    attributeid = Column(BigInteger, nullable=False, default=0, comment="Attribute ID")
    setnumber = Column(Integer, nullable=False, default=1, comment="Set number for multi-value")
    displayvalue = Column(MEDIUMTEXT, nullable=True, comment="Display value")
    absolutevalue = Column(Double, nullable=False, default=0, comment="Absolute numeric value")
    unitid = Column(Integer, nullable=False, default=0, comment="Unit ID")
    isabsolute = Column(Boolean, nullable=False, default=False, comment="Has absolute value")
    isactive = Column(Boolean, nullable=False, default=True, comment="Active status")
    localeid = Column(Integer, nullable=False, default=0, comment="Locale/Language ID")
    type = Column(SmallInteger, nullable=False, default=0, comment="Attribute type")
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
        PrimaryKeyConstraint(
            "productid", "localeid", "attributeid", "setnumber", name="pk_productattribute"
        ),
        Index("productattribute_attributeID", "attributeid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<ProductAttribute(productid={self.productid}, attributeid={self.attributeid})>"
