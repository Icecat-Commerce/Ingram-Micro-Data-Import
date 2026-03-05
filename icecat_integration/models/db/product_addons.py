"""Product addons database model."""

from sqlalchemy import Boolean, Column, Index, Integer, PrimaryKeyConstraint, String
from sqlalchemy.dialects.mysql import INTEGER

from .base import Base


# Addon type derivation (product_mapper.map_addons):
#
# The Icecat API does not return a type field directly. The mapper derives it
# at sync time by comparing CategoryIDs:
#
#   - Same category      → U (Upsell / Similar)
#   - Different category → C (Cross-sell / Accessories / Warranty)
#
# Only U and C are stored. D, W, Z are never written.
# Clients can detect Warranty by JOINing related product's category = 788.
#
# Supported ENUM values: C=Cross-sell, U=Upsell, D=Downsell, W=Warranty, Z=Compat


class ProductAddons(Base):
    """
    Product relationships: Cross-sell, Upsell, Downsell, Warranties, Compatibilities.
    Types: C=Crossell, U=Upsell, D=Downsell, W=Warranties, Z=Compatibilities
    """

    __tablename__ = "product_addons"

    productId = Column(String(30), nullable=False, comment="Base Product ID")
    relatedProductId = Column(String(30), nullable=False, comment="Related Product ID")
    type = Column(String(2), nullable=True, comment="Relationship type: C=Crossell, U=Upsell, D=Downsell, W=Warranties, Z=Compatibilities")
    source = Column(String(30), nullable=True, comment="Source of relationship")
    order = Column(INTEGER(unsigned=True), nullable=True, comment="Display order")
    available = Column(Integer, nullable=False, default=0, comment="Availability flag")
    isactive = Column(Boolean, nullable=False, default=True, comment="Active flag")
    createdBy = Column(String(30), nullable=True, comment="Creator")
    creationDate = Column(INTEGER(unsigned=True), nullable=True, comment="Unix epoch creation")
    modifiedBy = Column(String(30), nullable=True, comment="Modifier")
    modifiedDate = Column(INTEGER(unsigned=True), nullable=True, comment="Unix epoch modified")

    __table_args__ = (
        PrimaryKeyConstraint("productId", "relatedProductId", name="pk_product_addons"),
        Index("custom_index", "source", "type", "order"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<ProductAddons(productId={self.productId}, relatedProductId={self.relatedProductId}, type={self.type})>"
