"""Attribute names database model - aligned with schema v2.0."""

from sqlalchemy import BigInteger, Column, Index, Integer, String, UniqueConstraint

from .base import Base


class AttributeNames(Base):
    """
    Product specification attribute names per locale.
    Populated by taxonomy sync (update-taxonomy command).
    """

    __tablename__ = "attributenames"

    id = Column(Integer, primary_key=True, autoincrement=True)
    attributeid = Column(BigInteger, nullable=False, default=0, comment="Icecat Attribute ID")
    name = Column(String(110), nullable=False, default="", comment="Attribute name")
    localeid = Column(Integer, nullable=False, default=0, comment="Locale/Language ID")

    __table_args__ = (
        UniqueConstraint("attributeid", "localeid", name="uk_attributenames_attr_locale"),
        Index("attributenames_attributeID", "attributeid"),
        Index("attributenames_localeID", "localeid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<AttributeNames(attributeid={self.attributeid}, name={self.name})>"
