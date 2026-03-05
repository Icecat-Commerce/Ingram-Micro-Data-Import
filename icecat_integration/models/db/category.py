"""Category database models - aligned with schema v2.0."""

from sqlalchemy import Boolean, Column, Index, Integer, SmallInteger, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.sql import func

from .base import Base


class Category(Base):
    """
    Product category names per locale.
    Multiple rows per categoryid (one per language).
    """

    __tablename__ = "category"

    id = Column(Integer, primary_key=True, autoincrement=True)
    categoryid = Column(Integer, nullable=False, default=0, comment="Icecat Category ID")
    categoryname = Column(String(80), nullable=False, default="", comment="Category name")
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
        UniqueConstraint("categoryid", "localeid", name="uk_category_locale"),
        Index("cat_categoryID", "categoryid"),
        Index("cat_localeID", "localeid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<Category(categoryid={self.categoryid}, categoryname={self.categoryname})>"


class CategoryMapping(Base):
    """
    Category hierarchy mapping — one row per category.
    Unique on categoryid, referenced by product.categoryid FK.
    """

    __tablename__ = "categoryMapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    categoryid = Column(Integer, nullable=False, default=0, comment="Icecat Category ID")
    parentcategoryid = Column(Integer, nullable=True, comment="Parent Category ID")
    isactive = Column(Boolean, nullable=False, default=True, comment="Active status")
    ordernumber = Column(Integer, nullable=False, default=0, comment="Display order")
    catlevel = Column(SmallInteger, nullable=False, default=0, comment="Category level depth")
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
        UniqueConstraint("categoryid", name="uk_categoryMapping_catid"),
        Index("catery_parentcategoryid", "parentcategoryid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<CategoryMapping(categoryid={self.categoryid}, parent={self.parentcategoryid})>"
