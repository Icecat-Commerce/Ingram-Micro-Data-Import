"""Category display configuration database models.

Per SQL Table Structures document:
- categoryheader: Taxonomy header groups for organizing attributes
- categorydisplayattributes: Attribute display order per category
"""

from sqlalchemy import BigInteger, Boolean, Column, Index, Integer, String, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class CategoryHeader(Base):
    """
    Category feature group headers.
    Defines how attributes are grouped in category display.
    """

    __tablename__ = "categoryheader"

    id = Column(Integer, primary_key=True, autoincrement=True)
    categoryid = Column(
        Integer,
        nullable=False,
        comment="Icecat Category ID",
    )
    headerid = Column(
        Integer,
        nullable=False,
        comment="Feature group/header ID",
    )
    headername = Column(
        String(200),
        nullable=True,
        comment="Header display name",
    )
    localeid = Column(
        Integer,
        nullable=False,
        comment="Locale/Language ID",
    )
    displayorder = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Display order",
    )
    isactive = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="Is active flag",
    )
    created_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        comment="Creation date",
    )
    updated_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        comment="Last update date",
    )

    __table_args__ = (
        Index("idx_cat_header_category", "categoryid"),
        Index("idx_cat_header_locale", "categoryid", "localeid"),
        Index("idx_cat_header_group", "categoryid", "headerid", "localeid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<CategoryHeader(cat={self.categoryid}, header={self.headerid})>"


class CategoryDisplayAttributes(Base):
    """
    Attribute display configuration per category.
    Defines which attributes to show and in what order.
    """

    __tablename__ = "categorydisplayattributes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    categoryid = Column(
        Integer,
        nullable=False,
        comment="Icecat Category ID",
    )
    attributeid = Column(
        BigInteger,
        nullable=False,
        comment="Icecat Attribute/Feature ID",
    )
    headerid = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Feature group/header ID",
    )
    localeid = Column(
        Integer,
        nullable=False,
        comment="Locale/Language ID",
    )
    displayorder = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Display order within header",
    )
    isactive = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="Is active flag",
    )
    issearchable = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="Is searchable attribute",
    )
    created_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        comment="Creation date",
    )
    updated_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        comment="Last update date",
    )

    __table_args__ = (
        Index("idx_cat_disp_category", "categoryid"),
        Index("idx_cat_disp_attr", "categoryid", "attributeid"),
        Index("idx_cat_disp_locale", "categoryid", "localeid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<CategoryDisplayAttributes(cat={self.categoryid}, attr={self.attributeid})>"
