"""Vendor database model."""

from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class Vendor(Base):
    """
    Manufacturer/vendor master data.
    """

    __tablename__ = "vendor"

    vendorid = Column(Integer, primary_key=True, default=0, comment="Icecat Vendor ID")
    name = Column(String(60), nullable=False, default="", comment="Vendor/Brand name")
    logourl = Column(String(100), nullable=True, comment="Vendor logo URL")
    logoheight = Column(Integer, nullable=False, default=0, comment="Logo height in pixels")
    logowidth = Column(Integer, nullable=False, default=0, comment="Logo width in pixels")
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

    __table_args__ = ({"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},)

    def __repr__(self) -> str:
        return f"<Vendor(vendorid={self.vendorid}, name={self.name})>"
