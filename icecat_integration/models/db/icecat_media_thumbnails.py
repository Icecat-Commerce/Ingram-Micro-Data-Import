"""Media thumbnails database model - stores all image size variants.

All sizes for a single image are stored as separate rows in this table.
"""

from sqlalchemy import Boolean, Column, Index, Integer, String, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class IcecatMediaThumbnails(Base):
    """
    Image thumbnail variants for different sizes.
    Each row represents one size variant of an image.

    Fields: productId, localeID, thumbUrl, size, contentType, isactive, setnumber
    """

    __tablename__ = "icecat_media_thumbnails"

    id = Column(Integer, primary_key=True, autoincrement=True)
    productid = Column(Integer, nullable=False, default=0, comment="Icecat Product ID")
    localeid = Column(Integer, nullable=False, default=0, comment="Locale/Language ID")
    thumburl = Column(String(500), nullable=False, default="", comment="Thumbnail URL")
    size = Column(String(50), nullable=False, default="", comment="Size name (original/high/medium/low/thumb)")
    contenttype = Column(String(100), nullable=True, default="image/jpeg", comment="MIME content type")
    isactive = Column(Boolean, nullable=False, default=True, comment="Active status")
    setnumber = Column(Integer, nullable=False, default=1, comment="Set number")
    created_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        comment="Creation date",
    )

    __table_args__ = (
        Index("idx_thumb_productid", "productid"),
        Index("idx_thumb_productid_size", "productid", "size"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<IcecatMediaThumbnails(productid={self.productid}, size={self.size})>"
