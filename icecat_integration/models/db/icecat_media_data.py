"""Media data database model."""

from sqlalchemy import Boolean, Column, Index, Integer, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.sql import func

from .base import Base


class MediaData(Base):
    """
    Product media assets (images and PDFs).
    """

    __tablename__ = "media_data"

    # Auto-increment ID for ORM compatibility (not in original schema but needed)
    id = Column(Integer, primary_key=True, autoincrement=True)

    product_id = Column(Integer, nullable=False, comment="Icecat Product ID")
    original = Column(String(500), nullable=False, comment="Original image URL")
    original_media_type = Column(String(250), nullable=False, comment="Media type description")
    imageType = Column(String(70), nullable=False, default="", comment="Image type (Image/Rich Media)")
    localeid = Column(Integer, nullable=False, comment="Locale/Language ID")
    image500 = Column(String(500), nullable=True, comment="500px thumbnail URL")
    high = Column(String(500), nullable=False, comment="High resolution URL")
    medium = Column(String(500), nullable=False, comment="Medium resolution URL")
    low = Column(String(500), nullable=False, comment="Low resolution URL")
    deleted = Column(Boolean, nullable=False, default=False, comment="Soft delete flag")
    image_max_size = Column(String(25), nullable=False, default="", comment="Maximum image size")
    created_at = Column(
        TIMESTAMP, nullable=True, server_default=func.current_timestamp(), comment="Creation date"
    )
    updated_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        comment="Last updated date",
    )

    __table_args__ = (
        UniqueConstraint(
            "product_id", "original", "original_media_type", "localeid",
            name="image_unique"
        ),
        Index("productId", "product_id"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<MediaData(product_id={self.product_id}, imageType={self.imageType})>"
