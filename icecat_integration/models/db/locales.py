"""Locales database model."""

from sqlalchemy import Boolean, Column, Index, Integer, String

from .base import Base


class Locales(Base):
    """
    Language/locale reference data.
    """

    __tablename__ = "locales"

    localeid = Column(Integer, primary_key=True, default=0, comment="Icecat Locale/Language ID")
    isactive = Column(Boolean, nullable=False, default=True, comment="Active status")
    languagecode = Column(String(10), nullable=False, default="", comment="ISO language code")
    name = Column(String(190), nullable=False, default="", comment="Language name")

    __table_args__ = (
        Index("locales_languageCode", "languagecode"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<Locales(localeid={self.localeid}, languagecode={self.languagecode})>"
