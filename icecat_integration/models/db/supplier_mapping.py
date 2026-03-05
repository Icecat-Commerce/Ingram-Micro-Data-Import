"""Supplier mapping database model - maps distributor brand names to Icecat canonical names."""

from sqlalchemy import Column, Integer, String, Index, UniqueConstraint

from .base import Base


class SupplierMapping(Base):
    """
    Maps distributor brand aliases/symbols to Icecat canonical supplier names.

    Source: supplier_mapping.xml from Icecat reference data.
    Each row maps a distributor's brand name (symbol) to the Icecat supplier ID and canonical name.

    Example: symbol="HEWLETT PACKARD ENTERPRISE" → icecat_name="HPE", supplier_id=13357
    """

    __tablename__ = "supplier_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True)
    supplier_id = Column(Integer, nullable=False, comment="Icecat supplier ID (refs vendor.vendorid)")
    icecat_name = Column(String(255), nullable=False, comment="Canonical Icecat brand name")
    symbol = Column(String(255), nullable=False, comment="Distributor alias as-is")
    symbol_lower = Column(String(255), nullable=False, comment="Lowercased symbol for lookups")
    distributor_id = Column(Integer, nullable=True, comment="Optional distributor context ID")

    __table_args__ = (
        UniqueConstraint("symbol_lower", "distributor_id", name="uk_symbol_distributor"),
        Index("idx_supplier_id", "supplier_id"),
        Index("idx_symbol_lower", "symbol_lower"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return (
            f"<SupplierMapping(symbol={self.symbol!r}, "
            f"icecat_name={self.icecat_name!r}, supplier_id={self.supplier_id})>"
        )
