"""Icecat supplier/brand mapper - parses SuppliersList.xml and supplier_mapping.xml."""

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


@dataclass
class IcecatSupplier:
    """A supplier/brand from Icecat's SuppliersList.xml."""

    supplier_id: int
    name: str
    logo_pic: str = ""
    logo_low_pic: str = ""
    logo_medium_pic: str = ""
    logo_high_pic: str = ""
    logo_original: str = ""
    is_sponsor: bool = False
    localized_names: dict[str, str] = field(default_factory=dict)


class IcecatSupplierMapper:
    """
    Parse Icecat reference XML files for supplier/brand data.

    Two XML files:
    - SuppliersList.xml: canonical supplier data (ID, name, logos)
    - supplier_mapping.xml: distributor brand aliases → Icecat canonical names
    """

    def __init__(self):
        self._suppliers_by_id: dict[int, IcecatSupplier] = {}
        self._suppliers_by_name: dict[str, IcecatSupplier] = {}

    @property
    def supplier_count(self) -> int:
        return len(self._suppliers_by_id)

    # ── SuppliersList.xml parsing ──

    def load_from_xml(self, xml_path: str | Path) -> int:
        """
        Parse SuppliersList.xml into IcecatSupplier objects.

        Returns:
            Number of suppliers loaded
        """
        xml_path = Path(xml_path)
        if not xml_path.exists():
            raise FileNotFoundError(f"SuppliersList.xml not found: {xml_path}")

        logger.info(f"Loading suppliers from {xml_path}")
        count = 0
        for event, elem in ET.iterparse(str(xml_path), events=("end",)):
            if elem.tag == "Supplier":
                supplier = self._parse_supplier_element(elem)
                if supplier:
                    self._suppliers_by_id[supplier.supplier_id] = supplier
                    self._suppliers_by_name[supplier.name.lower()] = supplier
                    count += 1
                elem.clear()

        logger.info(f"Loaded {count:,} suppliers from SuppliersList.xml")
        return count

    def _parse_supplier_element(self, elem: ET.Element) -> IcecatSupplier | None:
        """Parse a single <Supplier> XML element."""
        supplier_id = elem.get("ID")
        name = elem.get("Name")
        if not supplier_id or not name:
            return None

        supplier = IcecatSupplier(
            supplier_id=int(supplier_id),
            name=name,
            logo_pic=elem.get("LogoPic", ""),
            logo_low_pic=elem.get("LogoLowPic", ""),
            logo_medium_pic=elem.get("LogoMediumPic", ""),
            logo_high_pic=elem.get("LogoHighPic", ""),
            logo_original=elem.get("LogoOriginal", ""),
            is_sponsor=elem.get("Sponsor", "0") == "1",
        )

        names_elem = elem.find("Names")
        if names_elem is not None:
            for name_elem in names_elem.findall("Name"):
                lang_id = name_elem.get("langid")
                loc_name = name_elem.get("Name")
                if lang_id and loc_name:
                    supplier.localized_names[lang_id] = loc_name

        return supplier

    def get_supplier_by_name(self, name: str) -> IcecatSupplier | None:
        return self._suppliers_by_name.get(name.lower())

    def get_supplier_by_id(self, supplier_id: int) -> IcecatSupplier | None:
        return self._suppliers_by_id.get(supplier_id)

    def get_logo_url(self, brand: str, size: str = "high") -> str:
        """Get logo URL for a brand by name."""
        supplier = self.get_supplier_by_name(brand)
        if not supplier:
            return ""

        size_map = {
            "thumb": supplier.logo_pic,
            "low": supplier.logo_low_pic,
            "medium": supplier.logo_medium_pic,
            "high": supplier.logo_high_pic,
            "original": supplier.logo_original,
        }
        return size_map.get(size, supplier.logo_high_pic)

    def iter_suppliers_for_vendor_table(self) -> Iterator[dict]:
        """
        Yield dicts ready for bulk upsert into the vendor table.

        Uses LogoHighPic as the primary logo URL.
        """
        for s in self._suppliers_by_id.values():
            yield {
                "vendorid": s.supplier_id,
                "name": s.name,
                "logourl": s.logo_high_pic or s.logo_low_pic or s.logo_pic or "",
            }

    # ── supplier_mapping.xml parsing ──

    @staticmethod
    def parse_supplier_mapping_xml(xml_path: str | Path) -> Iterator[dict]:
        """
        Parse supplier_mapping.xml and yield mapping records.

        Each <SupplierMapping supplier_id="13357" name="HPE"> contains
        <Symbol> children with distributor brand aliases.

        Yields:
            dict with keys: supplier_id, icecat_name, symbol, symbol_lower, distributor_id
        """
        xml_path = Path(xml_path)
        if not xml_path.exists():
            raise FileNotFoundError(f"supplier_mapping.xml not found: {xml_path}")

        logger.info(f"Parsing supplier mapping from {xml_path}")

        for event, elem in ET.iterparse(str(xml_path), events=("end",)):
            if elem.tag == "SupplierMapping":
                supplier_id_str = elem.get("supplier_id")
                icecat_name = elem.get("name")
                if not supplier_id_str or not icecat_name:
                    elem.clear()
                    continue

                supplier_id = int(supplier_id_str)

                for symbol_elem in elem.findall("Symbol"):
                    symbol_text = symbol_elem.text
                    if not symbol_text or not symbol_text.strip():
                        continue

                    symbol = symbol_text.strip()
                    dist_id_str = symbol_elem.get("distributor_id")
                    distributor_id = int(dist_id_str) if dist_id_str else None

                    yield {
                        "supplier_id": supplier_id,
                        "icecat_name": icecat_name,
                        "symbol": symbol,
                        "symbol_lower": symbol.lower(),
                        "distributor_id": distributor_id,
                    }

                elem.clear()
