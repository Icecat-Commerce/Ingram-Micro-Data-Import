"""Comparison service for validating JSON vs XML data parity.

Compares two merged product dicts (one from the JSON multi-language path,
one from the XML lang=INT parser) and reports field-level differences.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FieldDifference:
    """A single difference between JSON and XML output."""

    section: str
    path: str
    json_value: Any
    xml_value: Any

    def __str__(self) -> str:
        return (
            f"[{self.section}] {self.path}: "
            f"json={_truncate(self.json_value)} | xml={_truncate(self.xml_value)}"
        )


@dataclass
class ComparisonResult:
    """Result of comparing JSON and XML merged dicts for one product."""

    brand: str
    mpn: str
    json_ok: bool = True
    xml_ok: bool = True
    differences: list[FieldDifference] = field(default_factory=list)
    json_error: str = ""
    xml_error: str = ""

    @property
    def match(self) -> bool:
        return self.json_ok and self.xml_ok and len(self.differences) == 0

    @property
    def diff_count(self) -> int:
        return len(self.differences)


def _truncate(value: Any, max_len: int = 80) -> str:
    """Truncate a value for display."""
    s = repr(value)
    if len(s) > max_len:
        return s[:max_len - 3] + "..."
    return s


def _sort_key_for_section(section: str) -> Any:
    """Return a sort-key function appropriate for a given section."""
    key_map = {
        "descriptions": lambda d: d.get("localeid", 0),
        "marketing_info": lambda d: d.get("localeid", 0),
        "features": lambda d: (d.get("localeid", 0), d.get("ordernumber", 0)),
        "attributes": lambda d: (d.get("attributeid", 0), d.get("localeid", 0)),
        "search_attributes": lambda d: (d.get("attributeid", 0), d.get("localeid", 0)),
        "media": lambda d: (d.get("original", ""), d.get("imageType", "")),
        "thumbnails": lambda d: (d.get("thumburl", ""), d.get("size", "")),
        "addons": lambda d: d.get("relatedProductId", ""),
    }
    return key_map.get(section, lambda d: str(d))


class ComparisonService:
    """Compare JSON and XML merged product dicts field by field."""

    # Sections that contain lists of dicts keyed by locale or attribute
    LIST_SECTIONS = [
        "descriptions",
        "marketing_info",
        "features",
        "attributes",
        "search_attributes",
        "media",
        "thumbnails",
        "addons",
    ]

    # Scalar dict sections
    DICT_SECTIONS = ["product", "vendor", "category"]

    def compare(
        self,
        json_dict: dict[str, Any],
        xml_dict: dict[str, Any],
    ) -> list[FieldDifference]:
        """Compare two merged product dicts and return differences."""
        diffs: list[FieldDifference] = []

        # Compare scalar sections
        for section in self.DICT_SECTIONS:
            json_val = json_dict.get(section)
            xml_val = xml_dict.get(section)

            if json_val is None and xml_val is None:
                continue

            if json_val is None:
                diffs.append(FieldDifference(section, section, None, xml_val))
                continue
            if xml_val is None:
                diffs.append(FieldDifference(section, section, json_val, None))
                continue

            if isinstance(json_val, dict) and isinstance(xml_val, dict):
                diffs.extend(self._compare_dicts(section, json_val, xml_val))

        # Compare list sections
        for section in self.LIST_SECTIONS:
            json_list = json_dict.get(section, [])
            xml_list = xml_dict.get(section, [])
            diffs.extend(self._compare_lists(section, json_list, xml_list))

        return diffs

    def _compare_dicts(
        self,
        section: str,
        json_dict: dict[str, Any],
        xml_dict: dict[str, Any],
        prefix: str = "",
    ) -> list[FieldDifference]:
        """Compare two flat dicts and return per-key differences."""
        diffs: list[FieldDifference] = []
        all_keys = set(json_dict.keys()) | set(xml_dict.keys())

        for key in sorted(all_keys):
            path = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
            json_val = json_dict.get(key)
            xml_val = xml_dict.get(key)

            if not self._values_equal(json_val, xml_val):
                diffs.append(FieldDifference(section, path, json_val, xml_val))

        return diffs

    def _compare_lists(
        self,
        section: str,
        json_list: list[dict[str, Any]],
        xml_list: list[dict[str, Any]],
    ) -> list[FieldDifference]:
        """Compare two lists of dicts after sorting by natural key."""
        diffs: list[FieldDifference] = []

        sort_fn = _sort_key_for_section(section)

        try:
            json_sorted = sorted(json_list, key=sort_fn)
            xml_sorted = sorted(xml_list, key=sort_fn)
        except (KeyError, TypeError):
            json_sorted = json_list
            xml_sorted = xml_list

        # Count difference
        if len(json_sorted) != len(xml_sorted):
            diffs.append(FieldDifference(
                section,
                f"{section}.length",
                len(json_sorted),
                len(xml_sorted),
            ))

        # Compare element by element up to the shorter list
        for idx in range(min(len(json_sorted), len(xml_sorted))):
            json_item = json_sorted[idx]
            xml_item = xml_sorted[idx]

            item_diffs = self._compare_dicts(
                section, json_item, xml_item, prefix=f"[{idx}].",
            )
            diffs.extend(item_diffs)

        # Report extra items
        if len(json_sorted) > len(xml_sorted):
            for idx in range(len(xml_sorted), len(json_sorted)):
                diffs.append(FieldDifference(
                    section, f"[{idx}] (json only)", json_sorted[idx], None,
                ))
        elif len(xml_sorted) > len(json_sorted):
            for idx in range(len(json_sorted), len(xml_sorted)):
                diffs.append(FieldDifference(
                    section, f"[{idx}] (xml only)", None, xml_sorted[idx],
                ))

        return diffs

    @staticmethod
    def _values_equal(a: Any, b: Any) -> bool:
        """Compare two values with type-tolerant equality."""
        if a == b:
            return True

        # Normalize None vs empty string
        if (a is None and b == "") or (a == "" and b is None):
            return True

        # Normalize string-encoded numbers vs actual numbers (JSON IDs are often strings)
        if isinstance(a, str) and isinstance(b, (int, float)):
            try:
                return float(a) == float(b)
            except (ValueError, TypeError):
                pass
        if isinstance(b, str) and isinstance(a, (int, float)):
            try:
                return float(b) == float(a)
            except (ValueError, TypeError):
                pass

        # Normalize numeric comparisons (int vs float)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return float(a) == float(b)

        # Normalize 0 vs None for optional numeric fields
        if (a == 0 and b is None) or (a is None and b == 0):
            return True

        # Normalize bool vs int
        if isinstance(a, bool) and isinstance(b, int):
            return int(a) == b
        if isinstance(a, int) and isinstance(b, bool):
            return a == int(b)

        return False
