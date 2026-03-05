"""Streaming parser for CategoryFeaturesList.xml.gz using lxml iterparse.

Parses the Icecat taxonomy file (8.4 GB uncompressed) in a memory-efficient way,
yielding one ParsedCategory at a time. Filters to only supported language IDs.
"""

import gzip
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator

from lxml import etree

logger = logging.getLogger(__name__)


@dataclass
class ParsedFeature:
    """A single Feature within a Category, filtered to supported languages."""

    feature_id: int = 0
    category_feature_group_id: int = 0  # maps to headerid via lookup
    order_number: int = 0
    is_searchable: bool = False
    names: dict[int, str] = field(default_factory=dict)  # lang_id -> name


@dataclass
class ParsedCategoryFeatureGroup:
    """A CategoryFeatureGroup (header) within a Category."""

    cfg_id: int = 0  # CategoryFeatureGroup.ID attribute
    feature_group_id: int = 0  # FeatureGroup.ID = headerid in DB
    order_number: int = 0  # No attribute
    names: dict[int, str] = field(default_factory=dict)  # lang_id -> group name


@dataclass
class ParsedCategory:
    """A single Category with all its feature groups and features."""

    category_id: int = 0
    parent_category_id: int = 0  # UCATID attribute from XML
    names: dict[int, str] = field(default_factory=dict)  # lang_id -> name
    feature_groups: list[ParsedCategoryFeatureGroup] = field(default_factory=list)
    features: list[ParsedFeature] = field(default_factory=list)
    # Maps CategoryFeatureGroup.ID -> FeatureGroup.ID for headerid resolution
    cfg_id_to_header_id: dict[int, int] = field(default_factory=dict)


class CategoryFeaturesParser:
    """
    Streaming parser for CategoryFeaturesList.xml.gz.

    Uses lxml.etree.iterparse with start/end events. Maintains a context
    stack to track which Category/FeatureGroup/Feature we are inside,
    so that <Name> elements are attributed to the correct parent.

    Yields one ParsedCategory at a time. After processing each <Category>,
    calls elem.clear() and deletes preceding siblings to keep memory bounded.
    """

    TAGS_OF_INTEREST = frozenset({
        "Category",
        "CategoryFeatureGroup",
        "FeatureGroup",
        "Feature",
        "Name",
    })

    def __init__(
        self,
        file_path: Path,
        supported_lang_ids: set[int],
    ):
        self.file_path = file_path
        self.supported_lang_ids = supported_lang_ids

    def iter_categories(self) -> Generator[ParsedCategory, None, None]:
        """
        Yield one ParsedCategory at a time by streaming the gzipped XML.

        Memory usage: holds at most one category's worth of data plus
        the iterparse buffer. Expected peak: < 100 MB.
        """
        current_category: ParsedCategory | None = None
        current_cfg: ParsedCategoryFeatureGroup | None = None
        current_feature: ParsedFeature | None = None

        # Context stack for Name disambiguation
        context_stack: list[str] = []

        with gzip.open(self.file_path, "rb") as gz_file:
            context = etree.iterparse(
                gz_file,
                events=("start", "end"),
                tag=tuple(self.TAGS_OF_INTEREST),
            )

            for event, elem in context:
                if event == "start":
                    self._handle_start(
                        elem,
                        context_stack,
                        current_category,
                        current_cfg,
                        current_feature,
                    )

                    if elem.tag == "Category":
                        current_category = ParsedCategory(
                            category_id=int(elem.get("ID", "0")),
                            parent_category_id=int(elem.get("UCATID", "0")),
                        )
                        current_cfg = None
                        current_feature = None
                        context_stack.append("Category")

                    elif elem.tag == "CategoryFeatureGroup" and current_category is not None:
                        current_cfg = ParsedCategoryFeatureGroup(
                            cfg_id=int(elem.get("ID", "0")),
                            order_number=int(elem.get("No", "0")),
                        )
                        context_stack.append("CategoryFeatureGroup")

                    elif elem.tag == "FeatureGroup" and current_cfg is not None:
                        current_cfg.feature_group_id = int(elem.get("ID", "0"))
                        context_stack.append("FeatureGroup")

                    elif elem.tag == "Feature" and current_category is not None:
                        current_feature = ParsedFeature(
                            feature_id=int(elem.get("ID", "0")),
                            category_feature_group_id=int(
                                elem.get("CategoryFeatureGroup_ID", "0")
                            ),
                            order_number=int(elem.get("No", "0")),
                            is_searchable=elem.get("Searchable", "0") == "1",
                        )
                        context_stack.append("Feature")

                    elif elem.tag == "Name":
                        context_stack.append("Name")

                elif event == "end":
                    if elem.tag == "Name":
                        self._handle_name_end(
                            elem,
                            context_stack,
                            current_category,
                            current_cfg,
                            current_feature,
                        )
                        if context_stack and context_stack[-1] == "Name":
                            context_stack.pop()
                        elem.clear()

                    elif elem.tag == "FeatureGroup":
                        if context_stack and context_stack[-1] == "FeatureGroup":
                            context_stack.pop()
                        elem.clear()

                    elif elem.tag == "CategoryFeatureGroup":
                        if current_cfg is not None and current_category is not None:
                            current_category.feature_groups.append(current_cfg)
                            current_category.cfg_id_to_header_id[
                                current_cfg.cfg_id
                            ] = current_cfg.feature_group_id
                            current_cfg = None
                        if context_stack and context_stack[-1] == "CategoryFeatureGroup":
                            context_stack.pop()
                        elem.clear()

                    elif elem.tag == "Feature":
                        if current_feature is not None and current_category is not None:
                            current_category.features.append(current_feature)
                            current_feature = None
                        if context_stack and context_stack[-1] == "Feature":
                            context_stack.pop()
                        elem.clear()

                    elif elem.tag == "Category":
                        if current_category is not None:
                            yield current_category
                            current_category = None
                        if context_stack and context_stack[-1] == "Category":
                            context_stack.pop()

                        # Memory management: clear element and remove siblings
                        elem.clear()
                        while elem.getprevious() is not None:
                            parent = elem.getparent()
                            if parent is not None:
                                del parent[0]
                            else:
                                break

    def _handle_start(
        self,
        elem: etree._Element,
        context_stack: list[str],
        current_category: ParsedCategory | None,
        current_cfg: ParsedCategoryFeatureGroup | None,
        current_feature: ParsedFeature | None,
    ) -> None:
        """Handle start events - context setup is done in the caller."""
        pass

    def _handle_name_end(
        self,
        elem: etree._Element,
        context_stack: list[str],
        current_category: ParsedCategory | None,
        current_cfg: ParsedCategoryFeatureGroup | None,
        current_feature: ParsedFeature | None,
    ) -> None:
        """Process a </Name> end event - store the value in the correct parent."""
        lang_id_str = elem.get("langid")
        if lang_id_str is None:
            return

        lang_id = int(lang_id_str)
        if lang_id not in self.supported_lang_ids:
            return

        value = elem.get("Value", "")
        if not value:
            return

        # Determine parent context by looking back through the stack
        parent = self._get_parent_context(context_stack)

        if parent == "Feature" and current_feature is not None:
            current_feature.names[lang_id] = value
        elif parent == "FeatureGroup" and current_cfg is not None:
            current_cfg.names[lang_id] = value
        elif parent == "Category" and current_category is not None:
            current_category.names[lang_id] = value

    @staticmethod
    def _get_parent_context(stack: list[str]) -> str | None:
        """
        Get the parent context for a Name element.

        The stack looks like:
          [..., "Category", "Name"] -> parent is Category
          [..., "FeatureGroup", "Name"] -> parent is FeatureGroup
          [..., "Feature", "Name"] -> parent is Feature
        """
        for i in range(len(stack) - 1, -1, -1):
            if stack[i] != "Name":
                return stack[i]
        return None
