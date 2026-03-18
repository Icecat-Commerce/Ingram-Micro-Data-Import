"""XML product parser for Icecat xml_server3.cgi lang=INT responses.

Parses a single XML response containing all locales into the same merged
dict format produced by MultiLanguageProductMapper.get_merged_data().
This allows ProductSyncService._sync_product_with_logging() to consume
the output directly without any per-language loop.
"""

import logging
from typing import Any

from lxml import etree

from ..mappers.icecat_language_mapper import IcecatLanguageMapper
from ..utils.validators import sanitize_html, sanitize_string

logger = logging.getLogger(__name__)

SUPPORTED_LANG_IDS: frozenset[int] = frozenset(IcecatLanguageMapper.SUPPORTED_LANGUAGE_IDS)


class XmlProductParser:
    """Parse Icecat XML product response (lang=INT) into a merged dict."""

    def parse(self, root: etree._Element) -> dict[str, Any] | None:
        """Parse the XML root into a merged multi-language dict.

        Returns a dict with the same keys and value structure as
        MultiLanguageProductMapper.get_merged_data(), or None on failure.
        """
        product = root.find(".//Product")
        if product is None:
            return None

        product_id = self._int_attr(product, "ID")
        if not product_id:
            return None

        vendor = self._parse_vendor(product)
        category = self._parse_category(product)
        attributes, search_attributes = self._parse_attributes(product, product_id)

        return {
            "product": {
                "productid": product_id,
                "mfgpartno": sanitize_string(product.get("Prod_id", ""), 70),
                "vendorid": vendor["vendorid"] if vendor else 0,
                "categoryid": category["categoryid"] if category else 0,
                "isaccessory": False,
            },
            "vendor": vendor,
            "category": category,
            "descriptions": self._parse_descriptions(product),
            "marketing_info": self._parse_marketing_info(product),
            "features": self._parse_bullet_points(product),
            "media": self._parse_media(product),
            "thumbnails": self._parse_thumbnails(product),
            "attributes": attributes,
            "search_attributes": search_attributes,
            "addons": self._parse_addons(product, product_id, category),
        }

    # ------------------------------------------------------------------
    # Vendor
    # ------------------------------------------------------------------

    def _parse_vendor(self, product: etree._Element) -> dict[str, Any] | None:
        """Parse <Supplier ID="..." Name="...">."""
        supplier = product.find("Supplier")
        if supplier is None:
            return None
        vendor_id = self._int_attr(supplier, "ID")
        if not vendor_id:
            return None
        return {
            "vendorid": vendor_id,
            "name": sanitize_string(supplier.get("Name", ""), 190),
            "logourl": None,
        }

    # ------------------------------------------------------------------
    # Category
    # ------------------------------------------------------------------

    def _parse_category(self, product: etree._Element) -> dict[str, Any] | None:
        """Parse <Category ID="..."> with English name fallback."""
        cat = product.find("Category")
        if cat is None:
            return None
        cat_id = self._int_attr(cat, "ID")
        if not cat_id:
            return None

        cat_name = None
        for name_elem in cat.findall("Name"):
            if name_elem.get("langid") == "1":
                cat_name = name_elem.get("Value", "")
                break
        if cat_name is None:
            first = cat.find("Name")
            if first is not None:
                cat_name = first.get("Value", "")

        return {
            "categoryid": cat_id,
            "categoryname": sanitize_string(cat_name, 190),
        }

    # ------------------------------------------------------------------
    # Descriptions  (SummaryDescriptionLocal per language)
    # ------------------------------------------------------------------

    def _parse_descriptions(self, product: etree._Element) -> list[dict[str, Any]]:
        """Parse localized descriptions from SummaryDescriptionLocal.

        Only emits a description for languages that actually have localized
        data. No fallback to English/global — missing means no data.
        """
        descriptions: list[dict[str, Any]] = []

        # Collect per-language long summaries from SummaryDescriptionLocal
        long_by_lang: dict[int, str] = {}
        sdl = product.find("SummaryDescriptionLocal")
        if sdl is not None:
            for elem in sdl.findall("LongSummaryDescriptionLocal"):
                lang_id = self._int_attr(elem, "langid")
                if lang_id and lang_id in SUPPORTED_LANG_IDS and elem.text:
                    long_by_lang[lang_id] = elem.text

        # Authored ProductDescription per language
        authored_long: dict[int, str] = {}
        for pd in product.findall("ProductDescription"):
            lang_id = self._int_attr(pd, "langid")
            long_desc = pd.get("LongDesc", "").strip()
            if lang_id and lang_id in SUPPORTED_LANG_IDS and long_desc:
                authored_long[lang_id] = long_desc

        for lang_id in SUPPORTED_LANG_IDS:
            text = long_by_lang.get(lang_id) or authored_long.get(lang_id)
            text = sanitize_html(text) if text else None
            if text:
                descriptions.append({
                    "localeid": lang_id,
                    "description": text,
                    "isdefault": False,
                    "isactive": True,
                })

        return descriptions

    # ------------------------------------------------------------------
    # Marketing info  (ProductDescription LongDesc per language)
    # ------------------------------------------------------------------

    def _parse_marketing_info(self, product: etree._Element) -> list[dict[str, Any]]:
        """Parse marketing HTML from authored ProductDescription.LongDesc.

        Each ProductDescription element has a langid attribute. We emit one
        marketing entry per authored description, matching JSON path behavior
        where only the language calls that return a LongDesc produce entries.
        """
        marketing: list[dict[str, Any]] = []

        for pd in product.findall("ProductDescription"):
            lang_id = self._int_attr(pd, "langid")
            if not lang_id or lang_id not in SUPPORTED_LANG_IDS:
                continue

            long_desc = pd.get("LongDesc", "").strip()
            if not long_desc:
                continue

            text = sanitize_html(long_desc)
            if text:
                marketing.append({
                    "localeid": lang_id,
                    "marketing": text,
                    "isactive": True,
                })

        return marketing

    # ------------------------------------------------------------------
    # Bullet points
    # ------------------------------------------------------------------

    def _parse_bullet_points(self, product: etree._Element) -> list[dict[str, Any]]:
        """Parse authored BulletPoints per language.

        Authored bullets are typically only langid=1. For other languages
        we do NOT fall back to GeneratedBulletPoints — that mirrors the
        JSON path which only stores authored bullets.
        """
        features: list[dict[str, Any]] = []
        by_lang: dict[int, list[str]] = {}

        for bp in product.findall(".//BulletPoints/BulletPoint"):
            lang_id = self._int_attr(bp, "langid")
            value = bp.get("Value", "").strip()
            if lang_id and lang_id in SUPPORTED_LANG_IDS and value:
                by_lang.setdefault(lang_id, []).append(value)

        for lang_id, values in by_lang.items():
            for idx, text in enumerate(values):
                features.append({
                    "localeid": lang_id,
                    "ordernumber": idx + 1,
                    "text": sanitize_string(text, 1000),
                    "isactive": True,
                })

        return features

    # ------------------------------------------------------------------
    # Attributes  (ProductFeature with PresentationValues per language)
    # ------------------------------------------------------------------

    def _parse_attributes(
        self, product: etree._Element, product_id: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Parse ProductFeature elements into attributes and search_attributes.

        Each ProductFeature contains PresentationValues/PresentationValue
        elements with per-language display values.
        """
        attributes: list[dict[str, Any]] = []
        search_attributes: list[dict[str, Any]] = []

        for pf in product.findall("ProductFeature"):
            feature_elem = pf.find("Feature")
            if feature_elem is None:
                continue

            attr_id = self._int_attr(feature_elem, "ID")
            if not attr_id:
                continue

            is_searchable = pf.get("Searchable") == "1"
            value_id = self._int_attr(pf, "Value_ID") or 0

            # Measure unit ID from Feature/Measure
            measure = feature_elem.find("Measure")
            unit_id = self._int_attr(measure, "ID") if measure is not None else 0

            # Raw numeric value
            raw_value = pf.get("Value", "")
            absolute_value = 0.0
            try:
                absolute_value = float(raw_value) if raw_value else 0.0
            except (ValueError, TypeError):
                absolute_value = 0.0

            # Per-language presentation values
            pres_by_lang: dict[int, str] = {}
            pvs = pf.find("PresentationValues")
            if pvs is not None:
                for pv in pvs.findall("PresentationValue"):
                    lang_id = self._int_attr(pv, "langid")
                    if lang_id and lang_id in SUPPORTED_LANG_IDS:
                        pres_by_lang[lang_id] = pv.get("Value", "")

            # Per-language local values (inside LocalValues container)
            local_by_lang: dict[int, str] = {}
            lvs = pf.find("LocalValues")
            if lvs is not None:
                for lv in lvs.findall("LocalValue"):
                    lang_id = self._int_attr(lv, "langid")
                    if lang_id and lang_id in SUPPORTED_LANG_IDS:
                        local_by_lang[lang_id] = lv.get("Value", "")

            # Only emit for languages that have actual localized data.
            # No fallback to international Presentation_Value.
            for lang_id in SUPPORTED_LANG_IDS:
                display = (
                    pres_by_lang.get(lang_id)
                    or local_by_lang.get(lang_id)
                )
                if not display:
                    continue

                attr_dict: dict[str, Any] = {
                    "attributeid": attr_id,
                    "localeid": lang_id,
                    "displayvalue": sanitize_string(display, 1000) if display else None,
                    "absolutevalue": absolute_value if absolute_value != 0 else 0,
                    "unitid": unit_id,
                    "isabsolute": absolute_value != 0,
                    "setnumber": 1,
                    "isactive": True,
                }

                if is_searchable:
                    attr_dict["valueid"] = value_id
                    search_attributes.append(attr_dict)
                else:
                    attributes.append(attr_dict)

        return attributes, search_attributes

    # ------------------------------------------------------------------
    # Media  (ProductGallery + ProductMultimediaObject)
    # ------------------------------------------------------------------

    def _parse_media(self, product: etree._Element) -> list[dict[str, Any]]:
        """Parse gallery images and multimedia into media_data records."""
        media: list[dict[str, Any]] = []
        seen: set[tuple[str, str, int]] = set()

        for pic in product.findall(".//ProductGallery/ProductPicture"):
            pic_url = pic.get("Pic", "")
            if not pic_url:
                continue

            key = (pic_url, "", 0)
            if key in seen:
                continue
            seen.add(key)

            media.append({
                "original": pic_url,
                "imageType": "Image",
                "localeid": 0,
                "original_media_type": pic.get("Type", ""),
                "deleted": False,
                "image_max_size": pic.get("Size", ""),
                "image500": pic.get("Pic500x500", ""),
                "high": pic_url,
                "medium": pic.get("Pic500x500", ""),
                "low": pic.get("LowPic", ""),
            })

        for mm in product.findall(".//ProductMultimediaObject/MultimediaObject"):
            mm_url = mm.get("URL", "")
            if not mm_url:
                continue
            content_type = mm.get("ContentType", "")

            key = (mm_url, content_type, 0)
            if key in seen:
                continue
            seen.add(key)

            media.append({
                "original": mm_url,
                "imageType": "Rich Media",
                "localeid": 0,
                "original_media_type": content_type,
                "deleted": False,
                "image_max_size": "",
                "image500": "",
                "high": mm_url,
                "medium": "",
                "low": "",
            })

        return media

    # ------------------------------------------------------------------
    # Thumbnails
    # ------------------------------------------------------------------

    def _parse_thumbnails(self, product: etree._Element) -> list[dict[str, Any]]:
        """Parse gallery images into thumbnail records at multiple sizes."""
        thumbnails: list[dict[str, Any]] = []

        for pic in product.findall(".//ProductGallery/ProductPicture"):
            pic_url = pic.get("Pic", "")
            pic_500 = pic.get("Pic500x500", "")
            low_pic = pic.get("LowPic", "")

            sizes: dict[str, str] = {}
            if pic_url:
                sizes["original"] = pic_url
                sizes["high"] = pic_url
            if pic_500:
                sizes["medium"] = pic_500
                sizes["500x500"] = pic_500
            if low_pic:
                sizes["low"] = low_pic

            for size_name, url in sizes.items():
                if url:
                    thumbnails.append({
                        "localeid": 0,
                        "thumburl": url,
                        "size": size_name,
                        "contenttype": "image/jpeg",
                        "isactive": True,
                        "setnumber": 1,
                    })

        return thumbnails

    # ------------------------------------------------------------------
    # Addons (related products)
    # ------------------------------------------------------------------

    def _parse_addons(
        self,
        product: etree._Element,
        base_product_id: int,
        category: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        """Parse ProductRelated elements into addon records.

        The XML structure is:
          <ProductRelated ID="..." Category_ID="381" Preferred="0">
            <Product ID="38333310" Prod_id="DR-2400" .../>
          </ProductRelated>

        Category_ID is on the wrapper <ProductRelated>, not the inner <Product>.
        """
        addons: list[dict[str, Any]] = []
        base_cat_id = category["categoryid"] if category else None

        for idx, pr in enumerate(product.findall(".//ProductRelated")):
            inner = pr.find("Product")
            if inner is None:
                continue
            related_id = inner.get("ID", "")
            if not related_id:
                continue

            related_cat = self._int_attr(pr, "Category_ID")
            addon_type = None
            if related_cat is not None and base_cat_id is not None:
                addon_type = "U" if related_cat == base_cat_id else "C"

            addons.append({
                "relatedProductId": str(related_id),
                "type": addon_type,
                "source": "Icecat",
                "order": idx + 1,
                "available": 1,
                "isactive": True,
            })

        return addons

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _int_attr(elem: etree._Element | None, attr: str) -> int | None:
        """Safely extract an integer attribute from an XML element."""
        if elem is None:
            return None
        val = elem.get(attr)
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None
