"""Product data mapper for transforming Icecat FrontOffice API responses to database models."""

from typing import Any

from ..models.json.product_response import (
    ProductResponse,
    ProductData,
    GeneralInfo,
    FeaturesGroup,
    Feature,
    Gallery,
    Multimedia,
    BulletPointsInfo,
    DescriptionInfo,
    SummaryDescriptionInfo,
    TitleInfo,
    BrandInfo,
    CategoryInfo,
)
from ..utils.validators import sanitize_string, sanitize_html
from .icecat_language_mapper import IcecatLanguageMapper


class ProductMapper:
    """
    Maps Icecat FrontOffice API JSON responses to database model dictionaries.

    Transforms:
    - GeneralInfo → product table
    - Description + SummaryDescription → productdescriptions table
    - Description.LongDesc → productmarketinginfo table
    - BulletPoints.Values → productfeatures table
    - Gallery + Multimedia → icecat_media_data table
    - FeaturesGroups.Features → productattribute table
    """

    def __init__(self, default_language_id: int = 1):
        """
        Initialize mapper.

        Args:
            default_language_id: Default Icecat language ID for data without language info
        """
        self.default_language_id = default_language_id

    def map_product_response(
        self,
        response: ProductResponse | dict[str, Any],
        language_id: int | None = None,
    ) -> dict[str, Any] | None:
        """
        Map a full product response to all related database records.

        Args:
            response: ProductResponse or raw dict from Icecat API
            language_id: Language ID for this response

        Returns:
            Dictionary with all mapped data:
            {
                "product": {...},
                "descriptions": [...],
                "marketing_info": [...],
                "features": [...],
                "media": [...],
                "attributes": [...],
                "vendor": {...},
                "category": {...},
            }
        """
        if isinstance(response, dict):
            # Parse from raw dict
            data = response.get("data") or response
            if isinstance(data, dict):
                product_data = ProductData.model_validate(data)
            else:
                return None
        elif isinstance(response, ProductResponse):
            product_data = response.data
        else:
            return None

        if not product_data or not product_data.general_info:
            return None

        general_info = product_data.general_info
        lang_id = language_id or self.default_language_id

        # Get base product ID and category for addons mapping
        base_product_id = general_info.icecat_id
        base_category_id = general_info.category_id
        if general_info.category:
            base_category_id = general_info.category.category_id or base_category_id

        attributes, search_attributes = self.map_attributes(
            product_data.features_groups, lang_id, base_product_id
        )

        result = {
            "product": self.map_product(general_info),
            "descriptions": self.map_descriptions(general_info, lang_id),
            "marketing_info": self.map_marketing_info(general_info, lang_id),
            "features": self.map_features_bulletpoints(general_info, lang_id),
            "media": self.map_media(product_data.gallery, product_data.multimedia),
            "thumbnails": self.map_thumbnails(product_data.gallery),
            "attributes": attributes,
            "search_attributes": search_attributes,
            "vendor": self.map_vendor(general_info),
            "category": self.map_category(general_info),
            "addons": self.map_addons(product_data.product_related, base_product_id, base_category_id),
        }

        return result

    def map_product(self, general_info: GeneralInfo) -> dict[str, Any]:
        """
        Map GeneralInfo to product table record.

        Product table has productid, vendorid, mfgpartno, categoryid,
        isaccessory, creationdate, modifieddate.

        Args:
            general_info: GeneralInfo from Icecat response

        Returns:
            Dictionary for product table
        """
        product_id = general_info.icecat_id
        product_code = general_info.product_code
        brand_id = general_info.brand_id

        # Get category ID from nested CategoryInfo or fallback
        category_id = general_info.category_id
        if general_info.category:
            category_id = general_info.category.category_id or category_id

        return {
            "productid": product_id,
            "mfgpartno": sanitize_string(product_code, 70),
            "vendorid": brand_id if brand_id else 0,
            "categoryid": category_id if category_id else 0,
            "isaccessory": False,
        }

    def map_descriptions(
        self, general_info: GeneralInfo, language_id: int
    ) -> list[dict[str, Any]]:
        """
        Map descriptions to productdescriptions table records.

        Fields: productId, description, isdefault, localeID, isactive, creationdate, modifieddate

        Args:
            general_info: GeneralInfo from Icecat response
            language_id: Icecat language ID (numeric INT)

        Returns:
            List of dictionaries for productdescriptions table
        """
        # Extract long description from SummaryDescription or Description fallback
        long_desc = None

        if general_info.summary_description:
            sd = general_info.summary_description
            long_desc = sanitize_html(sd.long_summary_description) if sd.long_summary_description else None

        # Also check the Description object for additional content
        if not long_desc and general_info.description:
            desc = general_info.description
            if desc.long_desc:
                long_desc = sanitize_html(desc.long_desc)

        # Only return if we have a description
        if long_desc:
            return [{
                "localeid": language_id,  # Icecat language ID
                "description": long_desc,
                "isdefault": False,  # Default flag: always false for multi-language products
                "isactive": True,    # Active by default
                # creationdate and modifieddate handled by repository
            }]

        return []

    def map_marketing_info(
        self, general_info: GeneralInfo, language_id: int
    ) -> list[dict[str, Any]]:
        """
        Map marketing info to productmarketinginfo table records.

        Fields: productId, marketing, localeID, isactive, creationdate, modifieddate

        Args:
            general_info: GeneralInfo from Icecat response
            language_id: Icecat language ID (numeric INT)

        Returns:
            List of dictionaries for productmarketinginfo table
        """
        marketing = None

        # Marketing text from product description
        # Extracted from Description.LongDesc
        if general_info.description:
            desc = general_info.description
            if desc.long_desc:
                marketing = sanitize_html(desc.long_desc)

        if marketing:
            return [{
                "localeid": language_id,  # Icecat language ID
                "marketing": marketing,
                "isactive": True,         # Active by default
                # creationdate and modifieddate handled by repository
            }]

        return []

    def map_features_bulletpoints(
        self, general_info: GeneralInfo, language_id: int
    ) -> list[dict[str, Any]]:
        """
        Map bullet points to productfeatures table records.

        Fields: productId, productfeatureID, localeID, ordernumber, text, modifieddate, isactive

        Args:
            general_info: GeneralInfo from Icecat response
            language_id: Icecat language ID (numeric INT)

        Returns:
            List of dictionaries for productfeatures table
        """
        features = []

        # Extract from BulletPoints only (not generated/auto-numbered bullets)
        if general_info.bullet_points and general_info.bullet_points.values:
            bp = general_info.bullet_points
            for idx, bullet_point in enumerate(bp.values):
                if bullet_point:
                    features.append({
                        "localeid": language_id,  # Icecat language ID
                        "ordernumber": idx + 1,   # Sequential display order
                        "text": sanitize_string(bullet_point, 1000),  # Bullet point text content
                        "isactive": True,         # Active by default
                        # productfeatureid will be generated by repository
                        # modifieddate handled by repository
                    })

        # Only authored bullet points, not auto-generated

        return features

    def map_media(
        self,
        gallery: list[Gallery] | None,
        multimedia: list[Multimedia] | None,
    ) -> list[dict[str, Any]]:
        """
        Map gallery images and multimedia to icecat_media_data table records.

        Fields: productId, externalImage, imageType, localeID, mediaType,
                creationdate, modifieddate, deleted, image_max_size

        Args:
            gallery: Gallery images from Icecat response
            multimedia: Multimedia items from Icecat response

        Returns:
            List of dictionaries for media_data table
        """
        media_items = []

        # Map gallery images (imageType="Image")
        for img in gallery or []:
            # Images may not have language info — default localeID to 0 when unavailable
            lang_id = 0  # Default to 0 if no language info
            if hasattr(img, 'lang_id') and img.lang_id is not None:
                lang_id = img.lang_id

            # Get the main/high-res image URL (required)
            pic_url = img.pic or ""

            # Get thumbnail URLs from Gallery model
            # Include all image size URLs (image500, high, medium, low)
            pic_500 = img.pic_500x500 or "" if hasattr(img, 'pic_500x500') else ""
            low_pic = img.low_pic or "" if hasattr(img, 'low_pic') else ""

            media_items.append({
                "original": pic_url,                   # Original image URL
                "imageType": "Image",                  # Type identifier for image media
                "localeid": lang_id,                   # Icecat lang_id or 0
                "original_media_type": img.type if hasattr(img, 'type') else "",  # Media content type
                "deleted": False,                      # Not deleted
                "image_max_size": str(img.size) if img.size else "",  # Maximum image file size from Icecat
                # Thumbnail URLs at multiple resolutions
                "image500": pic_500,                   # 500px thumbnail URL
                "high": pic_url,                       # High res = original pic
                "medium": pic_500,                     # Medium = 500px (Icecat doesn't have separate medium)
                "low": low_pic,                        # Low resolution URL
                # creationdate and modifieddate handled by repository
            })

        # Map multimedia/document attachments (imageType="Rich Media")
        for mm in multimedia or []:
            mm_url = mm.url or ""
            media_items.append({
                "original": mm_url,                    # Multimedia URL
                "imageType": "Rich Media",             # Type identifier for document/multimedia
                "localeid": 0,                         # Multimedia typically language-independent
                "original_media_type": mm.type if hasattr(mm, 'type') else "",  # Media content type
                "deleted": False,                      # Not deleted
                "image_max_size": "",                  # PDFs don't have image size
                # Rich Media doesn't have thumbnail variants - use same URL
                "image500": "",
                "high": mm_url,                        # Use original URL for high
                "medium": "",
                "low": "",
                # creationdate and modifieddate handled by repository
            })

        # Deduplicate based on unique constraint (original, original_media_type, localeid)
        # Icecat API sometimes returns duplicate images
        seen = set()
        deduped_items = []
        for item in media_items:
            key = (item["original"], item["original_media_type"], item["localeid"])
            if key not in seen:
                seen.add(key)
                deduped_items.append(item)

        return deduped_items

    def map_thumbnails(
        self,
        gallery: list[Gallery] | None,
    ) -> list[dict[str, Any]]:
        """
        Map gallery images to icecat_media_thumbnails table records.

        All image sizes (original, high, medium, low, thumb)
        are stored as separate rows in icecat_media_thumbnails.

        Args:
            gallery: Gallery images from Icecat response

        Returns:
            List of dictionaries for icecat_media_thumbnails table
        """
        thumbnails = []

        for img in gallery or []:
            lang_id = 0
            if hasattr(img, 'lang_id') and img.lang_id is not None:
                lang_id = img.lang_id

            # Collect all available size URLs
            sizes = {}
            if img.pic:
                sizes["original"] = img.pic
                sizes["high"] = img.pic  # High = original
            if hasattr(img, 'pic_500x500') and img.pic_500x500:
                sizes["medium"] = img.pic_500x500
                sizes["500x500"] = img.pic_500x500
            if hasattr(img, 'low_pic') and img.low_pic:
                sizes["low"] = img.low_pic

            # Create a thumbnail row for each available size
            for size_name, url in sizes.items():
                if url:
                    thumbnails.append({
                        "localeid": lang_id,
                        "thumburl": url,
                        "size": size_name,
                        "contenttype": "image/jpeg",
                        "isactive": True,
                        "setnumber": 1,
                    })

        return thumbnails

    def map_attributes(
        self,
        features_groups: list[FeaturesGroup] | None,
        language_id: int,
        product_id: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Map feature groups to productattribute and search_attribute table records.

        productattribute = non-searchable attributes only,
        search_attribute = searchable attributes only. Split based on Icecat's
        Searchable flag from the Feature model.

        Args:
            features_groups: FeaturesGroups from Icecat response
            language_id: Icecat language ID (numeric INT)
            product_id: Icecat product ID (required for FK constraint)

        Returns:
            Tuple of (non_searchable_attributes, searchable_attributes)
        """
        if product_id is None:
            return [], []

        attributes = []
        search_attributes = []

        for group in features_groups or []:
            for feature in group.features or []:
                # Determine the best display value to use
                display_value = (
                    feature.presentation_value
                    or feature.local_value
                    or feature.value
                )

                # Try to extract numeric/absolute value
                absolute_value = None
                if feature.raw_value is not None:
                    try:
                        absolute_value = float(feature.raw_value)
                    except (ValueError, TypeError):
                        absolute_value = None

                # Get attribute ID - should be numeric Icecat feature ID
                attr_id = feature.feature_id or feature.id
                if attr_id is None:
                    continue  # Skip if no valid attribute ID

                # Extract unit of measurement from Icecat Feature.MeasureID
                unit_id = 0
                if feature.feature and feature.feature.measure_id:
                    unit_id = feature.feature.measure_id

                attr_dict = {
                    # Note: productid is added by repository.create_attributes()
                    "attributeid": attr_id,           # Icecat feature ID (bigint)
                    "localeid": language_id,          # Icecat language ID
                    "displayvalue": sanitize_string(display_value, 1000) if display_value else None,
                    "absolutevalue": absolute_value if absolute_value is not None else 0,
                    "unitid": unit_id,                # Icecat MeasureID
                    "isabsolute": absolute_value is not None and absolute_value != 0,
                    "setnumber": 1,                   # Single set per feature group
                    "isactive": True,                 # Active by default
                    # creationdate and modifieddate handled by repository
                }

                # Split by Searchable flag
                if feature.searchable:
                    attr_dict["valueid"] = feature.value_id or 0  # Icecat ValueID for faceted search
                    search_attributes.append(attr_dict)
                else:
                    attributes.append(attr_dict)

        return attributes, search_attributes

    def map_vendor(self, general_info: GeneralInfo) -> dict[str, Any] | None:
        """
        Map vendor/brand info for vendor table.

        Args:
            general_info: GeneralInfo from Icecat response

        Returns:
            Dictionary for vendor table or None
        """
        brand_id = general_info.brand_id
        brand_name = general_info.brand
        brand_logo = general_info.brand_logo

        # Extract from nested BrandInfo if available
        if general_info.brand_info:
            bi = general_info.brand_info
            brand_name = bi.brand_name or brand_name
            brand_logo = bi.brand_logo or brand_logo

        if not brand_id:
            return None

        return {
            "vendorid": brand_id,  # Icecat vendor ID (integer)
            "name": sanitize_string(brand_name, 190),  # varchar(190) - matches vendor.name column
            "logourl": None,
        }

    def map_category(self, general_info: GeneralInfo) -> dict[str, Any] | None:
        """
        Map category info for category table.

        Args:
            general_info: GeneralInfo from Icecat response

        Returns:
            Dictionary for category table or None
        """
        category_id = general_info.category_id
        category_name = None

        # Extract from nested CategoryInfo if available
        if general_info.category:
            cat = general_info.category
            category_id = cat.category_id or category_id
            # cat.name is a CategoryName (LocalizedValue-like) object
            if cat.name:
                category_name = cat.name.value

        if not category_id:
            return None

        return {
            "categoryid": category_id,  # Icecat category ID (integer)
            "categoryname": sanitize_string(category_name, 190),  # varchar(190)
        }

    def map_addons(
        self,
        product_related: list[Any] | None,
        base_product_id: int | None,
        base_category_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Map related products to product_addons table records.

        Addon type is derived by comparing the related product's CategoryID
        to the base product's CategoryID:
          - Same category      → U (Upsell / Similar)
          - Different category → C (Cross-sell / Accessories / Warranty)
        D, W, Z are never stored. Clients can detect Warranty via category 788 JOIN.

        Args:
            product_related: ProductRelated items from Icecat response
            base_product_id: The base product's Icecat ID
            base_category_id: The base product's CategoryID (for type derivation)

        Returns:
            List of dictionaries for product_addons table
        """
        if not product_related or not base_product_id:
            return []

        addons = []

        for idx, item in enumerate(product_related):
            # Get related product ID (try different field names)
            related_id = None
            if hasattr(item, 'icecat_id') and item.icecat_id:
                related_id = item.icecat_id
            elif hasattr(item, 'product_id') and item.product_id:
                related_id = item.product_id
            elif isinstance(item, dict):
                related_id = item.get('IcecatId') or item.get('ProductID')

            if not related_id:
                continue  # Skip if no valid related product ID

            # Derive addon type from category comparison
            # U = same category (Upsell/Similar), C = different category (Cross-sell)
            # Warranty (cat 788) is stored as C; clients can detect it via category JOIN
            related_category_id = None
            if hasattr(item, 'category_id') and item.category_id is not None:
                related_category_id = item.category_id
            elif isinstance(item, dict):
                related_category_id = item.get('CategoryID')

            addon_type = None
            if related_category_id is not None and base_category_id is not None:
                if related_category_id == base_category_id:
                    addon_type = "U"
                else:
                    addon_type = "C"

            # Get order
            order = None
            if hasattr(item, 'order') and item.order is not None:
                order = item.order
            elif isinstance(item, dict):
                order = item.get('Order')

            addons.append({
                # Note: productId is added by repository.create_addons()
                "relatedProductId": str(related_id),
                "type": addon_type,              # C/U/W or None
                "source": "Icecat",              # Source of relationship data
                "order": order if order is not None else idx + 1,  # Display order
                "available": 1,                  # Always available
                "isactive": True,                # Active by default
            })

        return addons


class MultiLanguageProductMapper:
    """
    Mapper for handling products with multiple language versions.

    Aggregates data from multiple language API calls into a single
    database record set.
    """

    def __init__(self):
        self.mapper = ProductMapper()
        self._product_data: dict[str, Any] | None = None
        self._descriptions: list[dict[str, Any]] = []
        self._marketing_info: list[dict[str, Any]] = []
        self._features: list[dict[str, Any]] = []
        self._media: list[dict[str, Any]] = []
        self._thumbnails: list[dict[str, Any]] = []
        self._attributes: list[dict[str, Any]] = []
        self._search_attributes: list[dict[str, Any]] = []
        self._addons: list[dict[str, Any]] = []
        self._vendor: dict[str, Any] | None = None
        self._category: dict[str, Any] | None = None

    def add_language_response(
        self,
        response: ProductResponse | dict[str, Any],
        language_id: int,
    ) -> None:
        """
        Add data from a language-specific API response.

        Args:
            response: ProductResponse or raw dict
            language_id: Icecat language ID for this response
        """
        mapped = self.mapper.map_product_response(response, language_id)
        if not mapped:
            return

        # Use first language response for base product data
        if self._product_data is None:
            self._product_data = mapped["product"]
            self._vendor = mapped["vendor"]
            self._category = mapped["category"]

        # Media, thumbnails, and addons are typically language-independent, take from first response
        if not self._media:
            self._media = mapped["media"]
        if not self._thumbnails:
            self._thumbnails = mapped.get("thumbnails", [])
        if not self._addons:
            self._addons = mapped.get("addons", [])

        # Accumulate language-specific data
        self._descriptions.extend(mapped["descriptions"])
        self._marketing_info.extend(mapped["marketing_info"])
        self._features.extend(mapped["features"])
        self._attributes.extend(mapped["attributes"])
        self._search_attributes.extend(mapped.get("search_attributes", []))

    def get_merged_data(self) -> dict[str, Any] | None:
        """
        Get the merged data from all languages.

        Returns:
            Dictionary with all accumulated data
        """
        if self._product_data is None:
            return None

        return {
            "product": self._product_data,
            "descriptions": self._descriptions,
            "marketing_info": self._marketing_info,
            "features": self._features,
            "media": self._media,
            "thumbnails": self._thumbnails,
            "attributes": self._attributes,
            "search_attributes": self._search_attributes,
            "addons": self._addons,
            "vendor": self._vendor,
            "category": self._category,
        }

    def reset(self) -> None:
        """Reset the mapper for a new product."""
        self._product_data = None
        self._descriptions = []
        self._marketing_info = []
        self._features = []
        self._media = []
        self._thumbnails = []
        self._attributes = []
        self._search_attributes = []
        self._addons = []
        self._vendor = None
        self._category = None
