"""Product response models from Icecat FrontOffice Live API."""

from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Field


def _coerce_str(v: Any) -> str | None:
    """Coerce numeric values to strings — Icecat API sometimes returns floats/ints for string fields."""
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return str(v)


# ============================================================================
# Localized Value Models
# ============================================================================


class LocalizedValue(BaseModel):
    """Localized string value with language."""

    value: Annotated[str | None, BeforeValidator(_coerce_str)] = Field(default=None, alias="Value")
    language: str | None = Field(default=None, alias="Language")

    model_config = {"populate_by_name": True}


# ============================================================================
# Nested Models for GeneralInfo
# ============================================================================


class TitleInfo(BaseModel):
    """Title information with generated and brand titles."""

    generated_int_title: str | None = Field(default=None, alias="GeneratedIntTitle")
    generated_local_title: LocalizedValue | None = Field(
        default=None, alias="GeneratedLocalTitle"
    )
    brand_local_title: LocalizedValue | None = Field(
        default=None, alias="BrandLocalTitle"
    )

    model_config = {"populate_by_name": True}


class BrandInfo(BaseModel):
    """Brand information."""

    brand_name: str | None = Field(default=None, alias="BrandName")
    brand_local_name: str | None = Field(default=None, alias="BrandLocalName")
    brand_logo: str | None = Field(default=None, alias="BrandLogo")

    model_config = {"populate_by_name": True}


class ProductNameInfo(BaseModel):
    """Product name information."""

    product_int_name: str | None = Field(default=None, alias="ProductIntName")
    product_local_name: LocalizedValue | None = Field(
        default=None, alias="ProductLocalName"
    )

    model_config = {"populate_by_name": True}


class CategoryName(BaseModel):
    """Category name with localization."""

    value: str | None = Field(default=None, alias="Value")
    language: str | None = Field(default=None, alias="Language")

    model_config = {"populate_by_name": True}


class CategoryInfo(BaseModel):
    """Category information as nested object."""

    category_id: str | None = Field(default=None, alias="CategoryID")
    name: CategoryName | None = Field(default=None, alias="Name")

    model_config = {"populate_by_name": True}


class DescriptionInfo(BaseModel):
    """Product description with multiple formats."""

    id: str | None = Field(default=None, alias="ID")
    long_desc: str | None = Field(default=None, alias="LongDesc")
    long_product_name: str | None = Field(default=None, alias="LongProductName")
    middle_desc: str | None = Field(default=None, alias="MiddleDesc")
    short_desc: str | None = Field(default=None, alias="ShortDesc")
    disclaimer: str | None = Field(default=None, alias="Disclaimer")
    manual_pdf_url: str | None = Field(default=None, alias="ManualPDFURL")
    manual_pdf_size: str | None = Field(default=None, alias="ManualPDFSize")
    leaflet_pdf_url: str | None = Field(default=None, alias="LeafletPDFURL")
    pdf_size: str | None = Field(default=None, alias="PDFSize")
    url: str | None = Field(default=None, alias="URL")
    warranty_info: str | None = Field(default=None, alias="WarrantyInfo")
    updated: str | None = Field(default=None, alias="Updated")
    language: str | None = Field(default=None, alias="Language")

    model_config = {"populate_by_name": True}


class SummaryDescriptionInfo(BaseModel):
    """Summary description with short and long versions."""

    short_summary_description: str | None = Field(
        default=None, alias="ShortSummaryDescription"
    )
    long_summary_description: str | None = Field(
        default=None, alias="LongSummaryDescription"
    )

    model_config = {"populate_by_name": True}


class BulletPointsInfo(BaseModel):
    """Bullet points with metadata."""

    bullet_points_id: str | None = Field(default=None, alias="BulletPointsId")
    language: str | None = Field(default=None, alias="Language")
    values: list[str] = Field(default_factory=list, alias="Values")
    updated: str | None = Field(default=None, alias="Updated")

    model_config = {"populate_by_name": True}


class GeneratedBulletPointsInfo(BaseModel):
    """Generated bullet points."""

    language: str | None = Field(default=None, alias="Language")
    values: list[str] = Field(default_factory=list, alias="Values")

    model_config = {"populate_by_name": True}


class GTINInfo(BaseModel):
    """GTIN/EAN with approval status."""

    gtin: str | None = Field(default=None, alias="GTIN")
    is_approved: bool | None = Field(default=None, alias="IsApproved")

    model_config = {"populate_by_name": True}


class ProductFamily(BaseModel):
    """Product family info."""

    family_id: str | None = Field(default=None, alias="FamilyID")
    name: str | None = Field(default=None, alias="Name")

    model_config = {"populate_by_name": True}


class ProductSeries(BaseModel):
    """Product series info."""

    series_id: str | None = Field(default=None, alias="SeriesID")
    name: str | None = Field(default=None, alias="Name")

    model_config = {"populate_by_name": True}


# ============================================================================
# Image Models
# ============================================================================


class ImageDto(BaseModel):
    """Product image data with all resolutions."""

    # High resolution
    high_pic: str | None = Field(default=None, alias="HighPic")
    high_pic_size: int | None = Field(default=None, alias="HighPicSize")
    high_pic_height: int | None = Field(default=None, alias="HighPicHeight")
    high_pic_width: int | None = Field(default=None, alias="HighPicWidth")

    # Low resolution
    low_pic: str | None = Field(default=None, alias="LowPic")
    low_pic_size: int | None = Field(default=None, alias="LowPicSize")
    low_pic_height: int | None = Field(default=None, alias="LowPicHeight")
    low_pic_width: int | None = Field(default=None, alias="LowPicWidth")

    # 500x500 resolution
    pic_500x500: str | None = Field(default=None, alias="Pic500x500")
    pic_500x500_size: int | None = Field(default=None, alias="Pic500x500Size")
    pic_500x500_height: int | None = Field(default=None, alias="Pic500x500Height")
    pic_500x500_width: int | None = Field(default=None, alias="Pic500x500Width")

    # Thumbnail
    thumb_pic: str | None = Field(default=None, alias="ThumbPic")
    thumb_pic_size: int | None = Field(default=None, alias="ThumbPicSize")

    model_config = {"populate_by_name": True}


# ============================================================================
# Gallery Model
# ============================================================================


class GalleryAttributes(BaseModel):
    """Gallery image attributes."""

    original_file_name: str | None = Field(default=None, alias="OriginalFileName")

    model_config = {"populate_by_name": True}


class Gallery(BaseModel):
    """Product gallery image with all metadata."""

    id: int | None = Field(default=None, alias="ID")

    # Thumbnail
    thumb_pic: str | None = Field(default=None, alias="ThumbPic")
    thumb_pic_size: int | None = Field(default=None, alias="ThumbPicSize")

    # Main image
    pic: str | None = Field(default=None, alias="Pic")
    size: int | None = Field(default=None, alias="Size")
    pic_height: int | None = Field(default=None, alias="PicHeight")
    pic_width: int | None = Field(default=None, alias="PicWidth")

    # Low resolution
    low_pic: str | None = Field(default=None, alias="LowPic")
    low_size: int | None = Field(default=None, alias="LowSize")
    low_height: int | None = Field(default=None, alias="LowHeight")
    low_width: int | None = Field(default=None, alias="LowWidth")

    # 500x500 resolution
    pic_500x500: str | None = Field(default=None, alias="Pic500x500")
    pic_500x500_size: int | None = Field(default=None, alias="Pic500x500Size")
    pic_500x500_height: int | None = Field(default=None, alias="Pic500x500Height")
    pic_500x500_width: int | None = Field(default=None, alias="Pic500x500Width")

    # Metadata
    no: int | None = Field(default=None, alias="No")
    is_main: bool | None = Field(default=None, alias="IsMain")
    is_private: bool | None = Field(default=None, alias="IsPrivate")
    type: str | None = Field(default=None, alias="Type")
    attributes: GalleryAttributes | None = Field(default=None, alias="Attributes")
    updated: str | None = Field(default=None, alias="Updated")

    model_config = {"populate_by_name": True}


# ============================================================================
# Multimedia Model
# ============================================================================


class Multimedia(BaseModel):
    """Multimedia object (video, 3D model, PDF, EU labels, etc.)."""

    id: str | None = Field(default=None, alias="ID")
    url: str | None = Field(default=None, alias="URL")
    thumb_url: str | None = Field(default=None, alias="ThumbUrl")
    type: str | None = Field(default=None, alias="Type")
    content_type: str | None = Field(default=None, alias="ContentType")
    keep_as_url: int | None = Field(default=None, alias="KeepAsUrl")
    description: str | None = Field(default=None, alias="Description")
    language: str | None = Field(default=None, alias="Language")
    is_video: bool | None = Field(default=None, alias="IsVideo")
    is_private: bool | None = Field(default=None, alias="IsPrivate")

    # Size fields
    size: int | None = Field(default=None, alias="Size")
    height: int | None = Field(default=None, alias="Height")
    width: int | None = Field(default=None, alias="Width")

    # EU Energy Label fields
    eprel_link: str | None = Field(default=None, alias="EprelLink")
    eprel_id: str | int | None = Field(default=None, alias="EprelId")
    label_type: str | None = Field(default=None, alias="LabelType")
    link: str | None = Field(default=None, alias="Link")

    # Converted file fields
    converted_url: str | None = Field(default=None, alias="ConvertedURL")
    converted_content_type: str | None = Field(default=None, alias="ConvertedContentType")
    converted_size: int | None = Field(default=None, alias="ConvertedSize")

    # Metadata
    updated: str | None = Field(default=None, alias="Updated")

    model_config = {"populate_by_name": True}


# ============================================================================
# Related Products Models
# ============================================================================


class ProductRelatedItem(BaseModel):
    """Related product item from Icecat API.

    The API does not return a Type field directly. The mapper derives the addon
    type by comparing this item's CategoryID to the base product's CategoryID.
    See product_mapper.map_addons() for derivation logic.
    """

    id: int | None = Field(default=None, alias="ID")
    product_id: int | None = Field(default=None, alias="ProductID")
    icecat_id: int | None = Field(default=None, alias="IcecatID")
    category_id: int | None = Field(default=None, alias="CategoryID")
    preferred: int | None = Field(default=None, alias="Preferred")
    type: str | None = Field(default=None, alias="Type")  # Always None — API doesn't provide this
    order: int | None = Field(default=None, alias="Order")

    model_config = {"populate_by_name": True}


# ============================================================================
# Feature Models
# ============================================================================


class FeatureLogo(BaseModel):
    """Feature logo data."""

    logo_id: int | None = Field(default=None, alias="LogoID")
    logo_url: str | None = Field(default=None, alias="LogoUrl")
    logo_thumb_url: str | None = Field(default=None, alias="LogoThumbUrl")
    logo_original_url: str | None = Field(default=None, alias="LogoOriginalUrl")
    feature_id: int | None = Field(default=None, alias="FeatureId")
    value_id: int | None = Field(default=None, alias="ValueId")
    language_id: int | None = Field(default=None, alias="LanguageId")

    model_config = {"populate_by_name": True}


class MeasureInfo(BaseModel):
    """Measure information for a feature."""

    id: str | None = Field(default=None, alias="ID")
    sign: str | None = Field(default=None, alias="Sign")
    signs: LocalizedValue | None = Field(default=None, alias="Signs")

    model_config = {"populate_by_name": True}


class FeatureInfo(BaseModel):
    """Feature metadata (nested inside Feature as 'Feature' key)."""

    id: int | None = Field(default=None, alias="ID")
    name: LocalizedValue | None = Field(default=None, alias="Name")
    sign: str | None = Field(default=None, alias="Sign")
    measure: MeasureInfo | None = Field(default=None, alias="Measure")
    measure_id: int | None = Field(default=None, alias="MeasureID")
    measure_sign: str | None = Field(default=None, alias="MeasureSign")
    local_name: str | None = Field(default=None, alias="LocalName")
    local_measure: str | None = Field(default=None, alias="LocalMeasure")

    model_config = {"populate_by_name": True}

    @property
    def name_value(self) -> str | None:
        """Get the name string value."""
        if self.name:
            return self.name.value
        return self.local_name

    @property
    def measure_sign_value(self) -> str | None:
        """Get the measure sign value."""
        if self.measure and self.measure.signs:
            return self.measure.signs.value
        if self.measure:
            return self.measure.sign
        return self.measure_sign


class Feature(BaseModel):
    """Product feature/specification."""

    # Identification
    id: int | None = Field(default=None, alias="ID")
    local_id: int | None = Field(default=None, alias="LocalID")
    localized: int | None = Field(default=None, alias="Localized")

    # Values — Icecat API may return floats/ints, coerce to str
    value: Annotated[str | None, BeforeValidator(_coerce_str)] = Field(default=None, alias="Value")
    local_value: Annotated[str | None, BeforeValidator(_coerce_str)] = Field(default=None, alias="LocalValue")
    raw_value: Annotated[str | None, BeforeValidator(_coerce_str)] = Field(default=None, alias="RawValue")
    presentation_value: Annotated[str | None, BeforeValidator(_coerce_str)] = Field(default=None, alias="PresentationValue")

    # Type
    type: str | None = Field(default=None, alias="Type")

    # Category feature metadata
    category_feature_id: int | None = Field(default=None, alias="CategoryFeatureId")
    category_feature_group_id: int | None = Field(
        default=None, alias="CategoryFeatureGroupID"
    )

    # Value ID (for searchable features - used in faceted search)
    value_id: int | None = Field(default=None, alias="ValueID")

    # Sort and flags
    sort_no: int | None = Field(default=None, alias="SortNo")
    mandatory: bool | None = Field(default=None, alias="Mandatory")
    searchable: bool | None = Field(default=None, alias="Searchable")
    optional: bool | None = Field(default=None, alias="Optional")

    # Description
    description: str | None = Field(default=None, alias="Description")

    # Nested feature info (contains name, measure, etc.)
    feature: FeatureInfo | None = Field(default=None, alias="Feature")

    model_config = {"populate_by_name": True}

    @property
    def name(self) -> str | None:
        """Get feature name from nested Feature object."""
        if self.feature:
            return self.feature.name_value or self.feature.local_name
        return None

    @property
    def measure_unit(self) -> str | None:
        """Get measure unit from nested Feature object."""
        if self.feature:
            return self.feature.measure_sign_value or self.feature.local_measure
        return None

    @property
    def feature_id(self) -> int | None:
        """Get feature ID from nested Feature object."""
        if self.feature:
            return self.feature.id
        return self.id


class FeatureGroupInfo(BaseModel):
    """Feature group info (nested inside FeaturesGroup as 'FeatureGroup' key)."""

    id: int | None = Field(default=None, alias="ID")
    name: LocalizedValue | None = Field(default=None, alias="Name")
    local_name: str | None = Field(default=None, alias="LocalName")

    model_config = {"populate_by_name": True}

    @property
    def name_value(self) -> str | None:
        """Get the name string value."""
        if self.name:
            return self.name.value
        return self.local_name


class FeaturesGroup(BaseModel):
    """Group of product features."""

    id: int | None = Field(default=None, alias="ID")
    sort_no: int | None = Field(default=None, alias="SortNo")
    feature_group: FeatureGroupInfo | None = Field(default=None, alias="FeatureGroup")
    features: list[Feature] = Field(default_factory=list, alias="Features")

    model_config = {"populate_by_name": True}

    @property
    def name(self) -> str | None:
        """Get group name from nested FeatureGroup object."""
        if self.feature_group:
            return self.feature_group.name_value
        return None


# ============================================================================
# GeneralInfo Model
# ============================================================================


class GeneralInfo(BaseModel):
    """General product information with nested objects."""

    # IDs and dates
    icecat_id: int | None = Field(default=None, alias="IcecatId")
    release_date: str | None = Field(default=None, alias="ReleaseDate")
    end_of_life_date: str | None = Field(default=None, alias="EndOfLifeDate")

    # Title and product info
    title: str | None = Field(default=None, alias="Title")
    title_info: TitleInfo | None = Field(default=None, alias="TitleInfo")

    # Brand info
    brand: str | None = Field(default=None, alias="Brand")
    brand_id: str | None = Field(default=None, alias="BrandID")
    brand_logo: str | None = Field(default=None, alias="BrandLogo")
    brand_info: BrandInfo | None = Field(default=None, alias="BrandInfo")

    # Product name
    product_name: str | None = Field(default=None, alias="ProductName")
    product_name_info: ProductNameInfo | None = Field(
        default=None, alias="ProductNameInfo"
    )
    brand_part_code: str | None = Field(default=None, alias="BrandPartCode")

    # GTIN (flat array of strings)
    gtin: list[str] = Field(default_factory=list, alias="GTIN")
    # GTINs (detailed list with approval status)
    gtins: list[GTINInfo] = Field(default_factory=list, alias="GTINs")

    # Category
    category: CategoryInfo | None = Field(default=None, alias="Category")

    # Product family and series
    product_family: ProductFamily | None = Field(default=None, alias="ProductFamily")
    product_series: ProductSeries | None = Field(default=None, alias="ProductSeries")

    # Descriptions
    description: DescriptionInfo | None = Field(default=None, alias="Description")
    summary_description: SummaryDescriptionInfo | None = Field(
        default=None, alias="SummaryDescription"
    )

    # Bullet points
    bullet_points: BulletPointsInfo | None = Field(default=None, alias="BulletPoints")
    generated_bullet_points: GeneratedBulletPointsInfo | None = Field(
        default=None, alias="GeneratedBulletPoints"
    )

    # Quality indicator
    quality: str | None = Field(default=None, alias="Quality")

    model_config = {"populate_by_name": True}

    @property
    def product_code(self) -> str | None:
        """Get product code (MPN)."""
        return self.brand_part_code or self.product_name

    @property
    def ean(self) -> str | None:
        """Get first EAN from GTIN list."""
        if self.gtin:
            return self.gtin[0]
        if self.gtins:
            return self.gtins[0].gtin
        return None

    @property
    def category_id(self) -> int | None:
        """Get category ID as int."""
        if self.category and self.category.category_id:
            try:
                return int(self.category.category_id)
            except (ValueError, TypeError):
                return None
        return None

    @property
    def category_name(self) -> str | None:
        """Get category name."""
        if self.category and self.category.name:
            return self.category.name.value
        return None


# ============================================================================
# Product Story and Reasons to Buy
# ============================================================================


class ProductStoryItem(BaseModel):
    """Product story item."""

    id: int | None = Field(default=None, alias="ID")
    title: str | None = Field(default=None, alias="Title")
    text: str | None = Field(default=None, alias="Text")
    image_url: str | None = Field(default=None, alias="ImageURL")
    language_id: int | None = Field(default=None, alias="LanguageID")

    model_config = {"populate_by_name": True}


class ReasonToBuy(BaseModel):
    """Reason to buy item."""

    id: int | None = Field(default=None, alias="ID")
    title: str | None = Field(default=None, alias="Title")
    value: str | None = Field(default=None, alias="Value")
    priority: int | None = Field(default=None, alias="Priority")
    language_id: int | None = Field(default=None, alias="LanguageID")

    model_config = {"populate_by_name": True}


class Review(BaseModel):
    """Product review."""

    id: int | None = Field(default=None, alias="ID")
    score: float | None = Field(default=None, alias="Score")
    review_count: int | None = Field(default=None, alias="ReviewCount")
    source: str | None = Field(default=None, alias="Source")

    model_config = {"populate_by_name": True}


# ============================================================================
# Main Product Data Container
# ============================================================================


class ProductData(BaseModel):
    """Main product data container."""

    # Account info
    demo_account: bool = Field(default=False, alias="DemoAccount")

    # Core data
    general_info: GeneralInfo | None = Field(default=None, alias="GeneralInfo")
    image: ImageDto | None = Field(default=None, alias="Image")

    # Lists
    gallery: list[Gallery] = Field(default_factory=list, alias="Gallery")
    multimedia: list[Multimedia] = Field(default_factory=list, alias="Multimedia")
    features_groups: list[FeaturesGroup] = Field(
        default_factory=list, alias="FeaturesGroups"
    )
    feature_logos: list[FeatureLogo] = Field(default_factory=list, alias="FeatureLogos")

    # Marketing content
    product_story: list[ProductStoryItem] = Field(
        default_factory=list, alias="ProductStory"
    )
    reasons_to_buy: list[ReasonToBuy] = Field(
        default_factory=list, alias="ReasonsToBuy"
    )
    reviews: list[Review] = Field(default_factory=list, alias="Reviews")

    # Related products
    product_related: list[ProductRelatedItem] = Field(default_factory=list, alias="ProductRelated")

    # Additional data
    catalog_object_cloud: dict[str, Any] = Field(
        default_factory=dict, alias="CatalogObjectCloud"
    )
    variants: list[Any] = Field(default_factory=list, alias="Variants")
    dictionary: dict[str, Any] = Field(default_factory=dict, alias="Dictionary")

    model_config = {"populate_by_name": True}


# ============================================================================
# Top-Level Response
# ============================================================================


class ProductResponse(BaseModel):
    """Response from Icecat FrontOffice Live API product fetch."""

    data: ProductData | None = Field(default=None, alias="data")
    msg: str | None = Field(default=None, alias="msg")

    model_config = {"populate_by_name": True}
