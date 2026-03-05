"""JSON response models for Icecat API."""

from .session import SessionData, SessionResponse
from .product_response import (
    ProductResponse,
    ProductData,
    GeneralInfo,
    FeaturesGroup,
    Feature,
    Gallery,
    ImageDto,
    Multimedia,
    FeatureLogo,
)
from .gallery import GalleryResponse, ImageData, ImageDetails, Locale
from .multimedia import MultimediaData, ImageBatch
from .product_features import ProductFeatureResponse, ProductFeature, ProductFeatureLocal
from .bullet_points import BulletPointResponse, BulletPointData, ProductBulletResponse, ProductBullet
from .product_descriptions import ProductDescriptionResponse, ProductDescriptionData, ProductDescription
from .product_search import ProductSearchResponse, ProductSearchData, ProductSearchProduct, ProductEanDetailResponse
from .language import IcecatLanguage

__all__ = [
    # Session
    "SessionData",
    "SessionResponse",
    # Product Response
    "ProductResponse",
    "ProductData",
    "GeneralInfo",
    "FeaturesGroup",
    "Feature",
    "Gallery",
    "ImageDto",
    "Multimedia",
    "FeatureLogo",
    # Gallery
    "GalleryResponse",
    "ImageData",
    "ImageDetails",
    "Locale",
    # Multimedia
    "MultimediaData",
    "ImageBatch",
    # Product Features
    "ProductFeatureResponse",
    "ProductFeature",
    "ProductFeatureLocal",
    # Bullet Points
    "BulletPointResponse",
    "BulletPointData",
    "ProductBulletResponse",
    "ProductBullet",
    # Product Descriptions
    "ProductDescriptionResponse",
    "ProductDescriptionData",
    "ProductDescription",
    # Product Search
    "ProductSearchResponse",
    "ProductSearchData",
    "ProductSearchProduct",
    "ProductEanDetailResponse",
    # Language
    "IcecatLanguage",
]
