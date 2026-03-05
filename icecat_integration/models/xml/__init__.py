"""XML response models for Icecat data."""

from .daily_index import (
    DailyIndexFileRoot,
    FilesIndex,
    ProductIndex,
    EANUPCS,
    EANUPC,
    CountryMarkets,
    CountryMarket,
)
from .feature_groups import (
    FeatureGroupsRoot,
    FeatureGroupsList,
    FeatureGroup,
    FeatureGroupName,
)
from .features_list import (
    FeaturesListRoot,
    FeaturesList,
    Feature,
    Names,
    Name,
    Descriptions,
    Description,
    Measure,
    Signs,
    Sign,
    RestrictedValues,
    RestrictedValue,
)

__all__ = [
    # Daily Index
    "DailyIndexFileRoot",
    "FilesIndex",
    "ProductIndex",
    "EANUPCS",
    "EANUPC",
    "CountryMarkets",
    "CountryMarket",
    # Feature Groups
    "FeatureGroupsRoot",
    "FeatureGroupsList",
    "FeatureGroup",
    "FeatureGroupName",
    # Features List
    "FeaturesListRoot",
    "FeaturesList",
    "Feature",
    "Names",
    "Name",
    "Descriptions",
    "Description",
    "Measure",
    "Signs",
    "Sign",
    "RestrictedValues",
    "RestrictedValue",
]
