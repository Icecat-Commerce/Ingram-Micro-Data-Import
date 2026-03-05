"""Product features response models for Icecat Back Office API."""

from typing import Any

from pydantic import BaseModel, Field


class ProductFeature(BaseModel):
    """Product feature value data."""

    id: int | None = Field(default=None, alias="id")
    value: str | None = Field(default=None, alias="value")
    raw_value: str | None = Field(default=None, alias="raw_value")
    restricted_value_id: int | None = Field(default=None, alias="restricted_value_id")
    restricted_value_ext_id: str | None = Field(default=None, alias="restricted_value_ext_id")
    is_hidden: bool = Field(default=False, alias="is_hidden")

    model_config = {"populate_by_name": True}


class ProductFeatureLocal(BaseModel):
    """Localized product feature data."""

    lang_id: int | None = Field(default=None, alias="lang_id")
    value: str | None = Field(default=None, alias="value")
    presentation_value: str | None = Field(default=None, alias="presentation_value")

    model_config = {"populate_by_name": True}


class ProductFeatureResponse(BaseModel):
    """
    Product feature specification response.

    Documentation: Section 7.1 "Read all International product specs"
    https://iceclog.com/manual-for-icecat-push-api-api-in/
    """

    category_feature_group_id: str | None = Field(default=None, alias="category_feature_group_id")
    category_feature_id: str | None = Field(default=None, alias="category_feature_id")
    feature_id: str | None = Field(default=None, alias="feature_id")
    feature: str | None = Field(default=None, alias="value")
    feature_group: str | None = Field(default=None, alias="group_value")
    is_mandatory: bool = Field(default=False, alias="mandatory")
    lang_id: int | None = Field(default=None, alias="langid")
    measure_id: str | None = Field(default=None, alias="measure_id")
    measure_sign: str | None = Field(default=None, alias="measure_sign")
    restricted_values: str | None = Field(default=None, alias="restricted_values")
    restricted_search_values: str | None = Field(default=None, alias="restricted_search_values")
    type: str | None = Field(default=None, alias="type")
    value_mapping: dict[str, str] | None = Field(default=None, alias="value_mapping")
    product_feature: ProductFeature | None = Field(default=None, alias="product_feature")
    product_feature_local: list[ProductFeatureLocal] = Field(
        default_factory=list, alias="product_features_local"
    )

    model_config = {"populate_by_name": True}
