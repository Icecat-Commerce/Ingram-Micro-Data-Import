"""Bullet points response models for Icecat Back Office API."""

from pydantic import BaseModel, Field


class BulletPointData(BaseModel):
    """Bullet point data."""

    id: int | None = Field(default=None, alias="Id")
    product_id: int | None = Field(default=None, alias="ProductId")
    language_id: int | None = Field(default=None, alias="LanguageId")
    value: str | None = Field(default=None, alias="Value")
    order: int | None = Field(default=None, alias="Order")

    model_config = {"populate_by_name": True}


class BulletPointResponse(BaseModel):
    """
    Response from Icecat Bullet Points API.

    Documentation: Section 11.1 "View bullet points"
    https://iceclog.com/manual-for-icecat-push-api-api-in/
    """

    data: list[BulletPointData] = Field(default_factory=list, alias="Data")

    model_config = {"populate_by_name": True}


class ProductBullet(BaseModel):
    """
    Product bullet / Reason to buy data.

    Documentation: Section 12.1 View "Reasons to buy"
    """

    id: int | None = Field(default=None, alias="Id")
    product_id: int | None = Field(default=None, alias="ProductId")
    language_id: int | None = Field(default=None, alias="LanguageId")
    title: str | None = Field(default=None, alias="Title")
    value: str | None = Field(default=None, alias="Value")
    order: int | None = Field(default=None, alias="Order")
    image_url: str | None = Field(default=None, alias="ImageUrl")

    model_config = {"populate_by_name": True}


class ProductBulletResponse(BaseModel):
    """Response from Icecat ProductBullet (Reasons to Buy) API."""

    data: list[ProductBullet] = Field(default_factory=list, alias="Data")

    model_config = {"populate_by_name": True}
