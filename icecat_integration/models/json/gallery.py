"""Gallery response models for Icecat Back Office API."""

from pydantic import BaseModel, Field


class Locale(BaseModel):
    """Locale information for gallery images."""

    language_id: int | None = Field(default=None, alias="LanguageId")
    language_code: str | None = Field(default=None, alias="LanguageCode")

    model_config = {"populate_by_name": True}


class ImageDetails(BaseModel):
    """Image details for different resolution levels."""

    url: str | None = Field(default=None, alias="Url")
    width: int | None = Field(default=None, alias="Width")
    height: int | None = Field(default=None, alias="Height")
    size: int | None = Field(default=None, alias="Size")

    model_config = {"populate_by_name": True}


class ImageData(BaseModel):
    """Image data from gallery API."""

    order_number: int | None = Field(default=None, alias="OrderNumber")
    md5: str | None = Field(default=None, alias="Md5")
    image_fingerprint: str | None = Field(default=None, alias="ImageFingerprint")
    origin: ImageDetails | None = Field(default=None, alias="Origin")
    high: ImageDetails | None = Field(default=None, alias="High")
    medium: ImageDetails | None = Field(default=None, alias="Medium")
    low: ImageDetails | None = Field(default=None, alias="Low")
    thumb: ImageDetails | None = Field(default=None, alias="Thumb")
    locales: list[Locale] = Field(default_factory=list, alias="Locales")

    model_config = {"populate_by_name": True}


class GalleryResponse(BaseModel):
    """Response from Icecat Gallery API."""

    data: list[ImageData] = Field(default_factory=list, alias="Data")
    can_delete: bool = Field(default=False, alias="CanDelete")
    allowed_languages: list[int] = Field(default_factory=list, alias="AllowedLanguages")
    current_date: str | None = Field(default=None, alias="CurrentDate")

    model_config = {"populate_by_name": True}
