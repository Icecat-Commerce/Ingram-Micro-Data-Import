"""Multimedia response models for Icecat Back Office API."""

from pydantic import BaseModel, Field


class ImageBatch(BaseModel):
    """Image batch data for multimedia."""

    language_id: int | None = Field(default=None, alias="LanguageId")
    image_url: str | None = Field(default=None, alias="ImageUrl")

    model_config = {"populate_by_name": True}


class MultimediaData(BaseModel):
    """Multimedia object data (videos, 3D models, documents, etc.)."""

    uuid: str | None = Field(default=None, alias="Uuid")
    lang_id: int | None = Field(default=None, alias="LangId")
    short_descr: str | None = Field(default=None, alias="ShortDescr")
    data_source_id: int | None = Field(default=None, alias="DataSourceId")
    link: str | None = Field(default=None, alias="Link")
    keep_as_url: bool = Field(default=False, alias="KeepAsUrl")
    is_private: bool = Field(default=False, alias="IsPrivate")
    visible: bool = Field(default=True, alias="Visible")
    type: str | None = Field(default=None, alias="Type")
    expiry_date: str | None = Field(default=None, alias="ExpiryDate")
    id: int | None = Field(default=None, alias="Id")
    preview_link: str | None = Field(default=None, alias="PreviewLink")
    updated: str | None = Field(default=None, alias="Updated")
    md5_origin: str | None = Field(default=None, alias="Md5Origin")
    size: int | None = Field(default=None, alias="Size")
    preview_height: int | None = Field(default=None, alias="PreviewHeight")
    preview_width: int | None = Field(default=None, alias="PreviewWidth")
    preview_size: int | None = Field(default=None, alias="PreviewSize")
    converted_link: str | None = Field(default=None, alias="ConvertedLink")
    converted_mime_type: str | None = Field(default=None, alias="ConvertedMimeType")
    converted_size: int | None = Field(default=None, alias="ConvertedSize")
    thumb_link: str | None = Field(default=None, alias="ThumbLink")
    content_type: str | None = Field(default=None, alias="ContentType")
    link_origin: str | None = Field(default=None, alias="LinkOrigin")
    expired: bool = Field(default=False, alias="Expired")
    energy_labelling_link: str | None = Field(default=None, alias="EnergyLabellingLink")
    is_new_energy_labelling: bool = Field(default=False, alias="IsNewEnergyLabelling")
    images_batch: list[ImageBatch] = Field(default_factory=list, alias="ImagesBatch")

    model_config = {"populate_by_name": True}
