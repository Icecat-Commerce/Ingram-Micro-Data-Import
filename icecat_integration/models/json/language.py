"""Language response models for Icecat Back Office API."""

from pydantic import BaseModel, Field


class IcecatLanguage(BaseModel):
    """
    Icecat language data from API.

    Documentation: Section 3.2.1 "GET available languages for a user"
    Endpoint: https://bo.icecat.biz/restful/v2/language
    """

    langid: int = Field(alias="langid")
    code: str = Field(alias="code")
    short_code: str = Field(alias="short_code")
    name: str | None = Field(default=None, alias="name")
    published: str | None = Field(default="Y", alias="published")
    backup_langid: int | None = Field(default=None, alias="backup_langid")
    separators: str | None = Field(default=".", alias="separators")
    measurement: str | None = Field(default="metric", alias="measurement")

    model_config = {"populate_by_name": True}
