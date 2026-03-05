"""Product description response models for Icecat Back Office API."""

from pydantic import BaseModel, Field


class ProductDescription(BaseModel):
    """Product description for a specific language."""

    id: int | None = Field(default=None, alias="Id")
    language_id: int | None = Field(default=None, alias="LanguageId")
    language_code: str | None = Field(default=None, alias="LanguageCode")
    short_description: str | None = Field(default=None, alias="ShortDescription")
    long_description: str | None = Field(default=None, alias="LongDescription")
    warranty_info: str | None = Field(default=None, alias="WarrantyInfo")
    manual_pdf_url: str | None = Field(default=None, alias="ManualPdfUrl")
    pdf_url: str | None = Field(default=None, alias="PdfUrl")
    url: str | None = Field(default=None, alias="Url")
    updated: str | None = Field(default=None, alias="Updated")

    model_config = {"populate_by_name": True}


class ProductDescriptionData(BaseModel):
    """Container for product descriptions."""

    descriptions: list[ProductDescription] = Field(
        default_factory=list, alias="Descriptions"
    )
    product_id: int | None = Field(default=None, alias="ProductId")

    model_config = {"populate_by_name": True}


class ProductDescriptionResponse(BaseModel):
    """
    Response from Icecat ProductDescriptions API.

    Documentation: Section 10.1 "View product description block"
    https://iceclog.com/manual-for-icecat-push-api-api-in/
    """

    data: ProductDescriptionData | None = Field(default=None, alias="Data")

    model_config = {"populate_by_name": True}
