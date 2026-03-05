"""Product search response models for Icecat Back Office API."""

from pydantic import BaseModel, Field


class ProductSearchProduct(BaseModel):
    """Product found in search results."""

    product_id: int | None = Field(default=None, alias="ProductId")
    supplier_id: int | None = Field(default=None, alias="SupplierId")
    supplier_name: str | None = Field(default=None, alias="SupplierName")
    product_code: str | None = Field(default=None, alias="ProductCode")
    category_id: int | None = Field(default=None, alias="CategoryId")
    category_name: str | None = Field(default=None, alias="CategoryName")
    eans: list[str] = Field(default_factory=list, alias="Eans")
    mpns: list[str] = Field(default_factory=list, alias="Mpns")
    thumb_pic: str | None = Field(default=None, alias="ThumbPic")
    quality: str | None = Field(default=None, alias="Quality")

    model_config = {"populate_by_name": True}


class ProductSearchData(BaseModel):
    """Container for product search results."""

    products: list[ProductSearchProduct] = Field(
        default_factory=list, alias="Products"
    )
    total_count: int = Field(default=0, alias="TotalCount")

    model_config = {"populate_by_name": True}


class ProductSearchResponse(BaseModel):
    """
    Response from Icecat Productsearch API.

    Documentation: Section 2 "Product search"
    https://iceclog.com/manual-for-icecat-push-api-api-in/
    """

    data: ProductSearchData | None = Field(default=None, alias="Data")
    message: str | None = Field(default=None, alias="Message")

    model_config = {"populate_by_name": True}


class ProductEanDetailResponse(BaseModel):
    """
    Response for product GTIN/EAN details.

    Documentation: Section 6.1 "Read product GTINs"
    """

    id: int | None = Field(default=None, alias="id")
    product_id: int | None = Field(default=None, alias="product_id")
    ean: str | None = Field(default=None, alias="ean")
    is_approved: bool = Field(default=False, alias="is_approved")
    format: str | None = Field(default=None, alias="format")
    country_id: str | None = Field(default=None, alias="country_id")

    model_config = {"populate_by_name": True}
