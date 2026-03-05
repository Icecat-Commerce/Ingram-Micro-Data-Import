"""Daily Index XML models for Icecat data."""

from datetime import datetime
from typing import Optional

from pydantic_xml import BaseXmlModel, attr, element


class EANUPC(BaseXmlModel, tag="EAN_UPC"):
    """Individual EAN/UPC code."""

    value: str = attr(name="Value")
    is_approved: str = attr(name="IsApproved", default="0")
    format: str = attr(name="Format", default="")


class EANUPCS(BaseXmlModel, tag="EAN_UPCS"):
    """Container for EAN/UPC codes."""

    ean_upcs: list[EANUPC] = element(tag="EAN_UPC", default_factory=list)


class CountryMarket(BaseXmlModel, tag="Country_Market"):
    """Country market information."""

    value: str = attr(name="Value")


class CountryMarkets(BaseXmlModel, tag="Country_Markets"):
    """Container for country markets."""

    country_markets: list[CountryMarket] = element(tag="Country_Market", default_factory=list)


class ProductIndex(BaseXmlModel, tag="file"):
    """
    Product entry in the daily index file.

    """

    product_id: str = attr(name="Product_ID")
    supplier_id: str = attr(name="Supplier_id")
    catid: str = attr(name="Catid")
    path: Optional[str] = attr(name="path", default=None)
    updated: Optional[str] = attr(name="Updated", default=None)
    quality: Optional[str] = attr(name="Quality", default=None)
    prod_id: Optional[str] = attr(name="Prod_ID", default=None)
    on_market: Optional[str] = attr(name="On_Market", default=None)
    model_name: Optional[str] = attr(name="Model_Name", default=None)
    product_view: Optional[str] = attr(name="Product_View", default=None)
    high_pic: Optional[str] = attr(name="HighPic", default=None)
    high_pic_size: Optional[str] = attr(name="HighPicSize", default=None)
    high_pic_width: Optional[str] = attr(name="HighPicWidth", default=None)
    high_pic_height: Optional[str] = attr(name="HighPicHeight", default=None)
    date_added: Optional[str] = attr(name="Date_Added", default=None)

    eans: Optional[EANUPCS] = element(tag="EAN_UPCS", default=None)
    country_markets: Optional[CountryMarkets] = element(tag="Country_Markets", default=None)


class FilesIndex(BaseXmlModel, tag="files.index"):
    """
    Container for daily index files.

    """

    files: list[ProductIndex] = element(tag="file", default_factory=list)
    generated: str = attr(name="Generated", default="")

    @property
    def generated_datetime(self) -> datetime | None:
        """Parse the Generated attribute to a datetime."""
        if not self.generated:
            return None
        try:
            return datetime.strptime(self.generated, "%Y%m%d%H%M%S")
        except ValueError:
            return None


class DailyIndexFileRoot(BaseXmlModel, tag="ICECAT-interface"):
    """
    Root element of the daily index file.

    """

    files_index: FilesIndex = element(tag="files.index")
