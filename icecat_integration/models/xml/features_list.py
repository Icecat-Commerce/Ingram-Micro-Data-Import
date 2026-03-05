"""Features List XML models for Icecat data."""

from datetime import datetime
from typing import Optional

from pydantic_xml import BaseXmlModel, attr, element


class Name(BaseXmlModel, tag="Name"):
    """Feature name in a specific language."""

    id: int = attr(name="ID")
    langid: int = attr(name="langid")
    value: str = attr(name="Value", default="")


class Names(BaseXmlModel, tag="Names"):
    """Container for feature names."""

    names: list[Name] = element(tag="Name", default_factory=list)


class Description(BaseXmlModel, tag="Description"):
    """Feature description in a specific language."""

    id: int = attr(name="ID")
    langid: int = attr(name="langid")
    value: str = attr(name="Value", default="")


class Descriptions(BaseXmlModel, tag="Descriptions"):
    """Container for feature descriptions."""

    descriptions: list[Description] = element(tag="Description", default_factory=list)


class Sign(BaseXmlModel, tag="Sign"):
    """Measure sign/unit in a specific language."""

    id: int = attr(name="ID")
    langid: int = attr(name="langid")
    value: Optional[str] = attr(name="Value", default=None)


class Signs(BaseXmlModel, tag="Signs"):
    """Container for measure signs."""

    signs: list[Sign] = element(tag="Sign", default_factory=list)


class Measure(BaseXmlModel, tag="Measure"):
    """Measure definition (units)."""

    id: int = attr(name="ID")
    sign: Optional[str] = attr(name="Sign", default=None)
    updated: Optional[str] = attr(name="Updated", default=None)
    signs: Optional[Signs] = element(tag="Signs", default=None)


class RestrictedValue(BaseXmlModel, tag="RestrictedValue"):
    """Restricted value for enumerated features."""

    id: int = attr(name="ID")
    value: str = attr(name="Value", default="")
    langid: Optional[int] = attr(name="langid", default=None)


class RestrictedValues(BaseXmlModel, tag="RestrictedValues"):
    """Container for restricted values."""

    values: list[RestrictedValue] = element(tag="RestrictedValue", default_factory=list)


class Feature(BaseXmlModel, tag="Feature"):
    """Feature (specification) definition."""

    id: int = attr(name="ID")
    feature_class: Optional[int] = attr(name="Class", default=None)
    default_display_unit: Optional[int] = attr(name="DefaultDisplayUnit", default=None)
    type: Optional[str] = attr(name="Type", default=None)
    updated: Optional[str] = attr(name="Updated", default=None)

    descriptions: Optional[Descriptions] = element(tag="Descriptions", default=None)
    measure: Optional[Measure] = element(tag="Measure", default=None)
    names: Optional[Names] = element(tag="Names", default=None)
    restricted_values: Optional[RestrictedValues] = element(tag="RestrictedValues", default=None)

    @property
    def date_last_updated(self) -> datetime | None:
        """Parse the Updated attribute to a datetime."""
        if not self.updated:
            return None
        try:
            return datetime.strptime(self.updated, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


class FeaturesList(BaseXmlModel, tag="FeaturesList"):
    """Container for features."""

    features: list[Feature] = element(tag="Feature", default_factory=list)


class FeaturesListResponse(BaseXmlModel, tag="Response"):
    """Response container for features list."""

    features_list: FeaturesList = element(tag="FeaturesList")


class FeaturesListRoot(BaseXmlModel, tag="ICECAT-interface"):
    """Root element of the features list file."""

    response: FeaturesListResponse = element(tag="Response")
