"""Feature Groups XML models for Icecat data."""

from typing import Optional

from pydantic_xml import BaseXmlModel, attr, element


class FeatureGroupName(BaseXmlModel, tag="Name"):
    """Feature group name in a specific language."""

    id: int = attr(name="ID")
    value: str = attr(name="Value")
    langid: int = attr(name="langid")


class FeatureGroup(BaseXmlModel, tag="FeatureGroup"):
    """Feature group definition."""

    id: int = attr(name="ID")
    names: list[FeatureGroupName] = element(tag="Name", default_factory=list)


class FeatureGroupsList(BaseXmlModel, tag="FeatureGroupsList"):
    """Container for feature groups."""

    feature_groups: list[FeatureGroup] = element(tag="FeatureGroup", default_factory=list)


class Response(BaseXmlModel, tag="Response"):
    """Response container for feature groups."""

    feature_groups_list: FeatureGroupsList = element(tag="FeatureGroupsList")


class FeatureGroupsRoot(BaseXmlModel, tag="ICECAT-interface"):
    """Root element of the feature groups list file."""

    response: Response = element(tag="Response")
