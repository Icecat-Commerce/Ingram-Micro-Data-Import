"""Icecat feature type enumeration."""

from enum import Enum


class IcecatFeatureType(str, Enum):
    """
    Feature type for filtering product features.
    Mirrors: IcecatFeatureType.cs
    """

    GRAY_FILL = "gray_fill"  # GrayFill - filled gray features
    GRAY_EMPTY = "gray_empty"  # GrayEmpty - empty gray features
    BLACK = "black"  # Black - regular features
    ALL = "all"  # All features
