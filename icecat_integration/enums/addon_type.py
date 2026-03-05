"""Product addon type enumeration."""

from enum import Enum


class AddonType(str, Enum):
    """
    Product addon relationship types.
    Matches database enum: C=Crossell, U=Upsell, D=Downsell, W=Warranties, Z=Compatibilities
    """

    CROSSELL = "C"
    UPSELL = "U"
    DOWNSELL = "D"
    WARRANTIES = "W"
    COMPATIBILITIES = "Z"
