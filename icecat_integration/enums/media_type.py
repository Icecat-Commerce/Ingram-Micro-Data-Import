"""Media type enumeration."""

from enum import Enum


class MediaType(str, Enum):
    """
    Media type values.
    1 = Image, 2 = Rich media
    """

    IMAGE = "1"
    RICH_MEDIA = "2"
