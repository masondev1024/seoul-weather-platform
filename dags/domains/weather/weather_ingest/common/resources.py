from enum import Enum

from common.pools import TRINO_WEATHER_HEAVY_POOL as TRINO_HEAVY_POOL


class DbtWorkload(str, Enum):
    LOCAL = "local"
    TRINO = "trino"


__all__ = ["DbtWorkload", "TRINO_HEAVY_POOL"]
