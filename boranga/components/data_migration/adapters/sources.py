from enum import Enum


class Source(str, Enum):
    TEC = "TEC"
    TPFL = "TPFL"
    TFAUNA = "TFAUNA"


ALL_SOURCES = [s.value for s in Source]
