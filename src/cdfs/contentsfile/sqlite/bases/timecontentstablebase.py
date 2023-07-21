"""contentstablebase.py
A node component which implements content information in its dataset.
"""
# Package Header #
from ....header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from datetime import datetime, tzinfo
from decimal import Decimal
import time
from typing import Any
import uuid
import zoneinfo

# Third-Party Packages #
from baseobjects.operations import timezone_offset
from dspobjects.time import nanostamp
import numpy as np
from sqlalchemy.orm import Mapped
from sqlalchemy.types import BigInteger
from sqlalchemy.orm import mapped_column

# Local Packages #
from .contentstablebase import ContentsTableBase


# Definitions #
# Classes #
class TimeContentsTableBase(ContentsTableBase):
    __mapper_args__ = {"polymorphic_identity": "timecontents"}
    tz_offset: Mapped[int]
    start: Mapped = mapped_column(BigInteger)
    end: Mapped = mapped_column(BigInteger)
    sample_rate: Mapped[float]

    @classmethod
    def format_entry_kwargs(
        cls,
        id_: str | uuid.UUID | None = None,
        path: str = "",
        axis: int = 0,
        min_shape: tuple[int] = (0,),
        max_shape: tuple[int] = (0,),
        timezone: str | tzinfo | int | None = None,
        start: datetime | float | int | np.dtype | None = None,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        **kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(timezone, str):
            if timezone.lower() == "local" or timezone.lower() == "localtime":
                timezone = time.localtime().tm_gmtoff
            else:
                timezone = zoneinfo.ZoneInfo(timezone)  # Raises an error if the given string is not a time zone.

        tz_offset = timezone_offset(timezone).total_seconds() if isinstance(timezone, tzinfo) else timezone

        kwargs.update(
            id_=uuid.uuid4() if id_ is None else uuid.UUID(hex=id_) if isinstance(id_, str) else id_,
            path=path,
            axis=axis,
            min_shape=str(min_shape).strip("()"),
            max_shape=str(max_shape).strip("()"),
            tz_offset=tz_offset,
            start=nanostamp(start),
            end=nanostamp(end),
            sample_rate=float(sample_rate)
        )
        return kwargs

