"""basetimecontentstable.py
A table which tracks the contents of multiple files with time-related metadata.
"""
# Package Header #
from ..header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
import datetime
from decimal import Decimal
import time
from typing import Any
import uuid
import zoneinfo

# Third-Party Packages #
from baseobjects.operations import timezone_offset
from dspobjects.time import nanostamp, Timestamp
import numpy as np
from sqlalchemy import select, func, lambda_stmt
from sqlalchemy.orm import Mapped, Session, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import BigInteger

# Local Packages #
from .basecontentstable import BaseContentsTable


# Definitions #
# Classes #
class BaseTimeContentsTable(BaseContentsTable):
    """A table which tracks the contents of multiple files with time-related metadata.

    This class extends BaseContentsTable to include time-related metadata such as timezone offset, start and end times,
    and sample rate.

    Class Attributes:
        __mapper_args__ (dict): Mapper arguments for SQLAlchemy.

    Columns:
        tz_offset: The timezone offset in seconds.
        start: The start time in nanoseconds.
        end: The end time in nanoseconds.
        sample_rate: The sample rate of the content.
    """

    # Class Attributes #
    __mapper_args__ = {"polymorphic_identity": "timecontents"}

    # Columns #
    tz_offset: Mapped[int]
    start = mapped_column(BigInteger)
    end = mapped_column(BigInteger)
    sample_rate: Mapped[float]

    # Class Methods #
    @classmethod
    def format_entry_kwargs(
        cls,
        id_: str | uuid.UUID | None = None,
        path: str = "",
        axis: int = 0,
        shape: tuple[int] = (0,),
        timezone: str | datetime.datetime | int | None = None,
        start: datetime.datetime | float | int | np.dtype | None = None,
        end: datetime.datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Formats entry keyword arguments for creating or updating table entries.

        Args:
            id_: The ID of the entry, if specified.
            path: The path of the content. Defaults to an empty string.
            axis: The axis of the content. Defaults to 0.
            shape: The shape of the content. Defaults to (0,).
            timezone: The timezone information. Defaults to None.
            start: The start time. Defaults to None.
            end: The end time. Defaults to None.
            sample_rate: The sample rate of the content. Defaults to None.
            **kwargs: Additional keyword arguments for the entry.

        Returns:
            dict[str, Any]: A dictionary of keyword arguments for the entry.
        """
        kwargs = super().format_entry_kwargs(id_=id_, path=path, axis=axis, shape=shape, **kwargs)

        if isinstance(timezone, str):
            if timezone.lower() == "local" or timezone.lower() == "localtime":
                timezone = time.localtime().tm_gmtoff
            else:
                timezone = zoneinfo.ZoneInfo(timezone)  # Raises an error if the given string is not a time zone.

        tz_offset = timezone_offset(timezone).total_seconds() if isinstance(timezone, datetime.tzinfo) else timezone

        kwargs.update(
            tz_offset=tz_offset,
            start=int(nanostamp(start)),
            end=int(nanostamp(end)),
            sample_rate=float(sample_rate)
        )
        return kwargs

    @classmethod
    def get_tz_offsets_distinct(cls, session: Session) -> tuple | None:
        """Gets distinct timezone offsets from the table.

        Args:
            session: The SQLAlchemy session to use for the query.

        Returns:
            tuple | None: A tuple of distinct timezone offsets, or None if no offsets are found.
        """
        offsets = session.execute(lambda_stmt(lambda: select(cls.tz_offset).distinct()))
        return None if offsets is None else tuple(offsets)

    @classmethod
    async def get_tz_offsets_distinct_async(cls, session: AsyncSession) -> tuple | None:
        """Asynchronously gets distinct timezone offsets from the table.

        Args:
            session: The SQLAlchemy async session to use for the query.

        Returns:
            tuple | None: A tuple of distinct timezone offsets, or None if no offsets are found.
        """
        offsets = await session.execute(lambda_stmt(lambda: select(cls.tz_offset).distinct()))
        return None if offsets is None else tuple(offsets)

    @classmethod
    def get_start_datetime(cls, session: Session) -> Timestamp | None:
        """Gets the earliest start datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query.

        Returns:
            Timestamp | None: The earliest start datetime, or None if no start time is found.
        """
        offset, nanostamp_ = session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.min(cls.start)))).first()
        if nanostamp_ is None:
            return None
        elif offset is None:
            return Timestamp.fromnanostamp(nanostamp_)
        else:
            return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @classmethod
    async def get_start_datetime_async(cls, session: AsyncSession) -> Timestamp | None:
        """Asynchronously gets the earliest start datetime from the table.

        Args:
            session: The SQLAlchemy async session to use for the query.

        Returns:
            Timestamp | None: The earliest start datetime, or None if no start time is found.
        """
        results = await session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.min(cls.start))))
        offset, nanostamp_ = results.first()
        if nanostamp_ is None:
            return None
        elif offset is None:
            return Timestamp.fromnanostamp(nanostamp_)
        else:
            return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @classmethod
    def get_end_datetime(cls, session: Session) -> Timestamp | None:
        """Gets the latest end datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query.

        Returns:
            Timestamp | None: The latest end datetime, or None if no end time is found.
        """
        offset, nanostamp_ = session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.max(cls.end)))).first()
        if nanostamp_ is None:
            return None
        elif offset is None:
            return Timestamp.fromnanostamp(nanostamp_)
        else:
            return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @classmethod
    async def get_end_datetime_async(cls, session: AsyncSession) -> Timestamp | None:
        """Asynchronously gets the latest end datetime from the table.

        Args:
            session: The SQLAlchemy async session to use for the query.

        Returns:
            Timestamp | None: The latest end datetime, or None if no end time is found.
        """
        results = await session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.max(cls.end))))
        offset, nanostamp_ = results.first()
        if nanostamp_ is None:
            return None
        elif offset is None:
            return Timestamp.fromnanostamp(nanostamp_)
        else:
            return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @classmethod
    def get_all_nanostamps(cls, session: Session) -> tuple[tuple[int, int, int], ...]:
        """Gets all nanostamps from the table, ordered by start time.

        Args:
            session: The SQLAlchemy session to use for the query.

        Returns:
            tuple[tuple[int, int, int], ...]: A tuple of tuples containing start, end, and timezone offset.
        """
        statement = lambda_stmt(lambda: select(cls.start, cls.end, cls.tz_offset).order_by(cls.start))
        return tuple(session.execute(statement))

    @classmethod
    async def get_all_nanostamps_async(cls, session: AsyncSession) -> tuple[tuple[int, int, int], ...]:
        """Asynchronously gets all nanostamps from the table, ordered by start time.

        Args:
            session: The SQLAlchemy async session to use for the query.

        Returns:
            tuple[tuple[int, int, int], ...]: A tuple of tuples containing start, end, and timezone offset.
        """
        statement = lambda_stmt(lambda: select(cls.start, cls.end, cls.tz_offset).order_by(cls.start))
        return tuple(await session.execute(statement))

    # Instance Methods #
    def update(self, dict_: dict[str, Any] | None = None, /, **kwargs) -> None:
        """Updates the row of the table with the provided dictionary or keyword arguments.

        Args:
            dict_: A dictionary of attributes/columns to update. Defaults to None.
            **kwargs: Additional keyword arguments for the attributes to update.
        """
        dict_ = ({} if dict_ is None else dict_) | kwargs

        if (timezone := dict_.get("timezone", None)) is not None:
            if isinstance(timezone, str):
                if timezone.lower() == "local" or timezone.lower() == "localtime":
                    timezone = time.localtime().tm_gmtoff
                else:
                    timezone = zoneinfo.ZoneInfo(timezone)  # Raises an error if the given string is not a time zone.

            if isinstance(timezone, datetime.tzinfo):
                self.tz_offset = timezone_offset(timezone).total_seconds()
            else:
                self.tz_offset = timezone

        if (start := dict_.get("start", None)) is not None:
            self.start = int(nanostamp(start))
        if (end := dict_.get("end", None)) is not None:
            self.end = int(nanostamp(end))
        if (sample_rate := dict_.get("sample_rate", None)) is not None:
            self.sample_rate = float(sample_rate)
        super().update(dict_)

    def as_dict(self) -> dict[str, Any]:
        """Creates a dictionary with all the contents of the row.

        Returns:
            dict[str, Any]: A dictionary representation of the row.
        """
        entry = super().as_dict()
        entry.update(
            tz_offset=self.tz_offset,
            start=self.start,
            end=self.end,
            sample_rate=self.sample_rate,
        )
        return entry

    def as_entry(self) -> dict[str, Any]:
        """Creates a dictionary with the entry contents of the row.

        Returns:
            dict[str, Any]: A dictionary representation of the entry.
        """
        entry = super().as_entry()
        tzone = datetime.timezone(datetime.timedelta(seconds=self.tz_offset))
        entry.update(
            tz_offset=tzone,
            start=Timestamp.fromnanostamp(self.start, tzone),
            end=Timestamp.fromnanostamp(self.end, tzone),
            sample_rate=self.sample_rate,
        )
        return entry