"""basetimecontentstable.py

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
import datetime
from decimal import Decimal
import time
from typing import Any
import uuid
import zoneinfo

# Third-Party Packages #
from baseobjects import singlekwargdispatch
from baseobjects.operations import timezone_offset
from dspobjects.time import nanostamp, Timestamp
import numpy as np
from sqlalchemy import select, func, lambda_stmt
from sqlalchemy.orm import Mapped, Session, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.types import BigInteger

# Local Packages #
from .basecontentstable import BaseContentsTable


# Definitions #
# Classes #
class BaseTimeContentsTable(BaseContentsTable):
    __mapper_args__ = {"polymorphic_identity": "timecontents"}
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
    def get_start_datetime(cls, session: Session) -> Timestamp:
        offset, nanostamp_ = session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.min(cls.start)))).first()
        return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def get_start_datetime_async(cls, session: async_sessionmaker[AsyncSession] | AsyncSession) -> Timestamp:
        raise TypeError(f"{type(session)} is not a valid type.")

    @get_start_datetime_async.register(async_sessionmaker)
    @classmethod
    async def _get_start_datetime_async(cls, session: async_sessionmaker[AsyncSession]) -> Timestamp:
        statement = lambda_stmt(lambda: select(cls.tz_offset, func.min(cls.start)))
        async with session() as async_session:
            results = await async_session.execute(statement)
        
        offset, nanostamp_ = results.first()
        return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @get_start_datetime_async.register(AsyncSession)
    @classmethod
    async def _get_start_datetime_async(cls, session: AsyncSession) -> Timestamp:
        results = await session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.min(cls.start))))
        offset, nanostamp_ = results.first()
        return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @classmethod
    def get_end_datetime(cls, session: Session) -> Timestamp:
        offset, nanostamp_ = session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.max(cls.end)))).first()
        return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def get_end_datetime_async(cls, session: async_sessionmaker[AsyncSession] | AsyncSession) -> Timestamp:
        raise TypeError(f"{type(session)} is not a valid type.")

    @get_end_datetime_async.register(async_sessionmaker)
    @classmethod
    async def _get_end_datetime_async(cls, session: async_sessionmaker[AsyncSession]) -> Timestamp:
        statement = lambda_stmt(lambda: select(cls.tz_offset, func.max(cls.end)))
        async with session() as async_session:
            results = await async_session.execute(statement)

        offset, nanostamp_ = results.first()
        return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @get_end_datetime_async.register(AsyncSession)
    @classmethod
    async def _get_end_datetime_async(cls, session: AsyncSession) -> Timestamp:
        results = await session.execute(lambda_stmt(lambda: select(cls.tz_offset, func.max(cls.end))))
        offset, nanostamp_ = results.first()
        return Timestamp.fromnanostamp(nanostamp_, datetime.timezone(datetime.timedelta(seconds=offset)))

    @classmethod
    def get_all_nanostamps(cls, session: Session) -> tuple[tuple[int, int, int], ...]:
        statement = lambda_stmt(lambda: select(cls.start, cls.end, cls.tz_offset).order_by(cls.start))
        return tuple(session.execute(statement))

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def get_all_nanostamps_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
    ) -> tuple[tuple[int, int, int], ...]:
        raise TypeError(f"{type(session)} is not a valid type.")

    @get_all_nanostamps_async.register(async_sessionmaker)
    @classmethod
    async def _get_all_nanostamps_async(
        cls,
        session: async_sessionmaker[AsyncSession],
    ) -> tuple[tuple[int, int, int], ...]:
        statement = lambda_stmt(lambda: select(cls.start, cls.end, cls.tz_offset).order_by(cls.start))
        async with session() as async_session:
            return tuple(await async_session.execute(statement))

    @get_all_nanostamps_async.register(AsyncSession)
    @classmethod
    async def _get_all_nanostamps_async(cls, session: AsyncSession) -> tuple[tuple[int, int, int], ...]:
        statement = lambda_stmt(lambda: select(cls.start, cls.end, cls.tz_offset).order_by(cls.start))
        return tuple(await session.execute(statement))

    # Instance Methods #
    def update(self, dict_: dict[str, Any] | None = None, /, **kwargs) -> None:
        dict_ = ({} if dict_ is None else dict_) | kwargs

        if timezone := dict_.get("time_zone", None) is not None:
            if isinstance(timezone, str):
                if timezone.lower() == "local" or timezone.lower() == "localtime":
                    timezone = time.localtime().tm_gmtoff
                else:
                    timezone = zoneinfo.ZoneInfo(timezone)  # Raises an error if the given string is not a time zone.

            if isinstance(timezone, datetime.tzinfo):
                self.tz_offset = timezone_offset(timezone).total_seconds()
            else:
                self.tz_offset = timezone

        if start := dict_.get("start", None) is not None:
            self.start = nanostamp(start)
        if end := dict_.get("end", None) is not None:
            self.end = nanostamp(end)
        if sample_rate := dict_.get("sample_rate", None) is not None:
            self.sample_rate = float(sample_rate)
        super().update(dict_)
        
    def as_dict(self) -> dict[str, Any]:
        entry = super().as_dict()
        entry.update(
            tz_offset=self.tz_offset,
            start=self.start,
            end=self.end,
            sample_rate=self.sample_rate,
        )
        return entry

    def as_entry(self) -> dict[str, Any]:
        entry = super().as_entry()
        tzone = datetime.timezone(datetime.timedelta(seconds=self.tz_offset)),
        entry.update(
            tz_offset=tzone,
            start=Timestamp.fromnanostamp(self.start, tzone),
            end=Timestamp.fromnanostamp(self.end, tzone),
            sample_rate=self.sample_rate,
        )
        return entry
