"""basetimecontentstable.py
A table which tracks the contents of multiple files with time-related data.
"""
# Package Header #
from ...header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from datetime import datetime, timedelta
from datetime import tzinfo as TZInfo
from datetime import timezone as Timezone
from decimal import Decimal
import time
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

# Third-Party Packages #
from baseobjects.operations import timezone_offset
from dspobjects.time import nanostamp, Timestamp
import numpy as np
from sqlalchemy import select, func, lambda_stmt
from sqlalchemy.orm import Mapped, Session, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import BigInteger

# Local Packages #
from .basecontentstable import BaseContentsTableSchema, ContentsTableManifestation


# Definitions #
# Classes #
class BaseTimeContentsTableSchema(BaseContentsTableSchema):
    """A table which tracks the contents of multiple files with time-related data.

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
    # Base
    @classmethod
    def to_sql_types(cls, dict_: dict[str, Any] | None = None, /, **kwargs) -> dict[str, Any]:
        """Casts Python types of an entry to SQLAlchemy types.
        
        Only table item elements (columns) which must cast to an SQLAlchemy type are cast to SQLAlchemy types. 
        Additionally, all elements are optional, such that they do not need to be provided. This way any subset of the
        elements can cast. For example: when updating a table item, a few elements can updated without providing all 
        elements.  
        
        Args:
            dict_: A dictionary representing the entry with Python types.
            **kwargs: Additional keyword arguments for the entry.

        Returns:
            dict[str, Any]: A dictionary representing the entry with SQLAlchemy types.
        """
        # Format parent entry
        sql_entry = super().to_sql_types(dict_, **kwargs)

        # Format
        if (tz_offset := sql_entry.get("tz_offset", None)) is not None:
            match tz_offset:
                case int():
                    pass
                case ZoneInfo() | Timezone():
                    sql_entry["tz_offset"] = int(timezone_offset(tz_offset).total_seconds())
                case str():
                    if tz_offset.lower() in {"local", "localtime"}:
                        sql_entry["tz_offset"] = time.localtime().tm_gmtoff
                    else:
                        sql_entry["tz_offset"] = int(timezone_offset(ZoneInfo(tz_offset)).total_seconds())

        if (start := sql_entry.get("start", None)) is not None:
            match start:
                case int():
                    pass
                case _:
                    sql_entry["start"] = int(nanostamp(start))
        
        if (end := sql_entry.get("end", None)) is not None:
            match end:
                case int():
                    pass
                case _:
                    sql_entry["end"] = int(nanostamp(end))
        
        if (sample_rate := sql_entry.get("sample_rate", None)) is not None:
            sql_entry["sample_rate"] = float(sample_rate)
        
        # Return formatted entry
        return sql_entry
    
    @classmethod
    def from_sql_types(cls, dict_: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        """Casts SQLAlchemy types of an entry to Python types.
        
        Only table item elements (columns) which must cast to a Python type are cast to Python types. Additionally, all 
        elements are optional, such that they do not need to be provided. This way any subset of the elements can cast. 
        For example: when querying a table item, a few columns can be selected without providing all columns.
        
        Args:
            dict_: A dictionary representing the entry with SQLAlchemy types.
            **kwargs: Additional keyword arguments for the entry.
            
        Returns:
            dict[str, Any]: A dictionary representing the entry with Python types.
        """
        # Format parent entry
        python_entry = super().from_sql_types(dict_, **kwargs)

        # Format
        t_zone = None
        if (tz_offset := python_entry.get("tz_offset", None)) is not None:
            python_entry["tz_offset"] = t_zone = Timezone(timedelta(seconds=tz_offset))
            
        if (start := python_entry.get("start", None)) is not None:
            python_entry["start"] = Timestamp.fromnanostamp(start, t_zone)
            
        if (end := python_entry.get("end", None)) is not None:
            python_entry["end"] = Timestamp.fromnanostamp(end, t_zone)
        
        # Return formatted entry
        return python_entry

    # Queries
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
            return Timestamp.fromnanostamp(nanostamp_, Timezone(timedelta(seconds=offset)))

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
            return Timestamp.fromnanostamp(nanostamp_, Timezone(timedelta(seconds=offset)))

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
            return Timestamp.fromnanostamp(nanostamp_, Timezone(timedelta(seconds=offset)))

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
            return Timestamp.fromnanostamp(nanostamp_, Timezone(timedelta(seconds=offset)))

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


class TimeContentsTableManifestation(ContentsTableManifestation):
    """The manifestation of a ContentsTable.

    Attributes:
        _database: A weak reference to the SQAlchemy database to interface with.
        table_schema: The SQLAlchemy declarative table which this object act as the interface for.

    Args:
        table_schema: The SQLAlchemy declarative table which this object act as the interface for.
        database: The SQAlchemy database to interface with.
        init: Determines if this object will construct.
        **kwargs: Additional keyword arguments.
    """

    # Attributes #
    table_schema: type[BaseTimeContentsTableSchema] | None = None

    # Properties #
    @property
    def start_datetime(self):
        """Gets the start datetime.

        Returns:
            Timestamp: The start datetime.
        """
        return self.get_start_datetime()

    @property
    def end_datetime(self):
        """Gets the end datetime.

        Returns:
            Timestamp: The end datetime.
        """
        return self.get_end_datetime()

    # Instance Methods #
    # Meta Information
    def get_tz_offsets_distinct(self, session: Session | None = None) -> tuple | None:
        """Gets distinct timezone offsets from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The distinct timezone offsets.
        """
        if session is not None:
            return self.table_schema.get_tz_offsets_distinct(session=session)
        else:
            with self.create_session() as session:
                return self.table_schema.get_tz_offsets_distinct(session=session)

    async def get_tz_offsets_distinct_async(self, session: AsyncSession | None = None) -> tuple | None:
        """Asynchronously gets distinct timezone offsets from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The distinct timezone offsets.
        """
        if session is not None:
            return await self.table_schema.get_tz_offsets_distinct_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table_schema.get_tz_offsets_distinct_async(session=session)

    def get_start_datetime(self, session: Session | None = None) -> Timestamp:
        """Gets the start datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The start datetime.
        """
        if session is not None:
            return self.table_schema.get_start_datetime(session=session)
        else:
            with self.create_session() as session:
                return self.table_schema.get_start_datetime(session=session)

    async def get_start_datetime_async(self, session: AsyncSession | None = None) -> Timestamp:
        """Asynchronously gets the start datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The start datetime.
        """
        if session is not None:
            return await self.table_schema.get_start_datetime_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table_schema.get_start_datetime_async(session=session)

    def get_end_datetime(self, session: Session | None = None) -> Timestamp:
        """Gets the end datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The end datetime.
        """
        if session is not None:
            return self.table_schema.get_end_datetime(session=session)
        else:
            with self.create_session() as session:
                return self.table_schema.get_end_datetime(session=session)

    async def get_end_datetime_async(self, session: AsyncSession | None = None) -> Timestamp:
        """Asynchronously gets the end datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The end datetime.
        """
        if session is not None:
            return await self.table_schema.get_end_datetime_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table_schema.get_end_datetime_async(session=session)

    def get_contents_nanostamps(self, session: Session | None = None) -> tuple[tuple[int, int, int], ...]:
        """Gets all nanostamps from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            tuple[tuple[int, int, int], ...]: The nanostamps.
        """
        if session is not None:
            return self.table_schema.get_all_nanostamps(session=session)
        else:
            with self.create_session() as session:
                return self.table_schema.get_all_nanostamps(session=session)

    async def get_contents_nanostamps_async(
        self,
        session: AsyncSession | None = None,
    ) -> tuple[tuple[int, int, int], ...]:
        """Asynchronously gets all nanostamps from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            tuple[tuple[int, int, int], ...]: The nanostamps.
        """
        if session is not None:
            return await self.table_schema.get_all_nanostamps_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table_schema.get_all_nanostamps_async(session=session)
