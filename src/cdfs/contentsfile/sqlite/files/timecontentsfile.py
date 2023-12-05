"""basetimecontentstable.py
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
import pathlib
from typing import Any

# Third-Party Packages #
from dspobjects.time import Timestamp
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession, async_sessionmaker

# Local Packages #
from ..bases import BaseMetaInformationTable, BaseTimeContentsTable
from .contentsfile import ContentsFile


# Definitions #
# Classes #
class TimeContentsFileAsyncSchema(AsyncAttrs, DeclarativeBase):
    pass


class TimeMetaInformationTable(BaseMetaInformationTable, TimeContentsFileAsyncSchema):
    pass


class TimeContentsTable(BaseTimeContentsTable, TimeContentsFileAsyncSchema):
    pass


class TimeContentsFile(ContentsFile):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    schema: type[DeclarativeBase] = TimeContentsFileAsyncSchema
    meta_information_table: type[BaseMetaInformationTable] = TimeMetaInformationTable
    contents: type[BaseTimeContentsTable] = TimeContentsTable

    # Magic Methods #
    # Construction/Destruction
    def get_start_datetime(self, session: Session | None = None) -> Timestamp:
        if session is not None:
            return self.contents.get_start_datetime(session=session)
        elif self.is_open:
            with self.create_session() as session:
                return self.contents.get_start_datetime(session=session)
        else:
            raise IOError("File not open")

    async def get_start_datetime_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
    ) -> Timestamp:
        if session is not None:
            return await self.contents.get_start_datetime_async(session=session)
        elif self.is_open:
            return await self.contents.get_start_datetime_async(session=self.async_session_maker)
        else:
            raise IOError("File not open")

    def get_end_datetime(self, session: Session | None = None) -> Timestamp:
        if session is not None:
            return self.contents.get_end_datetime(session=session)
        elif self.is_open:
            with self.create_session() as session:
                return self.contents.get_end_datetime(session=session)
        else:
            raise IOError("File not open")

    async def get_end_datetime_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
    ) -> Timestamp:
        if session is not None:
            return await self.contents.get_end_datetime_async(session=session)
        elif self.is_open:
            return await self.contents.get_end_datetime_async(session=self.async_session_maker)
        else:
            raise IOError("File not open")
    
    def get_contents_nanostamps(self, session: Session | None = None) -> tuple[tuple[int, int, int], ...]:
        if session is not None:
            return self.contents.get_all_nanostamps(session=session)
        elif self.is_open:
            with self.create_session() as session:
                return self.contents.get_all_nanostamps(session=session)
        else:
            raise IOError("File not open")

    async def get_contents_nanostamps_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
    ) -> tuple[tuple[int, int, int], ...]:
        if session is not None:
            return await self.contents.get_all_nanostamps_async(session=session)
        elif self.is_open:
            return await self.contents.get_all_nanostamps_async(session=self.async_session_maker)
        else:
            raise IOError("File not open")
