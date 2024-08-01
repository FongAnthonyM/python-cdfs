""" timecontentscdfscomponent.py.py

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
import pathlib
from typing import Any
from weakref import ref

# Third-Party Packages #
from baseobjects.cachingtools import CachingObject, timed_keyless_cache
from dspobjects.time import Timestamp
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# Local Packages #
from ..arrays import TimeContentsProxy
from ..tables import BaseTimeContentsTable
from .basetablecdfscomponent import BaseTableCDFSComponent


# Definitions #
# Classes #
class TimeContentsCDFSComponent(BaseTableCDFSComponent):
    """A component for managing time-based contents in a CDFS.

    Attributes:
        _table: The table class associated with this component.
        proxy_type: The proxy type for time contents.

    Properties:
        start_datetime: Gets the start datetime.
        end_datetime: Gets the end datetime.
    """
    # Attributes #
    _table: type[BaseTimeContentsTable] | None = None

    proxy_type: type[TimeContentsProxy] = TimeContentsProxy

    # Properties #
    @property
    def start_datetime(self):
        return self.get_start_datetime()

    @property
    def end_datetime(self):
        return self.get_end_datetime()

    # Instance Methods #
    # Contents
    def correct_contents(
        self,
        path: pathlib.Path,
        session: Session | None = None,
        begin: bool = False,
    ) -> None:
        if session is not None:
            self.table.correct_contents(session=session, path=path, begin=begin)
        else:
            with self.create_session() as session:
                self.table.correct_contents(session=session, path=path, begin=True)

    async def correct_contents_async(
        self,
        path: pathlib.Path,
        session: AsyncSession | None = None,
        begin: bool = False,
    ) -> None:
        if session is not None:
            await self.table.correct_contents_async(session=session, path=path, begin=begin)
        else:
            async with self.create_async_session() as session:
                await self.table.correct_contents_async(session=session, path=path, begin=True)

    # Meta Information
    def get_tz_offsets_distinct(self, session: Session | None = None) -> Timestamp:
        if session is not None:
            return self.table.get_tz_offsets_distinct(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_tz_offsets_distinct(session=session)

    async def get_tz_offsets_distinct_async(self, session: Session | None = None) -> Timestamp:
        if session is not None:
            return await self.table.get_tz_offsets_distinct_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_tz_offsets_distinct_async(session=session)

    def get_start_datetime(self, session: Session | None = None) -> Timestamp:
        if session is not None:
            return self.table.get_start_datetime(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_start_datetime(session=session)

    async def get_start_datetime_async(self, session: AsyncSession | None = None) -> Timestamp:
        if session is not None:
            return await self.table.get_start_datetime_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_start_datetime_async(session=session)

    def get_end_datetime(self, session: Session | None = None) -> Timestamp:
        if session is not None:
            return self.table.get_end_datetime(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_end_datetime(session=session)

    async def get_end_datetime_async(self, session: AsyncSession | None = None) -> Timestamp:
        if session is not None:
            return await self.table.get_end_datetime_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_end_datetime_async(session=session)

    def get_contents_nanostamps(self, session: Session | None = None) -> tuple[tuple[int, int, int], ...]:
        if session is not None:
            return self.table.get_all_nanostamps(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_all_nanostamps(session=session)

    async def get_contents_nanostamps_async(
        self,
        session: AsyncSession | None = None,
    ) -> tuple[tuple[int, int, int], ...]:
        if session is not None:
            return await self.table.get_all_nanostamps_async(session=session)
        else:
            return await self.table.get_all_nanostamps_async(session=session)

    # Contents Proxy #
    def create_contents_proxy(self, swmr: bool = True, **kwargs) -> TimeContentsProxy:
        """Creates a contents proxy for the CDFS component.

        Args:
            swmr: If True, enables single-writer multiple-reader mode. Defaults to True.
            **kwargs: Additional keyword arguments for the proxy.

        Returns:
            The created contents proxy.
        """
        composite = self._composite()
        return self.proxy_type(
            path=composite.path,
            cdfs_component=self,
            mode=composite.mode,
            swmr=swmr,
            **kwargs,
        )
