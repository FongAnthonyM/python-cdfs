""" timecontentscdfscomponent.py.py
A component for managing time-based contents in a CDFS.
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

# Third-Party Packages #
from dspobjects.time import Timestamp
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

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
    """
    # Attributes #
    _table: type[BaseTimeContentsTable] | None = None

    proxy_type: type[TimeContentsProxy] = TimeContentsProxy

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
    # Contents
    def correct_contents(
        self,
        path: pathlib.Path,
        session: Session | None = None,
        begin: bool = False,
    ) -> None:
        """Corrects the contents of the file.

        Args:
            path: The path to the file.
            session: The SQLAlchemy session to apply the modification. Defaults to None.
            begin: If True, begins a transaction for the operation. Defaults to False.
        """
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
        """Asynchronously corrects the contents of the file.

        Args:
            path: The path to the file.
            session: The SQLAlchemy session to apply the modification. Defaults to None.
            begin: If True, begins a transaction for the operation. Defaults to False.
        """
        if session is not None:
            await self.table.correct_contents_async(session=session, path=path, begin=begin)
        else:
            async with self.create_async_session() as session:
                await self.table.correct_contents_async(session=session, path=path, begin=True)

    # Meta Information
    def get_tz_offsets_distinct(self, session: Session | None = None) -> Timestamp:
        """Gets distinct timezone offsets from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The distinct timezone offsets.
        """
        if session is not None:
            return self.table.get_tz_offsets_distinct(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_tz_offsets_distinct(session=session)

    async def get_tz_offsets_distinct_async(self, session: Session | None = None) -> Timestamp:
        """Asynchronously gets distinct timezone offsets from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The distinct timezone offsets.
        """
        if session is not None:
            return await self.table.get_tz_offsets_distinct_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_tz_offsets_distinct_async(session=session)

    def get_start_datetime(self, session: Session | None = None) -> Timestamp:
        """Gets the start datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The start datetime.
        """
        if session is not None:
            return self.table.get_start_datetime(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_start_datetime(session=session)

    async def get_start_datetime_async(self, session: AsyncSession | None = None) -> Timestamp:
        """Asynchronously gets the start datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The start datetime.
        """
        if session is not None:
            return await self.table.get_start_datetime_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_start_datetime_async(session=session)

    def get_end_datetime(self, session: Session | None = None) -> Timestamp:
        """Gets the end datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The end datetime.
        """
        if session is not None:
            return self.table.get_end_datetime(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_end_datetime(session=session)

    async def get_end_datetime_async(self, session: AsyncSession | None = None) -> Timestamp:
        """Asynchronously gets the end datetime from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            Timestamp: The end datetime.
        """
        if session is not None:
            return await self.table.get_end_datetime_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_end_datetime_async(session=session)

    def get_contents_nanostamps(self, session: Session | None = None) -> tuple[tuple[int, int, int], ...]:
        """Gets all nanostamps from the table.

        Args:
            session: The SQLAlchemy session to use for the query. Defaults to None.

        Returns:
            tuple[tuple[int, int, int], ...]: The nanostamps.
        """
        if session is not None:
            return self.table.get_all_nanostamps(session=session)
        else:
            with self.create_session() as session:
                return self.table.get_all_nanostamps(session=session)

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
            return await self.table.get_all_nanostamps_async(session=session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_all_nanostamps_async(session=session)

    # Contents Proxy #
    def create_contents_proxy(self, swmr: bool = True, **kwargs) -> TimeContentsProxy:
        """Creates a contents proxy for the CDFS component.

        Args:
            swmr: If True, enables single-writer multiple-reader mode. Defaults to True.
            **kwargs: Additional keyword arguments for the proxy.

        Returns:
            TimeContentsProxy: The created contents proxy.
        """
        composite = self._composite()
        return self.proxy_type(
            path=composite.path,
            cdfs_component=self,
            mode=composite.mode,
            swmr=swmr,
            **kwargs,
        )
