""" metainformationcdfscomponent.py.py

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
from typing import Any

# Third-Party Packages #
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# Local Packages #
from ..tables import BaseMetaInformationTable
from .basetablecdfscomponent import BaseTableCDFSComponent


# Definitions #
# Classes #
class MetaInformationCDFSComponent(BaseTableCDFSComponent):
    # Attributes #
    table_name: str = "meta_information"
    _table: type[BaseMetaInformationTable] | None = None
    _meta_information: dict[str, Any] = {}

    # Properties #
    @property
    def meta_information(self) -> dict[str, Any]:
        if not self._meta_information:
            self.get_meta_information()
        return self._meta_information

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        composite: Any = None,
        table_name: str | None = None,
        init_info: dict[str, Any] | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # Attributes #
        self._meta_information = self._meta_information.copy()

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(composite, table_name, init_info, **kwargs)

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        composite: Any = None,
        table_name: str | None = None,
        init_info: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            **kwargs: Keyword arguments for inheritance.
        """
        self._meta_information.update(init_info)

        super().construct(composite, table_name, **kwargs)

    # File
    def load(self, *args: Any, **kwargs: Any) -> None:
        """Load the component."""
        self.get_meta_information()

    # Table
    def build_tables(self, *args: Any, **kwargs: Any) -> None:
        """Build the table for the component."""
        self.save_cached_meta_information()

    # Meta Information
    def create_meta_information(
        self,
        session: Session | None = None,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            self.table.create_information(session=session, entry=entry, begin=begin, **kwargs)
        else:
            with self.create_session() as session:
                self.table.create_information(session=session, entry=entry, begin=True, **kwargs)

    async def create_meta_information_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            await self.table.create_information_async(
                session=session,
                entry=entry,
                begin=begin,
                **kwargs,
            )
        else:
            async with self.create_async_session() as session:
                await self.table.create_information_async(
                    session=session,
                    entry=entry,
                    begin=begin,
                    **kwargs,
                )

    def get_meta_information(
        self,
        session: Session | None = None,
        as_entry: bool = True,
    ) -> dict[str, Any] | BaseMetaInformationTable:
        if session is not None:
            _meta_information = self.table.get_information(session, as_entry=False)
        else:
            with self.create_session() as session:
                _meta_information = self.table.get_information(session, as_entry=False)

        self._meta_information.update(_meta_information.as_entry())
        return self._meta_information.copy() if as_entry else _meta_information

    async def get_meta_information_async(
        self,
        session: AsyncSession | None = None,
        as_entry: bool = True,
    ) -> dict[str, Any] | BaseMetaInformationTable:
        if session is not None:
            _meta_information = await self.table.get_information_async(session, as_entry=False)
        else:
            async with self.create_async_session() as session:
                _meta_information = await self.table.get_information_async(session, as_entry=False)

        self._meta_information.update(_meta_information.as_entry())
        return self._meta_information.copy() if as_entry else _meta_information

    def set_meta_information(
        self,
        session: Session | None = None,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            self.table.set_information(session=session, entry=entry, begin=begin, **kwargs)
        else:
            with self.create_session() as session:
                self.table.set_information(session=session, entry=entry, begin=True, **kwargs)
        self._meta_information.clear()

    async def set_meta_information_async(
        self,
        session: AsyncSession | None = None,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            await self.table.set_information_async(session=session, entry=entry, begin=begin, **kwargs)
        else:
            async with self.create_async_session() as session:
                await self.table.set_information_async(
                    session=session,
                    entry=entry,
                    begin=True,
                    **kwargs,
                )
        self._meta_information.clear()

    def save_cached_meta_information(
        self,
        session: AsyncSession | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            self.table.set_information(session=session, entry=self._meta_information, begin=begin, **kwargs)
        else:
            with self.create_session() as session:
                self.table.set_information(session=session, entry=self._meta_information, begin=True, **kwargs)

    async def save_cached_meta_information_async(
        self,
        session: AsyncSession | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            await self.table.set_information_async(session=session, entry=self._meta_information, begin=begin, **kwargs)
        else:
            async with self.create_async_session() as session:
                await self.table.set_information_async(
                    session=session,
                    entry=self._meta_information,
                    begin=True,
                    **kwargs,
                )
