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
from .basecdfscomponent import BaseCDFSComponent


# Definitions #
# Classes #
class MetaInformationCDFSComponent(BaseCDFSComponent):
    # Attributes #
    table_name: str = "meta_information"
    _table: type[BaseMetaInformationTable] | None = None
    _meta_information: BaseMetaInformationTable | None = None

    # Properties #
    @property
    def meta_information(self) -> dict:
        if self._meta_information is None:
            return self.get_meta_information(as_entry=True)
        else:
            return self._meta_information.as_entry()

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        composite: Any = None,
        table_name: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(composite, table_name, **kwargs)

    # Instance Methods #
    # Constructors/Destructors
    def construct(self, composite: Any = None, table_name: str | None = None, **kwargs: Any) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            **kwargs: Keyword arguments for inheritance.
        """
        if table_name is not None:
            self.table_name = table_name

        super().construct(composite=composite, **kwargs)

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
            self._meta_information = self.table.get_information(session, as_entry=False)
        else:
            with self.create_session() as session:
                self._meta_information = self.table.get_information(session, as_entry=False)

        if as_entry:
            return self._meta_information.as_entry()
        else:
            return self._meta_information

    async def get_meta_information_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
        as_entry: bool = True,
    ) -> dict[str, Any] | BaseMetaInformationTable:
        if session is not None:
            self._meta_information = await self.table.get_information_async(session, as_entry=False)
        else:
            async with self.create_async_session() as session:
                self._meta_information = await self.table.get_information_async(
                    session,
                    as_entry=False,
                )

        if as_entry:
            return self._meta_information.as_entry()
        else:
            return self._meta_information

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

    async def set_meta_information_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
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
