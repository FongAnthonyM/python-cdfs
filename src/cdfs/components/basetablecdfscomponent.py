"""basecdfscomponent.py.py

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
from typing import Any, Iterable

# Third-Party Packages #
from sqlalchemy import Result
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

# Local Packages #
from ..tables import BaseTable
from .basecdfscomponent import BaseCDFSComponent


# Definitions #
# Classes #
class BaseTableCDFSComponent(BaseCDFSComponent):
    """A basic component object.

    Attributes:
        _composite: A weak reference to the object which this object is a component of.

    Args:
        composite: The object which this object is a component of.
        init: Determines if this object will construct.
        **kwargs: Keyword arguments for inheritance.
    """

    # Attributes #
    table_name: str = ""
    _table: type[BaseTable] | None = None

    # Properties #
    @property
    def table(self) -> type[BaseTable]:
        if self._table is None:
            self._table = self._composite().tables[self.table_name]
        return self._table

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

    # Table
    def get_all(self, session: Session | None = None, as_entries: bool = False) -> Result | list[dict[str, Any]]:
        """Fetches all entries from the table.

        Args:
            session: The SQLAlchemy session to use for the query.
            as_entries: If True, returns a list of dictionaries representing the entries; otherwise, returns a Result.

        Returns:
            The result of the query, either as a Result object or as a list of dictionaries.
        """
        if session is not None:
            return self.table.get_all(session, as_entries=as_entries)
        else:
            with self.create_session() as session:
                return self.table.get_all(session, as_entries=as_entries)

    async def get_all_async(
        self,
        session: AsyncSession | None = None,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        if session is not None:
            return await self.table.get_all_async(session, as_entries=as_entries)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_all_async(session, as_entries=as_entries)
    
    def insert(
        self,
        item: Any = None,
        entry: dict[str, Any] | None = None,
        session: Session | None = None,
        as_entry: bool = False,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            self.table.insert(session, item, entry, as_entry, begin, **kwargs)
        else:
            with self.create_session() as session:
                self.table.insert(session, item, entry, as_entry, begin, **kwargs)

    async def insert_async(
        self,
        item: Any = None,
        entry: dict[str, Any] | None = None,
        session: AsyncSession | None = None,
        as_entry: bool = False,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            await self.table.insert_async(session, item, entry, as_entry, begin, **kwargs)
        else:
            async with self.create_async_session() as session:
                await self.table.insert_async(session, item, entry, as_entry, begin, **kwargs)
    
    def insert_all(
        self,
        items: Iterable[Any] = (),
        session: Session | None = None,
        as_entries: bool = False,
        begin: bool = False,
    ) -> None:
        if session is not None:
            self.table.insert_all(session, items, as_entries, begin)
        else:
            with self.create_session() as session:
                self.table.insert_all(session, items, as_entries, begin)
    
    async def insert_all_async(
        self,
        items: Iterable[Any] = (),
        session: AsyncSession | None = None,
        as_entries: bool = False,
        begin: bool = False,
    ) -> None:
        if session is not None:
            await self.table.insert_all_async(session, items, as_entries, begin)
        else:
            async with self.create_async_session() as session:
                await self.table.insert_all_async(session, items, as_entries, begin)

    def update_entry(
        self,
        entry: dict[str, Any] | None = None,
        session: Session | None = None,
        key: str = "id_",
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            self.table.update_entry(session, entry, key, begin, **kwargs)
        else:
            with self.create_session() as session:
                self.table.update_entry(session, entry, key, begin, **kwargs)

    async def update_entry_async(
        self,
        entry: dict[str, Any] | None = None,
        session: AsyncSession | None = None,
        key: str = "id_",
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            await self.table.update_entry_async(session, entry, key, begin, **kwargs)
        else:
            async with self.create_async_session() as session:
                await self.table.update_entry_async(session, entry, key, begin, **kwargs)

    def update_entries(
        self,
        entries: Iterable[dict[str, Any]] | None = None,
        session: Session | None = None,
        key: str = "id_",
        begin: bool = False,
    ) -> None:
        if session is not None:
            self.table.update_entries(session, entries, key, begin)
        else:
            with self.create_session() as session:
                self.table.update_entries(session, entries, key, begin)

    async def update_entries_async(
        self,
        entries: Iterable[dict[str, Any]] | None = None,
        session: AsyncSession | None = None,
        key: str = "id_",
        begin: bool = False,
    ) -> None:
        if session is not None:
            await self.table.update_entries_async(session, entries, key, begin)
        else:
            async with self.create_async_session() as session:
                await self.table.update_entries_async(session, entries, key, begin)
    
    def delete_item(
        self,
        item: BaseTable,
        session: Session | None = None,
        begin: bool = False,
    ) -> None:
        if session is not None:
            self.table.delete_item(session, item, begin)
        else:
            with self.create_session() as session:
                self.table.delete_item(session, item, begin)

    async def delete_item_async(
        self,
        item: BaseTable,
        session: AsyncSession | None = None,
        begin: bool = False,
    ) -> None:
        if session is not None:
            await self.table.delete_item_async(session, item, begin)
        else:
            async with self.create_async_session() as session:
                await self.table.delete_item_async(session, item, begin)

    def get_last_update_id(self, session: Session | None = None) -> int | None:
        if session is not None:
            return self.table.get_last_update_id(session)
        else:
            with self.create_session() as session:
                return self.table.get_last_update_id(session)

    async def get_last_update_id_async(self, session: AsyncSession | None = None) -> int | None:
        if session is not None:
            return await self.table.get_last_update_id_async(session)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_last_update_id_async(session)

    def get_from_update(
        self,
        update_id: int,
        session: Session | None = None,
        inclusive: bool = True,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        if session is not None:
            return self.table.get_from_update(session, update_id, inclusive, as_entries)
        else:
            with self.create_session() as session:
                return self.table.get_from_update(session, update_id, inclusive, as_entries)
    
    async def get_from_update_async(
        self,
        update_id: int,
        session: AsyncSession | None = None,
        inclusive: bool = True,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        if session is not None:
            return await self.table.get_from_update_async(session, update_id, inclusive, as_entries)
        else:
            async with self.create_async_session() as session:
                return await self.table.get_from_update_async(session, update_id, inclusive, as_entries)

    # Contents
    def correct_contents(
        self,
        path: pathlib.Path,
        session: Session | None = None,
        begin: bool = False,
    ) -> None:
        """Correct the contents of the file."""

    async def correct_contents_async(
        self,
        path: pathlib.Path,
        session: AsyncSession | None = None,
        begin: bool = False,
    ) -> None:
        """Asynchronously correct the contents of the file."""
