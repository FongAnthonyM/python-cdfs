"""basecontentstable.py
A node component which implements content information in its dataset.
"""
# Package Header #
from cdfs.header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
import pathlib
from typing import Optional, Any

# Third-Party Packages #
from baseobjects.cachingtools import CachingObject, timed_keyless_cache
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncEngine, AsyncSession, create_async_engine, async_sessionmaker

# Local Packages #
from ..bases import BaseMetaInformationTable, BaseContentsTable


# Definitions #
# Classes #
class ContentsFileAsyncSchema(AsyncAttrs, DeclarativeBase):
    pass


class ContentsMetaInformationTable(BaseMetaInformationTable, ContentsFileAsyncSchema):
    pass


class ContentsTable(BaseContentsTable, ContentsFileAsyncSchema):
    pass


class ContentsFile(CachingObject):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    schema: type[DeclarativeBase] = ContentsFileAsyncSchema
    meta_information_table: type[BaseMetaInformationTable] = ContentsMetaInformationTable
    contents: type[BaseContentsTable] = ContentsTable

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: str | pathlib.Path | None = None,
        open_: bool = False,
        create: bool = False,
        init: bool = True,
        **kwargs,
    ) -> None:
        # New Attributes #
        self._path: pathlib.Path | None = None

        self.engine: Engine | None = None
        self.async_engine: AsyncEngine | None = None
        self._async_session_maker: async_sessionmaker | None = None

        self._meta_information: BaseMetaInformationTable | None = None

        # Parent Attributes #
        super().__init__()

        # Object Construction #
        if init:
            self.construct(path=path, open_=open_, create=create, **kwargs)

    @property
    def path(self) -> pathlib.Path:
        """The path to the file."""
        return self._path

    @path.setter
    def path(self, value: str | pathlib.Path) -> None:
        if isinstance(value, pathlib.Path) or value is None:
            self._path = value
        else:
            self._path = pathlib.Path(value)

    @property
    def is_open(self) -> bool:
        return self.engine is not None

    @property
    def async_session_maker(self) -> async_sessionmaker | None:
        if self._async_session_maker is None:
            self._async_session_maker = async_sessionmaker(self.async_engine)
        return self._async_session_maker

    @async_session_maker.setter
    def async_session_maker(self, value: async_sessionmaker) -> None:
        self._async_session_maker = value

    @property
    def meta_information(self) -> dict:
        if self._meta_information is None:
            return self.load_meta_information()
        else:
            return self._meta_information.as_entry()

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: str | pathlib.Path | None = None,
        open_: bool = False,
        create: bool = False,
        **kwargs,
    ) -> None:
        if path is not None:
            self.path = path

        if create:
            self.create_file()
            self.close()

        if open_:
            self.open(**kwargs)

    # File
    def create_engine(self, **kwargs) -> None:
        self.engine = create_engine(f"sqlite:///{self._path.as_posix()}", **kwargs)
        self.async_engine = create_async_engine(f"sqlite+aiosqlite:///{self._path.as_posix()}", **kwargs)

    def create_file(self, path: str | pathlib.Path | None = None, **kwargs) -> None:
        if path is not None:
            self.path = path

        if self.async_engine is None or path is not None:
            self.create_engine(**kwargs)

        self.schema.metadata.create_all(self.engine)
        self.create_meta_information(begin=True)

    async def create_file_async(self, path: str | pathlib.Path | None = None, **kwargs) -> None:
        if path is not None:
            self.path = path

        if self.async_engine is None or path is not None:
            self.create_engine(**kwargs)

        async with self.async_engine.begin() as conn:
            await conn.run_sync(self.schema.metadata.create_all)
            await self.create_meta_information_async(begin=True)

    def create_session(self) -> Session:
        return Session(self.engine)

    def create_async_session_maker(self, **kwargs) -> async_sessionmaker:
        self._async_session_maker = async_sessionmaker(self.async_engine, **kwargs)
        return self._async_session_maker

    def open(self,  **kwargs) -> "ContentsFile":
        self.create_engine(**kwargs)
        return self

    def close(self) -> bool:
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
        self.async_engine = None
        self._async_session_maker = None
        return self.engine is None

    async def close_async(self) -> bool:
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
        if self.async_engine is not None:
            await self.async_engine.dispose()
            self.async_engine = None
        self._async_session_maker = None
        return self.engine is None

    # Meta Information
    def create_meta_information(
        self,
        session: Session | None = None,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            self.meta_information_table.create_information(session=session, entry=entry, begin=begin, **kwargs)
        elif self.is_open:
            with self.create_session() as session:
                self.meta_information_table.create_information(session=session, entry=entry, begin=True, **kwargs)
        else:
            raise IOError("File not open")

    async def create_meta_information_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            await self.meta_information_table.create_information_async(
                session=session,
                entry=entry,
                begin=begin,
                **kwargs,
            )
        elif self.is_open:
            await self.meta_information_table.create_information_async(
                session=self.async_session_maker, 
                entry=entry, 
                begin=begin, 
                **kwargs,
            )
        else:
            raise IOError("File not open")
    
    def get_meta_information(self, session: Session | None = None, as_entry: bool = True) -> dict[str, Any]:
        if session is not None:
            self._meta_information = self.meta_information_table.get_information(session, as_entry=False)
        elif self.is_open:
            with self.create_session() as session:
                self._meta_information = self.meta_information_table.get_information(session, as_entry=False)
        else:
            raise IOError("File not open")

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
            self._meta_information = await self.meta_information_table.get_information_async(session, as_entry=False)
        elif self.is_open:
            self._meta_information = await self.meta_information_table.get_information_async(
                self.async_session_maker,
                as_entry=False,
            )
        else:
            raise IOError("File not open")

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
            self.meta_information_table.set_information(session=session, entry=entry, begin=begin, **kwargs)
        elif self.is_open:
            with self.create_session() as session:
                self.meta_information_table.set_information(session=session, entry=entry, begin=True, **kwargs)
        else:
            raise IOError("File not open")

    async def set_meta_information_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if session is not None:
            await self.meta_information_table.set_information_async(session=session, entry=entry, begin=begin, **kwargs)
        elif self.is_open:
            await self.meta_information_table.set_information_async(
                session=self.async_session_maker, 
                entry=entry,
                begin=True,
                **kwargs,
            )
        else:
            raise IOError("File not open")

    # Contents
    def correct_contents(
        self,
        path: pathlib.Path,
        session: Session | None = None,
        begin: bool = False,
    ) -> None:
        if session is not None:
            self.contents.correct_contents(session=session, path=path, begin=begin)
        elif self.is_open:
            with self.create_session() as session:
                self.contents.correct_contents(session=session, path=path, begin=True)
        else:
            raise IOError("File not open")

    async def correct_contents_async(
        self,
        path: pathlib.Path,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
        begin: bool = False,
    ) -> None:
        if session is not None:
            await self.contents.correct_contents_async(session=session, path=path, begin=begin)
        elif self.is_open:
            await self.contents.correct_contents_async(session=self.async_session_maker, path=path, begin=True)
        else:
            raise IOError("File not open")
