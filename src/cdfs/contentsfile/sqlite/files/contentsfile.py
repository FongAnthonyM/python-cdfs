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
from baseobjects import BaseObject
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


class ContentsFile(BaseObject):
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
            self._async_session_maker = async_sessionmaker(self.engine)
        return self._async_session_maker

    @async_session_maker.setter
    def async_session_maker(self, value: async_sessionmaker) -> None:
        self._async_session_maker = value

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

    def create_engine(self, **kwargs) -> None:
        self.engine = create_engine(f"sqlite:///{self._path.as_posix()}", **kwargs)
        self.async_engine = create_async_engine(f"sqlite+aiosqlite:///{self._path.as_posix()}", **kwargs)

    def create_file(self, path: str | pathlib.Path | None = None, **kwargs) -> None:
        if path is not None:
            self.path = path

        if self.async_engine is None or path is not None:
            self.create_engine(**kwargs)

        self.schema.metadata.create_all(self.engine)

    async def create_file_async(self, path: str | pathlib.Path | None = None, **kwargs) -> None:
        if path is not None:
            self.path = path

        if self.async_engine is None or path is not None:
            self.create_engine(**kwargs)

        async with self.async_engine.begin() as conn:
            await conn.run_sync(self.schema.metadata.create_all)

    def create_session(self) -> Session:
        return Session(self.engine)

    def create_async_session_maker(self, **kwargs) -> async_sessionmaker:
        self._async_session_maker = async_sessionmaker(self.async_engine, **kwargs)
        return self._async_session_maker

    def open(self,  **kwargs) -> "ContentsFile":
        self.create_engine(**kwargs)
        return self

    def close(self) -> bool:
        self.engine.dispose()
        self.engine = None
        self.async_engine = None
        self._async_session_maker = None
        return self.engine is None

    async def close_async(self) -> bool:
        self.engine.dispose()
        await self.async_engine.dispose()
        self.engine = None
        self.async_engine = None
        self._async_session_maker = None
        return self.engine is None

    # Meta Information
    def load_meta_information(self, session: Session | None = None) -> dict[str, Any]:
        if session is not None:
            self._meta_information = self.meta_information_table.get_information(session, as_entry=False)
        elif self.is_open:
            with self.create_session() as session:
                self._meta_information = self.meta_information_table.get_information(session, as_entry=False)
        else:
            raise IOError("File not open")

        return self._meta_information.as_entry()

    async def load_meta_information_async(
        self,
        session: async_sessionmaker[AsyncSession] | AsyncSession | None = None,
    ) -> dict[str, Any]:
        if session is not None:
            self._meta_information = await self.meta_information_table.get_information_async(session, as_entry=False)
        elif self.is_open:
            self._meta_information = await self.meta_information_table.get_information_async(
                self.async_session_maker,
                as_entry=False,
            )
        else:
            raise IOError("File not open")

        return self._meta_information.as_entry()

