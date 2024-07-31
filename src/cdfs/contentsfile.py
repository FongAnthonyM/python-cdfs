"""contentsfile.py

"""
# Package Header #
from .header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from asyncio import run
import pathlib
from typing import Any

# Third-Party Packages #
from baseobjects.cachingtools import CachingObject
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine, async_sessionmaker

# Local Packages #


# Definitions #
# Classes #
class ContentsFile(CachingObject):
    """An object interface for the contents file.

    Attributes:

    Args:

    """
    # Attributes #
    _path: pathlib.Path | None = None

    _engine: Engine | None = None
    _async_engine: AsyncEngine | None = None

    session_maker_kwargs: dict[str, Any] = {}
    _session_maker: sessionmaker | None = None

    async_session_maker_kwargs: dict[str, Any] = {}
    _async_session_maker: async_sessionmaker | None = None

    schema: type[DeclarativeBase] | None = None

    # Properties #
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
        return self._engine is not None and self._async_engine is not None

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: str | pathlib.Path | None = None,
        schema: type[DeclarativeBase] | None = None,
        open_: bool = False,
        create: bool = False,
        init: bool = True,
        **kwargs,
    ) -> None:
        # New Attributes #
        self.session_maker_kwargs = self.session_maker_kwargs.copy()
        self.async_session_maker_kwargs = self.async_session_maker_kwargs.copy()

        # Parent Attributes #
        super().__init__()

        # Object Construction #
        if init:
            self.construct(path, schema, open_, create, **kwargs)

    # Pickling
    def __getstate__(self) -> dict[str, Any]:
        """Creates a dictionary of attributes which can be used to rebuild this object

        Returns:
            dict: A dictionary of this object's attributes.
        """
        state = super().__getstate__()
        state["is_open"] = self.is_open
        for name in ("_engine", "_async_engine", "_session_maker", "_async_session_maker"):
            if name in state:
                del state[name]
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Builds this object based on a dictionary of corresponding attributes.

        Args:
            state: The attributes to build this object from.
        """
        was_open = state.pop("is_open")
        super().__setstate__(state=state)
        if was_open:
            self.open()

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: str | pathlib.Path | None = None,
        schema: type[DeclarativeBase] | None = None,
        open_: bool = False,
        create: bool = False,
        **kwargs,
    ) -> None:
        if path is not None:
            self.path = path

        if schema is not None:
            self.schema = schema

        if create:
            self.create_file()
            self.close()

        if open_:
            self.open(**kwargs)

        super().construct()

    # File
    def create_file(self, path: str | pathlib.Path | None = None, **kwargs) -> None:
        if path is not None:
            self.path = path

        if self._engine is None or path is not None:
            self.create_engine(**kwargs)

        self.schema.metadata.create_all(self._engine)

    async def create_file_async(self, path: str | pathlib.Path | None = None, **kwargs) -> None:
        if path is not None:
            self.path = path

        if self._async_engine is None or path is not None:
            self.create_engine(**kwargs)

        async with self._async_engine.begin() as conn:
            await conn.run_sync(self.schema.metadata.create_all)

    def open(self, **kwargs) -> "ContentsFile":
        self.create_engine(**kwargs)
        self.build_session_maker()
        self.build_async_session_maker()
        return self

    def close(self) -> bool:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._session_maker = None
        if self._async_engine is not None:
            run(self._async_engine.dispose())
            self._async_engine = None
        self._async_session_maker = None
        return self._engine is None

    async def close_async(self) -> bool:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        if self._async_engine is not None:
            await self._async_engine.dispose()
            self._async_engine = None
        self._async_session_maker = None
        return self._engine is None

    # Engine
    def create_engine(self, **kwargs) -> None:
        self._engine = create_engine(f"sqlite:///{self._path.as_posix()}", **kwargs)
        self._async_engine = create_async_engine(f"sqlite+aiosqlite:///{self._path.as_posix()}", **kwargs)

    # Session
    def build_session_maker(self, **kwargs) -> sessionmaker:
        self._session_maker = sessionmaker(self._engine, **kwargs)
        return self._session_maker

    def build_async_session_maker(self, **kwargs) -> async_sessionmaker:
        self._async_session_maker = async_sessionmaker(self._async_engine, **kwargs)
        return self._async_session_maker

    def create_session(self, *args: Any, **kwargs: Any) -> Session:
        if not self.is_open:
            raise IOError("File not open")
        return Session(self._engine, *args, **kwargs) if args or kwargs else self._session_maker()

    def create_async_session(self, *args: Any, **kwargs: Any) -> AsyncSession:
        if not self.is_open:
            raise IOError("File not open")
        return AsyncSession(self._async_engine, *args, **kwargs) if args or kwargs else self._async_session_maker()
