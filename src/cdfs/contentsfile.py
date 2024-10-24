"""contentsfile.py
Manages the contents file including creating, opening, and modifying the database.
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
    """Manages the contents file including creating, opening, and modifying the database.

    Attributes:
        _path: The file path to the database.
        _engine: The SQLAlchemy engine for synchronous operations.
        _async_engine: The SQLAlchemy engine for asynchronous operations.
        session_maker_kwargs: Keyword arguments for the synchronous session maker.
        _session_maker: Factory for creating synchronous sessions.
        async_session_maker_kwargs: Keyword arguments for the asynchronous session maker.
        _async_session_maker: Factory for creating asynchronous sessions.
        schema: The database schema class.

    Args:
        path: The path to the file.
        schema: The database schema class.
        open_: Whether to open the file.
        create: Whether to create the file.
        init: Whether to initialize the object.
        **kwargs: Additional keyword arguments.
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
        """The path to the file.

        Returns:
            pathlib.Path: The path to the file.
        """
        return self._path

    @path.setter
    def path(self, value: str | pathlib.Path) -> None:
        """Sets the path to the file.

        Args:
            value (str | pathlib.Path): The new path to the file.
        """
        if isinstance(value, pathlib.Path) or value is None:
            self._path = value
        else:
            self._path = pathlib.Path(value)

    @property
    def is_open(self) -> bool:
        """Checks if the file is open.

        Returns:
            bool: True if the file is open, False otherwise.
        """
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
        """Creates a dictionary of attributes which can be used to rebuild this object.

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
            state (dict[str, Any]): The attributes to build this object from.
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
        """Constructs the ContentsFile object.

        Args:
            path: The path to the file.
            schema: The database schema class.
            open_: Whether to open the file.
            create: Whether to create the file.
            **kwargs: Additional keyword arguments.
        """
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
        """Creates the contents file.

        Args:
            path: The path to the file.
            **kwargs: Additional keyword arguments.
        """
        if path is not None:
            self.path = path

        if self._engine is None or path is not None:
            self.create_engine(**kwargs)

        self.schema.metadata.create_all(self._engine)

    async def create_file_async(self, path: str | pathlib.Path | None = None, **kwargs) -> None:
        """Asynchronously creates the contents file.

        Args:
            path: The path to the file.
            **kwargs: Additional keyword arguments.
        """
        if path is not None:
            self.path = path

        if self._async_engine is None or path is not None:
            self.create_engine(**kwargs)

        async with self._async_engine.begin() as conn:
            await conn.run_sync(self.schema.metadata.create_all)

    def open(self, **kwargs) -> "ContentsFile":
        """Opens the contents file.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            ContentsFile: The opened contents file.
        """
        self.create_engine(**kwargs)
        self.build_session_maker()
        self.build_async_session_maker()
        return self

    def close(self) -> bool:
        """Closes the contents file.

        Returns:
            bool: True if the file is closed, False otherwise.
        """
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
        """Asynchronously closes the contents file.

        Returns:
            bool: True if the file is closed, False otherwise.
        """
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
        """Creates the SQLAlchemy engine.

        Args:
            **kwargs: Additional keyword arguments.
        """
        self._engine = create_engine(f"sqlite:///{self._path.as_posix()}", **kwargs)
        self._async_engine = create_async_engine(f"sqlite+aiosqlite:///{self._path.as_posix()}", **kwargs)

    # Session
    def build_session_maker(self, **kwargs) -> sessionmaker:
        """Builds the synchronous session maker.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            sessionmaker: The synchronous session maker.
        """
        self._session_maker = sessionmaker(self._engine, **kwargs)
        return self._session_maker

    def build_async_session_maker(self, **kwargs) -> async_sessionmaker:
        """Builds the asynchronous session maker.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            async_sessionmaker: The asynchronous session maker.
        """
        self._async_session_maker = async_sessionmaker(self._async_engine, **kwargs)
        return self._async_session_maker

    def create_session(self, *args: Any, **kwargs: Any) -> Session:
        """Creates a synchronous session.

        Args:
            *args: Positional arguments for session creation.
            **kwargs: Keyword arguments for session creation.

        Returns:
            Session: A new synchronous session.

        Raises:
            IOError: If the file is not open.
        """
        if not self.is_open:
            raise IOError("File not open")
        return Session(self._engine, *args, **kwargs) if args or kwargs else self._session_maker()

    def create_async_session(self, *args: Any, **kwargs: Any) -> AsyncSession:
        """Creates an asynchronous session.

        Args:
            *args: Positional arguments for session creation.
            **kwargs: Keyword arguments for session creation.

        Returns:
            AsyncSession: A new asynchronous session.

        Raises:
            IOError: If the file is not open.
        """
        if not self.is_open:
            raise IOError("File not open")
        return AsyncSession(self._async_engine, *args, **kwargs) if args or kwargs else self._async_session_maker()