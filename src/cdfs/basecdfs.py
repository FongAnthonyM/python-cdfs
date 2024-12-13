"""basecdfs.py
Base class for a Continuous Data File System (CDFS).
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
from pathlib import Path
from typing import ClassVar, Any

# Third-Party Packages #
from baseobjects import BaseComposite
from baseobjects.cachingtools import CachingObject
from sqlalchemy.orm import DeclarativeBase
from sqlalchemyobjects import TableManifestation

# Local Packages #
from .contentsdatabase import ContentsDatabase


# Definitions #
# Classes #
class BaseCDFS(CachingObject, BaseComposite):
    """Base class for a Continuous Data File System (CDFS).

    This class provides the foundational structure and operations for managing a composite distributed file system.
    It integrates caching mechanisms and composite design patterns to handle various components and their interactions.

    Class Attributes:
        default_component_types: A dictionary defining the default component types and their configurations.

    Attributes:
        _path: The file path to the CDFS.
        _is_open: Indicates if the CDFS is currently open.
        _mode: The mode in which the CDFS is opened (e.g., 'r' for read, 'w' for write).
        _swmr_mode: Indicates if Single-Writer-Multiple-Reader mode is enabled.
        schema: The contents database schema class.
        table_map: A map which outlines which table are within the contents database.
        contents_database_type: The type of the contents database file.
        contents_database_name: The name of the contents database file.
        contents_database: The contents database file object.

    Args:
        path: The path to the CDFS.
        mode: The mode in which the CDFS is opened.
        open_: Whether to open the CDFS.
        create: Whether to create the CDFS.
        build: Whether to build the CDFS.
        load: Whether to load the CDFS.
        contents_name: The name of the contents database file.
        init: Whether to initialize the object.
        **kwargs: Additional keyword arguments.
    """

    # Class Attributes #
    default_component_types: ClassVar[dict[str, tuple[type, dict[str, Any]]]] = {}

    # Attributes #
    _path: Path | None = None
    _is_open: bool = False
    _mode: str = "r"
    _swmr_mode: bool = False

    schema: type[DeclarativeBase] | None = None
    table_map: dict[str, tuple[type[TableManifestation], type[DeclarativeBase], dict[str, Any]]] = {}

    contents_database_type: type[ContentsDatabase] = ContentsDatabase
    contents_database_name: str = "contents.sqlite3"
    contents_database: ContentsDatabase | None = None

    # Properties #
    @property
    def path(self) -> Path:
        """Gets the path to the BaseCDFS.

        Returns:
            path: The path to the BaseCDFS.
        """
        return self._path

    @path.setter
    def path(self, value: str | Path) -> None:
        """Sets the path to the BaseCDFS.

        Args:
            value (str | Path): The new path to the BaseCDFS.
        """
        if isinstance(value, Path) or value is None:
            self._path = value
        else:
            self._path = Path(value)

    @property
    def is_open(self) -> bool:
        """Checks if the CDFS is open.

        Returns:
            bool: True if the CDFS is open, False otherwise.
        """
        return self._is_open

    @property
    def mode(self) -> str:
        """Gets the mode in which the CDFS is opened.

        Returns:
            str: The mode in which the CDFS is opened.
        """
        return self._mode

    @property
    def contents_path(self) -> Path:
        """Gets the path to the contents database.

        Returns:
            path: The path to the contents database.
        """
        return self.path / self.contents_database_name

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: Path | str | None = None,
        mode: str = "r",
        open_: bool = True,
        create: bool = False,
        build: bool = True,
        load: bool = True,
        contents_name: str | None = None,
        *,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # Attributes #

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                mode=mode,
                open_=open_,
                create=create,
                load=load,
                build=build,
                contents_name=contents_name,
                **kwargs,
            )

    def __bool__(self) -> bool:
        """Checks if the CDFS is open.

        Returns:
            bool: True if the CDFS is open, False otherwise.
        """
        return self._is_open

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: Path | str | None = None,
        mode: str | None = None,
        open_: bool = True,
        create: bool = False,
        build: bool = True,
        load: bool = True,
        contents_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs the BaseCDFS object.

        Args:
            path: The path to the CDFS.
            mode: The mode in which the CDFS is opened.
            open_: Whether to open the CDFS.
            create: Whether to create the CDFS.
            build: Whether to build the CDFS.
            load: Whether to load the CDFS.
            contents_name: The name of the contents database.
            **kwargs: Additional keyword arguments.
        """
        if path is not None:
            self.path = path

        if mode is not None:
            self._mode = mode

        if contents_name is not None:
            self.contents_database_name = contents_name

        super().construct(**kwargs)

        if open_ or load or create:
            self.open(load=load, create=create, build=build)

    # File
    def open(
        self,
        mode: str | None = None,
        create: bool = False,
        build: bool = True,
        load: bool = True,
        **kwargs: Any,
    ) -> None:
        """Opens the CDFS.

        Args:
            mode: The mode in which the CDFS is opened.
            create: Whether to create the CDFS.
            build: Whether to build the CDFS.
            load: Whether to load the CDFS.
            **kwargs: Additional keyword arguments.
        """
        if not self._is_open:
            if mode is not None:
                self._mode = mode

            if not self.path.is_dir():
                if create:
                    self.path.mkdir(exist_ok=True)
                else:
                    raise ValueError("CDFS does not exist.")

            if self.contents_path.exists():
                self.open_contents_database(**kwargs)
            elif create:
                self.open_contents_database(create=True, build=build, **kwargs)

            self._is_open = True

            if load:
                self.contents_database.load_tables()

    def close(self) -> bool:
        """Closes the CDFS.

        Returns:
            bool: True if the CDFS is closed, False otherwise.
        """
        if self.contents_database is not None:
            self.contents_database.close()
        self._is_open = False
        return True

    async def close_async(self) -> bool:
        """Asynchronously closes the CDFS.

        Returns:
            bool: True if the CDFS is closed, False otherwise.
        """
        if self.contents_database is not None:
            await self.contents_database.close_async()
        self._is_open = False
        return True

    # Contents Database
    def open_contents_database(self, create: bool = False, build: bool = True, **kwargs: Any) -> None:
        """Opens the contents database.

        Args:
            create: Whether to create the contents database.
            build: Whether to build the contents database.
            **kwargs: Additional keyword arguments.
        """
        new_kwargs = {
            "path": self.contents_path, 
            "schema": self.schema, 
            "table_map": self.table_map,
            "open_": True,
            "create": True,
        } | kwargs
        if self.contents_database is not None:
            self.contents_database.open(**kwargs)
        elif not self.contents_path.is_file():
            self.contents_database = self.contents_database_type(**new_kwargs)
            if create and build and self._mode in {"a", "w"}:
                self.contents_database.build_tables()
        else:
            raise ValueError("Contents database does not exist.")
