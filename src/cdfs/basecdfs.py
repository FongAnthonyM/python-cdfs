"""basecdfs.py

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
import pathlib
from typing import ClassVar, Any

# Third-Party Packages #
from baseobjects import BaseComposite
from baseobjects.cachingtools import CachingObject, timed_keyless_cache
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# Local Packages #
from .contentsfile import ContentsFile


# Definitions #
# Classes #
class BaseCDFS(CachingObject, BaseComposite):

    # Class Attributes #
    default_component_types: ClassVar[dict[str, tuple[type, dict[str, Any]]]] = {}

    # Attributes #
    _path: pathlib.Path | None = None
    _is_open: bool = False
    _mode: str = "r"
    _swmr_mode: bool = False

    schema: type[DeclarativeBase] | None = None

    contents_file_type: type[ContentsFile] = ContentsFile
    contents_file_name: str = "contents.sqlite3"
    contents_file: ContentsFile | None = None

    tables: dict[str, type[DeclarativeBase]] = {}

    # Properties #
    @property
    def path(self) -> pathlib.Path:
        """The path to the BaseCDFS."""
        return self._path

    @path.setter
    def path(self, value: str | pathlib.Path) -> None:
        if isinstance(value, pathlib.Path) or value is None:
            self._path = value
        else:
            self._path = pathlib.Path(value)

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def contents_path(self) -> pathlib.Path:
        return self.path / self.contents_file_name

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        mode: str = "r",
        open_: bool = True,
        load: bool = True,
        create: bool = False,
        build: bool = True,
        contents_name: str | None = None,
        *,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # Attributes #
        self.tables = self.tables.copy()

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                mode=mode,
                open_=open_,
                load=load,
                create=create,
                build=build,
                contents_name=contents_name,
                **kwargs,
            )

    def __bool__(self) -> bool:
        return self._is_open

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: pathlib.Path | str | None = None,
        mode: str | None = None,
        open_: bool = True,
        load: bool = True,
        create: bool = False,
        build: bool = True,
        contents_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            path: The path for this proxy to wrap.
            s_id: The subject ID.
            studies_path: The parent directory to this XLTEK study proxy.
            proxies: An iterable holding arrays/objects to store in this proxy.
            mode: Determines if the contents of this proxy are editable or not.
            update: Determines if this proxy will start_timestamp updating or not.
            open_: Determines if the arrays will remain open after construction.
            load: Determines if the arrays will be constructed.
            **kwargs: The keyword arguments to create contained arrays.
        """
        if path is not None:
            self.path = path

        if mode is not None:
            self._mode = mode
            
        if contents_name is not None:
            self.contents_file_name = contents_name

        super().construct(**kwargs)

        if open_ or load or create:
            self.open(load=load, create=create, build=build)

    # File
    def open(
        self,
        mode: str | None = None,
        load: bool = True,
        create: bool = False,
        build: bool = True,
        **kwargs: Any,
    ) -> None:
        if not self._is_open:
            if mode is not None:
                self._mode = mode

            if not self.path.is_dir():
                if create:
                    self.path.mkdir(exist_ok=True)
                else:
                    raise ValueError("CDFS does not exist.")

            if self.contents_path.exists():
                self.open_contents_file(**kwargs)
            elif create:
                self.open_contents_file(create=True, build=build, **kwargs)

            self._is_open = True

            if load:
                self.load_components()

    def close(self):
        if self.contents_file is not None:
            self.contents_file.close()
        self._is_open = False
        return True

    async def close_async(self) -> bool:
        if self.contents_file is not None:
            await self.contents_file.close_async()
        self._is_open = False
        return True

    # Contents File
    def open_contents_file(self, create: bool = False, build: bool = True, **kwargs: Any) -> None:
        if self.contents_file is not None:
            self.contents_file.open(**kwargs)
        elif self.contents_path.is_file():
            self.contents_file = self.contents_file_type(
                path=self.contents_path,
                schema=self.schema,
                open_=True,
                create=False,
                **kwargs,
            )
        elif not self.contents_path.is_file() and create:
            self.contents_file = self.contents_file_type(
                path=self.contents_path,
                schema=self.schema,
                open_=True,
                create=create,
                **kwargs,
            )
            if build and self._mode in {"a", "w"}:
                self.build_tables()
        else:
            raise ValueError("Contents file does not exist.")


    # Components
    def build_tables(self) -> None:
        for component in self.components.values():
            component.build_tables()

    def load_components(self) -> None:
        for component in self.components.values():
            component.load()
