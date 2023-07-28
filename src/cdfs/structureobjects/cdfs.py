"""cdfs.py

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
from abc import abstractmethod
import pathlib
from typing import Any

# Third-Party Packages #
from baseobjects import BaseComposite
from baseobjects.cachingtools import CachingObject, timed_keyless_cache
from framestructure import DirectoryTimeFrameInterface
from hdf5objects import HDF5File, HDF5Group

# Local Packages #
from ..contentsfile import TimeContentsFile, TimeContentsFrame, BaseContentsTable


# Definitions #
# Classes #
class CDFS(CachingObject, BaseComposite):
    """

    Class Attributes:

    Attributes:

    Args:

    """

    default_component_types: dict[str, tuple[type, dict[str, Any]]] = {}
    default_frame_type: type = TimeContentsFrame
    default_data_file_type: type = HDF5File
    default_content_file_name: str = "contents.sqlite3"
    contents_file_type: type = TimeContentsFile

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        mode: str = "r",
        update: bool = False,
        open_: bool = True,
        load: bool = True,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self._path: pathlib.Path | None = None
        self.mode: str = "r"
        self._swmr_mode: bool = False

        self.contents_file_name: str = self.default_content_file_name
        self.contents_file: TimeContentsFile | None = None

        self.data_file_type: type = self.default_data_file_type

        self.data: DirectoryTimeFrameInterface | None = None

        self.components: dict[str, Any] = {}

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                mode=mode,
                update=update,
                load=load,
                open_=open_,
                **kwargs,
            )

    @property
    def path(self) -> pathlib.Path:
        """The path to the CDFS."""
        return self._path

    @path.setter
    def path(self, value: str | pathlib.Path) -> None:
        if isinstance(value, pathlib.Path) or value is None:
            self._path = value
        else:
            self._path = pathlib.Path(value)

    @property
    def start_datetime(self):
        try:
            return self.get_start_datetime.caching_call()
        except AttributeError:
            return self.get_start_datetime()

    @property
    def end_datimetime(self):
        try:
            return self.get_end_datetime.caching_call()
        except AttributeError:
            return self.get_end_datetime()

    @property
    def contents_path(self) -> pathlib.Path:
        return self.path / self.contents_file_name

    def __bool__(self) -> bool:
        return bool(self.contents_file)

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: pathlib.Path | str | None = None,
        mode: str = "r",
        update: bool = False,
        open_: bool = False,
        load: bool = False,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            path: The path for this frame to wrap.
            s_id: The subject ID.
            studies_path: The parent directory to this XLTEK study frame.
            frames: An iterable holding frames/objects to store in this frame.
            mode: Determines if the contents of this frame are editable or not.
            update: Determines if this frame will start_timestamp updating or not.
            open_: Determines if the frames will remain open after construction.
            load: Determines if the frames will be constructed.
            **kwargs: The keyword arguments to create contained frames.
        """
        if path is not None:
            self.path = path

        if mode is not None:
            self.mode = mode

        super().construct(**kwargs)

        if open_:
            self.open(load=load)

    # CDFS Data
    def construct_data(self, mode: str = "r", swmr: bool = True, **kwargs):
        self.data = self.default_frame_type(
            path=self.path,
            contents_file=self.contents_file,
            mode=mode,
            swmr=swmr,
            **kwargs,
        )

    # File
    def open_contents_file(
        self,
        create: bool = False,
        async_: bool = False,
        **kwargs: Any,
    ) -> None:
        if not create:
            raise ValueError("Contents file does not exist.")
        else:
            self.path.mkdir(exist_ok=True)

        if self.contents_file is None:
            self.contents_file = self.contents_file_type(
                path=self.contents_path,
                open_=True,
                create=create,
                async_=async_,
                **kwargs,
            )
        else:
            self.contents_file.open(async_=async_, **kwargs)

    def open(
        self,
        load: bool | None = None,
        create: bool = False,
        async_: bool = False,
        **kwargs: Any,
    ) -> None:
        self.open_contents_file(create=create, async_=async_, **kwargs)

        if load:
            self.construct_data()

    def close(self):
        self.contents_file.close()

    def correct_contents(self) -> None:
        self.contents_file.contents.correct_contents()

    @timed_keyless_cache(call_method="clearing_call", local=True)
    def get_start_datetime(self):
        with self.contents_file.create_session() as session:
            return self.contents_file.contents.get_start_datetime(session=session)

    @timed_keyless_cache(call_method="clearing_call", local=True)
    def get_end_datetime(self):
        with self.contents_file.create_session() as session:
            return self.contents_file.contents.get_end_datetime(session=session)
