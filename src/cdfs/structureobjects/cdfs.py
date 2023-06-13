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
from collections.abc import Iterable
import pathlib
from typing import Any

# Third-Party Packages #
from baseobjects import BaseComposite
from baseobjects.cachingtools import CachingObject, timed_keyless_cache
from framestructure import DirectoryTimeFrameInterface, DirectoryTimeFrame
from hdf5objects import HDF5File, HDF5Group

# Local Packages #
from ..contentsfile import ContentsFile, TimeContentFrame, TimeContentGroupComponent


# Definitions #
# Classes #
class CDFS(CachingObject, BaseComposite):
    """

    Class Attributes:

    Attributes:

    Args:

    """

    default_component_types: dict[str, tuple[type, dict[str, Any]]] = {}
    default_frame_type: type = TimeContentFrame
    default_data_file_type: type = HDF5File
    default_content_file_name: str = "contents.h5"
    contents_file_type: type = ContentsFile

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
        self.contents_file: ContentsFile | None = None

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
    def swmr_mode(self) -> bool:
        return self._swmr_mode

    @swmr_mode.setter
    def swmr_mode(self, value: bool) -> None:
        self.set_swmr(value)

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

    @property
    def contents_root(self) -> HDF5Group:
        return self.contents_file.contents_root

    @property
    def contents_root_node(self) -> TimeContentGroupComponent:
        return self.contents_file.contents_root_node

    def __bool__(self) -> bool:
        return bool(self.contents_file)

    # Instance Methods
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
            content_map=self.contents_root,
            mode=mode,
            swmr=swmr,
            **kwargs,
        )

    # File
    def open_contents_file(
        self,
        mode: str | None = None,
        load: bool | None = None,
        create: bool = False,
        require: bool = False,
        **kwargs: Any,
    ) -> None:
        if mode is not None:
            self.mode = mode

        if self.contents_path.is_file():
            load = True if load is None else load
        elif not create:
            raise ValueError("Contents file does not exist.")
        else:
            self.path.mkdir(exist_ok=True)

        if self.contents_file is None:
            self.contents_file = self.contents_file_type(
                file=self.contents_path,
                mode=self.mode,
                open_=True,
                load=load,
                create=create,
                require=require,
                swmr=True if self.mode == "r" else False,
                **kwargs,
            )
        else:
            self.contents_file.require(
                mode=self.mode,
                load=load,
                require=require,
                **kwargs,
            )

    def open(
        self,
        mode: str | None = None,
        build: bool = False,
        load: bool | None = None,
        create: bool = False,
        require: bool = False,
        **kwargs: Any,
    ) -> None:
        self.open_contents_file(mode=mode, load=build or load, create=create, require=require, **kwargs)

        if load:
            self.construct_data()

    def close(self):
        self.contents_file.close()

    def correct_contents(self) -> None:
        self.contents_file.components["contents"].correct_contents()

    @abstractmethod
    def build_swmr(self, **kwargs):
        pass

    def set_swmr(self, value: bool) -> None:
        self.contents_file.swmr_mode = value
        self._swmr_mode = value

    @timed_keyless_cache(call_method="clearing_call", local=True)
    def get_start_datetime(self):
        return self.contents_root_node.get_start_datetime()

    @timed_keyless_cache(call_method="clearing_call", local=True)
    def get_end_datetime(self):
        return self.contents_root_node.get_end_datetime()
