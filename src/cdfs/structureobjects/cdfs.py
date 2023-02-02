""" cdfs.py

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
from collections.abc import Iterable
import pathlib
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject
from framestructure import DirectoryTimeFrameInterface, DirectoryTimeFrame
from hdf5objects import HDF5File

# Local Packages #
from ..contentsfile import ContentsFile, TimeContentFrame


# Definitions #
# Classes #
class CDFS(BaseObject):
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
        mode: str = 'r',
        update: bool = False,
        open_: bool = True,
        load: bool = True,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self._path: pathlib.Path | None = None
        self.mode: str = 'r'

        self.contents_file_name: str = self.default_content_file_name
        self.contents_file: ContentsFile | None = None

        self.data_file_type: type = self.default_data_file_type

        self.data: DirectoryTimeFrameInterface | None = None
        self.child_paths: set[pathlib.Path] = {}

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
    def contents_path(self) -> pathlib.Path:
        return self.path / self.contents_file_name

    # Instance Methods
    # Constructors/Destructors
    def construct(
        self,
        path: pathlib.Path | str | None = None,
        mode: str = 'r',
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

        self.construct_components()
        if self.contents_path.is_file():
            self.construct_contents_file()
            self.construct_data()

    def construct_components(self, **component_kwargs: Any) -> None:
        """Constructs the components.

        Args:
            component_kwargs: The keyword arguments for the components.
        """
        for name, (component, kwargs)  in self.default_component_types.items():
            new_kwargs = component_kwargs.get(name, {})
            temp_kwargs = kwargs | new_kwargs
            self.components[name] = component(composite=self, **temp_kwargs)

    def construct_contents_file(
        self,
        open_: bool = True,
        load: bool | None = None,
        create: bool = False,
        require: bool = False,
        **kwargs: Any,
    ) -> None:
        if load is None:
            load = self.contents_path.is_file()

        self.contents_file = self.contents_file_type(
            file=self.contents_path,
            mode=self.mode,
            open_=open_,
            load=load,
            create=create,
            require=require,
            **kwargs,
        )

    def construct_data(self, **kwargs):
        self.data = self.default_frame_type(
            path=self.path,
            content_map=self.contents_file.components["contents"].get_data_root(),
            **kwargs
        )

    def require(self, **kwargs):
        self.path.mkdir(exist_ok=True)

        if self.contents_file is None:
            self.construct_contents_file(create=True, require=True, **kwargs)
            self.construct_data()
        else:
            self.contents_file.require(**kwargs)
