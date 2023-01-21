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
from framestructure import DirectoryTimeFrameInterface, DirectoryTimeFrame

# Local Packages #
from ..contentsfile import ContentsFile


# Definitions #
# Classes #
class CDFS(DirectoryTimeFrame):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    default_frame_type = XLTEKDayFrame
    content_file_type = ContentsFile
    content_file_name = "contents.hdf5"

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        s_id: str | None = None,
        studies_path: pathlib.Path | str | None = None,
        frames: Iterable[DirectoryTimeFrameInterface] | None = None,
        mode: str = 'r',
        update: bool = False,
        open_: bool = True,
        load: bool = True,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.contents_file: ContentsFile | None = None

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                s_id=s_id,
                studies_path=studies_path,
                frames=frames,
                mode=mode,
                update=update,
                load=load,
                open_=open_,
                **kwargs,
            )

    # Instance Methods
    # Constructors/Destructors
    def construct(
        self,
        path: pathlib.Path | str | None = None,
        s_id: str | None = None,
        studies_path: pathlib.Path | str | None = None,
        frames: Iterable[DirectoryTimeFrameInterface] | None = None,
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

        super().construct(path=path, frames=frames, mode=mode, update=update, open_=open_, load=False)

        if self.path is not None and self.studies_path is None:
            self.studies_path = self.path.parent

    def construct_frames(self, open_=False, **kwargs) -> None:
        """Constructs the frames for this object.

        Args:
            open_: Determines if the frames will remain open after construction.
            **kwargs: The keyword arguments to create contained frames.
        """
        if self.contents_file is None:
            self.contents_file = self.content_file_type(file=self.path / self.content_file_name)

        with self.contents_file:
            for path in self.path.glob(self.glob_condition):
                if path not in self.frame_paths:
                    if self.frame_creation_condition(path):
                        self.frames.append(self.frame_type(s_id=self.subject_id, path=path, open_=open_, **kwargs))
                        self.frame_paths.add(path)
        self.frames.sort(key=lambda frame: frame.start_timestamp)
        self.clear_caches()

