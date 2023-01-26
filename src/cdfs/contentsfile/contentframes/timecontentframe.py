""" timecontentframe.py

"""
# Package Header #
from ...header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from collections.abc import Iterable
from decimal import Decimal
import pathlib
from typing import Any

# Third-Party Packages #
from baseobjects.cachingtools import timed_keyless_cache
from dspobjects.time import Timestamp, nanostamp
from framestructure import DirectoryTimeFrame, DirectoryTimeFrameInterface
from hdf5objects import HDF5Dataset
import numpy as np

# Local Packages #
from ..contentmaps import OrderNodeMap, TimeNodeMap, TimeLeafMap


# Definitions #
# Classes #
class TimeContentFrame(DirectoryTimeFrame):
    """A DirectoryTimeFrame object built with information from a dataset which maps out its contents.

    Class Attributes:
        default_node_frame_type: The default frame type to create when making a node.

    Attributes:
        content_map: A HDF5Dataset with the mapping information for creating the frame structure.
        node_frame_type: The frame type to create when making a node.

    Args:
        path: The path for this frame to wrap.
        content_map: A HDF5Dataset with the mapping information for creating the frame structure.
        frames: An iterable holding frames/objects to store in this frame.
        mode: Determines if the contents of this frame are editable or not.
        update: Determines if this frame will start_timestamp updating or not.
        open_: Determines if the frames will remain open after construction.
        build: Determines if the frames will be constructed.
        **kwargs: The keyword arguments to create contained frames.
        init: Determines if this object will construct.
    """
    default_node_frame_type: DirectoryTimeFrameInterface | None = None
    default_node_map: type = TimeNodeMap
    default_leaf_map: type = TimeLeafMap

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        content_map: HDF5Dataset | None = None,
        frames: Iterable[DirectoryTimeFrameInterface] | None = None,
        mode: str = 'a',
        update: bool = False,
        open_: bool = False,
        build: bool = True,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.content_map: HDF5Dataset | None = None
        self.node_frame_type: type | None = self.default_node_frame_type
        self.node_map: type = self.default_node_map
        self.leaf_map: type = self.default_leaf_map

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                content_map=content_map,
                frames=frames,
                mode=mode,
                update=update,
                open_=open_,
                build=build,
                **kwargs,
            )

    @property
    def start_datetime(self) -> Timestamp | None:
        """The start datetime of this frame."""
        return self.start_datetimes[0] if self.content_map is not None else None

    @property
    def start_nanostamp(self) -> float | None:
        """The start timestamp of this frame."""
        return self.start_nanostamps[0] if self.content_map is not None else None

    @property
    def start_timestamp(self) -> float | None:
        """The start timestamp of this frame."""
        return self.start_timestamps[0] if self.content_map is not None else None

    @property
    def end_datetime(self) -> Timestamp | None:
        """The end datetime of this frame."""
        return self.end_datetimes[-1] if self.content_map is not None else None

    @property
    def end_nanostamp(self) -> float | None:
        """The end timestamp of this frame."""
        return self.end_nanostamps[-1] if self.content_map is not None else None

    @property
    def end_timestamp(self) -> float | None:
        """The end timestamp of this frame."""
        return self.end_timestamps[-1] if self.content_map is not None else None

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: pathlib.Path | str | None = None,
        content_map: HDF5Dataset | None = None,
        frames: Iterable[DirectoryTimeFrameInterface] | None = None,
        mode: str = 'a',
        update: bool = False,
        open_: bool = False,
        build: bool = False,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            path: The path for this frame to wrap.
            content_map: A HDF5Dataset with the mapping information for creating the frame structure.
            frames: An iterable holding frames/objects to store in this frame.
            mode: Determines if the contents of this frame are editable or not.
            update: Determines if this frame will start_timestamp updating or not.
            open_: Determines if the frames will remain open after construction.
            build: Determines if the frames will be constructed.
            **kwargs: The keyword arguments to create contained frames.
        """
        if content_map is not None:
            self.content_map = content_map

        super().construct(path=path, frames=frames, mode=mode, update=update)

        if build:
            self.construct_frames(open_=open_, mode=self.mode, **kwargs)

    def construct_leaf_frames(self, open_=False, **kwargs) -> None:
        """Constructs the frames for this object when they are leaves.

        Args:
            open_: Determines if the frames will remain open after construction.
            **kwargs: The keyword arguments to create contained frames.
        """
        for frame_info in self.content_map.get_item_dicts_iter():
            path = self.path / frame_info["Path"]
            self.frame_paths.add(path)
            self.frames.append(self.frame_type(
                path,
                open_=open_,
                **kwargs,
            ))
        self.clear_caches()

    def construct_node_frames(self, open_=False, **kwargs) -> None:
        """Constructs the frames for this object when they are nodes.

        Args:
            open_: Determines if the frames will remain open after construction.
            **kwargs: The keyword arguments to create contained frames.
        """
        for frame_info in self.content_map.get_item_dicts_iter():
            path = self.path / frame_info["Path"]
            dataset = self.content_map.file[frame_info["Dataset"]]
            self.frame_paths.add(path)
            self.frames.append(self.node_frame_type(path=path, content_map=dataset, open_=open_, **kwargs))
        self.clear_caches()

    def construct_frames(self, open_=False, **kwargs) -> None:
        """Constructs the frames for this object.

        Args:
            open_: Determines if the frames will remain open after construction.
            **kwargs: The keyword arguments to create contained frames.
        """
        if isinstance(self.content_map.map, OrderNodeMap):
            self.construct_node_frames(open_=open_, **kwargs)
        else:
            self.construct_leaf_frames(open_=open_, **kwargs)

    # Getters and Setters
    @timed_keyless_cache(lifetime=1.0, call_method="clearing_call", collective=False)
    def get_min_shape(self) -> tuple[int]:
        """Get the minimum shapes from the contained frames/objects if they are different across axes.

        Returns:
            The minimum shapes of the contained frames/objects.
        """
        return tuple(int(d) for d in self.content_map.attributes["min_shape"])

    @timed_keyless_cache(lifetime=1.0, call_method="clearing_call", collective=False)
    def get_max_shape(self) -> tuple[int]:
        """Get the maximum shapes from the contained frames/objects if they are different across axes.

        Returns:
            The maximum shapes of the contained frames/objects.
        """
        return tuple(int(d) for d in self.content_map.attributes["max_shape"])

    @timed_keyless_cache(lifetime=1.0, call_method="clearing_call", collective=False)
    def get_lengths(self) -> tuple[int]:
        """Get the lengths of the contained frames/objects.

        Returns:
            All the lengths of the contained frames/objects.
        """
        return tuple(self.content_map.get_field("Length"))

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_start_datetimes(self) -> tuple[Timestamp | None]:
        """Get the start_timestamp datetimes of all contained frames.

        Returns:
            All the start_timestamp datetimes.
        """
        return self.content_map.components["start_times"].get_datetimes()

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_start_nanostamps(self) -> np.ndarray:
        """Get the start_nanostamp nanostamps of all contained frames.

        Returns:
            All the start_nanostamp nanostamps.
        """
        return self.content_map.components["start_times"].get_nanostamps()

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_start_timestamps(self) -> np.ndarray:
        """Get the start_timestamp timestamps of all contained frames.

        Returns:
            All the start_timestamp timestamps.
        """
        return self.content_map.components["start_times"].get_timestamps()

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_end_datetimes(self) -> tuple[Timestamp | None]:
        """Get the end_timestamp datetimes of all contained frames.

        Returns:
            All the end_timestamp datetimes.
        """
        return self.content_map.components["end_times"].get_datetimes()

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_end_nanostamps(self) -> np.ndarray:
        """Get the end_nanostamp nanostamps of all contained frames.

        Returns:
            All the end_nanostamp nanostamps.
        """
        return self.content_map.components["end_times"].get_nanostamps()

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_end_timestamps(self) -> np.ndarray:
        """Get the end_timestamp timestamps of all contained frames.

        Returns:
            All the end_timestamp timestamps.
        """
        return self.content_map.components["end_times"].get_timestamps()

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_sample_rates(self) -> tuple[float]:
        """Get the sample rates of all contained frames.

        Returns:
            The sample rates of all contained frames.
        """
        return tuple(float(r) for r in self.content_map.get_field("Sample Rate"))

    @timed_keyless_cache(call_method="clearing_call", collective=False)
    def get_sample_rates_decimal(self) -> tuple[Decimal]:
        """Get the sample rates of all contained frames.

        Returns:
            The sample rates of all contained frames.
        """
        return tuple(Decimal(r) for r in self.content_map.get_field("Sample Rate"))


# Assign Cyclic Definitions
TimeContentFrame.default_node_frame_type = TimeContentFrame
