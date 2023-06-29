"""timecontentframe.py

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
from warnings import warn

# Third-Party Packages #
from baseobjects.cachingtools import timed_keyless_cache
from dspobjects.time import Timestamp
from framestructure import DirectoryTimeFrame, DirectoryTimeFrameInterface
from hdf5objects import HDF5Group, HDF5Dataset
import numpy as np

# Local Packages #
from ..contentmaps import TimeContentGroupMap


# Definitions #
# Classes #
class TimeContentFrame(DirectoryTimeFrame):
    """A DirectoryTimeFrame object built with information from a dataset which maps out its contents.

    Class Attributes:
        default_node_frame_type: The default frame type to create when making a node.

    Attributes:
        content_map: A HDF5Group with the mapping information for creating the frame structure.
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
    default_node_map: type = TimeContentGroupMap

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        content_map: HDF5Group | None = None,
        frames: Iterable[DirectoryTimeFrameInterface] | None = None,
        mode: str = "r",
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
        self.valid_indices: list[int] = list()

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
        content_map: HDF5Group | None = None,
        frames: Iterable[DirectoryTimeFrameInterface] | None = None,
        mode: str = "r",
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

        super().construct(path=path, frames=frames, mode=mode, update=update, open_=open_, build=build, **kwargs)

    def construct_leaf_frames(self, open_=False, **kwargs) -> None:
        """Constructs the frames for this object when they are leaves.

        Args:
            open_: Determines if the frames will remain open after construction.
            **kwargs: The keyword arguments to create contained frames.
        """
        for i, frame_info in enumerate(self.content_map.components["tree_node"].node_map[...]):
            path = self.path / frame_info["Path"].decode()
            if path not in self.frame_paths:
                if path.is_file():
                    self.frame_paths.add(path)
                    self.valid_indices.append(i)
                    self.frames.append(self.frame_type(path, open_=open_, **kwargs))
                else:
                    warn(f"{path} is missing")
        self.clear_caches()

    def construct_node_frames(self, open_=False, **kwargs) -> None:
        """Constructs the frames for this object when they are nodes.

        Args:
            open_: Determines if the frames will remain open after construction.
            **kwargs: The keyword arguments to create contained frames.
        """
        for i, frame_info in enumerate(self.content_map.components["tree_node"].node_map[...]):
            path = self.path / frame_info["Path"].decode()
            if path in self.frame_paths:
                self.frames[i].update_frames()
            elif path.exists():
                self.frame_paths.add(path)
                self.valid_indices.append(i)
                group = self.content_map.file[frame_info["Node"]]
                self.frames.append(self.node_frame_type(path=path, content_map=group, open_=open_, **kwargs))

        self.clear_caches()

    def construct_frames(self, open_=False, **kwargs) -> None:
        """Constructs the frames for this object.

        Args:
            open_: Determines if the frames will remain open after construction.
            **kwargs: The keyword arguments to create contained frames.
        """
        self.frame_paths.clear()
        self.valid_indices.clear()
        self.frames.clear()
        if self.content_map.attributes["tree_type"] == "Node":
            self.construct_node_frames(open_=open_, **kwargs)
        else:
            self.construct_leaf_frames(open_=open_, **kwargs)

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def update_frames(self, open_=False, **kwargs) -> None:
        """Updates the frames for this object.

        Args:
            open_: Determines if the frames will remain open after the update.
            **kwargs: The keyword arguments to create contained frames.
        """
        if self.content_map.attributes["tree_type"] == "Node":
            self.construct_node_frames(open_=open_, **kwargs)
        else:
            self.construct_leaf_frames(open_=open_, **kwargs)

    # Getters and Setters
    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_min_shape(self, **kwargs) -> tuple[int]:
        """Get the minimum shapes from the contained frames/objects if they are different across axes.

        Returns:
            The minimum shapes of the contained frames/objects.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        return tuple(self.content_map.components["tree_node"].get_min_shape())

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_max_shape(self, **kwargs) -> tuple[int]:
        """Get the maximum shapes from the contained frames/objects if they are different across axes.

        Returns:
            The maximum shapes of the contained frames/objects.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        return tuple(self.content_map.components["tree_node"].get_max_shape())

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_lengths(self, **kwargs) -> tuple[int]:
        """Get the lengths of the contained frames/objects.

        Returns:
            All the lengths of the contained frames/objects.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        return self.content_map.components["tree_node"].get_lengths()

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_start_datetimes(self, **kwargs) -> tuple[Timestamp | None]:
        """Get the start_timestamp datetimes of all contained frames.

        Returns:
            All the start_timestamp datetimes.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        all_dt = self.content_map.components["tree_node"].node_map.components["start_times"].get_datetimes()
        return tuple(all_dt[i] for i in self.valid_indices)

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_start_nanostamps(self, **kwargs) -> np.ndarray:
        """Get the start_nanostamp nanostamps of all contained frames.

        Returns:
            All the start_nanostamp nanostamps.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        all_ns = self.content_map.components["tree_node"].node_map.components["start_times"].get_nanostamps()
        return tuple(all_ns[i] for i in self.valid_indices)

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_start_timestamps(self, **kwargs) -> np.ndarray:
        """Get the start_timestamp timestamps of all contained frames.

        Returns:
            All the start_timestamp timestamps.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        all_ts = self.content_map.components["tree_node"].node_map.components["start_times"].get_timestamps()
        return tuple(all_ts[i] for i in self.valid_indices)

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_end_datetimes(self, **kwargs) -> tuple[Timestamp | None]:
        """Get the end_timestamp datetimes of all contained frames.

        Returns:
            All the end_timestamp datetimes.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        all_dt = self.content_map.components["tree_node"].node_map.components["end_times"].get_datetimes()
        return tuple(all_dt[i] for i in self.valid_indices)

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_end_nanostamps(self, **kwargs) -> np.ndarray:
        """Get the end_nanostamp nanostamps of all contained frames.

        Returns:
            All the end_nanostamp nanostamps.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        all_ns = self.content_map.components["tree_node"].node_map.components["end_times"].get_nanostamps()
        return tuple(all_ns[i] for i in self.valid_indices)

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_end_timestamps(self, **kwargs) -> np.ndarray:
        """Get the end_timestamp timestamps of all contained frames.

        Returns:
            All the end_timestamp timestamps.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        all_ts = self.content_map.components["tree_node"].node_map.components["end_times"].get_timestamps()
        return tuple(all_ts[i] for i in self.valid_indices)

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_sample_rates(self, **kwargs) -> tuple[float]:
        """Get the sample rates of all contained frames.

        Returns:
            The sample rates of all contained frames.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        return tuple(
            float(self.content_map.components["tree_node"].node_map.get_field("Sample Rate")[i])
            for i in self.valid_indices
        )

    @timed_keyless_cache(lifetime=0.5, call_method="clearing_call", local=True)
    def get_sample_rates_decimal(self, **kwargs) -> tuple[Decimal]:
        """Get the sample rates of all contained frames.

        Returns:
            The sample rates of all contained frames.
        """
        if self.content_map.file.swmr_mode:
            self.update_frames.caching_call(**kwargs)
        return tuple(
            Decimal(self.content_map.components["tree_node"].node_map.get_field("Sample Rate")[i])
            for i in self.valid_indices
        )


# Assign Cyclic Definitions
TimeContentFrame.default_node_frame_type = TimeContentFrame
