""" timenodecomponent.py
A component for an HDF5Dataset which gives methods for a node in the content hierarchy.
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
from datetime import datetime
from decimal import Decimal
from typing import Any
import uuid

# Third-Party Packages #
from dspobjects.time import nanostamp
from hdf5objects import HDF5Map, HDF5Dataset
import numpy as np

# Local Packages #
from .basecontentcomponent import BaseContentComponent


# Definitions #
# Classes #
class TimeNodeComponent(BaseContentComponent):
    """A component for an HDF5Dataset which gives methods for a node in the content hierarchy."""
    # Instance Methods #
    # Constructors/Destructors
    def set_entry(
        self,
        index: int,
        path: str | None = None,
        map_: HDF5Map | None = None,
        start: datetime | float | int | np.dtype | None = None,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        length: int | None = None,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Set an entry's values based on the given parameters.

        Args:
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            start: The start time of the entry.
            end: The end time of the entry.
            sample_rate: The sample rate of the entry.
            length: The number of samples in the entry.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        item = {}
        if path is not None:
            item["Path"] = path

        if length is not None:
            item["Length"] = length

        if map_ is not None:
            item["Dataset"] = map_.get_object(require=True, file=self.composite.file).ref

        if min_shape is not None:
            item["Minimum ndim"] = len(min_shape)

        if max_shape is not None:
            item["Maximum ndim"] = len(max_shape)
            
        if sample_rate is not None:
            item["Sample Rate"] = sample_rate

        self.composite.set_item_dict(index, item)

        if id_ is not None:
            self.id_axis.components["axis"].insert_id(id_ if id_ is not None else uuid.uuid4(), index=index)

        if start is not None:
            self.start_axis.set_item(index, start)

        if end is not None:
            self.end_axis.set_item(index, end)

        self.join_min_shape(shape=min_shape)
        self.join_max_shape(shape=max_shape)

    def append_entry(
        self,
        path: str,
        map_: HDF5Map,
        start: datetime | float | int | np.dtype,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        length: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Append an entry to dataset.

        Args:
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            start: The start time of the entry.
            end: The end time of the entry.
            sample_rate: The sample rate of the entry.
            length: The number of samples in the entry.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        self.composite.append_data_item_dict({
            "Path": path,
            "Length": length,
            "Dataset": map_.get_object(require=True, file=self.composite.file).ref,
            "Minimum ndim": len(min_shape),
            "Maximum ndim": len(max_shape),
            "Sample Rate": float(sample_rate) if sample_rate is not None else np.nan,
        })
        self.id_axis.components["axis"].append_id(id_ if id_ is not None else uuid.uuid4())
        self.start_axis.append_data(nanostamp(start))
        self.end_axis.append_data(nanostamp(end if end is not None else start))
        self.join_min_shape(shape=min_shape)
        self.join_max_shape(shape=max_shape)

    def insert_entry_start(
        self,
        path: str,
        map_: HDF5Map,
        start: datetime | float | int | np.dtype,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        length: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Inserts an entry into dataset based on the start time.

        Args:
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            start: The start time of the entry.
            end: The end time of the entry.
            sample_rate: The sample rate of the entry.
            length: The number of samples in the entry.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        if self.composite.size == 0:
            self.append_entry(
                path=path,
                map_=map_,
                start=start,
                end=end,
                length=length,
                min_shape=min_shape,
                max_shape=max_shape,
                sample_rate=sample_rate,
                id_=id_,
            )
        else:
            index, dt = self.start_axis.components["axis"].find_time_index(start, approx=True, tails=True)

            if dt != start:
                self.composite.insert_data_item_dict(
                    {"Path": path,
                     "Length": length,
                     "Dataset": map_.get_object(require=True, file=self.composite.file).ref,
                     "Minimum ndim": len(min_shape),
                     "Maximum ndim": len(max_shape),
                     "Sample Rate": float(sample_rate) if sample_rate is not None else np.nan},
                    index=index,
                )
                self.id_axis.components["axis"].insert_id(id_ if id_ is not None else uuid.uuid4(), index=index)
                self.start_axis.insert_data(nanostamp(start), index=index)
                self.end_axis.insert_data(nanostamp(end if end is not None else start), index=index)
                self.join_min_shape(shape=min_shape)
                self.join_max_shape(shape=max_shape)
            else:
                raise ValueError("Entry already exists")

