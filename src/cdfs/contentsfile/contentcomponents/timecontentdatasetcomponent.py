""" contentdatasetcomponent.py
A node component which implements time content information in its dataset.
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
from hdf5objects.dataset import BaseDatasetComponent
import numpy as np

# Local Packages #
from .contentdatasetcomponent import ContentDatasetComponent


# Definitions #
# Classes #
class TimeContentDatasetComponent(ContentDatasetComponent):
    """A node component which implements time content information in its dataset.

    Class Attributes:
        default_i_axis: The default dimension which the ID axis is on.
        defaulte_id_name: The default name of the ID axis.

    Attributes:
        s_axis: The dimension which the start axis is on.
        start_name: The name of the start axis.
        _start_axis: The start time axis of the dataset
        e_axis: The dimension which the end axis is on.
        end_name: The name of the end axis.
        _end_axis: The end time axis of the dataset

    Args:
        composite: The object which this object is a component of.
        s_axis: The dimension which the start axis is on.
        start_name: The name of the start axis.
        e_axis: The dimension which the end axis is on.
        end_name: The name of the end axis.
        **kwargs: Keyword arguments for inheritance.
    """
    default_s_axis = 0
    default_start_name = "start_time_axis"
    default_e_axis = 0
    default_end_name = "end_time_axis"

    # Magic Methods
    # Constructors/Destructors
    def __init__(
        self,
        composite: Any = None,
        s_axis: int | None = None,
        start_name: str | None = None,
        e_axis: int | None = None,
        end_name: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.s_axis: int = self.default_s_axis
        self.start_name: str = self.default_start_name
        self._start_axis: HDF5Dataset | None = None

        self.e_axis: int = self.default_e_axis
        self.end_name: str = self.default_end_name
        self._end_axis: HDF5Dataset | None = None

        # Parent Attributes #
        super().__init__(self, init=False)

        # Object Construction #
        if init:
            self.construct(
                composite=composite,
                s_axis=s_axis,
                start_name=start_name,
                e_axis=e_axis,
                end_name=end_name,
                **kwargs,
            )

    @property
    def start_axis(self) -> HDF5Dataset | None:
        """Loads and returns the start time axis."""
        if self._start_axis is None:
            self._start_axis = self.composite.axes[self.s_axis][self.start_name]
        return self._start_axis

    @start_axis.setter
    def start_axis(self, value: HDF5Dataset | None) -> None:
        self._start_axis = value

    @property
    def end_axis(self) -> HDF5Dataset | None:
        """Loads and returns the end time axis."""
        if self._end_axis is None:
            self._end_axis = self.composite.axes[self.e_axis][self.end_name]
        return self._end_axis

    @end_axis.setter
    def end_axis(self, value: HDF5Dataset | None) -> None:
        self._end_axis = value

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        composite: Any = None,
        s_axis: int | None = None,
        start_name: str | None = None,
        e_axis: int | None = None,
        end_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            s_axis: The dimension which the start axis is on.
            start_name: The name of the start axis.
            e_axis: The dimension which the end axis is on.
            end_name: The name of the end axis.
            **kwargs: Keyword arguments for inheritance.
        """
        if s_axis is not None:
            self.s_axis = s_axis

        if start_name is not None:
            self.start_name = start_name

        if e_axis is not None:
            self.e_axis = e_axis

        if end_name is not None:
            self.end_name = end_name

        super().construct(composite=composite, **kwargs)

    # Node
    def set_entry(
        self,
        index: int,
        path: str | None = None,
        start: datetime | float | int | np.dtype | None = None,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        map_: HDF5Map | None = None,
        axis: int | None = None,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Set an entry's values based on the given parameters.

        Args:
            index: The index to set the given entry.
            path: The path name which the entry represents.
            start: The start time of the entry.
            end: The end time of the entry.
            sample_rate: The sample rate of the entry.
            map_: The map to the object that should be stored in the entry.
            axis: The axis dimension number which the data concatiated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        item = {}

        if path is not None:
            item["Path"] = path

        if axis is not None:
            item["Axis"] = axis

        if sample_rate is not None:
            item["Sample Rate"] = float(sample_rate)

        self.set_entry_dict(index, item, map_)

        self.region_references.set_reference_to(index=index, value=min_shape, ref_name=self.mins_name)
        self.region_references.set_reference_to(index=index, value=min_shape, ref_name=self.maxs_name)

        if id_ is not None:
            self.id_axis.components["axis"].insert_id(id_, index=index)

        if start is not None:
            self.start_axis.set_item(index, nanostamp(start))

        if end is not None:
            self.end_axis.set_item(index, nanostamp(end))

    def append_entry(
        self,
        path: str,
        start: datetime | float | int | np.dtype,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        map_: HDF5Map | None = None,
        axis: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Append an entry to dataset.

        Args:
            path: The path name which the entry represents.
            start: The start time of the entry.
            end: The end time of the entry.
            sample_rate: The sample rate of the entry.
            map_: The map to the object that should be stored in the entry.
            axis: The axis dimension number which the data concatiated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        self.region_references.get_object(ref_name=self.mins_name).append_data(min_shape)
        _, min_ref = self.region_references.generate_region_reference(
            (index, slice(None)),
            ref_name=self.mins_name,
        )
        self.region_references.get_object(ref_name=self.maxs_name).append_data(max_shape)
        _, max_ref = self.region_references.generate_region_reference(
            (index, slice(None)),
            ref_name=self.maxs_name,
        )

        self.append_entry_dict(
            item={
                "Path": path,
                "Axis": axis,
                "Minimum Shape": min_ref,
                "Maximum Shape": max_ref,
                "Sample Rate": float(sample_rate) if sample_rate is not None else np.nan,
            },
            map_=map_,
        )
        self.id_axis.components["axis"].append_id(id_ if id_ is not None else uuid.uuid4())
        self.start_axis.append_data(nanostamp(start))
        self.end_axis.append_data(nanostamp(end if end is not None else start))

    def insert_entry(
        self,
        index: int,
        path: str,
        start: datetime | float | int | np.dtype,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        map_: HDF5Map | None = None,
        axis: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Insert an entry into dataset.

        Args:
            index: The index to insert the given entry.
            path: The path name which the entry represents.
            start: The start time of the entry.
            end: The end time of the entry.
            sample_rate: The sample rate of the entry.
            map_: The map to the object that should be stored in the entry.
            axis: The axis dimension number which the data concatiated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        self.region_references.get_object(ref_name=self.mins_name).insert_data(index, min_shape)
        min_object, min_ref = self.region_references.generate_region_reference(
            (index, slice(None)),
            ref_name=self.mins_name,
        )
        self.region_references.get_object(ref_name=self.maxs_name).insert_data(index, max_shape)
        max_object, max_ref = self.region_references.generate_region_reference(
            (index, slice(None)),
            ref_name=self.maxs_name,
        )

        self.insert_entry_dict(
            index=index,
            item={
                "Path": path,
                "axis": axis,
                "Minimum Shape": min_ref,
                "Maximum Shape": max_ref,
                "Sample Rate": float(sample_rate) if sample_rate is not None else np.nan,
                "Sample Rate": float(sample_rate) if sample_rate is not None else np.nan,
            },
            map_=map_,
        )
        self.fix_shape_references()
        self.id_axis.components["axis"].append_id(id_ if id_ is not None else uuid.uuid4())
        self.start_axis.append_data(nanostamp(start))
        self.end_axis.append_data(nanostamp(end if end is not None else start))

    def insert_entry_start(
        self,
        path: str,
        map_: HDF5Map,
        start: datetime | float | int | np.dtype,
        end: datetime | float | int | np.dtype | None = None,
        sample_rate: float | str | Decimal | None = None,
        axis: int = 0,
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
            axis: The axis dimension number which the data concatiated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        if self.composite.size == 0:
            self.append_node_entry(
                path=path,
                map_=map_,
                start=start,
                end=end,
                axis=axis,
                min_shape=min_shape,
                max_shape=max_shape,
                sample_rate=sample_rate,
                id_=id_,
            )
        else:
            index, dt = self.start_axis.components["axis"].find_time_index(start, approx=True, tails=True)

            if dt != start:
                self.insert_entry(
                    index=index,
                    path=path,
                    map_=map_,
                    start=start,
                    end=end,
                    axis=axis,
                    min_shape=min_shape,
                    max_shape=max_shape,
                    sample_rate=sample_rate,
                    id_=id_,
                )
            else:
                raise ValueError("Entry already exists")

    def update_entry(self, index: int) -> None:
        """Updates an entry to the correct information of the child.

        Args:
            index: The index of the entry to update.
        """
        child = self.composite.file[self.composite.dtypes_dict[self.reference_field]]
        self.set_entry(
            index=index,
            start=child.get_start_datetime(),
            end=child.get_end_datetime(),
            axis=child.axis,
            min_shape=child.min_shape,
            max_shape=child.max_shape,
            sample_rate=child.sample_rate,
        )

    def update_entries(self) -> None:
        """Updates all entries to the correct information of their child."""
        child_refs = self.composite.get_field(reference_field)
        data = self.composite[...]
        starts = self.start_axis[...]
        ends = self.end_axis[...]
        for i, child_ref in enumerate(child_refs):
            child = self.composite.file[child_ref]
            min_shape = child.min_shape
            max_shape = child.max_shape

            self.region_references.set_reference_to(index=index, value=min_shape, ref_name=self.mins_name)
            _, min_ref = self.region_references.generate_region_reference(
                (i, slice(len(min_shape))),
                ref_name=self.mins_name,
            )
            self.region_references.set_reference_to(index=index, value=min_shape, ref_name=self.maxs_name)
            _, max_ref = self.region_references.generate_region_reference(
                (i, slice(len(max_shape))),
                ref_name=self.maxs_name,
            )

            new = {
                "Axis": child.axis,
                "Minimum Shape": min_ref,
                "Maximum Shape": max_ref,
                "Sample Rate": child.sample_rate,
            }
            data[i] = self.composite.item_to_dict(self.composite.item_to_dict(data[i]) | new)
            starts[i] = nanostamp(child.get_start_datetime())
            ends[i] = nanostamp(child.get_end_datetime())

        self.composite.data_exclusively(data)
        self.start_axis.data_exclusively(starts)
        self.start_axis.data_exclusively(ends)
