""" timenodecomponent.py

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
from typing import Any
import uuid

# Third-Party Packages #
from dspobjects.time import nanostamp
from hdf5objects import HDF5Dataset
from hdf5objects.dataset import BaseDatasetComponent

# Local Packages #


# Definitions #
# Classes #
class TimeNodeComponent(BaseDatasetComponent):
    # Magic Methods
    # Constructors/Destructors
    def __init__(
        self,
        composite: Any = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.i_axis: int = 0
        self.id_name: str = "id_axis"
        self._id_axis = None

        self.s_axis: int = 0
        self.start_name: str = "start_time_axis"
        self._start_axis = None

        self.e_axis: int = 0
        self.end_name: str = "end_time_axis"
        self._end_axis = None

        # Parent Attributes #
        super().__init__(self, init=False)

        # Object Construction #
        if init:
            self.construct(
                composite=composite,
                **kwargs,
            )

    @property
    def id_axis(self) -> HDF5Dataset | None:
        """Loads and returns the id axis."""
        if self._id_axis is None:
            self._id_axis = self.composite.axes[self.i_axis][self.id_name]
        return self._id_axis

    @id_axis.setter
    def id_axis(self, value: HDF5Dataset | None) -> None:
        self._id_axis = value

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
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            reference_fields: The fields of the dtype that contain object references.
            primary_reference_field: The name of the reference to get when the name is not given.
            **kwargs: Keyword arguments for inheritance.
        """
        super().construct(composite=composite, **kwargs)

    def set_item(self, index: int, path=None, map_=None, start=None, end=None, length=None, id_=None):
        item = {}
        if path is not None:
            item["Path"] = path

        if length is not None:
            item["Length"] = length

        if map_ is not None:
            item["Dataset"] = map_.get_object(require=True, file=self.composite.file).ref

        self.composite.set_item_dict(index, item)

        if id_ is not None:
            self.id_axis.components["axis"].insert_id(id_ if id_ is not None else uuid.uuid4(), index=index)

        if start is not None:
            self.start_axis.set_item(index, start)

        if end is not None:
            self.end_axis.set_item(index, end)

    def append_entry(self, path,  map_, start, end=None, length=0, id_=None):
        self.composite.append_data_item_dict({
            "Path": path,
            "Length": length,
            "Dataset": map_.get_object(require=True, file=self.composite.file).ref,
        })
        self.id_axis.components["axis"].append_id(id_ if id_ is not None else uuid.uuid4())
        self.start_axis.append_data(nanostamp(start))
        self.end_axis.append_data(nanostamp(end if end is not None else start))

    def insert_entry(self, path,  map_, start, end=None, length=0, id_=None):
        if self.composite.size == 0:
            self.append_entry(path=path, map_=map_, start=start, end=end, length=length, id_=id_)
        else:
            index, dt = self.start_axis.components["axis"].find_time_index(start, approx=True, tails=True)

            if dt != start:
                self.composite.insert_data_item_dict(
                    {"Path": path,
                     "Length": length,
                     "Dataset": map_.get_object(require=True, file=self.composite.file).ref},
                    index=index,
                )
                self.id_axis.components["axis"].insert_id(id_ if id_ is not None else uuid.uuid4(), index=index)
                self.start_axis.insert_data(nanostamp(start), index=index)
                self.end_axis.insert_data(nanostamp(end if end is not None else start), index=index)
            else:
                raise ValueError("Entry already exists")

