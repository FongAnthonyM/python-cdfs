""" contentdatasetcomponent.py
A node component which implements content information in its dataset.
"""
import h5py

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
from hdf5objects import HDF5Map, HDF5Dataset
from hdf5objects.treehierarchy import NodeDatasetComponent
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class ContentDatasetComponent(NodeDatasetComponent):
    """A node component which implements content information in its dataset.

    Class Attributes:
        default_i_axis: The default dimension which the ID axis is on.
        defaulte_id_name: The default name of the ID axis.

    Attributes:
        i_axis: The dimension which the ID axis is on.
        id_name: The name of the ID axis.
        _id_axis: The ID axis of the dataset

    Args:
        composite: The object which this object is a component of.
        i_axis: The dimension which the ID axis is on.
        id_name: The name of the ID axis.
        **kwargs: Keyword arguments for inheritance.
    """
    default_i_axis = 0
    default_id_name = "id_axis"

    # Magic Methods #
    # Constructors/Destructors
    def __init__(
        self,
        composite: Any = None,
        i_axis: int | None = None,
        id_name: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.i_axis: int = self.default_i_axis
        self.id_name: str = self.default_id_name
        self._id_axis: HDF5Dataset | None = None

        # Parent Attributes #
        super().__init__(self, init=False)

        # Object Construction #
        if init:
            self.construct(
                composite=composite,
                i_axis = i_axis,
                id_name = id_name,
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

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        composite: Any = None,
        i_axis: int | None = None,
        id_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            i_axis: The dimension which the ID axis is on.
            id_name: The name of the ID axis.
            **kwargs: Keyword arguments for inheritance.
        """
        if i_axis is not None:
            self.i_axis = i_axis

        if id_name is not None:
            self.id_name = id_name

        super().construct(composite=composite, **kwargs)
        
    def join_min_shape(self, shape: tuple[int]) -> None:
        """Set the minimum shape to the joint minimum of the current minimum and the given shape.
        
        Args:
            shape: The new shape to join with.
        """
        old_min = self.composite.attributes.get("min_shape", ())
        min_ndim = min(len(old_min), len(shape))
        if min_ndim == 0:
            true_min = np.array([])
        else:
            mins = np.zeros((2, min_ndim))
            mins[0, :min_ndim] = old_min
            mins[1, :min_ndim] = shape
            true_min = mins.min(0)
        self.composite.attributes["min_shape"] = true_min.astype(np.uint64)

    def join_max_shape(self, shape: tuple[int]) -> None:
        """Set the maximum shape to the joint maximum of the current maximum and the given shape.

        Args:
            shape: The new shape to join with.
        """
        old_max = self.composite.attributes.get("max_shape", ())
        old_ndim = len(old_max)
        new_ndim = len(shape)
        max_ndim = max(old_ndim, new_ndim)
        if max_ndim == 0:
            true_max = np.array([])
        else:
            maxs = np.zeros((2, max_ndim))
            maxs[0, :old_ndim] = old_max
            maxs[1, :new_ndim] = shape
            true_max = maxs.max(0)
        self.composite.attributes["max_shape"] = true_max.astype(np.uint64)

    # Node
    def set_entry(
        self,
        index: int,
        path: str | None = None,
        map_: HDF5Map | None = None,
        length: int | None = None,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Set an entry's values based on the given parameters.

        Args:
            index: The index to set the given entry.
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
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

        if min_shape is not None:
            item["Minimum ndim"] = len(min_shape)

        if max_shape is not None:
            item["Maximum ndim"] = len(max_shape)

        self.set_entry_dict(index, item, map_)

        self.join_min_shape(shape=min_shape)
        self.join_max_shape(shape=max_shape)

        if id_ is not None:
            self.id_axis.components["axis"].insert_id(id_, index=index)

    def append_entry(
        self,
        path: str,
        map_: HDF5Map | None = None,
        length: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Append an entry to dataset.

        Args:
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            length: The number of samples in the entry.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        self.append_entry_dict(
            item={
                "Path": path,
                "Length": length,
                "Minimum ndim": len(min_shape),
                "Maximum ndim": len(max_shape),
            },
            map_=map_,
        )
        self.id_axis.components["axis"].append_id(id_ if id_ is not None else uuid.uuid4())
        self.join_min_shape(shape=min_shape)
        self.join_max_shape(shape=max_shape)

    def insert_entry(
        self,
        index: int,
        path: str,
        map_: HDF5Map | None = None,
        length: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Insert an entry into dataset.

        Args:
            index: The index to insert the given entry.
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            length: The number of samples in the entry.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        self.insert_entry_dict(
            index=index,
            item={
                "Path": path,
                "Length": length,
                "Minimum ndim": len(min_shape),
                "Maximum ndim": len(max_shape),
            },
            map_=map_,
        )
        self.id_axis.components["axis"].append_id(id_ if id_ is not None else uuid.uuid4())
        self.join_min_shape(shape=min_shape)
        self.join_max_shape(shape=max_shape)

    def update_entry(self, index: int) -> None:
        """Updates an entry to the correct information of the child.

        Args:
            index: The index of the entry to update.
        """
        child = self.composite.file[self.composite.dtypes_dict[self.reference_field]]
        self.set_entry(lengh=child.length, min_shape=child.min_shape, max_shape=child.max_shape)

    def update_entries(self) -> None:
        """Updates all entries to the correct information of their child."""
        child_refs = self.composite.get_field(reference_field)
        data = self.composite[...]
        for i, child_ref in enumerate(child_refs):
            child = self.composite.file[child_ref]
            min_shape = child.min_shape
            max_shape = child.max_shape
            new = {"Length": child.length, "Minimum ndim": len(min_shape), "Maximum ndim": len(max_shape)}
            data[i] = self.composite.item_to_dict(self.composite.item_to_dict(data[i]) | new)
            self.join_min_shape(shape=min_shape)
            self.join_max_shape(shape=max_shape)

        self.composite.data_exclusively(data)
