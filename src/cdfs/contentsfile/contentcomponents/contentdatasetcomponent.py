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
from hdf5objects.dataset import ObjectReferenceComponent, RegionReferenceComponent
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
    default_i_axis: int = 0
    default_id_name: str = "id_axis"
    default_object_refernces_name: str = "object_reference"
    default_node_name: str = "node"
    default_region_refernces_name: str = "object_reference"
    default_mins_name: str = "min_shapes"
    default_maxs_name: str = "max_shapes"

    # Magic Methods #
    # Constructors/Destructors
    def __init__(
        self,
        composite: Any = None,
        i_axis: int | None = None,
        id_name: str | None = None,
        object_ref_name: str | None = None,
        node_name: str | None = None,
        region_ref_name: str | None = None,
        mins_name: str | None = None,
        maxs_name: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.i_axis: int = self.default_i_axis
        self.id_name: str = self.default_id_name
        self._id_axis: HDF5Dataset | None = None

        self.object_references_name: str = self.default_object_references_name
        self._object_refernces: ObjectReferenceComponent | None = None
        self.node_name: str = self.default_node_name

        self.region_references_name: str = self.default_region_references_name
        self._region_references: RegionReferenceComponent | None = None
        self.mins_name: str = self.default_mins_name
        self.maxs_name: str = self.default_maxs_name

        # Parent Attributes #
        super().__init__(self, init=False)

        # Object Construction #
        if init:
            self.construct(
                composite=composite,
                i_axis = i_axis,
                id_name = id_name,
                object_ref_name=object_ref_name,
                node_name=node_name,
                region_ref_name=region_ref_name,
                mins_name=mins_name,
                maxs_name=maxs_name,
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
    def object_references(self) -> OjectReferenceComponent | None:
        """Loads and returns the id axis."""
        if self._object_references is None:
            self._object_references = self.composite.components[self.object_references_name]
        return self._object_references

    @object_references.setter
    def object_references(self, value: ObjectReferenceComponent | None) -> None:
        self._object_references = value

    @property
    def region_references(self) -> RegionReferenceComponent | None:
        """Loads and returns the id axis."""
        if self._region_references is None:
            self._region_references = self.composite.components[self.region_references_name]
        return self._region_references

    @region_references.setter
    def region_references(self, value: RegionReferenceComponent | None) -> None:
        self._region_references = value


    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        composite: Any = None,
        i_axis: int | None = None,
        id_name: str | None = None,
        object_ref_name: str | None = None,
        node_name: str | None = None,
        region_ref_name: str | None = None,
        mins_name: str | None = None,
        maxs_name: str | None = None,
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

        if object_ref is not None:
            self.object_ref = object_ref

        if node_name is not None:
            self.node_name = node_name

        if region_ref_name is not None:
            self.region_ref_name = region_ref_name

        if mins_name is not None:
            self.mins_name = mins_name

        if maxs_name is not None:
            self.maxs_name = maxs_name

        super().construct(composite=composite, **kwargs)

    def fix_shape_references(self):
        for index in range(self.composite.shape[0]):
            self.region_references.set_region_reference(index, region=(index, slice(None)), ref_name=self.mins_name)
            self.region_references.set_region_reference(index, region=(index, slice(None)), ref_name=self.maxs_name)

    # Node
    def set_entry(
        self,
        index: int,
        path: str | None = None,
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

        self.set_entry_dict(index, item, map_)

        self.region_references.set_reference_to(index=index, value=min_shape, ref_name=self.mins_name)
        self.region_references.set_reference_to(index=index, value=min_shape, ref_name=self.maxs_name)

        if id_ is not None:
            self.id_axis.components["axis"].insert_id(id_, index=index)

    def append_entry(
        self,
        path: str,
        map_: HDF5Map | None = None,
        axis: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Append an entry to dataset.

        Args:
            path: The path name which the entry represents.
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
                "Node": child,
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
                "Node": child,
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
