""" contentgroupcomponent.py
A node component which implements an interface for a content dataset
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
from datetime import datetime, date, tzinfo
from decimal import Decimal
from typing import Any
import uuid

# Third-Party Packages #
from baseobjects.functions import MethodMultiplexer
from baseobjects.typing import AnyCallable
from dspobjects.time import Timestamp
from hdf5objects import HDF5Map, HDF5Dataset, HDF5Group
from hdf5objects.treehierarchy import NodeGroupComponent
import numpy as np

# Local Packages #


# Todo: Add a recursive way to create empty leafs.
# Definitions #
# Classes #
class ContentGroupComponent(NodeGroupComponent):
    """A node component which implements an interface for a content dataset.

    Attributes:
        insert_recursive_entry: The method to use as the insert recursive entry method.

    Args:
        composite: The object which this object is a component of.
        insert_method: The attribute name of the method to use as the insert recursive entry method.
        init: Determines if this object will construct.
        **kwargs: Keyword arguments for inheritance.
    """
    default_append_recursive: str = "append_recursive_entry_index"
    default_insert_recursive: str = "insert_recursive_entry_index"

    # Magic Methods #
    # Constructors/Destructors
    def __init__(
        self,
        composite: Any = None,
        append_method: str | None = None,
        insert_method: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.append_recursive_entry: MethodMultiplexer = MethodMultiplexer(
            instance=self,
            select=self.default_insert_recursive,
        )

        self.insert_recursive_entry: MethodMultiplexer = MethodMultiplexer(
            instance=self,
            select=self.default_insert_recursive,
        )

        # Parent Attributes #
        super().__init__(self, init=False)

        # Object Construction #
        if init:
            self.construct(composite=composite, append_methodq=append_method, insert_method=insert_method, **kwargs)

    @property
    def length(self) -> int:
        """The minimum shape of this node."""
        return  self.node_map.components[self.node_component_name].get_length()

    @property
    def min_shape(self) -> tuple[int] | None:
        """The minimum shape of this node."""
        return self.node_map.components[self.node_component_name].get_min_shape()

    @property
    def max_shape(self) -> tuple[int] | None:
        """The maximum shape of this node."""
        return self.node_map.components[self.node_component_name].get_max_shape()

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        composite: Any = None,
        append_method: str | None = None,
        insert_method: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            insert_method: The attribute name of the method to use as the insert recursive entry method.
            **kwargs: Keyword arguments for inheritance.
        """
        if append_method is not None:
            self.append_recursive_entry.select(name=append_method)

        if insert_method is not None:
            self.insert_recursive_entry.select(name=insert_method)

        super().construct(composite=composite, **kwargs)

    def require_component(self, **kwargs: Any) -> None:
        """Creates all the required parts of the group for this component if it does not exists.

        Args:
            **kwargs: The keyword arguments to require this component.
        """
        self.node_map.components[self.node_component_name].set_min_shapes_dataset(self.composite["min_shapes"])
        self.node_map.components[self.node_component_name].set_max_shapes_dataset(self.composite["max_shapes"])

    def get_lengths(self) -> tuple[int, ...]:
        return self.node_map.components[self.node_component_name].get_lengths()

    def get_min_shape(self) -> tuple[int, ...]:
        return self.node_map.components[self.node_component_name].get_min_shape()

    def get_max_shape(self) -> tuple[int, ...]:
        return self.node_map.components[self.node_component_name].get_max_shape()

    def create_child(
        self,
        index: int,
        path: str,
        map_: HDF5Map | None = None,
        axis: int | None = None,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
        **kwargs: Any,
    ) -> HDF5Group | None:
        """Creates a child node and inserts it as an entry.

        Args:
            index: The index to insert the given entry.
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            axis: The axis dimension number which the data concatenated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        if map_ is None and self.child_map_type is not None:
            map_ = self.child_map_type(name=f"{self.composite.name}/{path}")
            self.composite.map.set_item(map_)

        self.node_map.components[self.node_component_name].insert_entry(
            index=index,
            path=path,
            map_=map_,
            axis=axis,
            min_shape=min_shape,
            max_shape=max_shape,
            id_=id_,
        )

        return None if map_ is None else map_.get_object(require=True, file=self.composite.file)

    def require_child(
        self,
        index: int,
        path: str,
        map_: HDF5Map | None = None,
        axis: int | None = None,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
        **kwargs: Any,
    ) -> HDF5Dataset | None:
        """Gets a child node at an index or if it does not exist, creates and inserts it as an entry.

        Args:
            index: The index to insert the given entry.
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            axis: The axis dimension number which the data concatenated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        if self.node_map.size != 0 and index < len(self.node_map):
            return self.node_map.components["object_reference"].get_object(index, ref_name="node")
        else:
            return self.create_child(
                index=index,
                path=path,
                map_=map_,
                axis=axis,
                min_shape=min_shape,
                max_shape=max_shape,
                id_=id_,
            )

    def append_recursive_entry_index(
        self,
        indices: Iterable[int],
        paths: Iterable[str],
        map_: HDF5Map | None = None,
        axis: int | None = None,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        ids: Iterable[str | uuid.UUID | None] | None = None,
        **kwargs: Any,
    ) -> None:
        """Append an entry recursively into its children using indices.

        Args:
            indices: The indices to recursively append into.
            paths: The path names which the entry represents.
            map_: The map to the object that should be stored in the entry.
            axis: The axis dimension number which the data concatenated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            ids: The child IDs for the entry.
        """
        if not isinstance(indices, list):
            indices = list(indices)

        if not isinstance(paths, list):
            paths = list(paths)

        if ids is not None and not isinstance(ids, list):
            ids = list(ids)

        index = indices.pop(0)
        path = paths.pop(0)
        id_ = ids.pop(0) if ids else None
        child = self.require_child(
            index=index,
            path=path,
            map_=map_,
            axis=axis,
            min_shape=min_shape,
            max_shape=max_shape,
            id_=id_,
        )
        if paths:
            child_node_component = child.components[self.child_component_name]
            child_node_component.append_recursive_entry(
                indices=indices,
                paths=paths,
                map_=map_,
                axis=axis,
                min_shape=min_shape,
                max_shape=max_shape,
                ids=ids,
            )

            self.node_map.components[self.node_component_name].set_entry(
                index=index,
                min_shape=child_node_component.min_shape,
                max_shape=child_node_component.max_shape,
            )

    def insert_recursive_entry_index(
        self,
        indices: Iterable[int],
        paths: Iterable[str],
        map_: HDF5Map | None = None,
        axis: int | None = None,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        ids: Iterable[str | uuid.UUID | None] | None = None,
        **kwargs: Any,
    ) -> None:
        """Inserts an entry recursively into its children using indices.

        Args:
            indices: The indices to recursively insert into.
            paths: The path names which the entry represents.
            map_: The map to the object that should be stored in the entry.
            axis: The axis dimension number which the data concatenated along.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            ids: The child IDs for the entry.
        """
        if not isinstance(indices, list):
            indices = list(indices)

        if not isinstance(paths, list):
            paths = list(paths)

        if ids is not None and not isinstance(ids, list):
            ids = list(ids)

        index = indices.pop(0)
        path = paths.pop(0)
        id_ = ids.pop(0) if ids else None
        child = self.require_child(
            index=index,
            path=path,
            map_=map_,
            axis=axis,
            min_shape=min_shape,
            max_shape=max_shape,
            id_=id_,
        )
        if paths:
            child_node_component = child.components[self.child_component_name]
            child_node_component.insert_recursive_entry(
                indices=indices,
                paths=paths,
                map_=map_,
                axis=axis,
                min_shape=min_shape,
                max_shape=max_shape,
                ids=ids,
            )

            self.node_map.components[self.node_component_name].set_entry(
                index=index,
                min_shape=child_node_component.min_shape,
                max_shape=child_node_component.max_shape,
            )
