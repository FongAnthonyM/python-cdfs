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
from baseobjects.typing import AnyCallable
from dspobjects.time import Timestamp
from hdf5objects import HDF5Map, HDF5Dataset
from hdf5objects.treehierarchy import NodeGroupComponent
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class ContentGroupComponent(NodeGroupComponent):
    """A node component which implements an interface for a content dataset.

    Attributes:
        _insert_recursive_entry: The method to use as the insert recursive entry method.

    Args:
        composite: The object which this object is a component of.
        insert_name: The attribute name of the method to use as the insert recursive entry method.
        init: Determines if this object will construct.
        **kwargs: Keyword arguments for inheritance.
    """
    # Magic Methods #
    # Constructors/Destructors
    def __init__(
        self,
        composite: Any = None,
        insert_name: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self._insert_recursive_entry: AnyCallable = self.insert_recursive_entry_default.__func__

        # Parent Attributes #
        super().__init__(self, init=False)

        # Object Construction #
        if init:
            self.construct(
                composite=composite,
                insert_name=insert_name,
                **kwargs,
            )
    @property
    def length(self) -> int:
        """The minimum shape of this node."""
        return self.map_dataset.get_field("Length").sum()

    @property
    def min_shape(self) -> tuple[int] | None:
        """The minimum shape of this node."""
        return self.map_dataset.attributes.get("min_shape", None)

    @property
    def max_shape(self) -> tuple[int] | None:
        """The maximum shape of this node."""
        return self.map_dataset.attributes.get("max_shape", None)

    @property
    def insert_recursive_entry(self) -> AnyCallable:
        """A descriptor to create the bound insert recursive entry method."""
        return self._insert_recursive_entry.__get__(self, self.__class__)

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        composite: Any = None,
        insert_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            insert_name: The attribute name of the method to use as the insert recursive entry method.
            **kwargs: Keyword arguments for inheritance.
        """
        if insert_name is not None:
            self.set_insert_recursive_entry_method(insert_name)

        super().construct(composite=composite, **kwargs)

    def set_insert_recursive_entry_method(self, name: str) -> None:
        """Sets insert recursive entry method to a method within this object.

        Args:
            The attribute name of the method to use as the insert recursive entry method.
        """
        self._insert_recursive_entry = getattr(self, name).__func__

    def create_child(
        self,
        index: int,
        path: str,
        map_: HDF5Map | None = None,
        length: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        id_: str | uuid.UUID | None = None,
    ) -> None:
        """Creates a child node and inserts it as an entry.

        Args:
            index: The index to insert the given entry.
            path: The path name which the entry represents.
            map_: The map to the object that should be stored in the entry.
            length: The number of samples in the entry.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            id_: The ID of the entry.
        """
        if map_ is None and self.child_map_type is not None:
            map_ = self.child_map_type(name=f"{self.composite.name}/{path}")
            self.composite.map.set_item(map_)

        self.map_dataset.components[self.node_component_name].insert_entry(
            index=index,
            path=path,
            map_=map_,
            length=length,
            min_shape=min_shape,
            max_shape=max_shape,
            id_=id_,
        )

        return None if map_ is None else map_.get_object(require=True, file=self.composite.file)

    def insert_recursive_entry_default(
        self,
        indicies: Iterable[int],
        paths: Iterable[str],
        length: int = 0,
        min_shape: tuple[int] = (),
        max_shape: tuple[int] = (),
        ids: Iterable[str | uuid.UUID | None] | None = None,
    ) -> None:
        """Inserts an entry recursively into its children using indicies.

        Args:
            indicies: The indicies to recursively insert into.
            paths: The path names which the entry represents.
            map_: The map to the object that should be stored in the entry.
            length: The number of samples in the entry.
            min_shape: The minimum shape in the entry.
            max_shape: The maximum shape in the entry.
            ids: The child IDs for the entry.
        """
        if not isinstance(indicies, list):
            indicies = list(indicies)

        if not isinstance(paths, list):
            paths = list(paths)

        if ids is not None and not isinstance(ids, list):
            ids = list(ids)

        index = indicies.pop(0)
        path = paths.pop(0)
        id_ = ids.pop(0) if ids else None
        child = self.create_child(
            index=index,
            path=path,
            map_=map_,
            length=length,
            min_shape=min_shape,
            max_shape=max_shape,
            id_=id_,
        )
        if paths:
            child.components[self.node_component_name].insert_recursive_entry(
                paths=paths,
                map_=map_,
                length=length,
                min_shape=min_shape,
                max_shape=max_shape,
                ids=ids,
            )

            self.map_dataset.components[self.node_component_name].set_entry(
                index=index,
                length=child.length,
                min_shape=child.min_shape,
                max_shape=child.max_shape,
            )
