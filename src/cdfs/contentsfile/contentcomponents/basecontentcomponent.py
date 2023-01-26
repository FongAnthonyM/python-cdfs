""" basecontentcomponent.py

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
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class BaseContentComponent(BaseDatasetComponent):
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
            **kwargs: Keyword arguments for inheritance.
        """
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
