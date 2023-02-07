""" contentsfilecomponent.py

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

# Third-Party Packages #
from hdf5objects import HDF5Group, HDF5BaseComponent

# Local Packages #


# Definitions #
# Classes #
class ContentsFileComponent(HDF5BaseComponent):
    # Magic Methods
    # Constructors/Destructors
    def __init__(
        self,
        composite: Any = None,
        data_location: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.data_location: str = ""

        # Parent Attributes #
        super().__init__(self, init=False)

        # Object Construction #
        if init:
            self.construct(
                composite=composite,
                data_location=data_location,
                **kwargs,
            )

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        composite: Any = None,
        data_location: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            data_location: The location in the file where the root of the data hierarchy is.
            **kwargs: Keyword arguments for inheritance.
        """
        if data_location is not None:
            self.data_location = data_location

        super().construct(composite=composite, **kwargs)

    def get_data_root(self) -> HDF5Group:
        """Get the dataset which is the root of the data within this file.

        Returns:
            The root the data.
        """
        return self.composite[self.data_location]
