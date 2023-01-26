""" contentsfile.py
An HDF5 file which tracks the contents of the data in the CDFS.
"""
# Package Header #
from ..header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from collections.abc import Mapping

# Third-Party Packages #
from hdf5objects import BaseHDF5, BaseHDF5Map, HDF5Map

# Local Packages #
from .contentcomponents import ContentsFileComponent
from .contentmaps import OrderLeafMap


# Definitions #
# Classes #
class ContentsFileMap(BaseHDF5Map):
    """A map which outlines a generic content file."""
    default_map_names: Mapping[str, str] = {"data_content": "data_content"}
    default_maps: Mapping[str, HDF5Map] = {
        "data_content": OrderLeafMap(shape=(0,), maxshape=(None,)),
    }


class ContentsFile(BaseHDF5):
    """An HDF5 file which tracks the contents of the data in the CDFS."""
    FILE_TYPE: str = "ContentsFile"
    default_map: HDF5Map = ContentsFileMap()
    default_component_types = {"contents": (ContentsFileComponent, {})}
