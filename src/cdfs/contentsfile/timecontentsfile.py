""" timecontentsfile.py
An HDF5 file which tracks the contents of time data in the CDFS.
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
from hdf5objects import HDF5Map

# Local Packages #
from .contentmaps import TimeLeafMap
from .contentsfile import ContentsFileMap, ContentsFile


# Definitions #
# Classes #
class TimeContentsFileMap(ContentsFileMap):
    """A map which outlines a content file with time information."""
    default_maps: Mapping[str, HDF5Map] = {
        "data_content": TimeLeafMap(shape=(0,), maxshape=(None,)),
    }


class TimeContentsFile(ContentsFile):
    """An HDF5 file which tracks the contents of time data in the CDFS."""
    FILE_TYPE: str = "ContentsFile"
    default_map: HDF5Map = TimeContentsFileMap()
