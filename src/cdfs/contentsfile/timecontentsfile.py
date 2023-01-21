""" timecontentsfile.py

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
    """A map for BaseHDF5 files."""
    default_maps: Mapping[str, HDF5Map] = {
        "data_content": TimeLeafMap(shape=(0,), maxshape=(None,)),
    }


class TimeContentsFile(ContentsFile):
    FILE_TYPE: str = "ContentsFile"
    default_map: HDF5Map = TimeContentsFileMap()
