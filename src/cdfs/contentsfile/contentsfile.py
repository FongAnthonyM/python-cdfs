"""contentsfile.py
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
from hdf5objects import BaseHDF5, BaseHDF5Map, HDF5Map, HDF5Group

# Local Packages #
from .contentcomponents import ContentsFileComponent, ContentGroupComponent
from .contentmaps import ContentGroupMap


# Definitions #
# Classes #
class ContentsFileMap(BaseHDF5Map):
    """A map which outlines a generic content file."""

    default_map_names: Mapping[str, str] = {"contents": "contents"}
    default_maps: Mapping[str, HDF5Map] = {
        "contents": ContentGroupMap(),
    }


class ContentsFile(BaseHDF5):
    """An HDF5 file which tracks the contents of the data in the CDFS."""

    FILE_TYPE: str = "ContentsFile"
    default_map: HDF5Map = ContentsFileMap()
    default_component_types = {"contents": (ContentsFileComponent, {})}

    @property
    def contents_root(self) -> HDF5Group:
        return self.components["contents"].get_root()

    @property
    def contents_root_node(self) -> ContentGroupComponent:
        return self.components["contents"].get_root_node_component()

    def build_swmr(self, **kwargs) -> None:
        raise NotImplemented
