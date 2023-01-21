""" orderleafmap.py
A map for a dataset that outlines sequential data across multiple files.
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

# Third-Party Packages #
from hdf5objects import DatasetMap
from hdf5objects.dataset import SampleAxisMap, IDAxisMap
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class OrderLeafMap(DatasetMap):
    """A map for a dataset that outlines sequential data across multiple files."""
    default_attribute_names = {"map_type": "map_type"}
    default_attributes = {"map_type": "OrderLeafMap"}
    default_dtype = (
        ("Path", str),
        ("Length", np.uint64),
    )
    default_axis_maps = [{
        "id_axis": IDAxisMap(),
        "start_sample_axis": SampleAxisMap(),
        "end_sample_axis": SampleAxisMap(),
    }]
