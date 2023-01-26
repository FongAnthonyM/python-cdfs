""" ordernodemap.py
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
import h5py
from hdf5objects import DatasetMap
from hdf5objects.dataset import SampleAxisMap, IDAxisMap, ObjectReferenceComponent
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class OrderNodeMap(DatasetMap):
    """A map for a dataset that outlines sequential data across multiple files."""
    default_attribute_names = {"map_type": "map_type", "max_shape": "max_shape", "min_shape": "min_shape"}
    default_attributes = {"map_type": "OrderNodeMap"}
    default_dtype = (
        ("Path", str),
        ("Length", np.uint64),
        ("Minimum ndim", np.uint64),
        ("Maximum ndim", np.uint64),
        ("Dataset", h5py.ref_dtype),
    )
    default_axis_maps = [{
        "id_axis": IDAxisMap(),
        "start_sample_axis": SampleAxisMap(),
        "end_sample_axis": SampleAxisMap(),
    }]
    default_component_types = {
        "object_reference": (ObjectReferenceComponent, {"reference_fields": {"dataset": "Dataset"},
                                                        "primary_reference_field": "dataset",
                                                        }
                             ),
    }
