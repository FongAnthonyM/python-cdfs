""" contentmaps.py
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
from hdf5objects.treehierarchy import BaseNodeDatasetMap, BaseNodeGroupMap
import numpy as np

# Local Packages #
from ..contentcomponents import ContentDatasetComponent, ContentGroupComponent


# Definitions #
# Classes #
class ContentDatasetMap(BaseNodeDatasetMap):
    """A map for a dataset that outlines sequential data across multiple files."""
    default_attribute_names = {"max_shape": "max_shape", "min_shape": "min_shape"}
    default_dtype = (
        ("Node", h5py.ref_dtype),
        ("Path", str),
        ("Length", np.uint64),
        ("Minimum ndim", np.uint64),
        ("Maximum ndim", np.uint64),
    )
    default_axis_maps = [{
        "id_axis": IDAxisMap(component_kwargs = {"axis": {"is_uuid": True}}),
    }]
    default_component_types = {
        "object_reference": (ObjectReferenceComponent, {"reference_fields": {"node": "Node"},
                                                        "primary_reference_field": "node",
                                                        }),
        "tree_node": (ContentDatasetComponent, {}),
    }
    default_kwargs = {"shape": (0,), "maxshape": (None,)}


class ContentGroupMap(BaseNodeGroupMap):
    """A group map which outlines a group with basic node methods."""
    default_attribute_names = {"tree_type": "tree_type"}
    default_attributes = {"tree_type": "Node"}
    default_map_names = {"map_dataset": "map_dataset"}
    default_maps = {"map_dataset": ContentDatasetMap()}
    default_component_types = {
        "tree_node": (ContentGroupComponent, {}),
    }
