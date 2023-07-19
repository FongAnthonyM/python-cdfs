"""contentmaps.py
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
from hdf5objects.dataset import RegionReferenceAxisMap, IDAxisMap, ShapesMap
from hdf5objects.dataset import ObjectReferenceComponent, RegionReferenceComponent, RegionReferenceAxisComponent
from hdf5objects.treehierarchy import BaseNodeDatasetMap, BaseNodeGroupMap
import numpy as np

# Local Packages #
from ..contentcomponents import ContentDatasetComponent, ContentGroupComponent


# Definitions #
# Classes #
class ContentShapesDatasetMap(ShapesMap):
    default_axis_maps = [
        {
            "region_axis": RegionReferenceAxisMap(),
        }
    ]


class ContentDatasetMap(BaseNodeDatasetMap):
    """A map for a dataset that outlines sequential data across multiple files."""

    default_attribute_names = {"min_shapes_dataset": "min_shapes_dataset", "max_shapes_dataset": "max_shapes_dataset"}
    default_dtype = (
        ("Node", h5py.ref_dtype),
        ("Path", str),
        ("Axis", np.uint64),
        ("Minimum Shape", h5py.regionref_dtype),
        ("Maximum Shape", h5py.regionref_dtype),
    )
    default_axis_maps = [
        {
            "id_axis": IDAxisMap(component_kwargs={"axis": {"is_uuid": True}}),
        }
    ]
    default_component_types = {
        "object_reference": (
            ObjectReferenceComponent,
            {
                "reference_fields": {"node": "Node"},
                "primary_reference_field": "node",
            },
        ),
        "region_reference": (
            RegionReferenceComponent,
            {
                "single_reference_fields": {
                    "min_shapes": ("min_shapes_dataset", "Minimum Shape"),
                    "max_shapes": ("max_shapes_dataset", "Maximum Shape"),
                },
            },
        ),
        "tree_node": (ContentDatasetComponent, {}),
    }
    default_kwargs = {"shape": (0,), "maxshape": (None,)}


class ContentGroupMap(BaseNodeGroupMap):
    """A group map which outlines a group with basic node methods."""

    default_attribute_names = {"tree_type": "tree_type"}
    default_attributes = {"tree_type": "Node"}
    default_map_names = {"node_map": "node_map", "min_shapes": "min_shapes", "max_shapes": "max_shapes"}
    default_maps = {
        "node_map": ContentDatasetMap(),
        "min_shapes": ContentShapesDatasetMap(),
        "max_shapes": ContentShapesDatasetMap(),
    }
    default_component_types = {
        "tree_node": (ContentGroupComponent, {}),
    }
