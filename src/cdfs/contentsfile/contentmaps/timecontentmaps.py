""" timecontentmaps.py
A map for a dataset that outlines timed data across multiple files.
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
from hdf5objects.dataset import SampleAxisMap, TimeAxisMap, IDAxisMap
from hdf5objects.dataset import ObjectReferenceComponent, RegionReferenceComponent, TimeSeriesComponent
import numpy as np

# Local Packages #
from ..contentcomponents import TimeContentDatasetComponent, TimeContentGroupComponent
from .contentmaps import ContentShapesDatasetMap, ContentDatasetMap, ContentGroupMap


# Definitions #
# Classes #
class TimeContentDatasetMap(ContentDatasetMap):
    """A map for a dataset that outlines timed data across multiple files."""
    default_attribute_names = ContentDatasetMap.default_attribute_names | {"t_axis": "t_axis"}
    default_attributes = {"t_axis": 0}
    default_dtype = ContentDatasetMap.default_dtype + (("Sample Rate", np.float64),)
    default_axis_maps = [{
        "id_axis": IDAxisMap(component_kwargs = {"axis": {"is_uuid": True}}),
        "start_time_axis": TimeAxisMap(),
        "end_time_axis": TimeAxisMap(),
    }]
    default_component_types = {
        "object_reference": (ObjectReferenceComponent, {
            "reference_fields": {"node": "Node"},
            "primary_reference_field": "node",
        }),
        "region_reference": (RegionReferenceComponent, {
            "single_reference_fields": {
                "min_shapes": ("min_shapes_dataset", "Minimum Shape"),
                "max_shapes": ("max_shapes_dataset", "Maximum Shape"),
            },
        }),
        "start_times": (TimeSeriesComponent, {"scale_name": "start_time_axis"}),
        "end_times": (TimeSeriesComponent, {"scale_name": "end_time_axis"}),
        "tree_node": (TimeContentDatasetComponent, {}),
    }


class TimeContentGroupMap(ContentGroupMap):
    """A group map which outlines a group with basic node methods."""
    default_attributes = {"tree_type": "Node"}
    default_maps = {
        "node_map": TimeContentDatasetMap(),
        "min_shapes": ContentShapesDatasetMap(),
        "max_shapes": ContentShapesDatasetMap(),
    }
    default_component_types = {
        "tree_node": (TimeContentGroupComponent, {}),
    }
