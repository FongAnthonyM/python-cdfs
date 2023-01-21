""" timenodemap.py
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
from hdf5objects.dataset import ObjectReferenceComponent, TimeSeriesComponent

# Local Packages #
from ..contentcomponents import TimeNodeComponent
from .ordernodemap import OrderNodeMap


# Definitions #
# Classes #
class TimeNodeMap(OrderNodeMap):
    """A map for a dataset that outlines timed data across multiple files."""
    default_attribute_names = {"map_type": "TimeNodeMap", "t_axis": "t_axis"}
    default_attributes = {"t_axis": 0}
    default_axis_maps = [{
        "id_axis": IDAxisMap(component_kwargs = {"axis": {"is_uuid": True}}),
        "start_time_axis": TimeAxisMap(),
        "end_time_axis": TimeAxisMap(),
        "start_sample_axis": SampleAxisMap(),
        "end_sample_axis": SampleAxisMap(),
    }]
    default_component_types = {
        "object_reference": (ObjectReferenceComponent, {"reference_fields": {"dataset": "Dataset"},
                                                        "primary_reference_field": "dataset",
                                                        }
                             ),
        "start_times": (TimeSeriesComponent, {"scale_name": "start_time_axis"}),
        "end_times": (TimeSeriesComponent, {"scale_name": "end_time_axis"}),
        "node_content": (TimeNodeComponent, {}),
    }
    default_kwargs = {"shape": (0,), "maxshape": (None,)}
