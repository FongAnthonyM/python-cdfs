""" timeleafmap.py
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
from hdf5objects.dataset import SampleAxisMap, TimeAxisMap, TimeSeriesComponent, IDAxisMap
import numpy as np

# Local Packages #
from ..contentcomponents import TimeLeafComponent
from .orderleafmap import OrderLeafMap


# Definitions #
# Classes #
class TimeLeafMap(OrderLeafMap):
    """A map for a dataset that outlines timed data across multiple files."""
    default_attribute_names = OrderLeafMap.default_attribute_names | {"t_axis": "t_axis"}
    default_attributes = {"t_axis": 0, "map_type": "TimeLeafMap"}
    default_axis_maps = [{
        "id_axis": IDAxisMap(),
        "start_time_axis": TimeAxisMap(),
        "end_time_axis": TimeAxisMap(),
        "start_sample_axis": SampleAxisMap(),
        "end_sample_axis": SampleAxisMap(),
    }]
    default_dtype = OrderLeafMap.default_dtype + (
        ("Sample Rate", np.float64),
    )
    default_component_types = {
        "start_times": (TimeSeriesComponent, {"scale_name": "start_time_axis"}),
        "end_times": (TimeSeriesComponent, {"scale_name": "end_time_axis"}),
        "leaf_content": (TimeLeafComponent, {}),
    }
    default_kwargs = {"shape": (0,), "maxshape": (None,)}
