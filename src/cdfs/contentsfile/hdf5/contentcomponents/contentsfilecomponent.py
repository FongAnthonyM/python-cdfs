"""contentsfilecomponent.py

"""
# Package Header #
from ....header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__

# Imports #
# Standard Libraries #
from typing import Any

# Third-Party Packages #
from hdf5objects.treehierarchy import RootNodeComponent

# Local Packages #


# Definitions #
# Classes #
class ContentsFileComponent(RootNodeComponent):
    def correct_contents(self, path):
        raise NotImplementedError
