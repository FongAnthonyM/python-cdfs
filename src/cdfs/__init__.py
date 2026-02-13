"""__init__.py

"""

# Header #
__package_name__ = "cdfs"

__author__ = "Anthony Fong"
__credits__ = ["Anthony Fong"]
__maintainer__ = "Anthony Fong"
__email__ = ""

__copyright__ = "Copyright 2022, Anthony Fong"
__license__ = "MIT"

__version__ = "0.4.0"
__status__ = "Planning"

# Imports #
# Local Packages #
from cdfs.contentsdatabase.tables import *
from .components import *
from cdfs.contentsdatabase.contentsdatabase import ContentsDatabase
from .basecdfs import BaseCDFS


# Definitions #
