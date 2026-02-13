"""basecdfscomponent.py.py
A base class for CDFS components.
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
# Standard Libraries #
from weakref import ref

# Third-Party Packages #
from baseobjects import BaseComponent
from sqlalchemyobjects.tables import TableManifestation

# Local Packages #
from ..contentsdatabase import ContentsDatabase


# Definitions #
# Classes #
class BaseCDFSComponent(BaseComponent):
    """A base class for CDFS components.

    Attributes:
        _composite: A weak reference to the object which this object is a component of.

    Args:
        composite: The object which this object is a component of.
        init: Determines if this object will construct.
        **kwargs: Keyword arguments for inheritance.
    """

    # Attributes #
    _composite: ref[ContentsDatabase] | None = None

    # Properties #
    @property
    def contents_database(self) -> ContentsDatabase | None:
        """The contents database of the CDFS."""
        try:
            return self._composite().contents_database
        except TypeError:
            return None

    @property
    def contents_tables(self) -> dict[str, TableManifestation] | None:
        """The tables of the CDFS."""
        try:
            return self._composite().contents_database.tables
        except TypeError:
            return None
