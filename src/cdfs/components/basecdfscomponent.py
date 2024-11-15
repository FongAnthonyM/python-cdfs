"""basecdfscomponent.py.py
A base class for CDFS components.
"""
# Package Header #
from ..header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


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
