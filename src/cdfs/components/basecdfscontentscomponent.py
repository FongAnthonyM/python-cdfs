""" basecdfscontentscomponent.py.py
A base class for CDFS components that interact with a contents table.
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
from typing import Any

# Third-Party Packages #
from sqlalchemyobjects.tables import TableManifestation

# Local Packages #
from .basecdfscomponent import BaseCDFSComponent


# Definitions #
# Classes #
class BaseCDFSContentsComponent(BaseCDFSComponent):
    """A base class for CDFS components that interact with a contents table.

    Attributes:
        table_name: The name of the table that this component interacts with.
        _contents_table: A cached instance of the contents table. Initialized to None and set when accessed.
    """

    # Attributes #
    table_name: str = "contents"
    _contents_table: TableManifestation | None = None

    # Properties #
    @property
    def contents_table(self) -> TableManifestation:
        """Gets the contents table.

        Returns:
            The contents table.
        """
        if self._contents_table is None:
            self._contents_table = self._composite().contents_database.contents_tables[self.table_name]
        return self._contents_table

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        composite: Any = None,
        table_name: str | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(composite, table_name, **kwargs)

    # Pickling
    def __getstate__(self) -> dict[str, Any]:
        """Creates a dictionary of attributes which can be used to rebuild this object.

        Returns:
            dict: A dictionary of this object's attributes.
        """
        state = super().__getstate__()
        for name in ("_contents_table",):
            if name in state:
                del state[name]
        return state

    # Instance Methods #
    # Constructors/Destructors
    def construct(self, composite: Any = None, table_name: str | None = None, **kwargs: Any) -> None:
        """Constructs this object.

        Args:
            composite: The object which this object is a component of.
            table_name: The name of the table.
            **kwargs: Additional keyword arguments.
        """
        if table_name is not None:
            self.table_name = table_name

        super().construct(composite=composite, **kwargs)
