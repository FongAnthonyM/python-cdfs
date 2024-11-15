"""contentsdatabase.py
Manages the contents database including creating, opening, and modifying the database.
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

# Third-Party Packages #
from sqlalchemyobjects import Database

# Local Packages #


# Definitions #
# Classes #
class ContentsDatabase(Database):
    """Manages the contents database including creating, opening, and modifying the database.

    Attributes:
        _path: The file path to the database.
        url: The URL to the database.
        _engine: The SQLAlchemy engine for synchronous operations.
        _async_engine: The SQLAlchemy engine for asynchronous operations.
        session_maker_kwargs: Keyword arguments for the synchronous session maker.
        _session_maker: Factory for creating synchronous sessions.
        async_session_maker_kwargs: Keyword arguments for the asynchronous session maker.
        _async_session_maker: Factory for creating asynchronous sessions.
        schema: The database schema class.
        table_map: A map which outlines which table are within this database.
        tables: A dictionary of table_map within this database.

    Args:
        path: The path to the database file.
        schema: The database schema class.
        table_map: A map which outlines which table are within this database.
        open_: Whether to open the database. Defaults to False.
        create: Whether to create the database. Defaults to False.
        init: Whether to initialize the object.
        **kwargs: Additional keyword arguments.
    """
