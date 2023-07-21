"""contentstablebase.py
A node component which implements content information in its dataset.
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
from collections.abc import Iterable
from typing import Any
import uuid

# Third-Party Packages #
from baseobjects import BaseObject
from sqlalchemy import Uuid
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs
import numpy as np

# Local Packages #
from .bases import ContentsTableBase


# Definitions #
# Classes #
class ContentsFileAsyncDatabaseBase(AsyncAttrs, DeclarativeBase):
    pass


class ContentsTable(ContentsTableBase, ContentsFileAsyncDatabaseBase):
    pass


class ContentsFile(BaseObject):
    """

        Class Attributes:

        Attributes:

        Args:

        """

    # Magic Methods #
    # Construction/Destruction
    def __init__(self, init: bool = True) -> None:
        self.engine = None

    def construct(self) -> None:
        pass

    def create_engine(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///test.db")
