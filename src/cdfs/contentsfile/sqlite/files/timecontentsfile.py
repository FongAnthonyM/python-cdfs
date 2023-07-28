"""basetimecontentstable.py
A node component which implements content information in its dataset.
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
import pathlib

# Third-Party Packages #
from baseobjects import BaseObject
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs, create_async_engine, AsyncEngine

# Local Packages #
from ..bases import BaseMetaInformationTable, BaseTimeContentsTable
from .contentsfile import ContentsFile


# Definitions #
# Classes #
class TimeContentsFileAsyncSchema(AsyncAttrs, DeclarativeBase):
    pass


class TimeMetaInformationTable(BaseMetaInformationTable, TimeContentsFileAsyncSchema):
    pass


class TimeContentsTable(BaseTimeContentsTable, TimeContentsFileAsyncSchema):
    pass


class TimeContentsFile(ContentsFile):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    schema: type[DeclarativeBase] = TimeContentsFileAsyncSchema
    meta_information: type[BaseMetaInformationTable] = TimeMetaInformationTable
    contents: type[BaseTimeContentsTable] = TimeContentsTable

    # Magic Methods #
    # Construction/Destruction

