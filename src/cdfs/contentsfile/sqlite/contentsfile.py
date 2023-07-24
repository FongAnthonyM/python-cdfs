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
import pathlib
from typing import Any
import uuid

# Third-Party Packages #
from baseobjects import BaseObject
from sqlalchemy import Uuid
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs, create_async_engine
import numpy as np

# Local Packages #
from .bases import ContentsTableBase


# Definitions #
# Classes #
class ContentsFileAsyncSchema(AsyncAttrs, DeclarativeBase):
    pass


class ContentsTable(ContentsTableBase, ContentsFileAsyncSchema):
    pass


class ContentsFile(BaseObject):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    schema: type[DeclarativeBase] = ContentsFileAsyncSchema
    contents: type[ContentsTableBase] = ContentsTable

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: str | pathlib.Path | None = None,
        create: bool = False,
        init: bool = True,
    ) -> None:
        self._path: pathlib.Path | None = None
        self.engine = None

        if init:
            self.construct(path=path)

    @property
    def path(self) -> pathlib.Path:
        """The path to the file."""
        return self._path

    @path.setter
    def path(self, value: str | pathlib.Path) -> None:
        if isinstance(value, pathlib.Path) or value is None:
            self._path = value
        else:
            self._path = pathlib.Path(value)

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: str | pathlib.Path | None = None,
    ) -> None:
        if path is not None:
            self.path = path

    def create_async_engine(self):
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{self._path.as_posix()}")

    async def create_file_async(self, path: str | pathlib.Path | None = None):
        if path is not None:
            self.path = path

        if self.engine is None or path is not None:
            self.create_async_engine()

        async with self.engine.begin() as conn:
            await conn.run_sync(self.schema.metadata.create_all)
