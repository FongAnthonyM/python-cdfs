"""basecontentstable.py
A table which tracks the contents of multiple files.
"""
# Package Header #
from cdfs.header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from pathlib import Path
from typing import Any
import uuid

# Third-Party Packages #
from sqlalchemy.orm import Mapped, Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemyobjects.tables import BaseUpdateTable, UpdateTableManifestation

# Local Packages #


# Definitions #
# Classes #
class BaseContentsTable(BaseUpdateTable):
    """A table which tracks the contents of multiple files.

    This class defines a table which tracks the contents of multiple files and methods for formatting entry keyword
    arguments, correcting contents, and converting entries to dictionaries.

    Attributes:
        __tablename__: The name of the table.
        __mapper_args__: Mapper arguments for SQLAlchemy.
        path: The path of the content.
        axis: The axis of the content.
        shape: The shape of the content.
        file_type: The type of file which this table will track.
    """

    # Class Attributes #
    __tablename__ = "contents"
    __mapper_args__ = {"polymorphic_identity": "contents"}

    # Columns #
    path: Mapped[str]
    axis: Mapped[int]
    shape: Mapped[str]

    # Attributes #
    file_type: type | None = None

    # Class Methods #
    @classmethod
    def format_entry_kwargs(
        cls,
        id_: str | uuid.UUID | None = None,
        path: Path | str = "",
        axis: int = 0,
        shape: tuple[int] = (0,),
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Formats entry keyword arguments for creating or updating table entries.

        Args:
            id_: The ID of the entry, if specified.
            path: The path of the content. Defaults to an empty string.
            axis: The axis of the content. Defaults to 0.
            shape: The shape of the content. Defaults to (0,).
            **kwargs: Additional keyword arguments for the entry.

        Returns:
            dict[str, Any]: A dictionary of keyword arguments for the entry.
        """
        kwargs = super().format_entry_kwargs(id_=id_, **kwargs)
        kwargs.update(
            path=path.as_posix() if isinstance(path, Path) else path,
            axis=axis,
            shape=str(shape).strip("()"),
        )
        return kwargs

    @classmethod
    def _correct_contents(cls, session: Session, path: Path) -> None:
        """Corrects the contents of the table based on the provided path.

        Args:
            session: The SQLAlchemy session to use for the operation.
            path: The path of the content to correct.

        Raises:
            NotImplementedError: This method is not implemented.
        """
        raise NotImplemented

    @classmethod
    def correct_contents(cls, session: Session, path: Path, begin: bool = False) -> None:
        """Corrects the contents of the table based on the provided path.

        Args:
            session: The SQLAlchemy session to use for the operation.
            path: The path of the content to correct.
            begin: If True, begins a transaction for the operation. Defaults to False.
        """
        if begin:
            with session.begin():
                cls._correct_contents(session=session, path=path)
        else:
            cls._correct_contents(session=session, path=path)

    @classmethod
    async def _correct_contents_async(cls, session: AsyncSession, path: Path) -> None:
        """Asynchronously corrects the contents of the table based on the provided path.

        Args:
            session: The SQLAlchemy async session to use for the operation.
            path: The path of the content to correct.

        Raises:
            NotImplementedError: This method is not implemented.
        """
        raise NotImplemented

    @classmethod
    async def correct_contents_async(
        cls,
        session: AsyncSession,
        path: Path,
        begin: bool = False,
    ) -> None:
        """Asynchronously corrects the contents of the table based on the provided path.

        Args:
            session: The SQLAlchemy async session to use for the operation.
            path: The path of the content to correct.
            begin: If True, begins a transaction for the operation. Defaults to False.
        """
        if begin:
            async with session.begin():
                await cls._correct_contents_async(session=session, path=path)
        else:
            await cls._correct_contents_async(session=session, path=path)

    # Instance Methods #
    def update(self, dict_: dict[str, Any] | None = None, /, **kwargs) -> None:
        """Updates the row of the table with the provided dictionary or keyword arguments.

        Args:
            dict_: A dictionary of attributes/columns to update. Defaults to None.
            **kwargs: Additional keyword arguments for the attributes to update.
        """
        dict_ = ({} if dict_ is None else dict_) | kwargs
        if (path := dict_.get("path", None)) is not None:
            self.path = path.as_posix() if isinstance(path, pathlib.Path) else path
        if (axis := dict_.get("axis", None)) is not None:
            self.axis = axis
        if (shape := dict_.get("shape", None)) is not None:
            self.shape = str(shape).strip("()")
        super().update(dict_)

    def as_dict(self) -> dict[str, Any]:
        """Creates a dictionary with the contents of the row.

        Returns:
            dict[str, Any]: A dictionary representation of the row.
        """
        entry = super().as_dict()
        entry.update(
            path=self.path,
            axis=self.axis,
            shape=self.shape,
        )
        return entry

    def as_entry(self) -> dict[str, Any]:
        """Creates a dictionary with the entry contents of the row.

        Returns:
            dict[str, Any]: A dictionary representation of the entry.
        """
        entry = super().as_dict()
        entry.update(
            path=self.path,
            axis=self.axis,
            shape=tuple(int(i) for i in self.shape.split(", ")),
        )
        return entry


class ContentsTableManifestation(UpdateTableManifestation):
    """The manifestation of a ContentsTable.

    Attributes:
        _database: A weak reference to the SQAlchemy database to interface with.
        table: The SQLAlchemy declarative table which this object act as the interface for.

    Args:
        table: The SQLAlchemy declarative table which this object act as the interface for.
        database: The SQAlchemy database to interface with.
        init: Determines if this object will construct.
        **kwargs: Additional keyword arguments.
    """

    # Contents
    def correct_contents(
        self,
        path: Path,
        session: Session | None = None,
        begin: bool = False,
    ) -> None:
        """Corrects the contents of the file.

        Args:
            path: The path to the file.
            session: The SQLAlchemy session to apply the modification. Defaults to None.
            begin: If True, begins a transaction for the operation. Defaults to False.
        """
        if session is not None:
            self.table.correct_contents(session=session, path=path, begin=begin)
        else:
            with self.create_session() as session:
                self.table.correct_contents(session=session, path=path, begin=True)

    async def correct_contents_async(
        self,
        path: Path,
        session: AsyncSession | None = None,
        begin: bool = False,
    ) -> None:
        """Asynchronously corrects the contents of the file.

        Args:
            path: The path to the file.
            session: The SQLAlchemy session to apply the modification. Defaults to None.
            begin: If True, begins a transaction for the operation. Defaults to False.
        """
        if session is not None:
            await self.table.correct_contents_async(session=session, path=path, begin=begin)
        else:
            async with self.create_async_session() as session:
                await self.table.correct_contents_async(session=session, path=path, begin=True)
