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
from uuid import UUID

# Third-Party Packages #
from sqlalchemy.orm import Mapped, Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemyobjects.tables import BaseUpdateTableSchema, UpdateTableManifestation

# Local Packages #


# Definitions #
# Classes #
class BaseContentsTableSchema(BaseUpdateTableSchema):
    """A schema for a table which tracks the contents of multiple files.

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
    # Base
    @classmethod
    def to_sql_types(cls, dict_: dict[str, Any] | None = None, /, **kwargs) -> dict[str, Any]:
        """Casts Python types of an entry to SQLAlchemy types.
        
        Only table item elements (columns) which must cast to an SQLAlchemy type are cast to SQLAlchemy types. 
        Additionally, all elements are optional, such that they do not need to be provided. This way any subset of the
        elements can cast. For example: when updating a table item, a few elements can updated without providing all 
        elements.  
        
        Args:
            dict_: A dictionary representing the entry with Python types.
            **kwargs: Additional keyword arguments for the entry.

        Returns:
            dict[str, Any]: A dictionary representing the entry with SQLAlchemy types.
        """
        # Format parent entry
        sql_entry = super().to_sql_types(dict_, **kwargs)
        
        # Format
        if (path := sql_entry.get("path", None)) is not None:
            match path:
                case Path():
                    sql_entry["path"] = path.as_posix()
        if (shape := sql_entry.get("shape", None)) is not None:
            match shape:
                case tuple():
                    sql_entry["shape"] = str(shape).strip("()")
                case str():
                    sql_entry["shape"] = shape.strip("()")
        
        # Return formatted entry
        return sql_entry
    
    @classmethod
    def from_sql_types(cls, dict_: dict[str, Any] | None = None, /, **kwargs: Any) -> dict[str, Any]:
        """Casts SQLAlchemy types of an entry to Python types.
        
        Only table item elements (columns) which must cast to a Python type are cast to Python types. Additionally, all 
        elements are optional, such that they do not need to be provided. This way any subset of the elements can cast. 
        For example: when querying a table item, a few columns can be selected without providing all columns.
        
        Args:
            dict_: A dictionary representing the entry with SQLAlchemy types.
            **kwargs: Additional keyword arguments for the entry.
            
        Returns:
            dict[str, Any]: A dictionary representing the entry with Python types.
        """
        # Format parent entry
        python_entry = super().from_sql_types(dict_, **kwargs)
        
        # Format
        if (path := python_entry.get("path", None)) is not None:
            python_entry["path"] = Path(path)
                
        if (shape := python_entry.get("shape", None)) is not None:
            python_entry["shape"] = tuple(int(i) for i in python_entry["shape"].split(", "))
        
        # Return formatted entry
        return python_entry
    
    # Modification
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
    

class ContentsTableManifestation(UpdateTableManifestation):
    """The manifestation of a ContentsTable.

    Attributes:
        _database: A weak reference to the SQAlchemy database to interface with.
        table_schema: The SQLAlchemy declarative table which this object act as the interface for.

    Args:
        table_schema: The SQLAlchemy declarative table which this object act as the interface for.
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
            self.table_schema.correct_contents(session=session, path=path, begin=begin)
        else:
            with self.create_session() as session:
                self.table_schema.correct_contents(session=session, path=path, begin=True)

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
            await self.table_schema.correct_contents_async(session=session, path=path, begin=begin)
        else:
            async with self.create_async_session() as session:
                await self.table_schema.correct_contents_async(session=session, path=path, begin=True)
