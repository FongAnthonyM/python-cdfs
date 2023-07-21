"""contentstablebase.py
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
from collections.abc import Iterable
from typing import Any
import uuid

# Third-Party Packages #
from baseobjects import BaseObject
from sqlalchemy import Uuid
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class ContentsTableBase(BaseObject):
    __tablename__ = "contents"
    __mapper_args__ = {"polymorphic_on": "type", "polymorphic_identity": "contents"}
    id: Mapped = mapped_column(Uuid, primary_key=True)
    path: Mapped[str]
    axis: Mapped[int]
    min_shape: Mapped[str]
    max_shape: Mapped[str]

    @classmethod
    def format_entry_kwargs(
        cls,
        id_: str | uuid.UUID | None = None,
        path: str = "",
        axis: int = 0,
        min_shape: tuple[int] = (0,),
        max_shape: tuple[int] = (0,),
        **kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        kwargs.update(
            id_=uuid.uuid4() if id_ is None else uuid.UUID(hex=id_) if isinstance(id_, str) else id_,
            path=path,
            axis=axis,
            min_shape=str(min_shape).strip("()"),
            max_shape=str(max_shape).strip("()"),
        )
        return kwargs

    @classmethod
    def entry(cls, **kwargs: dict[str, Any]) -> "ContentsTableBase":
        return cls(**cls.format_entry_kwargs(**kwargs))

    @classmethod
    async def insert_entry_async(
        cls,
        async_session: async_sessionmaker[AsyncSession],
        commit: bool = False,
        **kwargs: dict[str, Any],
    ) -> None:
        entry = cls.entry(**kwargs)
        async with async_session() as session:
            async with session.begin():
                session.add(entry)

            if commit:
                await session.commit()

    @classmethod
    async def insert_entries_async(
        cls,
        async_session: async_sessionmaker[AsyncSession],
        entries: Iterable,
        commit: bool = False,
    ) -> None:
        async with async_session() as session:
            async with session.begin():
                session.add_all(entries)

            if commit:
                await session.commit()

    @classmethod
    async def delete_entry_async(
        cls,
        async_session: async_sessionmaker[AsyncSession],
        entry: "ContentsTableBase",
        commit: bool = False,
    ) -> None:
        async with async_session() as session:
            async with session.begin():
                await session.delete(entry)

            if commit:
                await session.commit()
