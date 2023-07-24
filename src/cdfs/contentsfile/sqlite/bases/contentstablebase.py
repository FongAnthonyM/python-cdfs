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
from baseobjects import BaseObject, singlekwargdispatch
from sqlalchemy import Uuid, select, Result
from sqlalchemy.orm import Mapped, MappedColumn, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class ContentsTableBase:
    __tablename__ = "contents"
    __mapper_args__ = {"polymorphic_identity": "contents"}
    id = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    update_id: Mapped[int]
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

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def insert_entry_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        commit: bool = False,
        **kwargs: dict[str, Any],
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @insert_entry_async.register(async_sessionmaker)
    @classmethod
    async def _insert_entry_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        commit: bool = False,
        **kwargs: dict[str, Any],
    ) -> None:
        entry = cls.entry(**kwargs)
        async with session() as async_session:
            async with async_session.begin():
                async_session.add(entry)

            if commit:
                await async_session.commit()

    @insert_entry_async.register(AsyncSession)
    @classmethod
    async def _insert_entry_async(
        cls,
        session: AsyncSession,
        commit: bool = False,
        **kwargs: dict[str, Any],
    ) -> None:
        entry = cls.entry(**kwargs)
        async with session.begin():
            session.add(entry)

        if commit:
            await session.commit()

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def insert_entries_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        entries: Iterable,
        commit: bool = False,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @insert_entries_async.register(async_sessionmaker)
    @classmethod
    async def _insert_entries_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        entries: Iterable,
        commit: bool = False,
    ) -> None:
        async with session() as async_session:
            async with async_session.begin():
                async_session.add_all(entries)

            if commit:
                await async_session.commit()

    @insert_entries_async.register(AsyncSession)
    @classmethod
    async def insert_entries_async(
        cls,
        session: AsyncSession,
        entries: Iterable,
        commit: bool = False,
    ) -> None:
        async with session.begin():
            session.add_all(entries)

        if commit:
            await session.commit()

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def delete_entry_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        entry: "ContentsTableBase",
        commit: bool = False,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @delete_entry_async.register(async_sessionmaker)
    @classmethod
    async def _delete_entry_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        entry: "ContentsTableBase",
        commit: bool = False,
    ) -> None:
        async with session() as async_session:
            async with async_session.begin():
                await async_session.delete(entry)

            if commit:
                await async_session.commit()

    @delete_entry_async.register(AsyncSession)
    @classmethod
    async def _delete_entry_async(
        cls,
        session: AsyncSession,
        entry: "ContentsTableBase",
        commit: bool = False,
    ) -> None:
        async with session.begin():
            await session.delete(entry)

        if commit:
            await session.commit()

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def get_from_update_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        update_id: int,
        inclusive: bool = True,
    ) -> Result:
        raise TypeError(f"{type(session)} is not a valid type.")

    @get_from_update_async.register(async_sessionmaker)
    @classmethod
    async def _get_from_update_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        update_id: int,
        inclusive: bool = True,
    ) -> Result:
        async with session() as async_session:
            if inclusive:
                return await async_session.execute(select(cls).where(cls.update_id >= update_id))
            else:
                return await async_session.execute(select(cls).where(cls.update_id > update_id))

    @get_from_update_async.register(AsyncSession)
    @classmethod
    async def _get_from_update_async(
        cls,
        session: AsyncSession,
        update_id: int,
        inclusive: bool = True,
    ) -> Result:
        if inclusive:
            return await session.execute(select(cls).where(cls.update_id >= update_id))
        else:
            return await session.execute(select(cls).where(cls.update_id > update_id))
