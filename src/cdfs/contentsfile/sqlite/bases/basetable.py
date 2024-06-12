"""basecontentstable.py

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
from baseobjects import singlekwargdispatch
from sqlalchemy import Uuid, Result, select, lambda_stmt, func
from sqlalchemy.orm import mapped_column, Session
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.types import BigInteger

# Local Packages #


# Definitions #
# Classes #
class BaseTable:
    __tablename__ = "base"
    __mapper_args__ = {"polymorphic_identity": "base"}
    id = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    update_id = mapped_column(BigInteger, default=0)

    # Class Methods #
    @classmethod
    def format_entry_kwargs(cls, id_: str | uuid.UUID | None = None, **kwargs: Any) -> dict[str, Any]:
        if id_ is not None:
            kwargs["id_"] = uuid.UUID(hex=id_) if isinstance(id_, str) else id_
        return kwargs

    @classmethod
    def item_from_entry(cls, dict_: dict[str, Any] | None = None, /, **kwargs) -> "BaseTable":
        return cls(**cls.format_entry_kwargs(**(({} if dict_ is None else dict_) | kwargs)))

    @classmethod
    def get_all(cls, session: Session, as_entries: bool = False) -> Result | list[dict[str, Any]]:
        results = session.execute(lambda_stmt(lambda: select(cls)))
        return [r.as_entry() for r in results.scalars()] if as_entries else results

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def get_all_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @get_all_async.__wrapped__.register(async_sessionmaker)
    async def _get_all_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        statement = lambda_stmt(lambda: select(cls))
        async with session() as async_session:
            results = await async_session.execute(statement)

        return [r.as_entry() for r in results.scalars()] if as_entries else results

    @classmethod
    @get_all_async.__wrapped__.register(AsyncSession)
    async def _get_all_async(
        cls,
        session: AsyncSession,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        results = await session.execute(lambda_stmt(lambda: select(cls)))
        return [r.as_entry() for r in results.scalars()] if as_entries else results

    @classmethod
    def insert(
        cls,
        session: Session,
        item: Any = None,
        entry: dict[str, Any] | None = None,
        as_entry: bool = False,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if as_entry:
            item = cls.item_from_entry(**(({} if entry is None else entry) | kwargs))

        if begin:
            with session.begin():
                session.add(item)
        else:
            session.add(item)

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def insert_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        item: Any = None,
        entry: dict[str, Any] | None = None,
        as_entry: bool = False,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @insert_async.__wrapped__.register(async_sessionmaker)
    async def _insert_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        item: Any = None,
        entry: dict[str, Any] | None = None,
        as_entry: bool = False,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if as_entry:
            item = cls.item_from_entry(**(({} if entry is None else entry) | kwargs))
        async with session() as async_session:
            async with async_session.begin():
                async_session.add(item)

    @classmethod
    @insert_async.__wrapped__.register(AsyncSession)
    async def _insert_async(
        cls,
        session: AsyncSession,
        item: Any = None,
        entry: dict[str, Any] | None = None,
        as_entry: bool = False,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if as_entry:
            item = cls.item_from_entry(**(({} if entry is None else entry) | kwargs))

        if begin:
            async with session.begin():
                session.add(item)
        else:
            session.add(item)

    @classmethod
    def insert_all(
        cls,
        session: Session,
        items: Iterable[Any],
        as_entries: bool = False,
        begin: bool = False,
    ) -> None:
        if as_entries:
            items = [cls.item_from_entry(i) for i in items]

        if begin:
            with session.begin():
                session.add_all(items)
        else:
            session.add_all(items)

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def insert_all_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        items: Iterable[Any],
        as_entries: bool = False,
        begin: bool = False,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @insert_all_async.__wrapped__.register(async_sessionmaker)
    async def _insert_all_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        items: Iterable[Any],
        as_entries: bool = False,
        begin: bool = False,
    ) -> None:
        if as_entries:
            items = [cls.item_from_entry(i) for i in items]
        async with session() as async_session:
            async with async_session.begin():
                async_session.add_all(items)

    @classmethod
    @insert_all_async.__wrapped__.register(AsyncSession)
    async def insert_all_async(
        cls,
        session: AsyncSession,
        items: Iterable[Any],
        as_entries: bool = False,
        begin: bool = False,
    ) -> None:
        if as_entries:
            items = [cls.item_from_entry(i) for i in items]

        if begin:
            async with session.begin():
                session.add_all(items)
        else:
            session.add_all(items)

    @classmethod
    def _create_find_statement(cls, key: str, value: Any):
        column = getattr(cls, key)
        statement = lambda_stmt(lambda: select(cls))
        statement += lambda s: s.where(column == value)
        return statement

    @classmethod
    def update_entry(
        cls,
        session: Session,
        entry: dict[str, Any] | None = None,
        key: str = "id_",
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        entry.update(kwargs)
        statement = cls._create_find_statement(key, entry[key])
        if begin:
            with session.begin():
                item = session.execute(statement).scalar()
                if item is None:
                    cls.insert(session=session, entry=entry, as_entry=True)
                else:
                    item.update(entry)
        else:
            item = session.execute(statement).scalar()
            if item is None:
                cls.insert(session=session, entry=entry, as_entry=True)
            else:
                item.update(entry)

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def update_entry_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        entry: dict[str, Any] | None = None,
        keys: str = "id_",
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @update_entry_async.__wrapped__.register(async_sessionmaker)
    async def _update_entry_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        entry: dict[str, Any] | None = None,
        key: str = "id_",
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        entry.update(kwargs)
        statement = cls._create_find_statement(key, entry[key])
        async with session() as async_session:
            async with async_session.begin():
                item = (await async_session.execute(statement)).scalar()
                if item is None:
                    await cls.insert_async(session=session, entry=entry, as_entry=True)
                else:
                    item.update(entry)

    @classmethod
    @update_entry_async.__wrapped__.register(AsyncSession)
    async def _update_entry_async(
        cls,
        session: AsyncSession,
        entry: dict[str, Any] | None = None,
        key: str = "id_",
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        entry.update(kwargs)
        statement = cls._create_find_statement(key, entry[key])
        if begin:
            async with session.begin():
                item = (await session.execute(statement)).scalar()
                if item is None:
                    await cls.insert_async(session=session, entry=entry, as_entry=True)
                else:
                    item.update(entry)
        else:
            item = (await session.execute(statement)).scalar()
            if item is None:
                await cls.insert_async(session=session, entry=entry, as_entry=True)
            else:
                item.update(entry)

    @classmethod
    def update_entries(
        cls,
        session: Session,
        entries: Iterable[dict[str, Any]] | None = None,
        key: str = "id_",
        begin: bool = False,
    ) -> None:
        items = []
        if begin:
            with session.begin():
                for entry in entries:
                    item = session.execute(cls._create_find_statement(key, entry[key])).scalar()
                    if item is None:
                        items.append(entry)
                    else:
                        item.update(entry)
                if items:
                    cls.insert_all(session=session, items=items, as_entries=True)
        else:
            for entry in entries:
                item = session.execute(cls._create_find_statement(key, entry[key])).scalar()
                if item is None:
                    items.append(entry)
                else:
                    item.update(entry)
            if items:
                cls.insert_all(session=session, items=items, as_entries=True)

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def update_entries_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        entries: Iterable[dict[str, Any]] | None = None,
        key: str = "id_",
        begin: bool = False,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @update_entries_async.__wrapped__.register(async_sessionmaker)
    async def _update_entries_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        entries: Iterable[dict[str, Any]] | None = None,
        key: str = "id_",
        begin: bool = False,
    ) -> None:
        items = []
        async with session() as async_session:
            async with async_session.begin():
                for entry in entries:
                    item = (await async_session.execute(cls._create_find_statement(key, entry[key]))).scalar()
                    if item is None:
                        items.append(entry)
                    else:
                        item.update(entry)
                if items:
                    await cls.insert_all_async(session=async_session, items=items, as_entries=True)

    @classmethod
    @update_entries_async.__wrapped__.register(AsyncSession)
    async def _update_entries_async(
        cls,
        session: AsyncSession,
        entries: Iterable[dict[str, Any]] | None = None,
        key: str = "id_",
        begin: bool = False,
    ) -> None:
        items = []
        if begin:
            async with session.begin():
                for entry in entries:
                    item = (await session.execute(cls._create_find_statement(key, entry[key]))).scalar()
                    if item is None:
                        items.append(entry)
                    else:
                        item.update(entry)
                if items:
                    await cls.insert_all_async(session=session, items=items, as_entries=True)
        else:
            for entry in entries:
                item = (await session.execute(cls._create_find_statement(key, entry[key]))).scalar()
                if item is None:
                    items.append(entry)
                else:
                    item.update(entry)
            if items:
                await cls.insert_all_async(session=session, items=items, as_entries=True)

    @classmethod
    def delete_item(
        cls,
        session: Session,
        item: "BaseTable",
        begin: bool = False,
    ) -> None:
        if begin:
            with session.begin():
                session.delete(item)
        else:
            session.delete(item)

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def delete_item_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        item: "BaseTable",
        begin: bool = False,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @delete_item_async.__wrapped__.register(async_sessionmaker)
    async def _delete_entry_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        item: "BaseTable",
        begin: bool = False,
    ) -> None:
        async with session() as async_session:
            async with async_session.begin():
                await async_session.delete(item)

    @classmethod
    @delete_item_async.__wrapped__.register(AsyncSession)
    async def _delete_item_async(
        cls,
        session: AsyncSession,
        item: "BaseTable",
        begin: bool = False,
    ) -> None:
        if begin:
            async with session.begin():
                await session.delete(item)
        else:
            await session.delete(item)

    @classmethod
    def get_last_update_id(cls, session: Session) -> int | None:
        return session.execute(lambda_stmt(lambda: select(func.max(cls.update_id)))).one_or_none()[0]

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def get_last_update_id_async(cls, session: async_sessionmaker[AsyncSession] | AsyncSession) -> int | None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @get_last_update_id_async.__wrapped__.register(async_sessionmaker)
    async def _get_last_update_id_async(cls, session: async_sessionmaker[AsyncSession]) -> int | None:
        statement = lambda_stmt(lambda: select(func.max(cls.update_id)))
        async with session() as async_session:
            return (await async_session.execute(statement)).one_or_none()[0]

    @classmethod
    @get_last_update_id_async.__wrapped__.register(AsyncSession)
    async def _get_last_update_id_async(cls, session: AsyncSession) -> int | None:
        return (await session.execute(lambda_stmt(lambda: select(func.max(cls.update_id))))).one_or_none()[0]

    @classmethod
    def get_from_update(
        cls,
        session: Session,
        update_id: int,
        inclusive: bool = True,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        update_statement = lambda_stmt(lambda: select(cls))
        if inclusive:
            update_statement += lambda s: s.where(cls.update_id >= update_id)
        else:
            update_statement += lambda s: s.where(cls.update_id > update_id)

        results = session.execute(update_statement)
        return [r.as_entry() for r in results.scalars()] if as_entries else results

    @classmethod
    @singlekwargdispatch(kwarg="session")
    async def get_from_update_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        update_id: int,
        inclusive: bool = True,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        raise TypeError(f"{type(session)} is not a valid type.")

    @classmethod
    @get_from_update_async.__wrapped__.register(async_sessionmaker)
    async def _get_from_update_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        update_id: int,
        inclusive: bool = True,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        update_statement = lambda_stmt(lambda: select(cls))
        if inclusive:
            update_statement += lambda s: s.where(cls.update_id >= update_id)
        else:
            update_statement += lambda s: s.where(cls.update_id > update_id)

        async with session() as async_session:
            results = await async_session.execute(update_statement)

        return [r.as_entry() for r in results.scalars()] if as_entries else results

    @classmethod
    @get_from_update_async.__wrapped__.register(AsyncSession)
    async def _get_from_update_async(
        cls,
        session: AsyncSession,
        update_id: int,
        inclusive: bool = True,
        as_entries: bool = False,
    ) -> Result | list[dict[str, Any]]:
        update_statement = lambda_stmt(lambda: select(cls))
        if inclusive:
            update_statement += lambda s: s.where(cls.update_id >= update_id)
        else:
            update_statement += lambda s: s.where(cls.update_id > update_id)

        results = await session.execute(update_statement)
        return [r.as_entry() for r in results.scalars()] if as_entries else results

    # Instance Methods #
    def update(self, dict_: dict[str, Any] | None = None, /, **kwargs) -> None:
        dict_ = ({} if dict_ is None else dict_) | kwargs
        if (update_id := dict_.get("update_id", None)) is not None:
            self.update_id = update_id

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "update_id": self.update_id}

    def as_entry(self) -> dict[str, Any]:
        return {"id": self.id, "update_id": self.update_id}
