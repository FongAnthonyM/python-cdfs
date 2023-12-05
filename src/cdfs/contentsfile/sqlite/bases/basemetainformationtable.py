"""basemetainformationtable.py

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
from typing import Any, Union

# Third-Party Packages #
from baseobjects import singlekwargdispatch
from sqlalchemy import select, lambda_stmt
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# Local Packages #
from .basetable import BaseTable


# Definitions #
# Classes #
class BaseMetaInformationTable(BaseTable):
    __tablename__ = "metainformation"
    __mapper_args__ = {"polymorphic_identity": "metainformation"}

    # Class Methods #
    @classmethod
    def create_information(
        cls,
        session: Session,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if begin:
            with session.begin():
                result = session.execute(lambda_stmt(lambda: select(cls))).scalar()
                if result is None:
                    cls.insert(session=session, entry=entry, as_entry=True, begin=False, **kwargs)
                else:
                    result.update(entry, **kwargs)
        else:
            result = session.execute(lambda_stmt(lambda: select(cls))).scalar()
            if result is None:
                cls.insert(session=session, entry=entry, as_entry=True, begin=False, **kwargs)
            else:
                result.update(entry, **kwargs)

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def create_information_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @create_information_async.register(async_sessionmaker)
    @classmethod
    async def _create_information_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        statement = lambda_stmt(lambda: select(cls))
        async with session() as async_session:
            async with async_session.begin():
                result = (await async_session.execute(statement)).scalar()
                if result is None:
                    await cls.insert_async(session=async_session, entry=entry, as_entry=True, begin=False, **kwargs)
                else:
                    result.update(entry, **kwargs)

    @create_information_async.register(AsyncSession)
    @classmethod
    async def _create_information_async(
        cls,
        session: AsyncSession,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        statement = lambda_stmt(lambda: select(cls))
        if begin:
            async with session.begin():
                result = (await session.execute(statement)).scalar()
                if result is None:
                    await cls.insert_async(session=session, entry=entry, as_entry=True, begin=False, **kwargs)
                else:
                    result.update(entry, **kwargs)
        else:
            result = (await session.execute(statement)).scalar()
            if result is None:
                await cls.insert_async(session=session, entry=entry, as_entry=True, begin=False, **kwargs)
            else:
                result.update(entry, **kwargs)

    @classmethod
    def get_information(
        cls,
        session: Session,
        as_entry: bool = True,
    ) -> Union[dict[str, Any], "BaseMetaInformationTable"]:
        result = session.execute(lambda_stmt(lambda: select(cls))).scalar()
        return (result.as_entry() if as_entry else result) if result is not None else {}

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def get_information_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        as_entry: bool = True,
    ) -> Union[dict[str, Any], "BaseMetaInformationTable"]:
        raise TypeError(f"{type(session)} is not a valid type.")

    @get_information_async.register(async_sessionmaker)
    @classmethod
    async def _get_information_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        as_entry: bool = True,
    ) -> Union[dict[str, Any], "BaseMetaInformationTable"]:
        statement = lambda_stmt(lambda: select(cls))
        async with session() as async_session:
            result = (await async_session.execute(statement)).scalar()

        return (result.as_entry() if as_entry else result) if result is not None else {}

    @get_information_async.register(AsyncSession)
    @classmethod
    async def _get_information_async(
        cls,
        session: AsyncSession,
        as_entry: bool = True,
    ) -> Union[dict[str, Any], "BaseMetaInformationTable"]:
        result = (await session.execute(lambda_stmt(lambda: select(cls)))).scalar()
        return (result.as_entry() if as_entry else result) if result is not None else {}

    @classmethod
    def set_information(
        cls,
        session: Session,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        if begin:
            with session.begin():
                session.execute(lambda_stmt(lambda: select(cls))).scalar().update(entry, **kwargs)
        else:
            session.execute(lambda_stmt(lambda: select(cls))).scalar().update(entry, **kwargs)

    @singlekwargdispatch(kwarg="session")
    @classmethod
    async def set_information_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        raise TypeError(f"{type(session)} is not a valid type.")

    @set_information_async.register(async_sessionmaker)
    @classmethod
    async def _set_information_async(
        cls,
        session: async_sessionmaker[AsyncSession],
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        statement = lambda_stmt(lambda: select(cls))
        async with session() as async_session:
            async with async_session.begin():
                (await async_session.execute(statement)).scalar().update(entry, **kwargs)

    @set_information_async.register(AsyncSession)
    @classmethod
    async def _set_information_async(
        cls,
        session: AsyncSession,
        entry: dict[str, Any] | None = None,
        begin: bool = False,
        **kwargs: Any,
    ) -> None:
        statement = lambda_stmt(lambda: select(cls))
        if begin:
            async with session.begin():
                (await session.execute(statement)).scalar().update(entry, **kwargs)
        else:
            (await session.execute(statement)).scalar().update(entry, **kwargs)
            