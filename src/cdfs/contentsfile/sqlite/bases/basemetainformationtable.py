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
    