"""basecdfscomponent.py.py

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
from pathlib import Path

# Third-Party Packages #
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemyobjects.database.components import BaseUpdateTableComponent

# Local Packages #



# Definitions #
# Classes #
class BaseContentsTableComponent(BaseUpdateTableComponent):
    """A base class for a component that manages the contents of a contentsfile."""

    # Contents
    def correct_contents(
        self,
        path: Path,
        session: Session | None = None,
        begin: bool = False,
    ) -> None:
        """Corrects the contents of the file. (Abstract)

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
        """Asynchronously corrects the contents of the file. (Abstract)

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
