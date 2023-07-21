# Imports #
# Standard Libraries #
import asyncio
from time import sleep, perf_counter_ns
from typing import List
from typing import Optional, Any

# Third-Party Packages #
from taskblocks import AsyncEvent
from taskblocks import AsyncQueue
from taskblocks import AsyncQueueInterface
from taskblocks import AsyncQueueManager
from taskblocks import TaskBlock

from sqlalchemy import String
from sqlalchemy import Uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


# Definitions #
# Classes #
class Base(AsyncAttrs, DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user_account"
    id: Mapped[Uuid] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    number: Mapped[Optional[int]]

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.number!r})"


class WriterTask(TaskBlock):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        name: str = "",
        sets_up: bool = True,
        tears_down: bool = True,
        is_process: bool = False,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.engine = None

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construct #
        if init:
            self.construct(
                name=name,
                sets_up=sets_up,
                tears_down=tears_down,
                is_process=is_process,
                s_kwargs=s_kwargs,
                t_kwargs=t_kwargs,
                d_kwargs=d_kwargs,
            )

    # Setup
    async def setup(self, *args: Any, **kwargs: Any) -> None:
        """The method to run before executing task."""
        self.engine = create_async_engine("sqlite+aiosqlite:///test.db")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    # TaskBlock
    async def task(self, *args: Any, **kwargs: Any) -> None:
        """The main method to execute."""
        async with AsyncSession(self.engine) as session:
            async with session.begin():
                session.add_all([User(name="same", number=perf_counter_ns())])
                await session.commit()

    # Teardown
    async def teardown(self, *args: Any, **kwargs: Any) -> None:
        """The method to run after executing task."""
        await self.engine.dispose()


class ReaderTask(TaskBlock):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        name: str = "",
        sets_up: bool = True,
        tears_down: bool = True,
        is_process: bool = False,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.engine = None

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construct #
        if init:
            self.construct(
                name=name,
                sets_up=sets_up,
                tears_down=tears_down,
                is_process=is_process,
                s_kwargs=s_kwargs,
                t_kwargs=t_kwargs,
                d_kwargs=d_kwargs,
            )

    # Setup
    async def setup(self, *args: Any, **kwargs: Any) -> None:
        """The method to run before executing task."""
        self.engine = create_async_engine("sqlite+aiosqlite:///test.sqlite3")

    # TaskBlock
    async def task(self, *args: Any, **kwargs: Any) -> None:
        """The main method to execute."""
        async with AsyncSession(self.engine) as session:
            async with session.begin():
                result = await session.execute(select(User))
                things = list(result.scalars())
                print(len(things))

    # Teardown
    async def teardown(self, *args: Any, **kwargs: Any) -> None:
        """The method to run after executing task."""
        await self.engine.dispose()


# Main #
if __name__ == "__main__":
    writer = WriterTask(is_process=True)
    reader = ReaderTask(is_process=True)
    writer.start()
    sleep(2)
    reader.start()

    sleep(10)
    writer.stop()
    reader.stop()
