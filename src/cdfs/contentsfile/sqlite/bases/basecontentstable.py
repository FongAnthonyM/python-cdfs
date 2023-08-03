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
import pathlib
from typing import Any
import uuid

# Third-Party Packages #
from sqlalchemy.orm import Mapped, Session
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


# Local Packages #
from .basetable import BaseTable


# Definitions #
# Classes #
class BaseContentsTable(BaseTable):
    __tablename__ = "contents"
    __mapper_args__ = {"polymorphic_identity": "contents"}
    path: Mapped[str]
    axis: Mapped[int]
    shape: Mapped[str]

    file_type: type | None = None

    # Class Methods #
    @classmethod
    def format_entry_kwargs(
        cls,
        id_: str | uuid.UUID | None = None,
        path: pathlib.Path | str = "",
        axis: int = 0,
        shape: tuple[int] = (0,),
        **kwargs: Any,
    ) -> dict[str, Any]:
        kwargs = super().format_entry_kwargs(id_=id_, **kwargs)
        kwargs.update(
            path=path.as_posix() if isinstance(path, pathlib.Path) else path,
            axis=axis,
            shape=str(shape).strip("()"),
        )
        return kwargs

    @classmethod
    def correct_contents(cls, session: Session, path: pathlib.Path, begin: bool = False) -> None:
        raise NotImplementedError

    @classmethod
    async def correct_contents_async(
        cls,
        session: async_sessionmaker[AsyncSession] | AsyncSession,
        path: pathlib.Path,
        begin: bool = False,
    ) -> None:
        raise NotImplementedError

    # Instance Methods #
    def update(self, dict_: dict[str, Any] | None = None, /, **kwargs) -> None:
        dict_ = ({} if dict_ is None else dict_) | kwargs
        if (path := dict_.get("path", None)) is not None:
            self.path = path.as_posix() if isinstance(path, pathlib.Path) else path
        if (axis := dict_.get("axis", None)) is not None:
            self.axis = axis
        if (shape := dict_.get("shape", None)) is not None:
            self.shape = str(shape).strip("()")
        super().update(dict_)
    
    def as_dict(self) -> dict[str, Any]:
        entry = super().as_dict()
        entry.update(
            path=self.path,
            axis=self.axis,
            shape=self.shape,
        )
        return entry

    def as_entry(self) -> dict[str, Any]:
        entry = super().as_dict()
        entry.update(
            path=self.path,
            axis=self.axis,
            shape=tuple(int(i) for i in self.shape.split(", ")),
        )
        return entry
