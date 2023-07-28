#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" test_contentsfile.py
Test for the baseobjects package.
"""
# Package Header #
from src.cdfs.header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
import asyncio
import abc
import datetime
import pathlib
from typing import Any

# Third-Party Packages #
import pytest
from dspobjects.time import nanostamp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# Local Packages #
from src.cdfs.contentsfile.sqlite import *
from .test_contentsfile import TestContentsFile


# Definitions #
# Functions #
@pytest.fixture
def tmp_dir(tmpdir):
    """A pytest fixture that turn the tmpdir into a Path object."""
    return pathlib.Path(tmpdir)


# Classes #
class TestTimeContentsFile(TestContentsFile):
    class_ = TimeContentsFile

    async def insert_async(self, path):
        db = self.class_(path=path)
        await db.create_file_async(echo=True)
        session = AsyncSession(db.engine)
        await db.contents.insert_async(
            session=session,
            as_entry=True,
            update_id=0,
            path="/example",
            axis=0,
            shape=(500, 100),
            timezone="local",
            start=datetime.datetime.now(),
            end=datetime.datetime.now(),
            sample_rate=1024,
        )
        result = await session.execute(select(db.contents).where(db.contents.path == "/example").limit(1))
        assert result.scalar().path == "/example"
        await db.close_async()

    def test_insert_entry_async(self, tmp_path):
        file_path = tmp_path / "test.db"
        asyncio.run(self.insert_async(path=file_path))
        assert file_path.is_file()

    def test_get_start_datetime(self, tmp_path):
        file_path = tmp_path / "test.db"
        db = self.class_(path=file_path, open_=True, create=True)
        time_contents = self.class_.contents
        with db.create_session() as session:
            for i in range(10):
                time_contents.insert(
                    session=session,
                    as_entry=True,
                    update_id=0,
                    path="/example",
                    axis=0,
                    shape=(500, 100),
                    timezone="local",
                    start=datetime.datetime.now(),
                    end=datetime.datetime.now(),
                    sample_rate=1024,
                )
            dt = time_contents.get_start_datetime(session=session)

        assert file_path.is_file()



