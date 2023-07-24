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
import pathlib
from typing import Any

# Third-Party Packages #
from taskblocks import TaskBlock
import pytest

# Local Packages #
from src.cdfs.contentsfile.sqlite import *


# Definitions #
# Functions #
@pytest.fixture
def tmp_dir(tmpdir):
    """A pytest fixture that turn the tmpdir into a Path object."""
    return pathlib.Path(tmpdir)


# Classes #
class TestContentsFile:

    async def create_contents_file(self, path):
        db = ContentsFile(path=path)
        await db.create_file_async()

    def test_async_contents_file(self, tmp_path):
        file_path = pathlib.Path.cwd() / "test.db"
        asyncio.run(self.create_contents_file(path=file_path))
        assert file_path.is_file()
