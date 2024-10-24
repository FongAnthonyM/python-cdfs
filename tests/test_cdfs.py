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
import pathlib

# Third-Party Packages #
import pytest
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession, async_sessionmaker

# Local Packages #
from src.cdfs import BaseCDFS
from src.cdfs.tables import BaseMetaInformationTable, BaseTimeContentsTable
from src.cdfs.components import MetaInformationCDFSComponent, TimeContentsCDFSComponent


# Definitions #
# Functions #
@pytest.fixture
def tmp_dir(tmpdir):
    """A pytest fixture that turn the tmpdir into a Path object."""
    return pathlib.Path(tmpdir)


# Classes #
class ContentsFileAsyncSchema(AsyncAttrs, DeclarativeBase):
    pass


class MetaInformationTable(BaseMetaInformationTable, ContentsFileAsyncSchema):
    pass


class TimeContentsTable(BaseTimeContentsTable, ContentsFileAsyncSchema):
    pass


class CDFSTest(BaseCDFS):
    schema = ContentsFileAsyncSchema
    default_component_types = {
        "meta_information": (MetaInformationCDFSComponent, {}),
        "contents": (TimeContentsCDFSComponent, {}),
    }


class TestCDFS:
    def test_create_cdfs(self, tmp_dir):
        cdfs = CDFSTest(path=tmp_dir.joinpath("testroot"), create=True)

        assert cdfs.is_open
        assert bool(cdfs)
        assert cdfs.contents_file.path.exists()
