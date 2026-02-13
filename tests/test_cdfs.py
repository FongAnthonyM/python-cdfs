""" test_contentsfile.py
Test for the baseobjects package.
"""

# Header #
__package_name__ = "cdfs"

__author__ = "Anthony Fong"
__credits__ = ["Anthony Fong"]
__maintainer__ = "Anthony Fong"
__email__ = ""

__copyright__ = "Copyright 2022, Anthony Fong"
__license__ = "MIT"

__version__ = "0.4.0"
__status__ = "Planning"

from src.cdfs.header import *

# Imports #
# Standard Libraries #
import pathlib

# Third-Party Packages #
import pytest
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs

# Local Packages #
from src.cdfs import BaseCDFS
from cdfs.contentsdatabase.tables import BaseMetaInformationTable, BaseTimeContentsTable
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
        "contentsdatabase": (TimeContentsCDFSComponent, {}),
    }


class TestCDFS:
    def test_create_cdfs(self, tmp_dir):
        cdfs = CDFSTest(path=tmp_dir.joinpath("testroot"), create=True)

        assert cdfs.is_open
        assert bool(cdfs)
        assert cdfs.contents_file.path.exists()
