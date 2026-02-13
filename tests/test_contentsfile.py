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
import asyncio
import pathlib

# Third-Party Packages #
import pytest

# Local Packages #


# Definitions #
# Functions #
@pytest.fixture
def tmp_dir(tmpdir):
    """A pytest fixture that turn the tmpdir into a Path object."""
    return pathlib.Path(tmpdir)


# Classes #
class TestContentsFile:
    class_ = ContentsFile

    def test_create_contents_file(self, tmp_path):
        file_path = tmp_path / "test.db"
        db = self.class_(path=file_path)
        db.create_file(echo=True)
        assert file_path.is_file()

    async def create_contents_file_async(self, path):
        db = self.class_(path=path)
        await db.create_file_async(echo=True)

    def test_create_async_contents_file(self, tmp_path):
        file_path = tmp_path / "test.db"
        asyncio.run(self.create_contents_file_async(path=file_path))
        assert file_path.is_file()

    def test_get_empty_meta_information(self, tmp_path):
        file_path = tmp_path / "test.db"
        db = self.class_(path=file_path, open_=True, create=True)
        info = db.get_meta_information()
        assert not info

    def test_get_meta_information(self, tmp_path):
        file_path = tmp_path / "test.db"
        db = self.class_(path=file_path, open_=True, create=True)
        info = db.get_meta_information()
        assert "id" in info
        assert db._meta_information.id == info["id"]
