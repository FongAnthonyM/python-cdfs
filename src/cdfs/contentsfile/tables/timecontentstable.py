"""contentstable.py
A node component which implements content information in its dataset.
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
from typing import Any
import uuid

# Third-Party Packages #
from sqlalchemy.orm import Mapped
from sqlalchemy.types import BigInteger
from sqlalchemy.orm import mapped_column

# Local Packages #
from .contentstable import ContentsTableBase


# Definitions #
# Classes #
class TimeContentsTableBase(ContentsTableBase):
    start: Mapped[BigInteger]
    end: Mapped[BigInteger]

    __mapper_args__ = {
        "polymorphic_on": "type",
        "polymorphic_identity": "timecontents",
    }
