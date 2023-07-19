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
from baseobjects import BaseObject
from sqlalchemy import Uuid
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class ContentsTableBase(BaseObject):
    __tablename__ = "contents"
    id: Mapped[Uuid] = mapped_column(primary_key=True)
    path: Mapped[str]
    axis: Mapped[int]
    min_shape: Mapped[str]
    max_shape: Mapped[str]

    __mapper_args__ = {
        "polymorphic_on": "type",
        "polymorphic_identity": "contents",
    }
