""" cdfstimecontentscomponent.py.py
A CDFS component for managing time-based contents in a CDFS.
"""
# Package Header #
from ..header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #

# Third-Party Packages #

# Local Packages #
from ..arrays import TimeContentsProxy
from .basecdfscontentscomponent import BaseCDFSContentsComponent


# Definitions #
# Classes #
class CDFSTimeContentsComponent(BaseCDFSContentsComponent):
    """A CDFS component for managing time-based contents in a CDFS.

    Attributes:
        proxy_type: The proxy type for time contents.
    """

    # Attributes #
    proxy_type: type[TimeContentsProxy] = TimeContentsProxy

    proxy: TimeContentsProxy | None = None

    # Properties #
    @property
    def start_datetime(self):
        """Gets the start datetime.

        Returns:
            Timestamp: The start datetime.
        """
        return self.contents_table.get_start_datetime()

    @property
    def end_datetime(self):
        """Gets the end datetime.

        Returns:
            Timestamp: The end datetime.
        """
        return self.contents_table.get_end_datetime()

    # Instance Methods #
    # Contents Proxy #
    def create_contents_proxy(self, swmr: bool = True, **kwargs) -> TimeContentsProxy:
        """Creates a contents proxy for the CDFS component.

        Args:
            swmr: If True, enables single-writer multiple-reader mode. Defaults to True.
            **kwargs: Additional keyword arguments for the proxy.

        Returns:
            TimeContentsProxy: The created contents proxy.
        """
        cdfs = self._composite()
        proxy = self.proxy_type(
            table=self.contents_table,
            swmr=swmr,
            **({"path": cdfs.path, "mode": cdfs.mode} | kwargs),
        )
        return proxy

    def require_contents_proxy(self, swmr: bool = True, **kwargs) -> TimeContentsProxy:
        """Requires a contents proxy for the CDFS component.

        Args:
            swmr: If True, enables single-writer multiple-reader mode. Defaults to True.
            **kwargs: Additional keyword arguments for the proxy.

        Returns:
            TimeContentsProxy: The required contents proxy.
        """
        cdfs = self._composite()
        if self.proxy is None:
            self.proxy = self.proxy_type(
                table=self.contents_table,
                swmr=swmr,
                **({"path": cdfs.path, "mode": cdfs.mode} | kwargs),
            )
        return self.proxy
