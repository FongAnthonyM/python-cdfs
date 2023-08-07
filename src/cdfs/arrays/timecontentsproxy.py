"""timecontentsproxy.py

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
from abc import abstractmethod
from collections.abc import Iterable
import datetime
from decimal import Decimal
import pathlib
from typing import Any
from warnings import warn

# Third-Party Packages #
from baseobjects.cachingtools import timed_keyless_cache
from dspobjects.time import Timestamp, nanostamp
from proxyarrays import BaseContainerFileTimeSeries, BaseDirectoryTimeSeries, DirectoryTimeSeriesProxy
import numpy as np

# Local Packages #
from ..contentsfile.sqlite import TimeContentsFile


# Definitions #
# Classes #
class BaseTimeContentsLeafContainer(BaseContainerFileTimeSeries):
    file_type: type | None = None

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        file: Any = None,
        mode: str | None = "r",
        shape: tuple[int] | None = None,
        axis: int | None = None,
        sample_rate: float | str | Decimal | None = None,
        sample_period: float | str | Decimal | None = None,
        start: datetime.datetime | float | int | np.dtype | np.ndarray = None,
        end: datetime.datetime | float | int | np.dtype | np.ndarray = None,
        tzinfo: datetime.tzinfo | None = None,
        *,
        path: str | pathlib.Path | None = None,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self._shape: tuple[int] | None = None
        self._sample_rate: Decimal | None = None
        self._tzinfo: datetime.tzinfo | None = None
        self._start: int | None = None
        self._end: int | None = None

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                file=file,
                path=path,
                shape=shape,
                axis=axis,
                sample_rate=sample_rate,
                sample_period=sample_period,
                start=start,
                end=end,
                tzinfo=tzinfo,
                mode=mode,
                **kwargs,
            )

    @property
    def is_open(self) -> bool:
        return self._is_open()

    @property
    def tzinfo(self) -> datetime.tzinfo | None:
        return self.get_tzinfo()

    @tzinfo.setter
    def tzinfo(self, value: datetime.tzinfo | None) -> None:
        self._tzinfo = value

    @property
    def start_datetime(self) -> Timestamp | None:
        """The start datetime of this proxy."""
        start = self.get_start_nanostamp()
        return Timestamp.fromnanostamp(start, tz=self.tzinfo) if start is not None else None

    @property
    def start_nanostamp(self) -> int | None:
        """The start timestamp of this proxy."""
        return self.get_start_nanostamp()

    @property
    def start_timestamp(self) -> float | None:
        """The start timestamp of this proxy."""
        start = self.get_start_nanostamp()
        return float(start) / 10**9 if start is not None else None

    @property
    def end_datetime(self) -> Timestamp | None:
        """The end datetime of this proxy."""
        end = self.get_end_nanostamp()
        return Timestamp.fromnanostamp(end, tz=self.tzinfo) if end is not None else None

    @property
    def end_nanostamp(self) -> float | None:
        """The end timestamp of this proxy."""
        return self.get_end_nanostamp()

    @property
    def end_timestamp(self) -> float | None:
        """The end timestamp of this proxy."""
        end = self.get_end_nanostamp()
        return float(end) / 10 ** 9 if end is not None else None

    @property
    def sample_period(self) -> float:
        """The sample period of this proxy."""
        return self.get_sample_period()

    @sample_period.setter
    def sample_period(self, value: float | str | Decimal) -> None:
        if not isinstance(value, Decimal):
            value = Decimal(value)
        self._sample_rate = 1 / value

    # Instance Methods
    # Constructors/Destructors
    def construct(
        self,
        file: Any = None,
        mode: str | None = None,
        shape: tuple[int] | None = None,
        axis: int | None = None,
        sample_rate: float | str | Decimal | None = None,
        sample_period: float | str | Decimal | None = None,
        start: datetime.datetime | float | int | np.dtype | np.ndarray = None,
        end: datetime.datetime | float | int | np.dtype | np.ndarray = None,
        tzinfo: datetime.tzinfo | None = None,
        *,
        path: str | pathlib.Path | None = None,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            file: The file object to wrap or a path to the file.
            mode: The mode this proxy and file will be in.
            shape: The shape of this proxy.
            axis: The axis of the data which this proxy extends for the contained data arrays.
            sample_rate: The sample rate of the data.
            sample_period: The sample period of this proxy.
            start: The start of this proxy.
            end: The end of this proxy.
            tzinfo: The time zone of the timestamps.
            path: The path of the file to wrap.
            **kwargs: The keyword arguments for constructing the file object.
        """
        if shape is not None:
            self._shape = shape

        if axis is not None:
            self.axis = axis
        
        if sample_period is not None:
            self._sample_rate = 1 / Decimal(sample_period)

        if sample_rate is not None:
            self._sample_rate = Decimal(sample_rate)
        
        if tzinfo is not None:
            self._tzinfo = tzinfo
            
        if start is not None:
            self._start = int(nanostamp(start))
            
        if end is not None:
            self._end = int(nanostamp(end))

        # Parent Construction
        super().construct(file=file, mode=mode, path=path, **kwargs)

    @abstractmethod
    def _is_open(self) -> bool:
        pass

    def update_defaults(
        self,
        shape: tuple[int] | None = None,
        axis: int | None = None,
        sample_rate: float | str | Decimal | None = None,
        sample_period: float | str | Decimal | None = None,
        start: datetime.datetime | float | int | np.dtype | np.ndarray = None,
        end: datetime.datetime | float | int | np.dtype | np.ndarray = None,
        tzinfo: datetime.tzinfo | None = None,
        **kwargs: Any,
    ) -> None:
        """Updates the default values for this proxy.

        Args:
            shape: The shape of this proxy.
            axis: The axis of the data which this proxy extends for the contained data arrays.
            sample_rate: The sample rate of the data.
            sample_period: The sample period of this proxy.
            start: The start of this proxy.
            end: The end of this proxy.
            tzinfo: The time zone of the timestamps.
        """
        if shape is not None:
            self._shape = shape

        if axis is not None:
            self.axis = axis

        if sample_period is not None:
            self._sample_rate = 1 / Decimal(sample_period)

        if sample_rate is not None:
            self._sample_rate = Decimal(sample_rate)

        if tzinfo is not None:
            self._tzinfo = tzinfo

        if start is not None:
            self._start = int(nanostamp(start))

        if end is not None:
            self._end = int(nanostamp(end))

    # Getters and Setters
    def _get_shape(self) -> tuple[int]:
        return self.data.shape

    @timed_keyless_cache(lifetime=1.0, call_method="clearing_call", local=True)
    def get_shape(self, **kwargs) -> tuple[int]:
        """Get the minimum shapes from the contained arrays/objects if they are different across axes.

        Returns:
            The minimum shapes of the contained arrays/objects.
        """
        if self.is_open:
            self._shape = self._get_shape()
        return self._shape

    def _get_sample_rate_decimal(self) -> Decimal:
        return self.time_axis.get_sample_rate_decimal()

    def get_sample_rate_decimal(self) -> Decimal | None:
        """Get the sample rate of this proxy from the contained arrays/objects.

        Returns:
            The shape of this proxy or the minimum sample rate of the contained arrays/objects.
        """
        if self.is_open:
            self._shape = self._get_sample_rate_decimal()
        return self._sample_rate

    def get_sample_rate(self) -> float | None:
        """Get the sample rate of this proxy from the contained arrays/objects.

        Returns:
            The sample rate of this proxy.
        """
        sample_rate = self.get_sample_rate_decimal()
        return float(sample_rate) if sample_rate is not None else None

    def get_sample_period(self) -> float:
        """Get the sample period of this proxy.

        If the contained arrays/object are different this will raise a warning and return the maximum period.

        Returns:
            The sample period of this proxy.
        """
        return float(1 / self.get_sample_rate_decimal())

    def get_sample_period_decimal(self) -> Decimal:
        """Get the sample period of this proxy.

        If the contained arrays/object are different this will raise a warning and return the maximum period.

        Returns:
            The sample period of this proxy.
        """
        return 1 / self.get_sample_rate_decimal()

    def _get_tzinfo(self) -> datetime.tzinfo | None:
        return self.time_axis.tzinfo

    def get_tzinfo(self) -> datetime.tzinfo | None:
        """Gets the time zone of the contained arrays.

        Args:
            tzinfo: The time zone to set.
        """
        if self.is_open:
            self._tzinfo = self._get_tzinfo()
        return self._tzinfo

    def _get_start_nanostamp(self) -> int | None:
        return self.time_axis.start_nanostamp
    
    def get_start_nanostamp(self) -> int | None:
        if self.is_open:
            self._start = self._get_start_nanostamp()
        return self._start

    def _get_end_nanostamp(self) -> int | None:
        return self.time_axis.end_nanostamp

    def get_end_nanostamp(self) -> int | None:
        if self.is_open:
            self._end = self._get_end_nanostamp()
        return self._end

    @abstractmethod
    def load(self) -> None:
        """Loads the file's information into memory.'"""
        pass

    # Getters
    @abstractmethod
    def get_data(self) -> Any:
        """Gets the data.

        Returns:
            The data object.
        """
        pass

    @abstractmethod
    def set_data(self, value: Any) -> None:
        """Sets the data.

        Args:
            value: A data object.
        """
        if self.mode == "r":
            raise IOError("not writable")

    @abstractmethod
    def get_time_axis(self) -> Any:
        """Gets the time axis.

        Returns:
            The time axis object.
        """
        pass

    @abstractmethod
    def set_time_axis(self, value: Any) -> None:
        """Sets the time axis

        Args:
            value: A time axis object.
        """
        if self.mode == "r":
            raise IOError("not writable")


class TimeContentsNodeProxy(DirectoryTimeSeriesProxy):
    default_node_type: type = None
    default_leaf_type: type[BaseTimeContentsLeafContainer] | None = None

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        proxies: Iterable[BaseDirectoryTimeSeries] | None = None,
        axis: int | None = None,
        precise: bool | None = None,
        tzinfo: datetime.tzinfo | None = None,
        mode: str = "r",
        update: bool = True,
        open_: bool = False,
        build: bool = True,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.node_type: type | None = self.default_node_type
        self.leaf_type: type | None = self.default_leaf_type

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                proxies=proxies,
                axis=axis,
                precise=precise,
                tzinfo=tzinfo,
                mode=mode,
                update=update,
                open_=open_,
                build=build,
                **kwargs,
            )

    def update_child(
        self,
        path: str | list[str],
        open_: bool = False,
        **kwargs
    ) -> None:
        """Creates a child proxy from the given child path.

        Args:
            path: The child path to create a proxy from.
            open_: Determines if the arrays will remain open after construction.
            **kwargs: The keyword arguments to create contained arrays.
        """
        path = path.split('/') if isinstance(path, str) else path.copy()

        child_path = self.path / path.pop(0)
        proxy = self.proxy_paths.get(child_path, None)
        if proxy is None:
            if path:
                proxy = self.node_type(path=child_path, mode=self.mode, open_=open_, build=False)
            else:
                proxy = self.leaf_type(path=child_path, mode=self.mode, open_=open_,  **kwargs)
            self.proxies.append(proxy)
            self.proxy_paths[child_path] = proxy

        if path:
            proxy.update_child(path=path, open_=open_, **kwargs)
        else:
            proxy.update_defaults(**kwargs)

        self.proxies.sort(key=lambda p: p.start_timestamp)
        self.clear_caches()

    def update_children(self, paths: list[dict], open_: bool = False, sort: bool = False, **kwargs) -> None:
        """Creates child arrays the given child paths.

        Args:
            paths: The child paths and keyword arguments to create arrays from.
            open_: Determines if the arrays will remain open after construction.
            sort: Determines if the arrays will be sorted after update.
            **kwargs: The keyword arguments to create contained arrays.
        """
        children_info = {}
        for path_kwargs in paths:
            path = path_kwargs["path"]
            path = path_kwargs["path"] = path.split('/') if isinstance(path, str) else path.copy()
            child_path = self.path / path.pop(0)
            info = children_info.get(child_path, None)
            if info is None:
                children_info[child_path] = {"kwargs": path_kwargs | {"path": child_path}, "children": [path_kwargs]}
            else:
                info["children"].append(path_kwargs)

        for child_path, info in children_info.items():
            proxy = self.proxy_paths.get(child_path, None)
            update_leaf = not info["children"] or (len(info["children"]) == 1 and not info["children"][0]["path"])
            if proxy is None:
                if update_leaf:
                    self.proxy_paths[child_path] = proxy = self.leaf_type(mode=self.mode, **(kwargs | info["kwargs"]))
                else:
                    self.proxy_paths[child_path] = proxy = self.node_type(
                        path=child_path,
                        mode=self.mode,
                        open_=open_,
                        build=False,
                    )
                self.proxies.append(proxy)
            if update_leaf:
                proxy.update_defaults(**info["kwargs"])
            else:
                proxy.update_children(paths=info["children"], open_=open_, sort=sort)

        if sort:
            self.proxies.sort(key=lambda p: p.start_timestamp)
            self.clear_caches()


class TimeContentsProxy(TimeContentsNodeProxy):
    """A DirectoryTimeproxy object built with information from a dataset which maps out its contents.

    Class Attributes:
        default_node_proxy_type: The default proxy type to create when making a node.

    Attributes:
        content_map: A HDF5Group with the mapping information for creating the proxy structure.
        node_proxy_type: The proxy type to create when making a node.

    Args:
        path: The path for this proxy to wrap.
        content_map: A HDF5Dataset with the mapping information for creating the proxy structure.
        proxies: An iterable holding arrays/objects to store in this proxy.
        mode: Determines if the contents of this proxy are editable or not.
        update: Determines if this proxy will start_timestamp updating or not.
        open_: Determines if the arrays will remain open after construction.
        build: Determines if the arrays will be constructed.
        **kwargs: The keyword arguments to create contained arrays.
        init: Determines if this object will construct.
    """
    default_proxy_type: type = TimeContentsNodeProxy
    default_node_type: type[TimeContentsNodeProxy] = TimeContentsNodeProxy

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        contents_file: TimeContentsFile | None = None,
        proxies: Iterable[BaseDirectoryTimeSeries] | None = None,
        mode: str = "r",
        update: bool = False,
        open_: bool = False,
        build: bool = True,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.contents_file: TimeContentsFile | None = None
        self.latest_update: int = 0

        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                contents_file=contents_file,
                proxies=proxies,
                mode=mode,
                update=update,
                open_=open_,
                build=build,
                **kwargs,
            )

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        path: pathlib.Path | str | None = None,
        contents_file: TimeContentsFile | None = None,
        proxies: Iterable[BaseDirectoryTimeSeries] | None = None,
        mode: str = "r",
        update: bool = False,
        open_: bool = False,
        build: bool = False,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            path: The path for this proxy to wrap.
            contents_file: A HDF5Dataset with the mapping information for creating the proxy structure.
            proxies: An iterable holding arrays/objects to store in this proxy.
            mode: Determines if the contents of this proxy are editable or not.
            update: Determines if this proxy will start_timestamp updating or not.
            open_: Determines if the arrays will remain open after construction.
            build: Determines if the arrays will be constructed.
            **kwargs: The keyword arguments to create contained arrays.
        """
        if contents_file is not None:
            self.contents_file = contents_file

        super().construct(path=path, proxies=proxies, mode=mode, update=update, open_=open_, build=build, **kwargs)

    def construct_proxies(self, open_=False, **kwargs) -> None:
        """Constructs the arrays for this object.

        Args:
            open_: Determines if the arrays will remain open after construction.
            **kwargs: The keyword arguments to create contained arrays.
        """
        self.proxy_paths.clear()
        with self.contents_file.create_session() as session:
            entries = self.contents_file.contents.get_all(session=session, as_entries=True)

        for entry in entries:
            del entry["id"]
            entry["tzinfo"] = entry.pop("tz_offset")
            update_id = entry.pop("update_id")
            if update_id > self.latest_update:
                self.latest_update = update_id

        self.update_children(paths=entries, open_=open_, sort=True, **kwargs)

    async def construct_proxies_async(self, open_=False, **kwargs) -> None:
        """Constructs the arrays for this object.

        Args:
            open_: Determines if the arrays will remain open after construction.
            **kwargs: The keyword arguments to create contained arrays.
        """
        self.proxy_paths.clear()
        entries = await self.contents_file.contents.get_all_async(
            session=self.contents_file.async_session_maker,
            as_entries=True,
        )

        for entry in entries:
            del entry["id"]
            update_id = entry.pop("update_id")
            if update_id > self.latest_update:
                self.latest_update = update_id

        self.update_children(paths=entries, open_=open_, sort=True, **kwargs)

    def update_proxies(self, open_=False, **kwargs) -> None:
        """Updates the arrays for this object.

        Args:
            open_: Determines if the arrays will remain open after the update.
            **kwargs: The keyword arguments to create contained arrays.
        """
        with self.contents_file.create_session() as session:
            entries = self.contents_file.contents.get_from_update(
                session=session,
                update_id=self.latest_update,
                inclusive=False,
                as_entries=True,
            )

        if entries:
            for entry in entries:
                del entry["id"]
                update_id = entry.pop("update_id")
                if update_id > self.latest_update:
                    self.latest_update = update_id

            self.update_children(paths=entries, open_=open_, sort=True, **kwargs)

    async def update_proxies_async(self, open_=False, **kwargs) -> None:
        """Updates the arrays for this object.

        Args:
            open_: Determines if the arrays will remain open after the update.
            **kwargs: The keyword arguments to create contained arrays.
        """
        entries = await self.contents_file.contents.get_from_update_async(
            session=self.contents_file.async_sessionmaker,
            update_id=self.latest_update,
            inclusive=False,
            as_entries=True,
        )

        if entries:
            for entry in entries:
                del entry["id"]
                update_id = entry.pop("update_id")
                if update_id > self.latest_update:
                    self.latest_update = update_id

            self.update_children(paths=entries, open_=open_, sort=True, **kwargs)
