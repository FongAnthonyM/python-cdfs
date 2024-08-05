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
from typing import Any, Optional

# Third-Party Packages #
from baseobjects.cachingtools import timed_keyless_cache
from dspobjects.time import Timestamp, nanostamp
from proxyarrays import BaseContainerFileTimeSeries, BaseDirectoryTimeSeries, DirectoryTimeSeriesProxy
import numpy as np

# Local Packages #


# Definitions #
# Classes #
class BaseTimeContentsLeafContainer(BaseContainerFileTimeSeries):
    """A base class for time contents leaf containers in a file time series.

    This class provides properties and methods to handle time-related attributes such as sample rate, start and end
    timestamps, and time zone information.

    Attributes:
        file_type: The type of the file.
        _shape: The shape of the data.
        _sample_rate: The sample rate of the data.
        _tzinfo: The time zone information.
        _start: The start timestamp in nanoseconds.
        _end: The end timestamp in nanoseconds.
    """

    # Attributes #
    file_type: type | None = None

    _shape: tuple[int] | None = None
    _sample_rate: Decimal | None = None
    _tzinfo: datetime.tzinfo | None = None
    _start: int | None = None
    _end: int | None = None

    # Properties #
    @property
    def is_open(self) -> bool:
        """bool: Indicates if the file is open."""
        return self._is_open()

    @property
    def tzinfo(self) -> datetime.tzinfo | None:
        """datetime.tzinfo | None: Gets or sets the time zone information."""
        return self.get_tzinfo()

    @tzinfo.setter
    def tzinfo(self, value: datetime.tzinfo | None) -> None:
        self._tzinfo = value

    @property
    def start_datetime(self) -> Timestamp | None:
        """Timestamp | None: The start datetime of this proxy."""
        start = self.get_start_nanostamp()
        return Timestamp.fromnanostamp(start, tz=self.tzinfo) if start is not None else None

    @property
    def start_nanostamp(self) -> int | None:
        """int | None: The start timestamp in nanoseconds."""
        return self.get_start_nanostamp()

    @property
    def start_timestamp(self) -> float | None:
        """float | None: The start timestamp in seconds."""
        start = self.get_start_nanostamp()
        return float(start) / 10 ** 9 if start is not None else None

    @property
    def end_datetime(self) -> Timestamp | None:
        """Timestamp | None: The end datetime of this proxy."""
        end = self.get_end_nanostamp()
        return Timestamp.fromnanostamp(end, tz=self.tzinfo) if end is not None else None

    @property
    def end_nanostamp(self) -> float | None:
        """float | None: The end timestamp in nanoseconds."""
        return self.get_end_nanostamp()

    @property
    def end_timestamp(self) -> float | None:
        """float | None: The end timestamp in seconds."""
        end = self.get_end_nanostamp()
        return float(end) / 10 ** 9 if end is not None else None

    @property
    def sample_rate(self) -> float:
        """float: The sample rate of this proxy."""
        return self.get_sample_rate()

    @property
    def sample_rate_decimal(self) -> Decimal:
        """Decimal: The sample rate as a Decimal object."""
        return self.get_sample_rate_decimal()

    @property
    def sample_period(self) -> float:
        """float: The sample period of this proxy."""
        return self.get_sample_period()

    @sample_period.setter
    def sample_period(self, value: float | str | Decimal) -> None:
        if not isinstance(value, Decimal):
            value = Decimal(value)
        self._sample_rate = 1 / value

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
        """Initializes a BaseTimeContentsLeafContainer instance.

        Args:
            file (Any): The file object to wrap or a path to the file.
            mode (str | None): The mode this proxy and file will be in.
            shape (tuple[int] | None): The shape of this proxy.
            axis (int | None): The axis of the data which this proxy extends for the contained data arrays.
            sample_rate (float | str | Decimal | None): The sample rate of the data.
            sample_period (float | str | Decimal | None): The sample period of this proxy.
            start (datetime.datetime | float | int | np.dtype | np.ndarray): The start of this proxy.
            end (datetime.datetime | float | int | np.dtype | np.ndarray): The end of this proxy.
            tzinfo (datetime.tzinfo | None): The time zone of the timestamps.
            path (str | pathlib.Path | None): The path of the file to wrap.
            init (bool): Determines if this object will construct.
            **kwargs (Any): Additional keyword arguments.
        """
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
            file (Any): The file object to wrap or a path to the file.
            mode (str | None): The mode this proxy and file will be in.
            shape (tuple[int] | None): The shape of this proxy.
            axis (int | None): The axis of the data which this proxy extends for the contained data arrays.
            sample_rate (float | str | Decimal | None): The sample rate of the data.
            sample_period (float | str | Decimal | None): The sample period of this proxy.
            start (datetime.datetime | float | int | np.dtype | np.ndarray): The start of this proxy.
            end (datetime.datetime | float | int | np.dtype | np.ndarray): The end of this proxy.
            tzinfo (datetime.tzinfo | None): The time zone of the timestamps.
            path (str | pathlib.Path | None): The path of the file to wrap.
            **kwargs (Any): Additional keyword arguments.
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
        """Abstract method to check if the file is open.

        Returns:
            bool: True if the file is open, False otherwise.
        """
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
            shape (tuple[int] | None): The shape of this proxy.
            axis (int | None): The axis of the data which this proxy extends for the contained data arrays.
            sample_rate (float | str | Decimal | None): The sample rate of the data.
            sample_period (float | str | Decimal | None): The sample period of this proxy.
            start (datetime.datetime | float | int | np.dtype | np.ndarray): The start of this proxy.
            end (datetime.datetime | float | int | np.dtype | np.ndarray): The end of this proxy.
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
    def get_shape(self, **kwargs: Any) -> tuple[int]:
        """Get the minimum shapes from the contained arrays/objects if they are different across axes.

        Returns:
            The minimum shapes of the contained arrays/objects.
        """
        if self.is_open:
            self._shape = self._get_shape()
        return self._shape

    def _get_sample_rate_decimal(self) -> Decimal:
        return self.time_axis.sample_rate_decimal

    def get_sample_rate_decimal(self) -> Decimal | None:
        """Get the sample rate of this proxy from the contained arrays/objects.

        Returns:
            The shape of this proxy or the minimum sample rate of the contained arrays/objects.
        """
        if self.is_open:
            self._sample_rate = self._get_sample_rate_decimal()
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
    """A proxy node in a time content tree where the children can be either nodes or leafs.

    Attributes:
        node_type: The type of the node proxy.
        leaf_type: The type of the leaf container proxy.
    """

    # Attributes #
    node_type: type | None = None
    leaf_type: type[BaseTimeContentsLeafContainer] | None = None

    # Instance Methods #
    def update_child(
        self,
        path: str | list[str],
        open_: bool = False,
        **kwargs: Any,
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

    def update_children(self, paths: list[dict], open_: bool = False, sort: bool = False, **kwargs: Any) -> None:
        """Creates child proxies from the given child paths.

        Args:
            paths: The child paths and keyword arguments to create arrays from.
            open_: Determines if the proxies will remain open after construction.
            sort: Determines if the proxies will be sorted after update.
            **kwargs: The keyword arguments to create contained proxies.
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
                proxy.update_children(paths=info["children"], open_=open_, sort=sort, **kwargs)

        if sort:
            self.proxies.sort(key=lambda p: p.start_timestamp)
            self.clear_caches()


class TimeContentsProxy(TimeContentsNodeProxy):
    """The root node proxy class for managing time contents in a directory time series.

    Attributes:
        default_proxy_type: The default type of the proxy.
        node_type: The type of the node proxy.
        cdfs_component: The CDFS component associated with this proxy.
        latest_update: The latest update timestamp.

    Args:
        path: The path for this proxy to manage.
        cdfs_component: The CDFS component to create the tree from.
        proxies: An iterable with tree objects to store in this proxy.
        mode: Determines if the contents of this proxy are editable or not.
        update: Determines if this proxy will start updating or not.
        open_: Determines if the proxies will remain open after construction.
        build: Determines if the proxies will be constructed.
        init: Determines if this object will construct.
        **kwargs: Additional keyword arguments.
    """

    # Class Attributes #
    default_proxy_type: type = TimeContentsNodeProxy
    node_type: type[TimeContentsNodeProxy] = TimeContentsNodeProxy

    # Attributes #
    cdfs_component: Optional["TimeContentsCDFSComponent"] = None
    latest_update: int = 0

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        path: pathlib.Path | str | None = None,
        cdfs_component: Optional["TimeContentsCDFSComponent"] = None,
        proxies: Iterable[BaseDirectoryTimeSeries] | None = None,
        mode: str = "r",
        update: bool = False,
        open_: bool = False,
        build: bool = True,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # Parent Attributes #
        super().__init__(init=False)

        # Object Construction #
        if init:
            self.construct(
                path=path,
                cdfs_component=cdfs_component,
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
        cdfs_component: Optional["TimeContentsCDFSComponent"] = None,
        proxies: Iterable[BaseDirectoryTimeSeries] | None = None,
        mode: str = "r",
        update: bool = False,
        open_: bool = False,
        build: bool = False,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            path (pathlib.Path | str | None): The path for this proxy to wrap.
            cdfs_component (Optional[TimeContentsCDFSComponent]): The CDFS component associated with this proxy.
            proxies (Iterable[BaseDirectoryTimeSeries] | None): An iterable holding arrays/objects to store in this proxy.
            mode (str): Determines if the contents of this proxy are editable or not.
            update (bool): Determines if this proxy will start updating or not.
            open_ (bool): Determines if the arrays will remain open after construction.
            build (bool): Determines if the arrays will be constructed.
            **kwargs (Any): Additional keyword arguments.
        """
        if cdfs_component is not None:
            self.cdfs_component = cdfs_component

        if self.cdfs_component is not None:
            try:
                self.get_tzinfo()
            except:
                pass

        super().construct(path=path, proxies=proxies, mode=mode, update=update, open_=open_, build=build, **kwargs)

    # Proxy
    def construct_proxies(self, open_=False, **kwargs: Any) -> None:
        """Constructs the proxies using the entry structure from the CDFS component.

        Args:
            open_: Determines if the arrays will remain open after construction.
            **kwargs: The keyword arguments to create contained arrays.
        """
        if self.tzinfo is None:
            self.get_tzinfo()

        self.proxy_paths.clear()
        entries = self.cdfs_component.get_all(as_entries=True)

        for entry in entries:
            del entry["id"]
            entry["tzinfo"] = entry.pop("tz_offset")
            update_id = entry.pop("update_id")
            if update_id > self.latest_update:
                self.latest_update = update_id

        self.update_children(paths=entries, open_=open_, sort=True, **kwargs)

    async def construct_proxies_async(self, open_=False, **kwargs: Any) -> None:
        """Constructs the arrays for this object asynchronously.

        Args:
            open_ (bool): Determines if the arrays will remain open after construction.
            **kwargs (Any): The keyword arguments to create contained arrays.
        """
        self.proxy_paths.clear()
        entries = await self.cdfs_component.get_all_async(as_entries=True)

        for entry in entries:
            del entry["id"]
            entry["tzinfo"] = entry.pop("tz_offset")
            update_id = entry.pop("update_id")
            if update_id > self.latest_update:
                self.latest_update = update_id

        self.update_children(paths=entries, open_=open_, sort=True, **kwargs)

    def update_proxies(self, open_=False, **kwargs: Any) -> None:
        """Updates the arrays for this object.

        Args:
            open_ (bool): Determines if the arrays will remain open after the update.
            **kwargs (Any): The keyword arguments to create contained arrays.
        """
        entries = self.cdfs_component.get_from_update(
            update_id=self.latest_update,
            inclusive=False,
            as_entries=True,
        )

        if entries:
            for entry in entries:
                del entry["id"]
                entry["tzinfo"] = entry.pop("tz_offset")
                update_id = entry.pop("update_id")
                if update_id > self.latest_update:
                    self.latest_update = update_id

            self.update_children(paths=entries, open_=open_, sort=True, **kwargs)

    async def update_proxies_async(self, open_=False, **kwargs: Any) -> None:
        """Updates the arrays for this object asynchronously.

        Args:
            open_ (bool): Determines if the arrays will remain open after the update.
            **kwargs (Any): The keyword arguments to create contained arrays.
        """
        entries = await self.cdfs_component.get_from_update_async(
            update_id=self.latest_update,
            inclusive=False,
            as_entries=True,
        )

        if entries:
            for entry in entries:
                del entry["id"]
                entry["tzinfo"] = entry.pop("tz_offset")
                update_id = entry.pop("update_id")
                if update_id > self.latest_update:
                    self.latest_update = update_id

            self.update_children(paths=entries, open_=open_, sort=True, **kwargs)

    # Time Information
    def get_tzinfo(self) -> datetime.tzinfo:
        """Gets the tzinfo from the contents file.

        Returns:
            datetime.tzinfo: The tzinfo from the contents file.
        """
        tz_offset = self.cdfs_component.get_tz_offsets_distinct()[0][0]
        self.tzinfo = datetime.timezone(datetime.timedelta(seconds=tz_offset))  # Make this a property in a parent
        return self.tzinfo


# Assign Cyclic Definition
TimeContentsNodeProxy.node_type = TimeContentsNodeProxy
