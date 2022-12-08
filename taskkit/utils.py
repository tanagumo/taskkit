from datetime import datetime, tzinfo
from logging import getLogger
from time import time
from typing import overload, Optional, Union


logger = getLogger('taskkit')


_LOCAL_TZ = datetime.now().astimezone().tzinfo


def local_tz() -> tzinfo:
    assert _LOCAL_TZ is not None
    return _LOCAL_TZ


cur_ts = time


@overload
def as_ts(value: Union[datetime, float]) -> float:
    ...


@overload
def as_ts(value: Union[datetime, float, None]) -> Optional[float]:
    ...


def as_ts(value: Union[datetime, float, None]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    return value


def from_ts(ts: float, tzinfo: tzinfo) -> datetime:
    return datetime.fromtimestamp(ts, tzinfo)
