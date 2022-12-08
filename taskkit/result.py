from contextlib import contextmanager
from contextvars import ContextVar
from enum import Enum
from time import time, sleep
from typing import Generic, TypeVar, overload, cast, Optional, Union

from .backend import Backend, NoResult
from .task import TaskHandler


T = TypeVar('T')


class _Sentinel(Enum):
    obj = object()


class ResultGetTimedOut(Exception):
    pass


class ResultGetPrevented(Exception):
    pass


_prevent_to_wait_result: ContextVar[tuple[bool, str]] =\
    ContextVar('prevent_to_wait_result', default=(False, ''))


@contextmanager
def prevent_to_wait_result(reason: str):
    token = _prevent_to_wait_result.set((True, reason))
    try:
        yield
    finally:
        _prevent_to_wait_result.reset(token)


class Result(Generic[T]):
    @overload
    def __init__(self,
                 backend: Backend,
                 handler: TaskHandler,
                 task_id: str):
        ...

    @overload
    def __init__(self,
                 backend: Backend,
                 handler: TaskHandler,
                 task_id: str,
                 *,
                 result: T):
        ...

    def __init__(self,
                 backend: Backend,
                 handler: TaskHandler,
                 task_id: str,
                 result: Union[T, _Sentinel] = _Sentinel.obj):
        self.backend = backend
        self.handler = handler
        self.task_id = task_id
        self._result = result

    def get(self,
            timeout: Optional[float] = None,
            avoid_assertion: bool = False) -> T:
        if not avoid_assertion:
            prevent, reason = _prevent_to_wait_result.get()
            if prevent:
                raise ResultGetPrevented(reason)

        if not isinstance(self._result, _Sentinel):
            return cast(T, self._result)

        try:
            return self.__get()
        except NoResult:
            pass

        i = 0
        t = time()
        while timeout is None or (time() - t) < timeout:
            try:
                return self.__get()
            except NoResult:
                sleep(min(i * 0.1, 1))
        raise ResultGetTimedOut()

    def __get(self) -> T:
        task, encoded = self.backend.get_result(self.task_id)
        ret = self.handler.decode_result(task, encoded)
        self._result = ret
        return ret
