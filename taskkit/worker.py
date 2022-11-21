from traceback import format_exc
from typing import Any
from uuid import uuid4

from .backend import Backend
from .result import Result, prevent_to_wait_result, ResultGetPrevented
from .task import Task, TaskHandler, DiscardTask
from .utils import logger


REASON_TO_PREVENT_WAIT_RESULT =\
    'You should avoid to wait results of other tasks because it may causes '\
    'deadlock. If you can assure that it is safe you can pass '\
    '`avoid_assertion=True` for the `get` method.'

WORKER_TTL_IN_SEC = 10


class Worker:
    def __init__(self,
                 group: str,
                 backend: Backend,
                 handler: TaskHandler):
        self.id = f'wk_{group}_{uuid4().hex}'
        self.group = group
        self.backend = backend
        self.handler = handler

    def __call__(self) -> bool:
        """Pop a task from the queue and handle the task.

        If there is a task to handle then it returns whether if task finished
        successfully otherwise returns False.
        """

        try:
            task = self.backend.assign_task(self.group, self.id)
            if task is None:
                return False
            else:
                self._handle_task(task)
                return True
        except Exception:
            logger.exception(f'[{self.id}] unexpected exception')
            return False

    def _handle_task(self, task: Task):
        logger.info(f'[{self.id}] handle task ({task.id}: {task.name})')
        try:
            with prevent_to_wait_result(REASON_TO_PREVENT_WAIT_RESULT):
                ret = self.handler.encode_result(
                    task, self.handler.handle(task))
        except DiscardTask:
            self._discard_task(task)
        except ResultGetPrevented:
            self._handle_prevented(task)
        except Exception as e:
            self._handle_error(task, e)
        else:
            self.backend.succeed(task, ret)

    def _discard_task(self, task: Task):
        logger.info(f'[{self.id}] task was discarded ({task.id}: {task.name})')
        self.backend.discard_tasks(task.id)

    def _handle_prevented(self, task: Task):
        logger.exception(f'[{self.id}] Result.get was prevented during '
                         f'handling a task ({task.name}).')
        self.backend.fail(task, format_exc())

    def _handle_error(self, task: Task, exception: Exception):
        try:
            with prevent_to_wait_result(REASON_TO_PREVENT_WAIT_RESULT):
                interval = self.handler.get_retry_interval(task, exception)
            if interval is None:
                logger.info(f'[{self.id}] task was failed '
                            f'({task.id}: {task.name})')
                self.backend.fail(task, format_exc())
                return
        except DiscardTask:
            self._discard_task(task)
        except ResultGetPrevented:
            self._handle_prevented(task)
        except Exception:
            handler_name = self.handler.__class__.__qualname__
            logger.exception(
                f'[{self.id}] {handler_name}.get_retry_interval raises error '
                f'so we assume that we should retry ({task.id}: {task.name}')
            self._retry(task, task.retry_count + 1)
        else:
            self._retry(task, interval)

    def _retry(self, task: Task, interval: float):
        clone = task.clone_for_retry(interval)
        logger.info(f'[{self.id}] retry n{clone.retry_count} '
                    f'({task.id}: {task.name})')
        self.backend.put_tasks(clone)


class EagerWorker(Worker):
    def __init__(self, backend: Backend, handler: TaskHandler):
        super().__init__('_', backend, handler)

    def __call__(self) -> bool:
        raise AssertionError('Can not call EagerWorker')

    def handle_task(self, task: Task) -> Result[Any]:
        self._handle_task(task)
        return Result(self.backend, self.handler, task.id)

    def _handle_error(self, task: Task, exception: Exception):
        self.backend.fail(task, format_exc())
