from typing import Protocol, Optional

from .task import Task
from .stage import StageInfo


class Lock(Protocol):
    def acquire(self) -> bool:
        """try to acquire the lock"""
        ...

    def release(self):
        """release the lock"""
        ...


class NotFound(Exception):
    pass


class NoResult(Exception):
    def __init__(self, task: Task):
        self.task = task


class Failed(Exception):
    def __init__(self, task: Task, message: str):
        self.task = task
        self.message = message


class Backend(Protocol):
    def set_worker_ttl(self, worker_ids: set[str], expires_at: float):
        """Update the workers lifetime"""
        ...

    def get_workers(self) -> list[tuple[str, float]]:
        """Get list of pairs of worker id and expiration timestamp"""
        ...

    def purge_workers(self, worker_ids: set[str]):
        """purge given workers"""
        ...

    def put_tasks(self, *tasks: Task):
        """Put the tasks to the queue

        If there is same task on the stage, it should be removed from the
        stage first because it will be called when the task should retry.
        """
        ...

    def get_queued_tasks(self, group: str, limit: int) -> list[Task]:
        """Get tasks in the queue"""
        ...

    def lookup_tasks(self, *task_ids: str) -> dict[str, Optional[Task]]:
        """Lookup tasks by given ids"""
        ...

    def assign_task(self, group: str, worker_id: str) -> Optional[Task]:
        """Take a task from the queue and assign it for the worker and put on
        the stage."""
        ...

    def discard_tasks(self, *task_ids: str):
        """Discard the tasks"""
        ...

    def succeed(self, task: Task, result: bytes):
        """Save the result and remove the task from the stage. It should
        save the task data if the task is missing in the backend."""
        ...

    def fail(self, task: Task, error_message: str):
        """Save the error message and remove the task from the stage. It should
        save the task data if the task is missing in the backend."""
        ...

    def get_result(self, task_id: str) -> tuple[Task, bytes]:
        """It returns a tuple of the task and the result for given task_id or
        raises suitable exception.

        It raises
        - NotFound if the task was not found
        - Failed if the task was failed
        - NoResult if the task is not done
        """
        ...

    def get_done_task_ids(self,
                          since: Optional[float],
                          until: Optional[float],
                          limit: int) -> list[str]:
        """Get done task ids matching given timestamp range

        It may contain failed task ids.
        """
        ...

    def get_disposable_task_ids(self, limit: int) -> list[str]:
        """Get disposable task ids

        Tasks will be treated as disposable after `ttl` seconds from
        the time the task was succeeded or failed.
        """
        ...

    def get_stage_info(self, limit: int) -> list[StageInfo]:
        """Get stage info"""
        ...

    def restore(self, info: StageInfo):
        """Remove the task from the stage and put it back to the queue"""
        ...

    def get_lock(self, target: str) -> Lock:
        """Get a lock object for the target"""
        ...

    def persist_scheduler_state_and_put_tasks(self,
                                              name: str,
                                              data: bytes,
                                              *tasks: Task):
        """Persist given data for the scheduler and put tasks"""
        ...

    def get_scheduler_state(self, name: str) -> Optional[bytes]:
        """Get data for the scheduler"""
        ...

    def destroy_all(self):
        """Destroy all data which associated with the backend. It is needed
        for unit tests."""
        ...
