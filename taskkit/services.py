from typing import Protocol, Sequence

from .backend import Backend
from .utils import cur_ts, logger
from .worker import WORKER_TTL_IN_SEC


class Service(Protocol):
    def __call__(self) -> float:
        """Call the service.

        It should returns the time interval to wait for next call.
        """
        ...


class WorkerThreadProtocol(Protocol):
    def get_id(self) -> str:
        ...

    def is_alive(self) -> bool:
        ...


class RefreshWorkerLifetime(Service):
    def __init__(self,
                 backend: Backend,
                 workers: Sequence[WorkerThreadProtocol],
                 **kwargs):
        self.backend = backend
        self.workers = list(workers)

    def __call__(self) -> float:
        self.backend.set_worker_ttl(
            {w.get_id() for w in self.workers if w.is_alive()},
            cur_ts() + WORKER_TTL_IN_SEC)
        return 1.0


class PurgeDeadWorkers(Service):
    def __init__(self, backend: Backend, **kwargs):
        self.backend = backend
        self.lock = backend.get_lock('purge_workers')

    def __call__(self) -> float:
        if self.lock.acquire():
            try:
                now = cur_ts()
                to_purge = {
                    wid for (wid, ttl) in self.backend.get_workers()
                    if ttl < now
                }
                if to_purge:
                    self.backend.purge_workers(to_purge)
            finally:
                self.lock.release()
        return 10.0


class RestoreAbandonedTasks(Service):
    def __init__(self, backend: Backend, **kwargs):
        self.backend = backend
        self.lock = backend.get_lock('restore_abandoned_tasks')

    def __call__(self) -> float:
        if self.lock.acquire():
            try:
                stage_info_list = self.backend.get_stage_info(50)
                now = cur_ts()
                active_worker_ids = {
                    wid for (wid, ttl) in self.backend.get_workers()
                    if ttl >= now
                }
                for stage_info in stage_info_list:
                    if stage_info.worker_id not in active_worker_ids:
                        logger.info(f'restore task `{stage_info.task_id}`')
                        self.backend.restore(stage_info)
            finally:
                self.lock.release()
        return 5.0


class DiscardDisposableTasks(Service):
    def __init__(self, backend: Backend, **kwargs):
        self.backend = backend
        self.lock = backend.get_lock('discard_disposable_tasks')

    def __call__(self) -> float:
        if self.lock.acquire():
            try:
                task_ids = self.backend.get_disposable_task_ids(limit=100)
                if task_ids:
                    logger.info(f'discard {len(task_ids)} tasks ({", ".join(task_ids)})')
                    self.backend.discard_tasks(*task_ids)
                if len(task_ids) == 100:
                    return 0
            finally:
                self.lock.release()
        return 60.0
