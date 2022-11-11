import os
from datetime import datetime, tzinfo
from typing import Any

from .backend import Backend
from .controller import Controller, Shutdown, Pause, Resume
from .process import MainProcess
from .result import Result
from .scheduler import ScheduleEntry
from .signal import SignalCaptured, capture_signals, signal
from .task import Task, TaskHandler, DEFAULT_TASK_TTL
from .utils import local_tz


class Kit:
    def __init__(self,
                 backend: Backend,
                 controller: Controller,
                 handler: TaskHandler):
        self.backend = backend
        self.controller = controller
        self.handler = handler

    def start(self,
              num_worker_threads_per_group: dict[str, int],
              num_processes: int = os.cpu_count() or 1,
              schedule_entries: dict[str, list[ScheduleEntry]] = {},
              tzinfo: tzinfo | None = None,
              daemon: bool = True,
              join: bool = False) -> list[MainProcess]:
        processes: list[MainProcess] = []
        for _ in range(num_processes):
            p = MainProcess(
                num_worker_threads_per_group=num_worker_threads_per_group,
                backend=self.backend,
                controller=self.controller,
                handler=self.handler,
                schedule_entries=schedule_entries,
                tzinfo=tzinfo or local_tz(),
                daemon=daemon)
            p.start()
            processes.append(p)
        if join:
            try:
                with capture_signals(signal.SIGTERM):
                    for p in processes:
                        p.join()
            except (SystemExit, SignalCaptured) as e:
                for p in processes:
                    p.terminate()
                for p in processes:
                    p.join()
                if isinstance(e, SignalCaptured):
                    raise SystemExit from e
                else:
                    raise
        return processes

    def initiate_task(self,
                      group: str,
                      name: str,
                      data: Any,
                      due: datetime | None = None,
                      ttl: float = DEFAULT_TASK_TTL) -> Result[Any]:
        encoded = self.handler.encode_data(group, name, data)
        task = Task.init(group, name=name, data=encoded, due=due, ttl=ttl)
        self.backend.put_tasks(task)
        return Result(self.backend, self.handler, task.id)

    def send_shutdown_event(self, groups: set[str] | None = None):
        event: Shutdown = {
            'name': 'shutdown',
            'groups': None if groups is None else list(groups),
        }
        self.controller.send_event(event)

    def send_pause_event(self, groups: set[str] | None = None):
        event: Pause = {
            'name': 'pause',
            'groups': None if groups is None else list(groups),
        }
        self.controller.send_event(event)

    def send_resume_event(self, groups: set[str] | None = None):
        event: Resume = {
            'name': 'resume',
            'groups': None if groups is None else list(groups),
        }
        self.controller.send_event(event)
