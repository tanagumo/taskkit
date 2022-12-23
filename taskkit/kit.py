import time
from datetime import datetime, tzinfo
from typing import Any, Callable, Mapping, Sequence, TypedDict, Optional, Union

from typing_extensions import NotRequired

from .backend import Backend
from .event import EventBridge, Shutdown, Pause, Resume
from .process import TaskkitProcess
from .result import Result
from .scheduler import ScheduleEntry, Schedule
from .signal import SignalCaptured, capture_signals, signal
from .task import Task, TaskHandler, DEFAULT_TASK_TTL
from .utils import local_tz
from .worker import EagerWorker


class ScheduleEntryDict(TypedDict):
    key: str
    schedule: Schedule
    group: str
    name: str
    data: Any
    result_ttl: NotRequired[Optional[float]]


class InitiateTaskArgs(TypedDict):
    group: str
    name: str
    data: Any
    due: NotRequired[Union[datetime, float, None]]
    ttl: NotRequired[float]
    eager: NotRequired[bool]


ScheduleEntryCompat = Union[ScheduleEntry, ScheduleEntryDict]
ScheduleEntriesCompat = Sequence[ScheduleEntryCompat]
ScheduleEntriesCompatMapping = Mapping[str, ScheduleEntriesCompat]


class Kit:
    def __init__(self,
                 backend: Backend,
                 bridge: EventBridge,
                 handler: TaskHandler):
        self.backend = backend
        self.bridge = bridge
        self.handler = handler
        self.eager_worker = EagerWorker(backend, handler)

    def start(self,
              num_processes: int,
              num_worker_threads_per_group: dict[str, int],
              schedule_entries: ScheduleEntriesCompatMapping = {},
              tzinfo: Optional[tzinfo] = None,
              should_restart: Callable[[TaskkitProcess], bool] = lambda _: False):

        schedule_entries = self._ensure_schedule_entries(schedule_entries)

        def _start():
            return self._start_process(
                num_worker_threads_per_group=num_worker_threads_per_group,
                schedule_entries=schedule_entries,
                tzinfo=tzinfo,
                daemon=True)

        processes = [_start() for _ in range(num_processes)]

        try:
            with capture_signals(signal.SIGTERM):
                while True:
                    for i, p in enumerate(list(processes)):
                        if not p.is_alive() or should_restart(p):
                            if p.is_active():
                                p.terminate()
                            p.join()
                            processes[i] = _start()
                    time.sleep(1)
        except (KeyboardInterrupt, SystemExit, SignalCaptured) as e:
            for p in processes:
                if p.is_active():
                    p.terminate()
            for p in processes:
                p.join()
            if isinstance(e, SignalCaptured):
                raise SystemExit from e
            else:
                raise

    def start_processes(self,
                        num_processes: int,
                        num_worker_threads_per_group: dict[str, int],
                        schedule_entries: ScheduleEntriesCompatMapping = {},
                        tzinfo: Optional[tzinfo] = None,
                        daemon: bool = True) -> list[TaskkitProcess]:
        schedule_entries = self._ensure_schedule_entries(schedule_entries)
        return [
            self._start_process(num_worker_threads_per_group,
                                schedule_entries,
                                tzinfo,
                                daemon)
            for _ in range(num_processes)
        ]

    def _start_process(self,
                       num_worker_threads_per_group: dict[str, int],
                       schedule_entries: dict[str, list[ScheduleEntry]] = {},
                       tzinfo: Optional[tzinfo] = None,
                       daemon: bool = True) -> TaskkitProcess:
        p = TaskkitProcess(
            num_worker_threads_per_group=num_worker_threads_per_group,
            backend=self.backend,
            bridge=self.bridge,
            handler=self.handler,
            schedule_entries=schedule_entries,
            tzinfo=tzinfo or local_tz(),
            daemon=daemon)
        p.start()
        return p

    def _ensure_schedule_entries(self, entries: ScheduleEntriesCompatMapping)\
            -> dict[str, list[ScheduleEntry]]:

        return {
            k: [self._ensure_schedule_entry(e) for e in v]
            for k, v in entries.items()
        }

    def _ensure_schedule_entry(self, entry: ScheduleEntryCompat)\
            -> ScheduleEntry:
        if isinstance(entry, dict):
            return ScheduleEntry(
                key=entry['key'],
                schedule=entry['schedule'],
                group=entry['group'],
                name=entry['name'],
                data=self.handler.encode_data(
                    entry['group'], entry['name'], entry['data']),
                result_ttl=entry.get('result_ttl'),
            )
        return entry

    def initiate_task(self,
                      group: str,
                      name: str,
                      data: Any,
                      due: Union[datetime, float, None] = None,
                      ttl: float = DEFAULT_TASK_TTL,
                      eager: bool = False) -> Result[Any]:
        return self.initiate_tasks({
            'group': group,
            'name': name,
            'data': data,
            'due': due,
            'ttl': ttl,
            'eager': eager,
        })[0]

    def initiate_tasks(self, *args: Union[Task, tuple[Task, bool], InitiateTaskArgs]) -> list[Result[Any]]:
        tasks: list[Task] = []
        results: list[Result[Any]] = []

        for item in args:
            if isinstance(item, Task):
                task = item
                eager = False
            elif isinstance(item, tuple):
                task, eager = item
            else:
                group = item['group']
                name = item['name']
                data = item['data']
                due = item.get('due')
                ttl = item.get('ttl', DEFAULT_TASK_TTL)

                encoded = self.handler.encode_data(group, name, data)
                task = Task.init(group, name=name, data=encoded, due=due, ttl=ttl)
                eager = item.get('eager') or False

            if eager:
                results.append(self.eager_worker.handle_task(task))
            else:
                results.append(Result(self.backend, self.handler, task.id))
                tasks.append(task)

        if tasks:
            self.backend.put_tasks(*tasks)
        return results

    def get_result(self, task_id: str) -> Result:
        return Result(self.backend, self.handler, task_id)

    def send_shutdown_event(self, groups: Optional[set[str]] = None):
        event: Shutdown = {
            'name': 'shutdown',
            'groups': None if groups is None else list(groups),
        }
        self.bridge.send_event(event)

    def send_pause_event(self, groups: Optional[set[str]] = None):
        event: Pause = {
            'name': 'pause',
            'groups': None if groups is None else list(groups),
        }
        self.bridge.send_event(event)

    def send_resume_event(self, groups: Optional[set[str]] = None):
        event: Resume = {
            'name': 'resume',
            'groups': None if groups is None else list(groups),
        }
        self.bridge.send_event(event)
