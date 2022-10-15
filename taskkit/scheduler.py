import json
from dataclasses import dataclass, asdict
from datetime import datetime, tzinfo
from typing import Any, Protocol, Literal

from .backend import Backend
from .services import Service
from .task import Task
from .utils import cur_ts, from_ts, as_ts, logger


# in seconds
SCHEDULE_POINT_INTERVAL = 5


class Schedule(Protocol):
    def get_timezone(self) -> tzinfo | None:
        ...

    def __call__(self,
                 schedule_points: list[datetime],
                 last_scheduled_at: datetime | None,
                 /) -> list[datetime]:
        ...


class DuplicationPolicy(Protocol):
    def __call__(self,
                 schedule_points: list[datetime],
                 last_scheduled_at: datetime | None,
                 /) -> list[datetime]:
        ...


class OnlyEarliest(DuplicationPolicy):
    def __call__(self,
                 schedule_points: list[datetime],
                 last_scheduled_at: datetime | None,
                 /) -> list[datetime]:
        return schedule_points[:1]


class OnlyLatest(DuplicationPolicy):
    def __call__(self,
                 schedule_points: list[datetime],
                 last_scheduled_at: datetime | None,
                 /) -> list[datetime]:
        return schedule_points[-1:]


class RegularSchedule(Schedule):
    _seconds = set(range(0, 60, SCHEDULE_POINT_INTERVAL))
    _minutes = set(range(60))
    _hours = set(range(24))
    _days = set(range(1, 32))
    _weekdays = set(range(7))
    _months = set(range(1, 13))

    def __init__(self,
                 seconds: Literal['*'] | set[int] | int | None = 0,
                 minutes: Literal['*'] | set[int] | int | None = None,
                 hours: Literal['*'] | set[int] | int | None = None,
                 days: Literal['*'] | set[int] | int | None = None,
                 weekdays: Literal['*'] | set[int] | int | None = None,
                 months: Literal['*'] | set[int] | int | None = None,
                 tzinfo: tzinfo | None = None,
                 duplication_policy: DuplicationPolicy = OnlyLatest()):
        self.seconds = self._ensure('seconds', seconds, self._seconds)
        self.minutes = self._ensure('minutes', minutes, self._minutes)
        self.hours = self._ensure('hours', hours, self._hours)
        self.days = self._ensure('days', days, self._days)
        self.weekdays = self._ensure('weekdays', weekdays, self._weekdays)
        self.months = self._ensure('months', months, self._months)
        self.tzinfo = tzinfo
        self.duplication_policy = duplication_policy

    def get_timezone(self) -> tzinfo | None:
        if self.tzinfo is None:
            return None
        return self.tzinfo

    @staticmethod
    def _ensure(key: str,
                value: Literal['*'] | set[int] | int | None,
                all_valid_values: set[int]) -> set[int]:
        if value is None or value == '*':
            return all_valid_values
        if isinstance(value, int):
            value = {value}
        assert all(t in all_valid_values for t in value),\
            f'All values of `{key}` must be in {all_valid_values}.'\
            f' The values are: `{value}`.'
        return value

    def __call__(self,
                 schedule_points: list[datetime],
                 last_scheduled_at: datetime | None,
                 /) -> list[datetime]:
        return self.duplication_policy(
            [p for p in schedule_points if self._filter(p)],
            last_scheduled_at)

    def _filter(self, schedule_point: datetime) -> bool:
        if schedule_point.second not in self.seconds:
            return False
        if schedule_point.minute not in self.minutes:
            return False
        if schedule_point.hour not in self.hours:
            return False
        if schedule_point.day not in self.days:
            return False
        if schedule_point.weekday() not in self.weekdays:
            return False
        if schedule_point.month not in self.months:
            return False
        return True


@dataclass
class ScheduleEntry:
    key: str
    schedule: Schedule
    group: str
    name: str
    data: Any


@dataclass
class SchedulerState:
    last_run_at: float

    # key: Entry.key
    # value: last scheudled timestamp for the entry
    last_scheduled_at: dict[str, float]


class Scheduler(Service):
    def __init__(self,
                 name: str,
                 backend: Backend,
                 entries: list[ScheduleEntry],
                 tzinfo: tzinfo):
        assert len(entries) == len({e.key for e in entries}),\
            'All entries must have unique keys'
        self.name = name
        self.backend = backend
        self.entries = entries
        self.lock = backend.get_lock(f'scheduler.{name}')
        self.tzinfo = tzinfo

    def __call__(self) -> float:
        """It schedules entries and returns time interval indicating when
        should this method be called next time."""

        if self.entries and self.lock.acquire():
            try:
                self._schedule_entries()
            finally:
                self.lock.release()

        return self._round(SCHEDULE_POINT_INTERVAL)

    def _schedule_entries(self):
        state = self._get_state()
        schedule_points = self._list_schedule_points(state)
        if not schedule_points:
            return

        ls_at = state.last_scheduled_at if state else {}
        new_state = SchedulerState(
            last_run_at=schedule_points[-1],
            last_scheduled_at={},
        )
        tasks: list[Task] = []
        for e in self.entries:
            last = ls_at.get(e.key)
            tz = e.schedule.get_timezone() or self.tzinfo
            if last is not None:
                new_state.last_scheduled_at[e.key] = last
                last = from_ts(last, tz)
            for sp in e.schedule(
                    [from_ts(sp, tz) for sp in schedule_points], last):
                new_state.last_scheduled_at[e.key] = as_ts(sp)
                task = Task.init(group=e.group, name=e.name, data=e.data,
                                 due=sp, scheduled=sp)
                tasks.append(task)
                logger.info(f'schedule task `{task.id}` at {sp}')

        self.backend.persist_scheduler_state_and_put_tasks(
            self.name,
            self._encode_state(new_state),
            *tasks)

    def _encode_state(self, state: SchedulerState) -> bytes:
        return json.dumps(asdict(state)).encode()

    def _get_state(self) -> SchedulerState | None:
        data = self.backend.get_scheduler_state(self.name)
        if data is None:
            return None

        return SchedulerState(**json.loads(data.decode()))

    def _list_schedule_points(
            self, state: SchedulerState | None) -> list[float]:
        at = self._round(cur_ts())
        if state is None:
            return [at]
        if state.last_run_at >= at:
            return []

        n = int((at - state.last_run_at) // SCHEDULE_POINT_INTERVAL)
        return [
            state.last_run_at + (i * SCHEDULE_POINT_INTERVAL)
            for i in range(1, n + 1)
        ]

    def _round(self, ts: float) -> float:
        return ts // SCHEDULE_POINT_INTERVAL * SCHEDULE_POINT_INTERVAL
