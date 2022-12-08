from dataclasses import dataclass, fields
from datetime import datetime, timezone
from typing import Any, Protocol, Optional, Union
from uuid import uuid4

from .utils import cur_ts, as_ts, from_ts, local_tz


DEFAULT_TASK_TTL = 60 * 60 * 24 * 7


@dataclass(frozen=True)
class Task:
    id: str
    name: str
    data: bytes
    due: float
    created: float
    scheduled: Optional[float]
    retry_count: int
    ttl: float

    @classmethod
    def init(cls,
             group: str,
             name: str,
             data: bytes,
             due: Union[datetime, float, None] = None,
             scheduled: Union[datetime, float, None] = None,
             ttl: float = DEFAULT_TASK_TTL) -> 'Task':
        created = cur_ts()
        return cls(id=cls.make_id(group),
                   name=name,
                   data=data,
                   due=as_ts(due or created),
                   created=created,
                   scheduled=as_ts(scheduled),
                   retry_count=0,
                   ttl=ttl)

    @property
    def group(self) -> str:
        return self.get_group_from_id(self.id)

    def due_dt(self, tzinfo: Optional[timezone] = None) -> datetime:
        return from_ts(self.due, tzinfo or local_tz())

    def created_dt(self, tzinfo: Optional[timezone] = None) -> datetime:
        return from_ts(self.created, tzinfo or local_tz())

    def scheduled_dt(self, tzinfo: Optional[timezone] = None) -> Optional[datetime]:
        return from_ts(self.scheduled, tzinfo or local_tz())\
            if self.scheduled is not None else None

    @staticmethod
    def make_id(group: str) -> str:
        return f'{group}_{uuid4().hex}'

    @staticmethod
    def get_group_from_id(task_id: str) -> str:
        return task_id.rsplit('_', 1)[0]

    @staticmethod
    def group_tasks(*tasks: 'Task') -> dict[str, list['Task']]:
        result: dict[str, list[Task]] = dict()
        for task in tasks:
            group = task.group
            if group not in result:
                result[group] = [task]
            else:
                result[group].append(task)
        return result

    @staticmethod
    def group_task_ids(*task_ids: str) -> dict[str, list[str]]:
        result: dict[str, list[str]] = dict()
        for tid in task_ids:
            group = Task.get_group_from_id(tid)
            if group not in result:
                result[group] = [tid]
            else:
                result[group].append(tid)
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
        }

    def clone_for_retry(self, interval: float) -> 'Task':
        d = self.to_dict()
        d['due'] = cur_ts() + interval
        d['retry_count'] += 1
        return Task(**d)


class DiscardTask(Exception):
    pass


class TaskHandler(Protocol):
    def handle(self, task: Task) -> Any:
        """Handle the task or raise DiscardTask to ignore the task"""
        ...

    def get_retry_interval(self,
                           task: Task,
                           exception: Exception) -> Optional[float]:
        """Return interval in seconds if you want to retry the failed task.

        This method will be called if the handle method raises exceptions. You
        should return how long time should be wait to retry the task in seconds
        as float. If you don't want to retry the task, you can return None to
        make the task fail or raise DiscardTask to discard the task.
        """
        ...

    def encode_data(self, group: str, task_name: str, data: Any) -> bytes:
        ...

    def encode_result(self, task: Task, result: Any) -> bytes:
        ...

    def decode_result(self, task: Task, encoded: bytes) -> Any:
        ...
