import json
from dataclasses import dataclass, asdict
from typing import Any

from .task import Task
from .utils import cur_ts


@dataclass
class StageInfo:
    worker_id: str
    task_id: str
    began: float

    @classmethod
    def init(cls,
             worker_id: str,
             task: Task) -> 'StageInfo':
        return cls(worker_id=worker_id,
                   task_id=task.id,
                   began=cur_ts())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_data: str) -> 'StageInfo':
        return cls(**json.loads(json_data))
