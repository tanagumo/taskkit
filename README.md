# taskkit

pypi: https://pypi.org/project/taskkit/

## Overview

`taskkit` is a distributed task runner.

## How to use

### 1. Implement TaskEncoder

If you use only json serializable objects for the task arguments and results, following encoder should be enough:

```python
import json
from typing import Any
from taskkit import TaskEncoder


class Encoder(TaskEncoder):
    def encode_data(self, group: str, task_name: str, data: Any) -> bytes:
        return json.dumps(data).encode()

    def decode_data(self, group: str, task_name, encoded: bytes) -> Any:
        return json.loads(encoded)

    def encode_result(self, group: str, task_name: str, result: Any) -> bytes:
        return json.dumps(result).encode()

    def decode_result(self, group: str, task_name: str, encoded: bytes) -> Any:
        return json.loads(encoded)
```

### 2. Implement TaskHandler

This is the core part.

```python
from taskkit import TaskHandler, Task, DiscardTask


class Handler(TaskHandler):
    def handle(self, task: Task):
        # Use `tagk.group` and `task.name` to determine how to handle the task
        if task.name == '...':
            # do something with `task.data`
            ...
            # return result of the task which can be encoded by the Encoder
            return ...

        # you should raise DiscardTask if you want to discard the task
        raise DiscardTask

    def get_retry_interval(self,
                           task: Task,
                           exception: Exception) -> float | None:
        # This method will be called if the handle method raises exceptions.
        # You should return how long time should be wait to retry the task in
        # seconds as float. You can return None if you don't want to retry.
        return task.retry_count if task.retry_count < 10 else None
```

### 3. Make Kit

Now taskkit supports only redis as a backend. So you should do like this:

```python
from redis.client import Redis
from taskkit.impl.redis import make_kit

REDIS_HOST = '...'
REDIS_PORT = '...'

redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
kit = make_kit(redis, Encoder(), Handler())
```


### 4. Run workers

```python
GROUP_NAME = 'Any task group name'

kit.start(
    # number of worker threads per process
    num_worker_threads_per_group={GROUP_NAME: 3},
    # number of processes
    num_processes=3,
    # if join is True, it wait until all processes stopped
    join=True)
```


### 5. Initiate task

```python
from datetime import timedelta
from taskkit import ResultGetTimedOut


result = kit.initiate_task(
    GROUP_NAME,
    # task name
    'your task name',
    # task data which can be encoded by the Encoder
    dict(some_data=1),
    # run the task after 10 or more seconds.
    due=datetime.now() + timedelta(seconds=10))

try:
    value = result.get(timeout=10)
except ResultGetTimedOut:
    ...
```

### Scheduled Tasks

```python
from datetime import timezone, timedelta
from taskkit import ScheduleEntry, RegularSchedule

# define entries
# key is a name for scheduler
# value is a list of instances of ScheduleEntry
schedule_entries = {
    'scheduler_name': [
        ScheduleEntry(
            # A key which can identify the schedule in the list
            key='...',
            # group name
            group=GROUP_NAME,
            # task name
            name='test2',
            # task data
            data=None,

            # It means that the scheduler will initiate the task twice
            # an hour at **:00:00 and **:30:00.
            schedule=RegularSchedule(
                seconds={0},
                minutes={0, 30},
            ),
        ),
    ],

    # You can have multiple schedulers
    'another_scheduler': [
        # other entries ...
    ],
}

# pass the entries with kit.start
kit.start(
    num_worker_threads_per_group={GROUP_NAME: 3},
    num_processes=3,

    schedule_entries=schedule_entries,
    tzinfo=timezone(timedelta(hours=9), 'JST'),

    join=True)
```
