import json
from typing import Generator, Optional

from redis.client import Redis, Pipeline
from redis.exceptions import LockError
from redis.lock import Lock as _RedisLock

from ..backend import Backend, Lock, NotFound, NoResult, Failed
from ..event import EventBridge, ControlEvent, encode_control_event, decode_control_event
from ..kit import Kit
from ..stage import StageInfo
from ..task import Task, TaskHandler
from ..utils import cur_ts


class RedisEventBridge(EventBridge):
    def __init__(self, redis: Redis):
        self.channel = 'taskkit.redis_bridge.channel'
        self.redis = redis

    def receive_events(self) -> Generator[ControlEvent, None, None]:
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.channel)
        while True:
            for m in pubsub.listen():
                if not isinstance(m['data'], bytes):
                    continue
                if (event := decode_control_event(m['data'])):
                    yield event

    def send_event(self, event: ControlEvent):
        self.redis.publish(self.channel, encode_control_event(event))


class RedisLock(Lock):
    def __init__(self, lock: _RedisLock):
        self.lock = lock

    def acquire(self) -> bool:
        return self.lock.acquire(blocking=False)

    def release(self):
        try:
            self.lock.release()
        except LockError:
            pass


_KEY_PREFIX = 'taskkit.backend'


class RedisBackend(Backend):
    def __init__(self, redis: Redis):
        self.redis = redis

    def workers_key(self) -> str:
        return f'{_KEY_PREFIX}.workers'

    def queue_key(self, group: str) -> str:
        return f'{_KEY_PREFIX}.{group}.queue'

    def data_store_key(self) -> str:
        return f'{_KEY_PREFIX}.data_store'

    def task_info_key(self) -> str:
        return f'{_KEY_PREFIX}.task_info'

    def stage_queue_key(self) -> str:
        return f'{_KEY_PREFIX}.stage_queue'

    def stage_info_key(self) -> str:
        return f'{_KEY_PREFIX}.stage_info'

    def done_queue_key(self) -> str:
        return f'{_KEY_PREFIX}.done_queue'

    def disposable_queue_key(self) -> str:
        return f'{_KEY_PREFIX}.disposable_queue'

    def result_key(self) -> str:
        return f'{_KEY_PREFIX}.result'

    def error_message_key(self) -> str:
        return f'{_KEY_PREFIX}.error_message'

    def scheduler_data_key(self, name: str) -> str:
        return f'{_KEY_PREFIX}.scheduler.{name}'

    def lock_key_prefix(self, target: str) -> str:
        return f'{_KEY_PREFIX}.locks.{target}'

    def set_worker_ttl(self, worker_ids: set[str], expires_at: float):
        if not worker_ids:
            return
        self.redis.zadd(self.workers_key(), {
            worker_id: expires_at
            for worker_id in worker_ids
        })

    def get_workers(self) -> list[tuple[str, float]]:
        return [
            (worker_id.decode(), ttl) for worker_id, ttl in
            self.redis.zrange(self.workers_key(), 0, -1, withscores=True)
        ]

    def purge_workers(self, worker_ids: set[str]):
        if worker_ids:
            self.redis.zrem(self.workers_key(), *worker_ids)

    def put_tasks(self, *tasks: Task):
        if not tasks:
            return
        with self.redis.pipeline() as pipe:
            self._put_tasks(pipe, *tasks)
            pipe.execute()

    def _put_tasks(self, pipe: Pipeline, *_tasks: Task):
        if not _tasks:
            return
        for group, tasks in Task.group_tasks(*_tasks).items():
            task_ids = [t.id for t in tasks]
            pipe.zrem(self.stage_queue_key(), *task_ids)
            pipe.hdel(self.stage_info_key(), *task_ids)
            self._put_task_data(pipe, *_tasks)
            pipe.zadd(self.queue_key(group), {t.id: t.due for t in tasks})

    def _put_task_data(self, pipe: Pipeline, *tasks: Task):
        if not tasks:
            return
        pipe.hset(self.data_store_key(), mapping={
            t.id: t.data for t in tasks
        })
        pipe.hset(self.task_info_key(), mapping={
            t.id: self._encode_info(t) for t in tasks
        })

    def get_queued_tasks(self, group: str, limit: int) -> list[Task]:
        task_ids = list(
            self.redis.zrange(self.queue_key(group), 0, limit))
        tasks = self.lookup_tasks(*task_ids)
        return [t for tid in task_ids if (t := tasks.get(tid))]

    def lookup_tasks(self, *task_ids: str) -> dict[str, Optional[Task]]:
        if not task_ids:
            return dict()

        info = self.redis.hmget(self.task_info_key(), *task_ids)
        data = self.redis.hmget(self.data_store_key(), *task_ids)

        return {
            tid: Task(**json.loads(i), data=d)
            if (i is not None and d is not None) else None
            for tid, i, d in zip(task_ids, info, data)
        }

    def assign_task(self, group: str, worker_id: str) -> Optional[Task]:
        def _pop(pipe):
            ret: list[tuple[bytes, float]] =\
                pipe.zrange(self.queue_key(group), 0, 0, withscores=True)
            if not ret:
                return None
            task_id, score = ret[0]
            if score > cur_ts():
                return None
            info = json.loads(
                pipe.hget(self.task_info_key(), task_id).decode())
            task = Task(**info, data=pipe.hget(self.data_store_key(), task_id))

            pipe.multi()
            pipe.zrem(self.queue_key(group), task_id)
            pipe.zadd(self.stage_queue_key(), {task_id: task.due})
            pipe.hset(self.stage_info_key(),
                      task.id, StageInfo.init(worker_id, task).to_json())
            return task

        return self.redis.transaction(
            _pop, self.queue_key(group), value_from_callable=True)

    def discard_tasks(self, *task_ids: str):
        if not task_ids:
            return

        with self.redis.pipeline() as pipe:
            for group, _task_ids in Task.group_task_ids(*task_ids).items():
                pipe.zrem(self.queue_key(group), *_task_ids)
            pipe.zrem(self.stage_queue_key(), *task_ids)
            pipe.hdel(self.stage_info_key(), *task_ids)
            pipe.hdel(self.data_store_key(), *task_ids)
            pipe.hdel(self.task_info_key(), *task_ids)
            pipe.zrem(self.done_queue_key(), *task_ids)
            pipe.zrem(self.disposable_queue_key(), *task_ids)
            pipe.hdel(self.result_key(), *task_ids)
            pipe.hdel(self.error_message_key(), *task_ids)
            pipe.execute()

    def succeed(self, task: Task, result: bytes):
        group = task.group
        with self.redis.pipeline() as pipe:
            pipe.zrem(self.queue_key(group), task.id)
            pipe.zrem(self.stage_queue_key(), task.id)
            pipe.hdel(self.stage_info_key(), task.id)
            done = cur_ts()
            pipe.zadd(self.done_queue_key(), {task.id: done})
            pipe.zadd(self.disposable_queue_key(),
                      {task.id: done + task.ttl})
            pipe.hset(self.result_key(), task.id, result)
            self._put_task_data(pipe, task)
            pipe.execute()

    def fail(self, task: Task, error_message: str):
        group = task.group
        with self.redis.pipeline() as pipe:
            pipe.zrem(self.queue_key(group), task.id)
            pipe.zrem(self.stage_queue_key(), task.id)
            pipe.hdel(self.stage_info_key(), task.id)
            done = cur_ts()
            pipe.zadd(self.done_queue_key(), {task.id: done})
            pipe.zadd(self.disposable_queue_key(),
                      {task.id: done + task.ttl})
            pipe.hset(self.error_message_key(), task.id,
                      error_message.encode('utf-8'))
            self._put_task_data(pipe, task)
            pipe.execute()

    def get_result(self, task_id: str) -> tuple[Task, bytes]:
        task = self.lookup_tasks(task_id).get(task_id)
        if task is None:
            raise NotFound

        ret = self.redis.hget(self.result_key(), task_id)
        if ret is not None:
            return (task, ret)

        message = self.redis.hget(self.error_message_key(), task_id)
        if message is not None:
            raise Failed(task, message.decode('utf-8'))

        raise NoResult(task)

    def get_done_task_ids(self,
                          since: Optional[float],
                          until: Optional[float],
                          limit: int) -> list[str]:
        return [
            v.decode() for v in self.redis.zrangebyscore(
                self.done_queue_key(),
                since or 0,
                until or cur_ts(),
                start=0,
                num=limit)
        ]

    def get_disposable_task_ids(self, limit: int) -> list[str]:
        return [
            v.decode() for v in self.redis.zrangebyscore(
                self.disposable_queue_key(), 0, cur_ts(), start=0, num=limit)
        ]

    def get_stage_info(self, limit: int) -> list[StageInfo]:
        task_ids = list(
            self.redis.zrange(self.stage_queue_key(), 0, limit))
        if not task_ids:
            return []
        info_data = self.redis.hmget(self.stage_info_key(), *task_ids)
        return [StageInfo.from_json(d) for d in info_data if d is not None]

    def restore(self, info: StageInfo):
        task = self.lookup_tasks(info.task_id).get(info.task_id)
        if task is None:
            return
        with self.redis.pipeline() as pipe:
            pipe.zrem(self.stage_queue_key(), task.id)
            pipe.hdel(self.stage_info_key(), task.id)
            pipe.zadd(self.queue_key(task.group), {task.id: task.due})
            pipe.execute()

    def get_lock(self, target: str) -> Lock:
        return RedisLock(
            self.redis.lock(self.lock_key_prefix(target), timeout=10))

    def persist_scheduler_state_and_put_tasks(self,
                                              name: str,
                                              data: bytes,
                                              *tasks: Task):
        with self.redis.pipeline() as pipe:
            pipe.set(self.scheduler_data_key(name), data)
            self._put_tasks(pipe, *tasks)
            pipe.execute()

    def get_scheduler_state(self, name: str) -> Optional[bytes]:
        return self.redis.get(self.scheduler_data_key(name))

    def destroy_all(self):
        keys = self.redis.keys(f'{_KEY_PREFIX}.*')
        if keys:
            self.redis.delete(*keys)

    def _encode_info(self, task: Task) -> str:
        data = task.to_dict()
        del data['data']
        return json.dumps(data)


def make_kit(redis: Redis,
             handler: TaskHandler) -> Kit:
    backend = RedisBackend(redis)
    bridge = RedisEventBridge(redis)
    return Kit(backend, bridge, handler)
