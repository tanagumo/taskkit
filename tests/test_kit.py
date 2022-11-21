import json
from datetime import datetime, timezone
from typing import Any
from unittest import TestCase

from taskkit.kit import Kit
from taskkit.result import ResultGetTimedOut
from taskkit.task import Task, TaskHandler


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Handler(TaskHandler):
    def handle(self, task: Task) -> Any:
        return 'ok'

    def get_retry_interval(self,
                           task: Task,
                           exception: Exception) -> float | None:
        return 1

    def encode_data(self, group: str, task_name: str, data: Any) -> bytes:
        return json.dumps(data).encode()

    def encode_result(self, task: Task, result: Any) -> bytes:
        return json.dumps(result).encode()

    def decode_result(self, task: Task, encoded: bytes) -> Any:
        return json.loads(encoded)


class KitTests(TestCase):
    def make_kit(self, handler: TaskHandler) -> Kit:
        raise NotImplementedError

    @property
    def kit(self) -> Kit:
        return self.make_kit(self.handler)

    handler: TaskHandler = Handler()

    def skip_if_needed(self):
        try:
            self.kit
        except NotImplementedError:
            self.skipTest('abstract class')

    def tearDown(self):
        self.skip_if_needed()
        self.kit.backend.destroy_all()

    def test_initiate_task(self):
        self.skip_if_needed()

        kit = self.kit

        GROUP = 'test_group'
        NAME = 'test'
        DATA = 'test_data'
        DUE = _now()
        TTL = 123456

        for eager in [True, False]:
            ret = kit.initiate_task(group=GROUP, name=NAME, data=DATA,
                                    due=DUE, ttl=TTL, eager=eager)

            task = kit.backend.lookup_tasks(ret.task_id)[ret.task_id]
            assert task
            self.assertEqual(task.id, ret.task_id)
            self.assertEqual(task.group, GROUP)
            self.assertEqual(task.name, NAME)
            self.assertEqual(task.data,
                             kit.handler.encode_data(GROUP, NAME, DATA))
            self.assertEqual(task.due, DUE.timestamp())
            self.assertEqual(task.scheduled, None)
            self.assertEqual(task.retry_count, 0)
            self.assertEqual(task.ttl, TTL)

            if eager:
                # no error
                ret.get(timeout=0.1)
            else:
                with self.assertRaises(ResultGetTimedOut):
                    ret.get(timeout=0.2)

            kit.backend.discard_tasks(task.id)
