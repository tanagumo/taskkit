import time
from datetime import datetime, timezone, timedelta
from threading import Thread
from unittest import TestCase

from taskkit.task import Task
from taskkit.backend import Backend, NotFound, NoResult, Failed


GROUP = 'test_group'


def _now() -> datetime:
    return datetime.now(timezone.utc)


class BackendTests(TestCase):
    backend: Backend

    def skip_if_needed(self):
        if not hasattr(self, 'backend'):
            self.skipTest('abstract class')

    def tearDown(self):
        self.skip_if_needed()
        self.backend.destroy_all()

    def test_workers(self):
        self.skip_if_needed()

        self.assertEqual(self.backend.get_workers(), [])

        worker_a = 'a'
        ex_a = (_now() + timedelta(minutes=1)).timestamp()
        worker_b = 'b'
        ex_b = (_now() + timedelta(minutes=1)).timestamp()

        self.backend.set_worker_ttl({worker_a}, ex_a)
        self.backend.set_worker_ttl({worker_b}, ex_b)
        self.assertEqual(self.backend.get_workers(),
                         [(worker_a, ex_a), (worker_b, ex_b)])

        self.backend.purge_workers({worker_a})
        self.assertEqual(self.backend.get_workers(),
                         [(worker_b, ex_b)])

        self.backend.purge_workers({worker_b})
        self.assertEqual(self.backend.get_workers(), [])

    def test_tasks(self):
        self.skip_if_needed()

        task_a = Task.init(GROUP, 'a', b'a')
        task_b = Task.init(GROUP, 'b', b'b', ttl=0.5)

        self.backend.put_tasks(task_a, task_b)

        self.assertEqual(
            self.backend.get_queued_tasks(GROUP, limit=10),
            [task_a, task_b],
        )

        self.assertEqual(
            self.backend.lookup_tasks(task_a.id, task_b.id, 'dummy'),
            {
                task_a.id: task_a,
                task_b.id: task_b,
                'dummy': None
            })

        with self.subTest('assign_task'):
            worker_a = 'a'
            worker_b = 'b'

            self.assertEqual(self.backend.assign_task(GROUP, worker_a), task_a)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_b],
            )
            self.assertEqual(self.backend.assign_task(GROUP, worker_b), task_b)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [],
            )

            self.assertEqual(
                self.backend.lookup_tasks(task_a.id, task_b.id, 'dummy'),
                {
                    task_a.id: task_a,
                    task_b.id: task_b,
                    'dummy': None
                })

            stage_info = self.backend.get_stage_info(limit=10)
            self.assertEqual(len(stage_info), 2)
            si_a, si_b = stage_info

            self.assertEqual(si_a.task_id, task_a.id)
            self.assertEqual(si_a.worker_id, worker_a)
            self.assertEqual(si_b.task_id, task_b.id)
            self.assertEqual(si_b.worker_id, worker_b)

            # check due
            task_c = Task.init(GROUP, 'c', b'c',
                               due=_now() + timedelta(seconds=0.5))
            self.backend.put_tasks(task_c)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_c],
            )
            self.assertIsNone(self.backend.assign_task(GROUP, worker_a))
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_c],
            )
            time.sleep(0.5)
            self.assertEqual(self.backend.assign_task(GROUP, worker_a), task_c)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [],
            )
            self.backend.discard_tasks(task_c.id)

        with self.subTest('retry_task'):
            self.assertEqual(len(self.backend.get_stage_info(limit=10)), 2)
            self.backend.retry_task(task_a)
            self.assertEqual(self.backend.get_stage_info(limit=10)[0].task_id, task_b.id)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_a],
            )
            self.backend.retry_task(task_b)
            self.assertEqual(self.backend.get_stage_info(limit=10), [])
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_a, task_b],
            )

        with self.subTest('restore'):
            self.backend.assign_task(GROUP, worker_a)
            self.backend.assign_task(GROUP, worker_b)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [],
            )
            si_a, si_b = self.backend.get_stage_info(limit=10)
            self.backend.restore(si_b)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_b],
            )
            self.backend.restore(si_a)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_a, task_b],
            )

        with self.subTest('succeed and fail and get_result'):
            self.backend.assign_task(GROUP, worker_a)
            self.assertEqual(
                self.backend.get_queued_tasks(GROUP, limit=10),
                [task_b],
            )
            self.assertEqual(
                self.backend.get_stage_info(limit=10)[0].task_id,
                task_a.id,
            )

            with self.assertRaises(NoResult) as ctx:
                self.backend.get_result(task_a.id)
            self.assertEqual(ctx.exception.task, task_a)

            RESULT = b'result'
            self.backend.succeed(task_a, RESULT)
            self.assertEqual(self.backend.get_result(task_a.id),
                             (task_a, RESULT))
            self.assertEqual(self.backend.get_done_task_ids(None, None, 10),
                             [task_a.id])
            self.assertEqual(self.backend.get_disposable_task_ids(10), [])

            ERROR_MESSAGE = 'error'
            self.backend.fail(task_b, ERROR_MESSAGE)
            with self.assertRaises(Failed) as ctx:
                self.backend.get_result(task_b.id)
            self.assertEqual(ctx.exception.task, task_b)
            self.assertEqual(ctx.exception.message, ERROR_MESSAGE)

            self.assertEqual(self.backend.get_done_task_ids(None, None, 10),
                             [task_a.id, task_b.id])

            # task_b's ttl is 0.5
            self.assertEqual(self.backend.get_disposable_task_ids(10), [])
            time.sleep(0.5)
            self.assertEqual(self.backend.get_disposable_task_ids(10), [task_b.id])

            self.assertEqual(self.backend.get_queued_tasks(GROUP, limit=10), [])
            self.assertEqual(self.backend.get_stage_info(limit=10), [])

            with self.assertRaises(NotFound):
                self.backend.get_result('invalid_id')

        with self.subTest('discard_tasks'):
            self.assertEqual(
                self.backend.lookup_tasks(task_a.id, task_b.id),
                {
                    task_a.id: task_a,
                    task_b.id: task_b,
                })

            self.backend.discard_tasks(task_a.id)
            self.assertEqual(
                self.backend.lookup_tasks(task_a.id, task_b.id),
                {
                    task_a.id: None,
                    task_b.id: task_b,
                })
            with self.assertRaises(NotFound):
                self.backend.get_result(task_a.id)

            self.backend.discard_tasks(task_b.id)
            self.assertEqual(
                self.backend.lookup_tasks(task_a.id, task_b.id),
                {
                    task_a.id: None,
                    task_b.id: None,
                })
            with self.assertRaises(NotFound):
                self.backend.get_result(task_b.id)

    def test_get_lock(self):
        self.skip_if_needed()

        lock_a = self.backend.get_lock('a')
        self.assertTrue(lock_a.acquire())
        try:
            lock_a.release()
            self.assertTrue(lock_a.acquire())

            def _thread():
                lock_a = self.backend.get_lock('a')
                lock_b = self.backend.get_lock('b')
                self.assertFalse(lock_a.acquire())
                try:
                    self.assertTrue(lock_b.acquire())
                finally:
                    lock_b.release()

            thread = Thread(target=_thread)
            thread.start()
            thread.join()
        finally:
            lock_a.release()

    def test_scheduler(self):
        self.skip_if_needed()

        scheduled_a = _now()
        task_a = Task.init(GROUP, 'a', b'a', scheduled=scheduled_a)
        scheduled_b = _now()
        task_b = Task.init(GROUP, 'b', b'b', scheduled=scheduled_b)

        name = 'scheduler_name'
        scheduler_state_data = b'test'

        self.backend.persist_scheduler_state_and_put_tasks(
            name, scheduler_state_data, task_a, task_b)

        self.assertEqual(self.backend.get_scheduler_state(name),
                         scheduler_state_data)

        tasks = self.backend.get_queued_tasks(GROUP, limit=10)
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks, [task_a, task_b])
