import time
from threading import Thread
from .backend import Backend
from .services import Service
from .task import TaskHandler
from .utils import logger
from .worker import Worker


class ServiceThread(Thread):
    def __init__(self,
                 service: Service,
                 **kwargs):
        self.should_stop = False
        self.service = service
        super().__init__(**kwargs)

    def run(self):
        while not self.should_stop:
            try:
                time_to_wait = self.service()
                while time_to_wait > 0:
                    time.sleep(min(1, time_to_wait))
                    time_to_wait -= 1
                    if self.should_stop:
                        return
            except Exception:
                logger.exception(
                    'Unexpected exception occurred during service call '
                    f'({self.service.__class__.__qualname__})')
                time.sleep(1)


class WorkerThread(ServiceThread):
    def __init__(self,
                 group: str,
                 backend: Backend,
                 handler: TaskHandler,
                 **kwargs):
        self.group = group
        self.worker = Worker(group, backend, handler)
        super().__init__(service=self.worker, **kwargs)

    @property
    def id(self) -> str:
        return self.worker.id

    def get_id(self) -> str:
        return self.worker.id

    @property
    def paused(self) -> bool:
        return self.worker.paused

    @paused.setter
    def paused(self, paused: bool):
        self.worker.paused = paused

    def run(self):
        logger.info(f'[{self.id}] worker thread started')
        super().run()
        logger.info(f'[{self.id}] worker thread stopped')
