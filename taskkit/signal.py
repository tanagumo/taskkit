import signal
from contextlib import contextmanager
from types import FrameType
from typing import Optional


class SignalCaptured(BaseException):
    def __init__(self, signal: int, frame: Optional[FrameType]):
        self.signal = signal
        self.frame = frame

    @classmethod
    def handler(cls, signal: int, frame: Optional[FrameType]):
        raise cls(signal, frame)


@contextmanager
def capture_signals(*signals: signal.Signals):
    originals = [signal.getsignal(s) for s in signals]

    def handler(*args, **kwargs):
        SignalCaptured.handler(*args, **kwargs)

    for s in signals:
        signal.signal(s, handler)

    try:
        yield
    finally:
        for s, org in zip(signals, originals):
            signal.signal(s, org)
