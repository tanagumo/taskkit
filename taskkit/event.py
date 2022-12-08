import json
from typing import Protocol, Generator, Literal, TypedDict, Optional, Union


class Shutdown(TypedDict):
    name: Literal['shutdown']
    groups: Optional[list[str]]


class Pause(TypedDict):
    name: Literal['pause']
    groups: Optional[list[str]]


class Resume(TypedDict):
    name: Literal['resume']
    groups: Optional[list[str]]


ControlEvent = Union[Shutdown, Pause, Resume]


class EventBridge(Protocol):
    def receive_events(self) -> Generator[ControlEvent, None, None]:
        ...

    def send_event(self, event: ControlEvent):
        ...


def encode_control_event(event: ControlEvent) -> bytes:
    return json.dumps(event).encode('utf-8')


def decode_control_event(encoded: bytes) -> Optional[ControlEvent]:
    try:
        event: ControlEvent = json.loads(encoded.decode('utf-8'))
        assert isinstance(event['name'], str)
        assert event['groups'] is None or isinstance(event['groups'], list)
    except Exception:
        return None
    return event
