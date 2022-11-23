import json
from typing import TypeAlias, Protocol, Generator, Literal, TypedDict


class Shutdown(TypedDict):
    name: Literal['shutdown']
    groups: list[str] | None


class Pause(TypedDict):
    name: Literal['pause']
    groups: list[str] | None


class Resume(TypedDict):
    name: Literal['resume']
    groups: list[str] | None


ControlEvent: TypeAlias = Shutdown | Pause | Resume


class EventBridge(Protocol):
    def receive_events(self) -> Generator[ControlEvent, None, None]:
        ...

    def send_event(self, event: ControlEvent):
        ...


def encode_control_event(event: ControlEvent) -> bytes:
    return json.dumps(event).encode('utf-8')


def decode_control_event(encoded: bytes) -> ControlEvent | None:
    try:
        event: ControlEvent = json.loads(encoded.decode('utf-8'))
        assert isinstance(event['name'], str)
        assert event['groups'] is None or isinstance(event['groups'], list)
    except Exception:
        return None
    return event
