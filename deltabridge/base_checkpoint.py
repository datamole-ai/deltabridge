import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class StreamOffset:
    """Last version of the Delta table whose rows were delivered."""

    table_id: str
    version: int


class BaseCheckpoint(ABC):
    """
    Interface for checkpoints, which store the offset of a stream.
    """

    @abstractmethod
    def _read(self) -> bytes | None:
        """Stored data, or None if nothing is stored."""
        ...

    @abstractmethod
    def _write(self, data: bytes) -> None:
        """Store the data, replacing the previous data."""
        ...

    def load(self) -> StreamOffset | None:
        """Load the offset, or None if no offset is stored."""
        data = self._read()
        return None if data is None else StreamOffset(**json.loads(data))

    def save(self, offset: StreamOffset) -> None:
        """Save the offset."""
        self._write(json.dumps(asdict(offset)).encode())
