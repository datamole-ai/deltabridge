from pathlib import Path

from deltabridge.base_checkpoint import BaseCheckpoint


class LocalCheckpoint(BaseCheckpoint):
    """Checkpoint stored in a local file.

    Parameters
    ----------
    path: str
        Path of the file.
    """

    def __init__(self, path: str):
        self._path = Path(path)

    def _read(self) -> bytes | None:
        try:
            return self._path.read_bytes()
        except FileNotFoundError:
            return None

    def _write(self, data: bytes) -> None:
        # Replace the file atomically, so that a crash leaves no partial file
        tmp_path = self._path.with_name(f'{self._path.name}.tmp')
        tmp_path.write_bytes(data)
        tmp_path.replace(self._path)
