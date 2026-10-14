from deltabridge.base_checkpoint import StreamOffset
from deltabridge.local import LocalDeltaClient


def test_local_checkpoint_save_and_load(tmp_path):
    path = tmp_path / 'checkpoint.json'
    checkpoint = LocalDeltaClient().get_checkpoint(str(path))
    offset = StreamOffset('table-id', 42)

    assert checkpoint.load() is None
    checkpoint.save(offset)

    assert checkpoint.load() == offset
    assert list(tmp_path.iterdir()) == [path], 'Temporary file should be gone'
