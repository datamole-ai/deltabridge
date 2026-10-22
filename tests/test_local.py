from deltabridge.base_checkpoint import StreamOffset
from deltabridge.local.checkpoint import LocalCheckpoint


def test_local_checkpoint_save_and_load(tmp_path):
    path = tmp_path / 'checkpoint.json'
    checkpoint = LocalCheckpoint(str(path))
    offset = StreamOffset('table-id', 42)

    assert checkpoint.load() is None
    checkpoint.save(offset)

    assert checkpoint.load() == offset
    assert list(tmp_path.iterdir()) == [path], 'Temporary file should be gone'
