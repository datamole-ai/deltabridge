import threading

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from deltabridge.base_checkpoint import StreamOffset
from deltabridge.client import DeltaTableClient
from deltabridge.local.checkpoint import LocalCheckpoint
from deltabridge.stream import StartingOffset


@pytest.fixture
def table_uri(tmp_path):
    return str(tmp_path / 'table')


def write(table_uri, ids, **kwargs):
    df = pl.DataFrame(
        {'id': ids, 'body': [f'row {i}' for i in ids]},
        schema={'id': pl.Int64, 'body': pl.Utf8},
    )
    write_deltalake(table_uri, df, mode='append', **kwargs)


def load_stream(table_uri, start=StartingOffset.EARLIEST, max_versions=None):
    client = DeltaTableClient(
        table_uri=table_uri, storage_options_fn=lambda: {}
    )
    checkpoint = LocalCheckpoint(f'{table_uri}.json')
    return client.load_as_stream(checkpoint, start, max_versions)


def next_batch(stream):
    """Frames passed to the function by the next batch."""
    frames = []
    stream._process_batch(frames.append)
    return frames


def read_ids(stream):
    """Sorted IDs of the rows of the next batch."""
    return sorted(id_ for df in next_batch(stream) for id_ in df['id'])


def test_start_earliest(table_uri):
    write(table_uri, [1, 2])
    write(table_uri, [3])
    stream = load_stream(table_uri)

    assert read_ids(stream) == [1, 2, 3]
    assert next_batch(stream) == []


def test_start_latest(table_uri):
    write(table_uri, [1, 2])
    stream = load_stream(table_uri, StartingOffset.LATEST)

    assert next_batch(stream) == []
    write(table_uri, [3, 4])
    assert read_ids(stream) == [3, 4]


def test_max_versions(table_uri):
    write(table_uri, [1])
    stream = load_stream(table_uri, StartingOffset.LATEST, max_versions=2)
    assert next_batch(stream) == []
    for ids in [[2], [3, 4], [5]]:
        write(table_uri, ids)

    assert read_ids(stream) == [2, 3, 4]
    assert read_ids(stream) == [5]
    assert next_batch(stream) == []


def test_offset_saved_after_func(table_uri):
    write(table_uri, [1])
    assert next_batch(load_stream(table_uri, StartingOffset.LATEST)) == []
    write(table_uri, [2, 3])

    def fail(df):
        raise RuntimeError('export failed')

    with pytest.raises(RuntimeError):
        load_stream(table_uri)._process_batch(fail)
    # The batch is read again, as its offset is not saved
    assert read_ids(load_stream(table_uri)) == [2, 3]
    assert next_batch(load_stream(table_uri)) == []


def test_other_table_id(table_uri):
    write(table_uri, [1])
    LocalCheckpoint(f'{table_uri}.json').save(
        StreamOffset('other-table-id', 0)
    )

    with pytest.raises(ValueError, match='another table'):
        next_batch(load_stream(table_uri))


def test_column_mapping(table_uri):
    def table(ids):
        return pa.table(
            {
                'id': pa.array(ids, pa.int64()),
                'body': [f'row {i}' for i in ids],
                'attributes': pa.array(
                    [[('key', f'value {i}')] for i in ids],
                    pa.map_(pa.string(), pa.string()),
                ),
            }
        )

    write_deltalake(
        table_uri,
        table([1]),
        configuration={
            'delta.columnMapping.mode': 'name',
            'delta.minReaderVersion': '3',
            'delta.minWriterVersion': '7',
        },
    )
    stream = load_stream(table_uri, StartingOffset.LATEST)
    assert next_batch(stream) == []
    write_deltalake(table_uri, table([2, 3]), mode='append')

    [df] = next_batch(stream)
    assert_frame_equal(df.sort('id'), pl.DataFrame(table([2, 3])))


def test_delete_fails(table_uri):
    write(table_uri, [1, 2])
    stream = load_stream(table_uri, StartingOffset.LATEST)
    assert next_batch(stream) == []
    write(table_uri, [3])
    DeltaTable(table_uri).delete('id = 1')

    # The versions before the delete are delivered first
    assert read_ids(stream) == [3]
    for _ in range(2):
        with pytest.raises(ValueError, match='Version 2 deletes'):
            next_batch(stream)


def test_optimize_skipped(table_uri):
    write(table_uri, [1])
    write(table_uri, [2])
    stream = load_stream(table_uri, StartingOffset.LATEST)
    assert next_batch(stream) == []
    DeltaTable(table_uri).optimize.compact()

    assert next_batch(stream) == []
    write(table_uri, [3])
    assert read_ids(stream) == [3]


def test_for_each_batch(table_uri):
    write(table_uri, [1])
    stop = threading.Event()
    batches = []

    def func(df):
        batches.append(df['id'].to_list())
        if len(batches) == 1:
            write(table_uri, [2])
        else:
            stop.set()

    load_stream(table_uri).for_each_batch(func, interval=0, stop=stop)

    assert batches == [[1], [2]]
