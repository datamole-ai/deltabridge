import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from deltabridge.client import DeltaTableClient
from deltabridge.local.checkpoint import LocalCheckpoint
from deltabridge.stream import StartingOffset


@pytest.fixture
def table_uri(tmp_path):
    return str(tmp_path / 'table')


def write(table_uri, ids):
    write_deltalake(table_uri, pl.DataFrame({'id': ids}), mode='append')


def load_stream(table_uri, start=StartingOffset.EARLIEST, max_versions=None):
    client = DeltaTableClient(table_uri, storage_options_fn=lambda: {})
    checkpoint = LocalCheckpoint(f'{table_uri}.json')
    return client.load_as_stream(checkpoint, start, max_versions)


def read_ids(stream):
    ids = []
    stream.process_batch(lambda df: ids.extend(df['id'].to_list()))
    return sorted(ids)


def rewrite(table_uri, version, write_file):
    """Rewrite the files added by a version and log their new sizes."""
    log = Path(table_uri, '_delta_log', f'{version:020}.json')
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if 'add' in action:
            path = Path(table_uri, action['add']['path'])
            write_file(path)
            action['add']['size'] = path.stat().st_size
    log.write_text(''.join(json.dumps(action) + '\n' for action in actions))


@pytest.fixture
def stream(table_uri):
    write(table_uri, [1])
    stream = load_stream(table_uri, StartingOffset.LATEST)
    assert read_ids(stream) == []
    return stream


@pytest.mark.parametrize(
    ('start', 'expected'),
    [(StartingOffset.EARLIEST, [1, 2]), (StartingOffset.LATEST, [])],
)
def test_start(table_uri, start, expected):
    write(table_uri, [1])
    write(table_uri, [2])
    stream = load_stream(table_uri, start)

    assert read_ids(stream) == expected
    write(table_uri, [3])
    assert read_ids(stream) == [3]
    assert stream.process_batch(pytest.fail) is None


@pytest.mark.parametrize('start', list(StartingOffset))
def test_internal_columns(table_uri, start):
    write(table_uri, [1])
    stream = load_stream(table_uri, start)
    if start == StartingOffset.LATEST:
        read_ids(stream)
    write(table_uri, [2])
    # Materialized row tracking columns are not part of the Delta schema.
    df = pl.DataFrame({'id': [2], '_row-id-col-test': [42]})
    rewrite(table_uri, 1, df.write_parquet)
    frames = []
    stream.process_batch(frames.append)
    assert frames[0].columns == ['id']
    assert sorted(frames[0]['id']) == (
        [1, 2] if start == StartingOffset.EARLIEST else [2]
    )


@pytest.mark.parametrize('start', list(StartingOffset))
def test_int96_timestamps(table_uri, start):
    times = [
        datetime(1500, 1, 1, tzinfo=UTC),
        datetime(2024, 5, 6, 7, 8, 9, 123456, tzinfo=UTC),
        datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
    ]
    df = pl.DataFrame({'id': [1, 2, 3], 'ts': times})
    write_deltalake(table_uri, df.clear())
    stream = load_stream(table_uri, start)
    if start == StartingOffset.LATEST:
        read_ids(stream)
    write_deltalake(table_uri, df, mode='append')
    # Spark writes TIMESTAMP as INT96 without the Arrow schema.
    rewrite(
        table_uri,
        1,
        lambda path: pq.write_table(
            df.to_arrow(),
            path,
            use_deprecated_int96_timestamps=True,
            store_schema=False,
        ),
    )
    frames = []
    stream.process_batch(frames.append)
    assert_frame_equal(frames[0], df)


def test_absolute_path_fails(table_uri, stream):
    write(table_uri, [2])
    log = Path(table_uri, '_delta_log', f'{1:020}.json')
    log.write_text(
        log.read_text().replace('"path":"', f'"path":"file://{table_uri}/')
    )

    with pytest.raises(NotImplementedError, match='relative'):
        read_ids(stream)


def test_max_versions(table_uri):
    write(table_uri, [1])
    stream = load_stream(table_uri, StartingOffset.LATEST, max_versions=2)
    assert read_ids(stream) == []
    for ids in [[2], [3, 4], [5]]:
        write(table_uri, ids)

    assert read_ids(stream) == [2, 3, 4]
    assert read_ids(stream) == [5]
    assert read_ids(stream) == []


def test_delete_fails(table_uri, stream):
    write(table_uri, [3])
    DeltaTable(table_uri).delete('id = 1')

    # The versions before the delete are delivered first
    assert read_ids(stream) == [3]
    for _ in range(2):
        with pytest.raises(ValueError, match='Version 2 deletes'):
            read_ids(stream)


@pytest.mark.parametrize('pending', [False, True])
def test_optimize(table_uri, stream, pending):
    write(table_uri, [2])
    if not pending:
        assert read_ids(stream) == [2]
    DeltaTable(table_uri).optimize.compact()

    assert read_ids(stream) == ([2] if pending else [])
    assert stream._checkpoint.load().version == 2


@pytest.mark.parametrize('restart', [False, True])
def test_callback_failure(table_uri, stream, restart):
    write(table_uri, [2])

    def fail(df):
        raise RuntimeError('export failed')

    with pytest.raises(RuntimeError, match='export failed'):
        stream.process_batch(fail)
    assert stream._checkpoint.load().version == 0
    if restart:
        stream = load_stream(table_uri)
    assert read_ids(stream) == [2]
    assert read_ids(stream) == []


def test_checkpoint_failure_retries_same_stream(table_uri, stream, mocker):
    write(table_uri, [2])
    save = stream._checkpoint.save
    mocked_save = mocker.patch.object(
        stream._checkpoint, 'save', side_effect=OSError('save failed')
    )
    frames = []
    with pytest.raises(OSError, match='save failed'):
        stream.process_batch(frames.append)
    assert stream._checkpoint.load().version == 0
    mocked_save.side_effect = save
    assert stream.process_batch(frames.append) is None
    assert [df['id'].to_list() for df in frames] == [[2], [2]]
    assert stream._checkpoint.load().version == 1


@pytest.mark.parametrize('writes', [1, 3])
def test_recreated_table(table_uri, stream, writes):
    shutil.rmtree(table_uri)
    for i in range(writes):
        write(table_uri, [i + 100])

    with pytest.raises(ValueError, match='another table'):
        read_ids(stream)
    assert stream._checkpoint.load().version == 0


@pytest.mark.parametrize('restart', [False, True])
def test_schema_change_beyond_batch_limit(table_uri, restart):
    write(table_uri, [1])
    stream = load_stream(table_uri, StartingOffset.LATEST, max_versions=1)
    read_ids(stream)
    write(table_uri, [2])
    write_deltalake(
        table_uri,
        pl.DataFrame({'id': [3], 'different': ['replacement']}),
        mode='overwrite',
        schema_mode='overwrite',
    )
    if restart:
        stream = load_stream(table_uri, max_versions=1)
    with pytest.raises(ValueError, match='schema changed'):
        read_ids(stream)
    assert stream._checkpoint.load().version == 0
