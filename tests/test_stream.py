import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Event

import polars as pl
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, Field, TableFeatures
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from deltabridge.client import DeltaTableClient
from deltabridge.local.checkpoint import LocalCheckpoint


@pytest.fixture
def table_uri(tmp_path):
    return str(tmp_path / 'table')


def write(table_uri, ids):
    write_deltalake(table_uri, pl.DataFrame({'id': ids}), mode='append')


def overwrite(table_uri, ids):
    df = pl.DataFrame({'id': ids})
    # An empty list also removes partition columns, None keeps them
    write_deltalake(
        table_uri,
        df,
        mode='overwrite',
        schema_mode='overwrite',
        partition_by=[],
    )


def partition(table_uri):
    # Delta tables need a column outside the partition columns
    df = pl.DataFrame({'id': [3], 'name': ['c']})
    write_deltalake(
        table_uri,
        df,
        mode='overwrite',
        schema_mode='overwrite',
        partition_by=['id'],
    )


def add_column(table_uri):
    DeltaTable(table_uri).alter.add_columns(Field('name', 'string'))


def load_stream(table_uri, max_versions=None):
    client = DeltaTableClient(table_uri, storage_options_fn=lambda: {})
    checkpoint = LocalCheckpoint(f'{table_uri}.json')
    return client.load_as_stream(checkpoint, max_versions)


def read_ids(stream):
    ids = []
    stream.process_batch(lambda df: ids.extend(df['id'].to_list()))
    return sorted(ids)


def edit_adds(table_uri, version, edit):
    """Apply a function to the add actions of a version in the Delta log."""
    log = Path(table_uri, '_delta_log', f'{version:020}.json')
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if 'add' in action:
            edit(action['add'])
    log.write_text(''.join(json.dumps(action) + '\n' for action in actions))


def commit_protocol(table_uri, *features):
    """Commit a protocol by hand, with reader version 2 if no features."""
    version = DeltaTable(table_uri).version() + 1
    protocol = {'minReaderVersion': 2, 'minWriterVersion': 7}
    protocol['writerFeatures'] = list(features)
    if features:
        protocol |= {'minReaderVersion': 3, 'readerFeatures': list(features)}
    log = Path(table_uri, '_delta_log', f'{version:020}.json')
    log.write_text(json.dumps({'protocol': protocol}))


@pytest.fixture
def stream(table_uri):
    write(table_uri, [1])
    stream = load_stream(table_uri)
    assert read_ids(stream) == []
    return stream


def test_start(table_uri):
    write(table_uri, [1])
    # Deletion vectors are enabled, but no file has one
    commit_protocol(table_uri, 'deletionVectors')
    write(table_uri, [2])
    stream = load_stream(table_uri)

    assert read_ids(stream) == []
    write(table_uri, [3])
    assert read_ids(stream) == [3]
    assert stream.process_batch(pytest.fail) is None


def test_max_versions(table_uri):
    write(table_uri, [1])
    with pytest.raises(ValueError, match='positive'):
        load_stream(table_uri, max_versions=0)
    stream = load_stream(table_uri, max_versions=2)
    assert read_ids(stream) == []
    for ids in [[2], [3, 4], [5]]:
        write(table_uri, ids)

    assert read_ids(stream) == [2, 3, 4]
    assert read_ids(stream) == [5]
    assert read_ids(stream) == []


@pytest.mark.parametrize('tz', [None, 'UTC'], ids=['ntz', 'int96'])
def test_timestamps(table_uri, tz):
    times = [
        datetime(1500, 1, 1),
        datetime(2024, 5, 6, 7, 8, 9, 123456),
        datetime(9999, 12, 31, 23, 59, 59, 999999),
    ]
    df = pl.DataFrame({'id': [1, 2, 3], 'ts': times})
    df = df.with_columns(pl.col('ts').dt.replace_time_zone(tz))
    # Naive datetimes create a TIMESTAMP_NTZ column
    write_deltalake(table_uri, df.clear())
    stream = load_stream(table_uri)
    stream.process_batch(pytest.fail)
    write_deltalake(table_uri, df, mode='append')
    # Spark writes TIMESTAMP as INT96 without the Arrow schema. Materialized
    # row tracking columns are not part of the Delta schema.
    (path,) = DeltaTable(table_uri).file_uris()
    pq.write_table(
        df.with_columns(pl.lit(42).alias('_row-id-col-test')).to_arrow(),
        path,
        use_deprecated_int96_timestamps=tz is not None,
        store_schema=False,
    )
    size = Path(path).stat().st_size
    edit_adds(table_uri, 1, lambda add: add.update(size=size))
    frames = []
    stream.process_batch(frames.append)
    assert_frame_equal(frames[0], df)


@pytest.mark.parametrize(
    'features',
    [[], ['columnMapping'], ['deletionVectors', 'typeWidening']],
    ids=['reader version 2', 'columnMapping', 'typeWidening'],
)
def test_unsupported_reader_feature_fails(table_uri, features):
    write(table_uri, [1])
    commit_protocol(table_uri, *features)

    with pytest.raises(NotImplementedError, match='reader version'):
        read_ids(load_stream(table_uri))


def test_absolute_path_fails(table_uri, stream):
    write(table_uri, [2])
    edit_adds(
        table_uri,
        1,
        lambda add: add.update(path=f'file://{table_uri}/{add["path"]}'),
    )

    with pytest.raises(NotImplementedError, match='relative'):
        read_ids(stream)


@pytest.mark.parametrize(
    'change',
    [
        lambda table: table.alter.set_table_properties(
            {'delta.logRetentionDuration': 'interval 60 days'}
        ),
        lambda table: table.alter.set_column_metadata(
            'id', {'comment': 'Row id'}
        ),
        lambda table: table.optimize.compact(),
        # Writer-only feature, the reader version stays 1
        lambda table: table.alter.add_feature(
            TableFeatures.AppendOnly, allow_protocol_versions_increase=True
        ),
        lambda table: table.alter.add_feature(
            TableFeatures.DeletionVectors,
            allow_protocol_versions_increase=True,
        ),
        lambda table: table.alter.add_feature(
            TableFeatures.V2Checkpoint, allow_protocol_versions_increase=True
        ),
    ],
    ids=[
        'properties',
        'comment',
        'optimize',
        'appendOnly',
        'deletionVectors',
        'v2Checkpoint',
    ],
)
def test_no_data_change(table_uri, stream, change):
    write(table_uri, [2])
    change(DeltaTable(table_uri))
    # The checkpoint of a v2Checkpoint table has the V2 format
    DeltaTable(table_uri).create_checkpoint()
    write(table_uri, [3])

    assert read_ids(stream) == [2, 3]
    assert read_ids(stream) == []


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize(
    'change',
    [add_column, lambda uri: overwrite(uri, ['2'])],
    ids=['new column', 'changed type'],
)
def test_schema_change_fails(table_uri, stream, change, restart):
    write(table_uri, [2])
    change(table_uri)
    if restart:
        # The checkpoint version's schema applies beyond the batch limit
        stream = load_stream(table_uri, max_versions=1)

    with pytest.raises(ValueError, match='schema changed'):
        read_ids(stream)
    assert stream._checkpoint.load().version == 0


@pytest.mark.parametrize(
    ('changes', 'error', 'text'),
    [
        (
            [lambda uri: DeltaTable(uri).delete('id = 1')],
            ValueError,
            'deletes or updates rows',
        ),
        # The current table is supported again
        (
            [add_column, lambda uri: overwrite(uri, [3])],
            ValueError,
            'changes column names or types',
        ),
        (
            [partition, lambda uri: overwrite(uri, [3])],
            NotImplementedError,
            'makes the table partitioned',
        ),
        (
            [
                lambda uri: commit_protocol(uri, 'typeWidening'),
                lambda uri: commit_protocol(uri, 'deletionVectors'),
            ],
            NotImplementedError,
            'requires an unsupported reader protocol',
        ),
    ],
    ids=['delete', 'schema', 'partition', 'protocol'],
)
def test_version_change_fails(table_uri, stream, changes, error, text):
    write(table_uri, [2])
    for change in changes:
        change(table_uri)

    # The versions before the change are delivered first
    assert read_ids(stream) == [2]
    for _ in range(2):
        with pytest.raises(error, match=f'Version 2 {text}'):
            read_ids(stream)


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('failing', ['callback', 'checkpoint'])
def test_failure_retries(table_uri, stream, mocker, failing, restart):
    write(table_uri, [2])
    func = mocker.Mock(side_effect=OSError if failing == 'callback' else None)
    if failing == 'checkpoint':
        mocker.patch.object(stream._checkpoint, 'save', side_effect=OSError)

    with pytest.raises(OSError):
        stream.process_batch(func)
    # Delivery is at-least-once
    func.assert_called_once()
    assert stream._checkpoint.load().version == 0
    mocker.stopall()
    if restart:
        stream = load_stream(table_uri)
    assert read_ids(stream) == [2]
    assert read_ids(stream) == []


def test_recreated_table(table_uri, stream):
    shutil.rmtree(table_uri)
    write(table_uri, [100])

    with pytest.raises(ValueError, match='another table'):
        read_ids(stream)
    assert stream._checkpoint.load().version == 0


def test_overlapping_call_returns(table_uri, stream):
    write(table_uri, [2])
    ids = []
    blocked, release = Event(), Event()

    def block(df):
        ids.extend(df['id'].to_list())
        blocked.set()
        release.wait(timeout=10)

    with ThreadPoolExecutor(1) as pool:
        running = pool.submit(stream.process_batch, block)
        assert blocked.wait(timeout=10)
        write(table_uri, [3])
        # Reading now would deliver row 2 again before the offset is saved
        assert stream.process_batch(pytest.fail) is None
        release.set()
        running.result(timeout=10)
    assert ids == [2]
    assert stream._checkpoint.load().version == 1
    # The next call reads the rows which the overlapping call skipped
    assert read_ids(stream) == [3]
    assert stream._checkpoint.load().version == 2
