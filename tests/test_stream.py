import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import polars as pl
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, Field, TableFeatures
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from deltabridge.client import DeltaTableClient
from deltabridge.local.checkpoint import LocalCheckpoint
from deltabridge.stream import StartingOffset

# Inline deletion vector of the rows 0 and 1
DELETION_VECTOR = {
    'storageType': 'i',
    'pathOrInlineDv': '^Bg9^0rr910000000000iXQKl0rr91000315c8Xg00031',
    'sizeInBytes': 36,
    'cardinality': 2,
}


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


def commit(table_uri, *actions):
    """Append a hand-written commit to the Delta log."""
    version = DeltaTable(table_uri).version() + 1
    log = Path(table_uri, '_delta_log', f'{version:020}.json')
    log.write_text(''.join(json.dumps(action) + '\n' for action in actions))


def protocol(*features):
    """Protocol action requiring reader version 3 and the features."""
    return {
        'protocol': {
            'minReaderVersion': 3,
            'minWriterVersion': 7,
            'readerFeatures': list(features),
            'writerFeatures': list(features),
        }
    }


def append(table_uri, ids, **add):
    """Append rows by hand; delta-rs cannot write some features."""
    path = Path(table_uri, f'{uuid4()}.parquet')
    pl.DataFrame({'id': ids}).write_parquet(path)
    add = {
        'path': path.name,
        'partitionValues': {},
        'size': path.stat().st_size,
        'modificationTime': 0,
        'dataChange': True,
        'stats': json.dumps({'numRecords': len(ids)}),
    } | add
    commit(table_uri, {'add': add})


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


@pytest.mark.parametrize('start', list(StartingOffset))
def test_timestamp_ntz(table_uri, start):
    times = [
        datetime(1500, 1, 1),
        datetime(2024, 5, 6, 7, 8, 9, 123456),
        datetime(9999, 12, 31, 23, 59, 59, 999999),
    ]
    df = pl.DataFrame({'id': [1, 2, 3], 'ts': times})
    # Naive datetimes create a TIMESTAMP_NTZ column
    write_deltalake(table_uri, df.clear())
    stream = load_stream(table_uri, start)
    if start == StartingOffset.LATEST:
        read_ids(stream)
    write_deltalake(table_uri, df, mode='append')
    frames = []
    stream.process_batch(frames.append)
    assert frames[0].schema['ts'] == pl.Datetime('us')
    assert_frame_equal(frames[0], df)


@pytest.mark.parametrize('start', list(StartingOffset))
@pytest.mark.parametrize(
    'feature',
    ['deletionVectors', 'v2Checkpoint', 'vacuumProtocolCheck', 'timestampNtz'],
)
def test_supported_reader_feature(table_uri, feature, start):
    write(table_uri, [1])
    commit(table_uri, protocol(feature))
    # The checkpoint of a v2Checkpoint table has the V2 format
    DeltaTable(table_uri).create_checkpoint()
    stream = load_stream(table_uri, start)

    assert read_ids(stream) == (
        [1] if start == StartingOffset.EARLIEST else []
    )
    append(table_uri, [2])
    assert read_ids(stream) == [2]


@pytest.mark.parametrize(
    'action',
    [
        {'protocol': {'minReaderVersion': 2, 'minWriterVersion': 5}},
        protocol('columnMapping'),
        protocol('deletionVectors', 'typeWidening'),
    ],
    ids=['reader version 2', 'columnMapping', 'typeWidening'],
)
def test_unsupported_reader_feature_fails(table_uri, action):
    write(table_uri, [1])
    commit(table_uri, action)

    with pytest.raises(NotImplementedError, match='reader version'):
        read_ids(load_stream(table_uri))


def test_unsupported_reader_feature_in_version_fails(table_uri, stream):
    write(table_uri, [2])
    commit(table_uri, protocol('typeWidening'))
    # Dropping the feature again leaves a supported current protocol
    commit(table_uri, protocol('deletionVectors'))

    assert read_ids(stream) == [2]
    with pytest.raises(
        ValueError,
        match='Version 2 changes the protocol to an unsupported reader',
    ):
        read_ids(stream)


def test_earliest_with_deletion_vectors_fails(table_uri):
    write(table_uri, [1])
    commit(table_uri, protocol('deletionVectors'))
    append(table_uri, [2, 3, 4], deletionVector=DELETION_VECTOR)

    with pytest.raises(NotImplementedError, match='deletion vectors'):
        read_ids(load_stream(table_uri))
    # LATEST skips the rows already in the table
    assert read_ids(load_stream(table_uri, StartingOffset.LATEST)) == []


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
        with pytest.raises(
            ValueError, match='Version 2 deletes or updates rows'
        ):
            read_ids(stream)


@pytest.mark.parametrize(
    'alter',
    [
        lambda alter: alter.set_table_properties(
            {'delta.logRetentionDuration': 'interval 60 days'}
        ),
        lambda alter: alter.set_column_metadata('id', {'comment': 'Row id'}),
        # Writer-only feature, the reader version stays 1
        lambda alter: alter.add_feature(
            TableFeatures.AppendOnly, allow_protocol_versions_increase=True
        ),
        lambda alter: alter.add_feature(
            TableFeatures.DeletionVectors,
            allow_protocol_versions_increase=True,
        ),
    ],
    ids=['properties', 'comment', 'writer feature', 'reader feature'],
)
def test_metadata_change(table_uri, stream, alter):
    write(table_uri, [2])
    alter(DeltaTable(table_uri).alter)
    write(table_uri, [3])

    assert read_ids(stream) == [2, 3]
    assert read_ids(stream) == []


@pytest.mark.parametrize(
    'change',
    [
        lambda uri: DeltaTable(uri).alter.add_columns(Field('name', 'string')),
        lambda uri: write_deltalake(
            uri,
            pl.DataFrame({'id': ['2']}),
            mode='overwrite',
            schema_mode='overwrite',
        ),
    ],
    ids=['new column', 'changed type'],
)
def test_schema_change_fails(table_uri, stream, change):
    write(table_uri, [2])
    change(table_uri)

    with pytest.raises(ValueError, match='schema changed'):
        read_ids(stream)
    assert stream._checkpoint.load().version == 0


def test_reverted_schema_change_fails(table_uri, stream):
    write(table_uri, [2])
    DeltaTable(table_uri).alter.add_columns(Field('name', 'string'))
    # Only the commit shows the change; the current schema matches again
    write_deltalake(
        table_uri,
        pl.DataFrame({'id': [3]}),
        mode='overwrite',
        schema_mode='overwrite',
    )

    assert read_ids(stream) == [2]
    for _ in range(2):
        with pytest.raises(
            ValueError, match='Version 2 changes the schema or partitioning'
        ):
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
