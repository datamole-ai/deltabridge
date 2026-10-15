import json
import shutil
from pathlib import Path
from urllib.parse import quote, unquote

import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

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
    stream.process_batch(frames.append)
    return frames


def read_ids(stream):
    """Sorted IDs of the rows of the next batch."""
    return sorted(id_ for df in next_batch(stream) for id_ in df['id'])


@pytest.fixture
def stream(table_uri):
    write(table_uri, [1])
    stream = load_stream(table_uri, StartingOffset.LATEST)
    assert next_batch(stream) == []
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
    assert next_batch(stream) == []


def test_max_versions(table_uri):
    write(table_uri, [1])
    stream = load_stream(table_uri, StartingOffset.LATEST, max_versions=2)
    assert next_batch(stream) == []
    for ids in [[2], [3, 4], [5]]:
        write(table_uri, ids)

    assert read_ids(stream) == [2, 3, 4]
    assert read_ids(stream) == [5]
    assert next_batch(stream) == []


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


@pytest.mark.parametrize('pending', [False, True])
def test_optimize(table_uri, stream, pending):
    write(table_uri, [2])
    if not pending:
        assert read_ids(stream) == [2]
    DeltaTable(table_uri).optimize.compact()

    assert read_ids(stream) == ([2] if pending else [])
    assert stream._checkpoint.load().version == 2
    assert next_batch(stream) == []
    write(table_uri, [3])
    assert read_ids(stream) == [3]


def test_process_batch(table_uri):
    write(table_uri, [1])
    stream = load_stream(table_uri)
    batches = []

    def func(df):
        batches.append(df['id'].to_list())
        write(table_uri, [2])

    assert stream.process_batch(func)
    assert batches == [[1]]
    assert read_ids(stream) == [2]
    assert not stream.process_batch(batches.append)


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
    assert next_batch(stream) == []


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
    assert stream.process_batch(frames.append)
    assert [df['id'].to_list() for df in frames] == [[2], [2]]
    assert stream._checkpoint.load().version == 1


@pytest.mark.parametrize('missing', ['data', 'log'])
def test_read_failure_retries_same_stream(table_uri, stream, missing):
    write(table_uri, [2])
    log = Path(table_uri) / '_delta_log' / '00000000000000000001.json'
    if missing == 'data':
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        path = Path(table_uri) / next(
            a['add']['path'] for a in actions if 'add' in a
        )
    else:
        # The current snapshot remains readable without this individual log.
        DeltaTable(table_uri).create_checkpoint()
        path = log
    backup = path.with_suffix('.bak')
    path.rename(backup)
    with pytest.raises((OSError, pl.exceptions.ComputeError)):
        next_batch(stream)
    assert stream._checkpoint.load().version == 0
    backup.rename(path)
    assert read_ids(stream) == [2]


@pytest.mark.parametrize('max_versions', [0, -1])
def test_invalid_max_versions(table_uri, max_versions):
    write(table_uri, [1])
    with pytest.raises(ValueError, match='must be positive'):
        load_stream(table_uri, max_versions=max_versions)


@pytest.mark.parametrize(
    ('table_id', 'version', 'error'),
    [
        ('other-table-id', 0, 'another table'),
        (None, -1, 'out of range'),
        (None, 2, 'out of range'),
    ],
)
def test_invalid_checkpoint(table_uri, stream, table_id, version, error):
    checkpoint = stream._checkpoint
    checkpoint.save(
        StreamOffset(table_id or checkpoint.load().table_id, version)
    )
    with pytest.raises(ValueError, match=error):
        next_batch(load_stream(table_uri))


@pytest.mark.parametrize('writes', [1, 3])
def test_recreated_table(table_uri, writes):
    write(table_uri, [1])
    write(table_uri, [2])
    stream = load_stream(table_uri, StartingOffset.LATEST)
    next_batch(stream)
    shutil.rmtree(table_uri)
    for i in range(writes):
        write(table_uri, [i + 100])

    with pytest.raises(ValueError, match='another table'):
        next_batch(stream)
    assert stream._checkpoint.load().version == 1


@pytest.mark.parametrize('restart', [False, True])
def test_schema_change_beyond_batch_limit(table_uri, restart):
    write(table_uri, [1])
    stream = load_stream(table_uri, StartingOffset.LATEST, max_versions=1)
    next_batch(stream)
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
        next_batch(stream)
    assert stream._checkpoint.load().version == 0


def test_metadata_change(table_uri, stream):
    DeltaTable(table_uri).alter.set_table_properties(
        {'delta.appendOnly': 'true'}
    )
    with pytest.raises(ValueError, match='changes table metadata'):
        next_batch(stream)


@pytest.mark.parametrize('absolute', [False, True])
def test_encoded_file_paths(table_uri, stream, absolute):
    write(table_uri, [2])
    log = Path(table_uri) / '_delta_log' / '00000000000000000001.json'
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if 'add' in action:
            add = action['add']
            path = Path(table_uri) / unquote(add['path'])
            target = path.with_name('space %25 [1].parquet')
            path.rename(target)
            add['path'] = target.as_uri() if absolute else quote(target.name)
    log.write_text('\n'.join(json.dumps(a) for a in actions) + '\n')
    assert read_ids(stream) == [2]


@pytest.mark.parametrize(
    'options',
    [
        {'partition_by': ['id']},
        {'configuration': {'delta.columnMapping.mode': 'name'}},
        {'configuration': {'delta.enableDeletionVectors': 'true'}},
    ],
)
def test_unsupported_table(table_uri, options):
    write(table_uri, [1], **options)
    with pytest.raises(NotImplementedError, match='reader version 1'):
        next_batch(load_stream(table_uri))


def test_idle_poll_skips_commit_and_data_reads(stream, mocker):
    read_versions = mocker.spy(stream, '_read_versions')
    scan = mocker.spy(pl, 'scan_parquet')
    assert not stream.process_batch(lambda df: pytest.fail('Unexpected batch'))
    read_versions.assert_not_called()
    scan.assert_not_called()
