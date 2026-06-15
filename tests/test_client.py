import json
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.exceptions import DeltaProtocolError
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from deltabridge.client import DeltaTableClient


@pytest.fixture
def sample_df():
    return pl.DataFrame(
        {
            'id': [1, 2, 2, 3],
            'value': ['a', 'b', 'c', 'c'],
            'datetime': [
                datetime.now(),
                datetime.now(),
                datetime.now(),
                datetime.now(),
            ],
        },
        schema={'id': pl.Int64, 'value': pl.Utf8, 'datetime': pl.Datetime},
    )


@pytest.fixture
def temp_delta_table_uri(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        write_deltalake(tmpdir, sample_df, partition_by=['id', 'value'])
        yield tmpdir


def test_load_as_delta(temp_delta_table_uri):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    loaded_delta_table = delta_table_client.load_as_delta()
    assert isinstance(loaded_delta_table, DeltaTable)
    # delta-rs started prefixing file:// to the table URI in an unknwon version
    # removing the prefix ensures compatibility with both old and new versions
    # samefile() compares by inode, so symlinked temp dirs
    # (e.g. macOS /var -> /private/var) still match instead of needing
    # identical textual paths.
    assert Path(loaded_delta_table.table_uri.replace('file:', '')).samefile(
        temp_delta_table_uri
    )


def test_load_as_polars(temp_delta_table_uri, sample_df):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    assert_frame_equal(
        # Sort both frames to ensure the order is the same
        delta_table_client.load_as_polars().sort('id', 'value').collect(),
        sample_df,
    )


@pytest.fixture
def deletion_vector_table_uri():
    # delta-rs cannot *write* deletion-vector tables, so we generate a
    # normal table and rewrite its protocol action to advertise the
    # `deletionVectors` reader feature (reader v3). This reproduces the
    # protocol gate that modern Databricks (Unity Catalog) tables hit:
    # the legacy pyarrow scan path rejects such tables, the native path
    # accepts them.
    df = pl.DataFrame({'id': [1, 2, 3, 4], 'value': ['a', 'b', 'c', 'd']})
    with tempfile.TemporaryDirectory() as tmpdir:
        write_deltalake(tmpdir, df)
        log_dir = Path(tmpdir) / '_delta_log'
        first_commit = sorted(log_dir.glob('*.json'))[0]
        patched = []
        for line in first_commit.read_text().splitlines():
            action = json.loads(line)
            if 'protocol' in action:
                action['protocol'] = {
                    'minReaderVersion': 3,
                    'minWriterVersion': 7,
                    'readerFeatures': ['deletionVectors'],
                    'writerFeatures': ['deletionVectors'],
                }
            patched.append(json.dumps(action))
        first_commit.write_text('\n'.join(patched) + '\n')
        yield tmpdir


def test_load_as_polars_reads_deletion_vector_table(
    deletion_vector_table_uri,
):
    # The native path used by load_as_polars reads the deletion-vector
    # table that the legacy pyarrow scan path rejects with a
    # DeltaProtocolError. This guards against regressing to use_pyarrow.
    delta_table_client = DeltaTableClient(
        table_uri=deletion_vector_table_uri,
        storage_options_fn=lambda: {},
    )
    loaded_df = delta_table_client.load_as_polars().collect()
    assert loaded_df.height == 4


@pytest.fixture
def column_mapping_table_uri():
    # delta-rs cannot *write* column-mapping tables, so we write the parquet
    # under physical column names and rewrite the log to map logical names
    # (`id`, `value`) to them, enabling `columnMapping` (reader v3). This is a
    # faithful column-mapping table: physical names differ from logical names.
    df = pl.DataFrame({'col-aaaa': [1, 2, 3], 'col-bbbb': ['a', 'b', 'c']})
    schema = json.dumps(
        {
            'type': 'struct',
            'fields': [
                {
                    'name': 'id',
                    'type': 'long',
                    'nullable': True,
                    'metadata': {
                        'delta.columnMapping.id': 1,
                        'delta.columnMapping.physicalName': 'col-aaaa',
                    },
                },
                {
                    'name': 'value',
                    'type': 'string',
                    'nullable': True,
                    'metadata': {
                        'delta.columnMapping.id': 2,
                        'delta.columnMapping.physicalName': 'col-bbbb',
                    },
                },
            ],
        }
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        write_deltalake(tmpdir, df)
        log_dir = Path(tmpdir) / '_delta_log'
        first_commit = sorted(log_dir.glob('*.json'))[0]
        patched = []
        for line in first_commit.read_text().splitlines():
            action = json.loads(line)
            if 'protocol' in action:
                action['protocol'] = {
                    'minReaderVersion': 3,
                    'minWriterVersion': 7,
                    'readerFeatures': ['columnMapping'],
                    'writerFeatures': ['columnMapping'],
                }
            if 'metaData' in action:
                action['metaData']['schemaString'] = schema
                action['metaData'].setdefault('configuration', {}).update(
                    {
                        'delta.columnMapping.mode': 'name',
                        'delta.columnMapping.maxColumnId': '2',
                    }
                )
            patched.append(json.dumps(action))
        first_commit.write_text('\n'.join(patched) + '\n')
        yield tmpdir


def test_scan_delta_yields_null_columns_without_guard(
    column_mapping_table_uri,
):
    # Justifies the guard: passing a DeltaTable to scan_delta bypasses polars'
    # protocol check, so a column-mapping table is read as all-null columns
    # (the logical names are absent from the physically-named parquet).
    loaded_df = pl.scan_delta(DeltaTable(column_mapping_table_uri)).collect()
    assert loaded_df.height == 3
    assert all(
        loaded_df[c].null_count() == loaded_df.height
        for c in loaded_df.columns
    )


def test_load_as_polars_rejects_unsupported_reader_feature(
    column_mapping_table_uri,
):
    # The guard turns the silent-null read above into a clear error.
    delta_table_client = DeltaTableClient(
        table_uri=column_mapping_table_uri,
        storage_options_fn=lambda: {},
    )
    with pytest.raises(DeltaProtocolError, match='columnMapping'):
        delta_table_client.load_as_polars()


def test_storage_options_rotation_rebuilds_table(temp_delta_table_uri, mocker):
    options = {'token': 'old'}
    client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: dict(options),
    )
    new_table = mocker.Mock(spec=DeltaTable)
    mocker.patch.object(client, '_create_delta_table', return_value=new_table)

    options['token'] = 'new'
    result = client.load_as_delta()

    assert result is new_table
    assert client._delta_table is new_table
    assert client._storage_options == {'token': 'new'}
    # The rebuild must use the REFRESHED options, not the stale ones.
    client._create_delta_table.assert_called_once_with({'token': 'new'})


def test_storage_options_rotation_failed_rebuild(temp_delta_table_uri, mocker):
    options = {'token': 'old'}
    client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: dict(options),
    )
    original_table = client._delta_table
    new_table = mocker.Mock(spec=DeltaTable)
    mocker.patch.object(
        client,
        '_create_delta_table',
        side_effect=[ConnectionError('transient'), new_table],
    )

    options['token'] = 'new'

    # First call: rebuild fails -> error propagates, state unchanged
    with pytest.raises(ConnectionError):
        client.load_as_delta()
    assert client._delta_table is original_table
    assert client._storage_options == {'token': 'old'}

    # Second call: rebuild succeeds -> new table committed
    result = client.load_as_delta()
    assert result is new_table
    assert client._storage_options == {'token': 'new'}
    assert client._create_delta_table.call_count == 2
