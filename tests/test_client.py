import logging
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from deltabridge import PartitionFilterOperator
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
    assert Path(loaded_delta_table.table_uri.replace('file:', '')) == Path(
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


@pytest.mark.parametrize(
    ('partition_filter', 'expected_filter'),
    [
        (
            [],
            lambda df: df,
        ),
        (
            [
                ('id', PartitionFilterOperator.EQUAL, '2'),
                ('value', '=', 'c'),  # Test with string literal
            ],
            lambda df: df.filter(
                (pl.col('id') == 2) & (pl.col('value') == 'c')
            ),
        ),
        (
            [('id', PartitionFilterOperator.IN, ['2', '3'])],
            lambda df: df.filter(pl.col('id').is_in([2, 3])),
        ),
        (
            [('value', PartitionFilterOperator.NOT_IN, ['b', 'c'])],
            lambda df: df.filter(~pl.col('value').is_in(['b', 'c'])),
        ),
        (
            [('id', PartitionFilterOperator.NOT_EQUAL, '2')],
            lambda df: df.filter(pl.col('id') != 2),
        ),
    ],
    ids=['empty', 'eq', 'in', 'not-in', 'not-eq'],
)
def test_load_as_polars_with_partition_operators(
    temp_delta_table_uri, sample_df, partition_filter, expected_filter
):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    loaded_df = (
        delta_table_client.load_as_polars(
            partition_filter=partition_filter,
        )
        .sort('id', 'value')
        .collect()
    )
    correct_partition_df = expected_filter(sample_df).sort('id', 'value')
    assert_frame_equal(
        loaded_df,
        correct_partition_df,
    )


def test_load_invalid_partition_filter(temp_delta_table_uri):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    with pytest.raises(ValueError):
        delta_table_client.load_as_polars(
            partition_filter=[('id', 'invalid', '2')],
        )


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


def test_storage_options_rotation_failed_rebuild_keeps_previous_state(
    temp_delta_table_uri, mocker, caplog
):
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

    # First call: rebuild fails -> old table served, state unchanged
    with caplog.at_level(logging.WARNING):
        result = client.load_as_delta()
    assert result is original_table
    assert client._storage_options == {'token': 'old'}
    assert 'retrying on next read' in caplog.text

    # Second call: rebuild succeeds -> new table committed
    result = client.load_as_delta()
    assert result is new_table
    assert client._storage_options == {'token': 'new'}
    assert client._create_delta_table.call_count == 2
