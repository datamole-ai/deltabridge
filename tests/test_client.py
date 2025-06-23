import tempfile
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from dtml.delta.client import DeltaTableClient


@pytest.fixture
def sample_df():
    return pl.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})


@pytest.fixture
def temp_delta_table_uri(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        write_deltalake(tmpdir, sample_df, partition_by=['id'])
        yield tmpdir


def test_load_as_delta(temp_delta_table_uri):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    loaded_delta_table = delta_table_client.load_as_delta()
    assert isinstance(loaded_delta_table, DeltaTable)
    assert Path(loaded_delta_table.table_uri) == Path(temp_delta_table_uri)


def test_load_as_polars(temp_delta_table_uri, sample_df):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    assert_frame_equal(
        # Sort both frames to ensure the order is the same
        delta_table_client.load_as_polars().sort('id').collect(),
        sample_df,
    )


def test_load_as_polars_with_partition(temp_delta_table_uri, sample_df):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    selected_id = 1
    loaded_df = (
        delta_table_client.load_as_polars(
            partitioned_column_name='id',
            partitioned_column_value=str(selected_id),
        )
        .sort('id')
        .collect()
    )
    correct_partition_df = sample_df.filter(pl.col('id') == selected_id)
    assert_frame_equal(
        loaded_df,
        correct_partition_df,
    )
