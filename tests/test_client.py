import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from polars.testing import assert_frame_equal

from dtml.delta.client import DeltaTableClient


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
    # delta-rs started prefixing file:// to the table URI in an unknown version
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


def test_load_as_polars_with_partition(temp_delta_table_uri, sample_df):
    delta_table_client = DeltaTableClient(
        table_uri=temp_delta_table_uri,
        storage_options_fn=lambda: {},
    )
    selected_id = 1
    selected_value = 'c'
    loaded_df = (
        delta_table_client.load_as_polars(
            partition_filter=[
                ('id', str(selected_id)),
                ('value', selected_value),
            ],
        )
        .sort('id')
        .collect()
    )
    correct_partition_df = sample_df.filter(
        (pl.col('id') == selected_id) & (pl.col('value') == selected_value)
    )
    assert_frame_equal(
        loaded_df,
        correct_partition_df,
    )
