# deltabridge
Thin Python wrapper for reading Delta tables from object storage (currently 
Azure Blob Storage) or a local filesystem, with low and stable latency. 
Optimized for repeated reads from long-running Python services.
A typical use case is exposing the final products of a data pipeline 
via a REST API, where request latency should stay predictable.

A typical use case is exposing final products of a data pipeline hosted on
Azure Databricks via a REST API, where request latency should stay predictable.

 > **Note**: The efficiency is achieved by using Rust-based loading of Delta tables through [delta-rs](https://github.com/delta-io/delta-rs)
 > and automatic incremental caching of Delta transaction logs.

## Installation

```bash
pip install deltabridge
```

Or, with [uv](https://docs.astral.sh/uv/):

```bash
uv add deltabridge
```

## Usage

### Examples

#### Azure

```python
import os

import deltalake
import polars as pl

from deltabridge import PartitionFilterOperator
from deltabridge.azure import AzureDeltaClient

azure_delta_client = AzureDeltaClient()
table_client = azure_delta_client.get_table_client(
    table_uri=os.environ['MY_TABLE_STORAGE_URI'],
)

# Get a DeltaTable instance
delta_table: deltalake.DeltaTable = table_client.load_as_delta()

# Load the data as a Polars LazyFrame
table_ldf: pl.LazyFrame = table_client.load_as_polars()
# Collect to a Polars DataFrame
table_df: pl.DataFrame = table_ldf.filter(pl.col('x') > 3).collect()

# For partitioned tables, push filters down to the partition columns so that
# only matching partitions are read from storage (avoiding a full scan).
# Multiple partition filters are combined using the logical AND operator.
table_df = table_client.load_as_polars(
    partition_filter=[
        ('country', PartitionFilterOperator.IN, ['CZ', 'SK']),
        ('year', PartitionFilterOperator.EQUAL, '2024'),
    ],
).collect()
```

#### Local filesystem

```python
import polars as pl

from deltabridge.local import LocalDeltaClient

MY_TABLE_PATH = '/tmp/my_table'

# Write a table to a local filesystem
pl.DataFrame({'x': [1, 2, 3]}).write_delta(
    target=MY_TABLE_PATH
)

local_delta_client = LocalDeltaClient()
table_client = local_delta_client.get_table_client(
    table_uri=MY_TABLE_PATH  # File path can be used as table URI
)

# Load the data as a Polars LazyFrame and collect it into a DataFrame
table_df = table_client.load_as_polars().collect()
print(table_df)
```

### Databricks tables
If your Delta tables are managed by Databricks (Unity Catalog), they are 
still stored as ordinary Delta tables in object storage. Deltabridge can read 
them directly from the storage, so you can access them without a Databricks 
SQL warehouse or cluster:
* Use the table's storage location (in Azure Blob Storage) as the table URI.
    * You can find it in the Databricks Catalog Explorer UI under *Details* of the table.
* The reading identity needs at least the *Storage Blob Data Reader* permission on the storage location (storage account/container).

## Writing to Delta tables

Writing to Delta tables is currently **not supported** by this package.
The main reasons are:
* it is much harder to support write use cases in general
* writes are typically handled upstream by the systems that produce the tables (often Spark/PySpark pipelines)

## Cloud provider support
Object storage support currently covers Azure Blob Storage (plus the local 
filesystem).
