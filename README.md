# deltabridge
Thin Python wrapper for reading Delta tables from object storage (currently 
Azure Blob Storage) or a local filesystem, with low and stable latency. 
Optimized for repeated reads from long-running Python services.
A typical use case is exposing the final products of a data pipeline 
via a REST API, where request latency should stay predictable.

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

# A plain .filter() on the LazyFrame also works, but is less efficient on large
# partitioned tables on object storage (see "Partition pruning on large tables"
# below). It is, however, the only option for deletion-vector tables.
table_df = (
    table_client.load_as_polars()
    .filter(
        pl.col('country').is_in(['CZ', 'SK']) & (pl.col('year') == 2024)
    )
    .collect()
)
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

### Partition pruning on large tables

By default `load_as_polars()` uses Polars' native Delta reader, which reads
deletion-vector tables (e.g. modern Databricks/Unity Catalog tables) but does
not prune partitions efficiently ([pola-rs/polars#20998](https://github.com/pola-rs/polars/issues/20998)):
it skips the *data* of non-matching partitions, yet still handles per-file
metadata for every partition when building the scan. On object storage that
per-file step is a network request, so reading a few partitions of a table with
*many* partitions can become very slow.

For that case, pass `partition_filter` to read via pyarrow, which pushes the
partition predicate into delta-rs's file enumeration so non-matching partitions
are never touched:

```python
from deltabridge import PartitionFilterOperator

# Fast on tables with many partitions: only the matching partitions are listed.
df = table_client.load_as_polars(
    partition_filter=[
        ('country', PartitionFilterOperator.EQUAL, 'CZ'),
        ('year', '=', '2024'),
    ]
).collect()
```

Trade-off: the pyarrow reader **cannot read deletion-vector tables** and raises
`DeltaProtocolError` for them, so `partition_filter` is not usable on such a
table. Read those natively instead and filter the returned `LazyFrame` with
Polars expressions:

```python
df = table_client.load_as_polars().filter(pl.col('country') == 'CZ').collect()
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

deltabridge is **read-focused**: it provides no write API, and its optimizations don't apply to writes. This is deliberate:
* write use cases are more varied and harder to abstract well - appends, overwrites, merges/upserts, schema evolution and concurrency control all behave differently
* writes are typically handled upstream by the systems that produce the tables (often Spark/PySpark pipelines)

Writing is still possible: `load_as_delta()` returns a [`deltalake.DeltaTable`](https://delta-io.github.io/delta-rs/) with deltabridge's auth already configured, which you can pass to `deltalake`'s write API:

```python
import deltalake

deltalake.write_deltalake(table_client.load_as_delta(), df, mode='append')
```

## Cloud provider support
Object storage support currently covers Azure Blob Storage (plus the local 
filesystem).
