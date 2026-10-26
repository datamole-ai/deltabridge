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

### Access
A client gives a table client for each table, which [batch reads](#batch-reads) 
use. Reuse the table client across 
reads; each load refreshes the table as needed.

#### Storage access (own credentials)

##### Azure

```python
import os

from deltabridge.azure import AzureDeltaClient

azure_delta_client = AzureDeltaClient()
table_client = azure_delta_client.get_table_client(
    table_uri=os.environ['MY_TABLE_STORAGE_URI'],
)
```

##### Local filesystem

```python
import polars as pl

from deltabridge.local import LocalDeltaClient

MY_TABLE_PATH = '/tmp/my_table'

# Write a table to a local filesystem
pl.DataFrame({'x': [1, 2, 3]}).write_delta(target=MY_TABLE_PATH)

local_delta_client = LocalDeltaClient()
table_client = local_delta_client.get_table_client(
    table_uri=MY_TABLE_PATH  # File path can be used as table URI
)
```

#### Unity Catalog access (vended credentials)
`AzureDatabricksDeltaClient` reads the table by its full name with short-lived 
credentials [vended by Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/external-access/credential-vending) 
for that table instead of your own storage credentials, so access follows 
Unity Catalog grants:

```python
from deltabridge.azure import AzureDatabricksDeltaClient

databricks_client = AzureDatabricksDeltaClient(
    workspace_url='https://adb-1234567890.12.azuredatabricks.net',
)
table_client = databricks_client.get_table_client('catalog.schema.table')
```

* External data access must be enabled on the metastore.
* The reading identity needs the `EXTERNAL USE SCHEMA` privilege on the table's schema.
* Not supported: views, materialized views, streaming tables, shallow clones, tables shared via Delta Sharing, and tables with row filters or column masks. For Delta table features, see [Supported Delta features](#supported-delta-features).

Databricks tables can also be read with storage access:
* Use the table's storage location (in Azure Blob Storage) as the table URI.
    * You can find it in the Databricks Catalog Explorer UI under *Details* of the table.
* The reading identity needs at least the *Storage Blob Data Reader* permission on the storage location (storage account/container).
* Row filters and column masks are not applied.

### Batch reads
`load_as_delta()` and `load_as_polars()` read the latest version of the table:

```python
import deltalake
import polars as pl

from deltabridge import PartitionFilterOperator

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

## Supported Delta features

### Features that change the read protocol

| Feature | `load_as_polars()` | `load_as_delta().scan()` | `load_as_delta().to_pyarrow_dataset()`¹ |
| --- | --- | --- | --- |
| [Deletion vectors](https://learn.microsoft.com/en-us/azure/databricks/tables/features/deletion-vectors) | ❌ | ✅ | ❌ |
| [Column mapping](https://learn.microsoft.com/en-us/azure/databricks/tables/features/column-mapping) | ❌ | ✅ | ❌² |
| [Checkpoint V2](https://learn.microsoft.com/en-us/azure/databricks/tables/features/checkpoint-v2) | ❌ | ✅ | ❌ |
| [`TIMESTAMP_NTZ`](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/data-types/timestamp-ntz-type) | ✅ | ✅ | ✅ |
| [Type widening](https://learn.microsoft.com/en-us/azure/databricks/tables/features/type-widening) | ❌ | ❌ | ❌ |
| [Catalog commits](https://learn.microsoft.com/en-us/azure/databricks/tables/features/catalog-commits) | ❌ | ❌ | ❌ |

¹ Also `to_pyarrow_table()` and `to_pandas()`, which wrap it.  
² Reads the columns as nulls instead of failing on tables with reader version 2.

### Features that don't change the read protocol

| Feature | `load_as_polars()` | `load_as_delta().scan()` | `load_as_delta().to_pyarrow_dataset()`¹ |
| --- | --- | --- | --- |
| [Liquid clustering](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering) | ✅³ | ✅ | ✅³ |
| [Z-ordering](https://learn.microsoft.com/en-us/azure/databricks/tables/data-skipping#what-is-z-ordering) | ✅ | ✅ | ✅ |

³ Databricks turns on Checkpoint V2 for new liquid clustering tables. Create them with `'delta.checkpointPolicy' = 'classic'` to read them here.

### What each reader returns

| | `load_as_polars()` | `load_as_delta().scan()` | `load_as_delta().to_pyarrow_dataset()` |
| --- | --- | --- | --- |
| Returns | lazy [`pl.LazyFrame`](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) | one-shot Arrow `RecordBatchReader` | lazy `pyarrow.dataset.Dataset` |

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
