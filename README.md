# deltabridge
Thin Python wrapper for reading Delta tables from object storage (currently 
Azure Blob Storage and AWS S3) or a local filesystem, with low and stable 
latency. 
Optimized for repeated reads from long-running Python services.
A typical use case is exposing the final products of a data pipeline 
via a REST API, where request latency should stay predictable.

 > **Note**: The efficiency is achieved by using Rust-based loading of Delta tables through [delta-rs](https://github.com/delta-io/delta-rs)
 > and automatic incremental caching of Delta transaction logs.

## Installation
Install the extra for your object storage provider, or both. The local 
filesystem needs no extra.

```bash
pip install 'deltabridge[azure]'      # Azure Blob Storage
pip install 'deltabridge[aws]'        # AWS S3
pip install 'deltabridge[azure,aws]'  # both
```

Or, with [uv](https://docs.astral.sh/uv/):

```bash
uv add 'deltabridge[azure]'
```

## Usage

### Access
A client gives a table client for each table, which [batch reads](#batch-reads) 
and [streaming reads](#streaming-reads) use. Reuse the table client across 
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

##### AWS

```python
import os

from deltabridge.aws import AwsDeltaClient

# Credentials and region come from the default boto3 session (environment
# variables, shared config and credentials files, instance role, ...).
# Pass your own boto3.Session to use a specific profile or assumed role.
aws_delta_client = AwsDeltaClient()
table_client = aws_delta_client.get_table_client(
    table_uri=os.environ['MY_TABLE_STORAGE_URI'],  # e.g. s3://bucket/path
)
```

* Temporary credentials (e.g. of an assumed role) are refreshed by boto3 before they expire. Static credentials, e.g. from the shared credentials file, are read once and never refreshed.
* The region comes from the boto3 session, or from `AWS_REGION` if the session has none, and must be the region of the bucket.

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
* Use the table's storage location (in Azure Blob Storage or AWS S3) as the table URI.
    * You can find it in the Databricks Catalog Explorer UI under *Details* of the table.
* The reading identity needs read access to the storage location: at least the *Storage Blob Data Reader* permission on the storage account/container in Azure, or `s3:GetObject` and `s3:ListBucket` on the location in AWS.
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

### Streaming reads
`load_as_stream()` reads the rows added by new table versions, like a Spark 
structured streaming read of a Delta table. It finds the added files in the 
Delta log, so the change data feed is not needed. It is meant for one 
process in one pod, and each batch must fit in its memory. If a batch does 
not fit or you need more throughput, use Spark Structured Streaming.

A checkpoint stores the last read version, the offset of the stream. 
`LocalCheckpoint` stores it in a local file:

```python
import polars as pl

from deltabridge.local.checkpoint import LocalCheckpoint
from deltabridge.stream import DeltaTableStream

checkpoint = LocalCheckpoint(
    '/data/my_checkpoint.json'  # on a persistent volume
)
stream: DeltaTableStream = table_client.load_as_stream(
    checkpoint, max_versions=100
)


def process_batch(table_df: pl.DataFrame) -> None:
    print(table_df)


# Create the stream once. Your API's scheduler calls this every 30 seconds.
def poll_once() -> None:
    stream.process_batch(process_batch)
```

* `process_batch()` reads at most one batch and returns `None`, including when no new data is available. It calls the function only for nonempty batches and raises on failure. The application owns scheduling.
* Batches never overlap, inspired by Spark Structured Streaming, which runs one micro-batch at a time. A call made while another call on the same stream is still running, e.g. when a batch takes longer than the scheduler's interval, returns `None` at once without reading, so calls do not pile up. The next call reads the rows the skipped call would have read.
* The stream owns its offset. Delivery is at-least-once: both the checkpoint and in-memory offset advance after the function returns and the checkpoint is saved. If a read, callback or save fails, retry the call; processing must tolerate duplicates.
* A new stream starts at the current table version and skips the rows already in the table.
* Versions which only rearrange data, e.g. OPTIMIZE, are skipped, and so are metadata changes which keep column names and types, e.g. table properties, comments, constraints or supported protocol features. Updates, deletes and changes of column names or types raise `StreamError`, which retrying does not fix; start a new stream with a new checkpoint. Schema changes fail before processing a batch, even when the change is beyond `max_versions`.
* Requires an unpartitioned table with reader version 1, or reader version 3 with only the reader features deletion vectors, Checkpoint V2, `vacuumProtocolCheck` and `TIMESTAMP_NTZ`. Partitioning, other reader features, e.g. column mapping or type widening, and data files referenced by absolute paths, e.g. by shallow clones, raise `StreamError` too.
* A stream stopped for longer than the Delta log or `VACUUM` retention cannot resume.
* Files are read using the Delta schema. Columns missing from a file are read as nulls; other columns, e.g. internal row tracking columns, are ignored. INT96 timestamps, which Spark and Databricks write, are read in microseconds, so dates outside 1677-2262 stay correct.
* `max_versions` must be positive; it limits commits, not rows or memory.
* Run one stream instance per checkpoint. Instances sharing a checkpoint, e.g. pods during a rolling update, deliver some batches twice; the `Recreate` strategy avoids it. Keep the checkpoint on a persistent volume, so a new pod resumes from it.

## Supported Delta features

### Features that change the read protocol

| Feature | `load_as_polars()` | `load_as_delta().scan()` | `load_as_delta().to_pyarrow_dataset()`¹ | `load_as_stream()` |
| --- | --- | --- | --- | --- |
| [Deletion vectors](https://learn.microsoft.com/en-us/azure/databricks/tables/features/deletion-vectors) | ❌ | ✅ | ❌ | ✅³ |
| [Column mapping](https://learn.microsoft.com/en-us/azure/databricks/tables/features/column-mapping) | ❌ | ✅ | ❌² | ❌ |
| [Checkpoint V2](https://learn.microsoft.com/en-us/azure/databricks/tables/features/checkpoint-v2) | ❌ | ✅ | ❌ | ✅ |
| [`TIMESTAMP_NTZ`](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/data-types/timestamp-ntz-type) | ✅ | ✅ | ✅ | ✅ |
| [Type widening](https://learn.microsoft.com/en-us/azure/databricks/tables/features/type-widening) | ❌ | ❌ | ❌ | ❌ |
| [Catalog commits](https://learn.microsoft.com/en-us/azure/databricks/tables/features/catalog-commits) | ❌ | ❌ | ❌ | ❌ |

¹ Also `to_pyarrow_table()` and `to_pandas()`, which wrap it.  
² Reads the columns as nulls instead of failing on tables with reader version 2.  
³ Reads the appended rows. A version that deletes or updates rows fails, with or without deletion vectors.

### Features that don't change the read protocol

| Feature | `load_as_polars()` | `load_as_delta().scan()` | `load_as_delta().to_pyarrow_dataset()`¹ | `load_as_stream()` |
| --- | --- | --- | --- | --- |
| [Liquid clustering](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering) | ✅⁴ | ✅ | ✅⁴ | ✅ |
| [Z-ordering](https://learn.microsoft.com/en-us/azure/databricks/tables/data-skipping#what-is-z-ordering) | ✅ | ✅ | ✅ | ✅ |

⁴ Databricks turns on Checkpoint V2 for new liquid clustering tables. Create them with `'delta.checkpointPolicy' = 'classic'` to read them with `load_as_polars()` or `to_pyarrow_dataset()`.

### What each reader returns

| | `load_as_polars()` | `load_as_delta().scan()` | `load_as_delta().to_pyarrow_dataset()` | `load_as_stream()` |
| --- | --- | --- | --- | --- |
| Returns | lazy [`pl.LazyFrame`](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) | one-shot Arrow `RecordBatchReader` | lazy `pyarrow.dataset.Dataset` | a [`pl.DataFrame`](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) per batch, passed to the `process_batch()` function |

## Writing to Delta tables

deltabridge is **read-focused**: it provides no write API, and its optimizations don't apply to writes. This is deliberate:
* write use cases are more varied and harder to abstract well - appends, overwrites, merges/upserts, schema evolution and concurrency control all behave differently
* writes are typically handled upstream by the systems that produce the tables (often Spark/PySpark pipelines)

Writing is still possible: `load_as_delta()` returns a [`deltalake.DeltaTable`](https://delta-io.github.io/delta-rs/) with deltabridge's auth already configured, which you can pass to `deltalake`'s write API:

```python
import deltalake

deltalake.write_deltalake(table_client.load_as_delta(), df, mode='append')
```

On AWS S3, delta-rs also needs a locking provider for safe concurrent writes
(`AWS_S3_LOCKING_PROVIDER=dynamodb`). See [Writing to S3 with a locking provider](https://delta-io.github.io/delta-rs/latest/usage/writing/writing-to-s3-with-locking-provider/).
