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

Databricks tables are stored as ordinary Delta tables in cloud object storage,
so deltabridge can read them without a Databricks SQL warehouse or cluster -
either through Unity Catalog credential vending (recommended) or by reading the
storage location directly.

For tables governed by Unity Catalog, the recommended approach is *credential
vending*: rather than relying on standing storage-level access, Unity Catalog
issues scoped, short-lived credentials for the specific table (see Databricks'
[temporary table credentials](https://learn.microsoft.com/en-us/azure/databricks/external-access/credential-vending)).
It works for managed and external Delta tables registered in Unity Catalog.
deltabridge implements this for Azure in `AzureDatabricksDeltaClient`, which
reads a table by its full name and refreshes the vended credentials
automatically before they expire:

```python
import polars as pl

from deltabridge.azure import AzureDatabricksDeltaClient

databricks_client = AzureDatabricksDeltaClient(
    workspace_url='https://adb-1234567890.12.azuredatabricks.net',
)
table_client = databricks_client.get_table_client('catalog.schema.table')

table_df = table_client.load_as_polars().collect()
```

The Databricks REST API is authenticated with a Microsoft Entra ID token
obtained via [`DefaultAzureCredential`](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential)
(managed identity, Azure CLI, a service principal, ...); pass a custom
`credential` to override it. This requires, on the Databricks side:

* external data access enabled on the metastore.
* that identity registered as a workspace principal with the
  `EXTERNAL USE SCHEMA` privilege on the schema (or parent catalog).
* a table without row filters or column masks (views and masked tables are
  rejected by credential vending).

If credential vending isn't possible - for example the table isn't in Unity
Catalog (such as a legacy Hive metastore table), or external data access isn't
enabled on the metastore - you can instead read the table's storage location
directly with the Delta client for the underlying cloud (Azure, AWS, or GCP;
though only Azure's [`AzureDeltaClient`](#azure) is implemented in deltabridge
today).
Find the storage location in the Databricks Catalog Explorer UI under the table's
*Details*; the reading identity needs at least the *Storage Blob Data Reader*
role on the storage account / container.

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
