# core-tools-delta-client
Thin wrapper for accessing Delta tables stored in Azure Blob Storage in Python.


## Usage

### Examples

```python
import os

import deltalake
import polars as pl

from dtml.delta.azure.client import AzureDeltaTableClient

table_client = AzureDeltaTableClient(
    table_uri=os.environ['MY_TABLE_STORAGE_URI'],
)

# Get a DeltaTable instance
delta_table: deltalake.DeltaTable = table_client.load_as_delta()

# Load the data as a Polars DataFrame
table_ldf: pl.LazyFrame = loader.load_as_polars()
table_df: pl.DataFrame = table_ldf.filter(pl.col('x') > 3).collect()
```

### With Delta tables stored in Databricks Delta Lake
To use this package to load tables stored in Databricks Delta Lake:
* Specify the storage in location (in Azure Blob Storage) where the table
is stored as the table URI
    * This can be found for example in the Databricks Catalog Explorer UI under *Details* of a table
* The reading identity has to have at least *Storage Blob Data Reader* permission
on the storage location (storage account/container)
