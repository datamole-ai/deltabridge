# core-tools-delta-loader
Thin wrapper for accessing Delta tables stored in Azure Blob Storage in Python.


## Usage

### Examples

```python
import os

import polars as pl

from dtml.delta.client import DeltaClient, DeltaTableClient

token_client = TokenClient()
table_client = DeltaTableClient(
    table_uri=os.environ['MY_TABLE_STORAGE_URI'],
    token_client=token_client,
)

# Load some data
table_ldf: pl.LazyFrame = loader.load_as_polars()
table_df: pl.DataFrame = table_ldf.filter(pl.col('x') > 3).collect()

# Recommended: Sharing a TokenClient minimizes the number of required
# token refreshes
table_client_2 = DeltaTableClient(
    table_uri=os.environ['MY_OTHER_TABLE_STORAGE_URI'],
    token_client=client,
)
```

### With Delta tables stored in Databricks Delta Lake
To use this package to load tables stored in Databricks Delta Lake:
* Specify the storage in location (in Azure Blob Storage) where the table
is stored as the table URI
    * This can be found for example in the Databricks Catalog Explorer UI under *Details* of a table
* The reading identity has to have at least *Storage Blob Data Reader* permission
on the storage location (storage account/container)
