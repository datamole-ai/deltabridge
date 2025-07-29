# core-tools-delta-client
Thin wrapper for accessing Delta tables stored in Azure Blob Storage in Python.

Use this package if you need to read Delta tables stored in Azure Blob Storage
using Python without dependending on services provided by Databricks
(SQL endpoints, general-purpose compute).

A typical use case is exposing final products of a data pipeline (hosted
on Azure Databricks) in a REST API.

## 🚀 Usage

### Examples

#### Azure

```python
import os

import deltalake
import polars as pl

from dtml.delta.azure.client import AzureDeltaClient

azure_delta_client = AzureDeltaClient()
table_client = AzureDeltaClient.get_table_client(
    table_uri=os.environ['MY_TABLE_STORAGE_URI'],
)

# Get a DeltaTable instance
delta_table: deltalake.DeltaTable = table_client.load_as_delta()

# Load the data as a Polars LazyFrame
table_ldf: pl.LazyFrame = table_client.load_as_polars()
# Collect to a Polars DataFrame
table_df: pl.DataFrame = table_ldf.filter(pl.col('x') > 3).collect()
```

### With Delta tables stored in Azure Databricks Delta Lake
To use this package to load tables stored in Azure Databricks Delta Lake:
* Specify the storage in location (in Azure Blob Storage) where the table
is stored as the table URI.
    * This can be found for example in the Databricks Catalog Explorer UI under *Details* of a table.
* The reading identity has to have at least *Storage Blob Data Reader* permission
on the storage location (storage account/container).

 > **Note**: Delta tables with deletion vectors enabled cannot be accessed using this package.
 > We recommend disabling the feature on tables which are to be read using this package.
 > This a limitation of the upstream `deltalake` library (a Python wrapper of `delta-rs`).
 > See https://github.com/delta-io/delta-rs/issues/1094

## ✍️ Writing to Delta tables

Writing to Delta tables is currently **not supported** by this package.
The main reason are:
* it is much harder to support write use cases in general
* most current write use cases run in pipelines hosted on Databricks,
  with spark/pyspark being used for writes

However, feel free to contact ML Engineering Team if you have a use case where write support would be beneficial.

## ☁️ Cloud provider support
The package is focused on Delta tables stored in Azure Blob Storage.
However, it is designed to be easily extensible to support storage offerings
from different cloud providers.


## ✨ Delta to HTTP

A ready-made general-purpose Docker image exposing Delta tables via a HTTP
using this package is in progress. Stay tuned!
