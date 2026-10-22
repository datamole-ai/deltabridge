from pyarrow.dataset import ParquetReadOptions

# Spark writes TIMESTAMP as INT96; nanoseconds overflow outside 1677-2262.
# Batch and streaming reads share the options to return the same types.
PARQUET_READ_OPTIONS = ParquetReadOptions(coerce_int96_timestamp_unit='us')
