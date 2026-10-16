from __future__ import annotations

from enum import StrEnum
from typing import Callable
from urllib.parse import unquote, urlsplit

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from deltalake import DeltaTable
from deltalake.fs import DeltaStorageHandler

from deltabridge.base_checkpoint import BaseCheckpoint, StreamOffset

# Fields of the commit actions which the stream reads; others are ignored
_COMMIT_SCHEMA = {
    'add': pl.Struct(
        {
            'path': pl.String,
            'size': pl.Int64,
            'dataChange': pl.Boolean,
            'deletionVector': pl.Struct({'storageType': pl.String}),
        }
    ),
    'remove': pl.Struct({'dataChange': pl.Boolean}),
    'metaData': pl.Struct({'id': pl.String}),
    'protocol': pl.Struct({'minReaderVersion': pl.Int64}),
}


class StartingOffset(StrEnum):
    """Where a stream starts if its checkpoint stores no offset."""

    LATEST = 'latest'
    EARLIEST = 'earliest'


class DeltaTableStream:
    """Stream of appended rows, created by `DeltaTableClient.load_as_stream`.

    The application schedules `process_batch` calls. Only one stream may use
    a checkpoint, and calls must not overlap.
    """

    def __init__(
        self,
        table_uri: str,
        storage_options_fn: Callable[[], dict[str, str]],
        checkpoint: BaseCheckpoint,
        start: StartingOffset = StartingOffset.LATEST,
        max_versions: int | None = None,
    ):
        if max_versions is not None and max_versions <= 0:
            raise ValueError('max_versions must be positive.')
        self._table_uri = table_uri
        self._storage_options_fn = storage_options_fn
        self._checkpoint = checkpoint
        self._start = StartingOffset(start)
        self._max_versions = max_versions
        self._offset = checkpoint.load()
        self._schema = None

    def process_batch(self, func: Callable[[pl.DataFrame], None]) -> None:
        """Process one batch and save its offset after the function returns.

        Parameters
        ----------
        func
            Function called with the rows of a nonempty batch.

        Notes
        -----
        Delivery is at-least-once. If reading, processing or saving fails,
        the in-memory offset stays unchanged so the call can be retried.

        Raises
        ------
        ValueError
            If the offset is invalid, or a version updates/deletes rows or
            changes table metadata. Missing logs or files also fail the read.
        NotImplementedError
            If the table is partitioned, requires reader version above 1,
            or references data files by absolute paths.
        """
        rows, next_offset = self._read_batch()
        if next_offset == self._offset:
            return
        batch = rows.collect()
        if not batch.is_empty():
            func(batch)
        self._checkpoint.save(next_offset)
        self._offset = next_offset

    def _read_batch(self) -> tuple[pl.LazyFrame, StreamOffset]:
        options = self._storage_options_fn()
        # Cached incremental replay can miss a table recreated at the same URI.
        table = DeltaTable(self._table_uri, storage_options=options)
        offset = self._offset
        if offset is not None:
            if offset.table_id != table.metadata().id:
                raise ValueError('The checkpoint belongs to another table.')
            if not 0 <= offset.version <= table.version():
                raise ValueError('The checkpoint version is out of range.')
        if (
            table.metadata().partition_columns
            or table.protocol().min_reader_version != 1
        ):
            raise NotImplementedError(
                'Streams require an unpartitioned table with reader version 1.'
            )
        if self._schema is None:
            previous = (
                table
                if offset is None
                else DeltaTable(
                    self._table_uri,
                    version=offset.version,
                    storage_options=options,
                )
            )
            if previous.metadata().id != table.metadata().id:
                raise ValueError('The checkpoint belongs to another table.')
            self._schema = previous.schema()
        if self._schema != table.schema():
            raise ValueError('The table schema changed since the last batch.')

        end = table.version()
        sizes = {}
        if offset is None:
            if self._start == StartingOffset.EARLIEST:
                actions = table.get_add_actions()
                files = pl.DataFrame(actions.select(['path', 'size_bytes']))
                sizes = dict(zip(files['path'], files['size_bytes']))
        elif offset.version < end:
            if self._max_versions is not None:
                end = min(end, offset.version + self._max_versions)
            sizes, end = self._read_versions(
                table, options, offset.version, end
            )

        next_offset = StreamOffset(table.metadata().id, end)
        schema = pa.schema(table.schema().to_arrow())
        if not sizes:
            return pl.from_arrow(schema.empty_table()).lazy(), next_offset
        return _scan_files(table, options, sizes, schema), next_offset

    def _read_versions(
        self, table: DeltaTable, options: dict[str, str], after: int, end: int
    ) -> tuple[dict[str, int], int]:
        root = table.table_uri.rstrip('/')
        # Polars fetches the commits concurrently, not one request per version
        actions = pl.scan_ndjson(
            [
                f'{root}/_delta_log/{version:020}.json'
                for version in range(after + 1, end + 1)
            ],
            schema=_COMMIT_SCHEMA,
            storage_options=options,
            credential_provider=None,
            include_file_paths='commit',
        )
        add = pl.col('add').struct
        commits = actions.select(
            version=pl.col('commit').str.extract(r'(\d{20})\.json$').cast(int),
            path=add.field('path'),
            size=add.field('size'),
            added=add.field('dataChange'),
            change=pl.any_horizontal(
                pl.col('remove').struct.field('dataChange'),
                add.field('dataChange')
                & add.field('deletionVector').is_not_null(),
                pl.col('metaData').is_not_null(),
                pl.col('protocol').is_not_null(),
            ),
        ).collect()
        changed = commits.filter('change')['version'].min()
        if changed is not None:
            if changed == after + 1:
                raise ValueError(
                    f'Version {changed} deletes or updates rows, '
                    'or changes table metadata.'
                )
            # Deliver earlier versions first; the next call raises.
            end = changed - 1
        added = commits.filter('added', pl.col('version') <= end)
        return dict(zip(added['path'], added['size'])), end


def _scan_files(
    table: DeltaTable,
    options: dict[str, str],
    sizes: dict[str, int],
    schema: pa.Schema,
) -> pl.LazyFrame:
    """Scan data files given by their add action paths and sizes."""
    known_sizes = {}
    for path, size in sizes.items():
        if urlsplit(path).scheme or path.startswith('/'):
            raise NotImplementedError(
                'Streams require data file paths relative to the table.'
            )
        known_sizes[unquote(path)] = size
    # Known sizes spare a HEAD request per file, which holds the GIL.
    fs = pa_fs.PyFileSystem(
        DeltaStorageHandler(table.table_uri, options, known_sizes)
    )
    # Spark writes TIMESTAMP as INT96; nanoseconds overflow outside 1677-2262.
    fmt = ds.ParquetFileFormat(
        read_options=ds.ParquetReadOptions(coerce_int96_timestamp_unit='us'),
        default_fragment_scan_options=ds.ParquetFragmentScanOptions(
            pre_buffer=True
        ),
    )
    fragments = [
        fmt.make_fragment(path, filesystem=fs) for path in known_sizes
    ]
    return pl.scan_pyarrow_dataset(
        ds.FileSystemDataset(fragments, schema, fmt, fs)
    )
