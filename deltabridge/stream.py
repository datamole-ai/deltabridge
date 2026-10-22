from __future__ import annotations

from typing import Callable
from urllib.parse import unquote, urlsplit

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from deltalake import DeltaTable, Schema
from deltalake.fs import DeltaStorageHandler

from deltabridge.base_checkpoint import BaseCheckpoint, StreamOffset
from deltabridge.parquet import PARQUET_READ_OPTIONS

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
    'metaData': pl.Struct(
        {'schemaString': pl.String, 'partitionColumns': pl.List(pl.String)}
    ),
    'protocol': pl.Struct(
        {'minReaderVersion': pl.Int64, 'readerFeatures': pl.List(pl.String)}
    ),
}

# Reader features which do not change how the stream reads appended files
_SUPPORTED_READER_FEATURES = {
    'deletionVectors',
    'v2Checkpoint',
    'vacuumProtocolCheck',
    'timestampNtz',
}


class StreamError(Exception):
    """The stream cannot continue from its checkpoint.

    Retrying does not help. Start a new stream with a new checkpoint, after
    changing the table if it is not supported.
    """


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
        max_versions: int | None = None,
    ):
        if max_versions is not None and max_versions <= 0:
            raise ValueError('max_versions must be positive.')
        self._table_uri = table_uri
        self._storage_options_fn = storage_options_fn
        self._checkpoint = checkpoint
        self._max_versions = max_versions
        self._offset = checkpoint.load()
        self._column_types = None

    def process_batch(self, func: Callable[[pl.DataFrame], None]) -> None:
        """Process one batch and save its offset after the function returns.

        Parameters
        ----------
        func
            Function called with the rows of a nonempty batch.

        Raises
        ------
        StreamError
            If the stream cannot continue from its checkpoint: the
            checkpoint belongs to another table, or a version after it
            deletes or updates rows, changes columns or partitioning, or
            the table is not supported. Retrying does not help.

        Notes
        -----
        Delivery is at-least-once. Other errors, e.g. from storage, the
        function or saving the checkpoint, leave the offset unchanged, so
        the call can be retried. Missing logs or files also fail the read.
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
        offset, end = self._offset, table.version()
        if offset is not None:
            if offset.table_id != table.metadata().id:
                raise StreamError('The checkpoint belongs to another table.')
            if not 0 <= offset.version <= end:
                raise StreamError(
                    f'The checkpoint version {offset.version} is outside '
                    f'the table versions 0 to {end}.'
                )
        if table.metadata().partition_columns:
            raise StreamError(
                'The table is partitioned, which streams do not support.'
            )
        protocol = table.protocol()
        reader = _unsupported_reader(
            protocol.min_reader_version, protocol.reader_features
        )
        if reader:
            raise StreamError(
                f'The table {reader}, which streams do not support.'
            )
        # A new stream skips the rows already in the table.
        sizes = {}
        if offset is not None and offset.version < end:
            if self._column_types is None:
                previous = DeltaTable(
                    self._table_uri,
                    version=offset.version,
                    storage_options=options,
                )
                self._column_types = _column_types(previous.schema())
            if self._column_types != _column_types(table.schema()):
                raise StreamError(
                    'Column names or types changed after version '
                    f'{offset.version}.'
                )
            if self._max_versions is not None:
                end = min(end, offset.version + self._max_versions)
            sizes, end = self._read_versions(
                table.table_uri, options, offset.version, end
            )
        next_offset = StreamOffset(table.metadata().id, end)
        schema = pa.schema(table.schema().to_arrow())
        if not sizes:
            return pl.from_arrow(schema.empty_table()).lazy(), next_offset
        rows = _scan_files(table.table_uri, options, sizes, schema)
        return rows, next_offset

    def _read_versions(
        self, table_uri: str, options: dict[str, str], after: int, end: int
    ) -> tuple[dict[str, int], int]:
        root = table_uri.rstrip('/')
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
            'metaData',
            'protocol',
            version=pl.col('commit').str.extract(r'(\d{20})\.json$').cast(int),
            path=add.field('path'),
            size=add.field('size'),
            added=add.field('dataChange'),
            deleted=pl.any_horizontal(
                pl.col('remove').struct.field('dataChange'),
                add.field('dataChange')
                & add.field('deletionVector').is_not_null(),
            ),
        ).collect()
        # Why the stream cannot deliver a version, per version
        changes = {
            version: 'deletes or updates rows'
            for version in commits.filter('deleted')['version']
        }
        # Metadata and protocol actions are rare; check them in Python.
        metadata_commits = commits.filter(
            pl.col('metaData').is_not_null() | pl.col('protocol').is_not_null()
        )
        for row in metadata_commits.iter_rows(named=True):
            metadata, protocol = row['metaData'], row['protocol']
            reader = protocol and _unsupported_reader(
                protocol['minReaderVersion'], protocol['readerFeatures']
            )
            if metadata and metadata['partitionColumns']:
                change = (
                    'makes the table partitioned, which streams do not support'
                )
            elif metadata and (
                _column_types(Schema.from_json(metadata['schemaString']))
                != self._column_types
            ):
                change = 'changes column names or types'
            elif reader:
                change = f'{reader}, which streams do not support'
            else:
                continue
            changes[row['version']] = change
        if changes:
            changed = min(changes)
            if changed == after + 1:
                raise StreamError(f'Version {changed} {changes[changed]}.')
            # Deliver earlier versions first; the next call raises.
            end = changed - 1
        added = commits.filter('added', pl.col('version') <= end)
        return dict(zip(added['path'], added['size'])), end


def _unsupported_reader(
    min_reader_version: int, reader_features: list[str] | None
) -> str | None:
    """What a reader protocol requires that streams do not support."""
    if min_reader_version == 1:
        return None
    if min_reader_version != 3:
        return f'requires reader version {min_reader_version}'
    unsupported = set(reader_features or []) - _SUPPORTED_READER_FEATURES
    if not unsupported:
        return None
    return f'requires the reader features {", ".join(sorted(unsupported))}'


def _column_types(schema: Schema) -> pl.Schema:
    """Column names and types of the rows, without nullability or metadata."""
    return pl.from_arrow(pa.schema(schema.to_arrow()).empty_table()).schema


def _scan_files(
    table_uri: str,
    options: dict[str, str],
    sizes: dict[str, int],
    schema: pa.Schema,
) -> pl.LazyFrame:
    """Scan data files given by their add action paths and sizes."""
    known_sizes = {}
    for path, size in sizes.items():
        if urlsplit(path).scheme or path.startswith('/'):
            raise StreamError(
                'Data files use absolute paths, e.g. in shallow clones, '
                'which streams do not support.'
            )
        known_sizes[unquote(path)] = size
    # Known sizes spare a HEAD request per file, which holds the GIL.
    fs = pa_fs.PyFileSystem(
        DeltaStorageHandler(table_uri, options, known_sizes)
    )
    fmt = ds.ParquetFileFormat(read_options=PARQUET_READ_OPTIONS)
    fragments = [
        fmt.make_fragment(path, filesystem=fs) for path in known_sizes
    ]
    return pl.scan_pyarrow_dataset(
        ds.FileSystemDataset(fragments, schema, fmt, fs)
    )
