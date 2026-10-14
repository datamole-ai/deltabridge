from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from typing import Callable
from urllib.parse import unquote

import polars as pl
from deltalake import DeltaTable
from deltalake.fs import DeltaStorageHandler

from deltabridge.base_checkpoint import BaseCheckpoint, StreamOffset

# Field metadata with the column name in the data files under column mapping
_PHYSICAL_NAME = 'delta.columnMapping.physicalName'

# Rows of a batch, and the offset after them
_Batch = tuple[pl.DataFrame, StreamOffset]


class StartingOffset(StrEnum):
    """Where a stream starts if its checkpoint stores no offset."""

    # Skip the rows already in the table
    LATEST = 'latest'
    # Read the rows already in the table first
    EARLIEST = 'earliest'


class DeltaTableStream:
    """
    Stream of the rows added to a Delta table.

    Notes
    -----
    The stream should never be initialized directly. Instead, use the method
    `load_as_stream` of `DeltaTableClient`.

    Parameters
    ----------
    load_table: Callable[[], DeltaTable]
        Function which returns the refreshed Delta table.
    storage_options_fn: Callable[[], dict[str, str]]
        Function which returns the storage options for the Delta table.
    checkpoint: BaseCheckpoint
        Checkpoint which stores the offset of the stream.
    start: StartingOffset
        Where to start if the checkpoint stores no offset.
    max_versions: int | None
        Maximum number of table versions per batch.
    """

    def __init__(
        self,
        load_table: Callable[[], DeltaTable],
        storage_options_fn: Callable[[], dict[str, str]],
        checkpoint: BaseCheckpoint,
        start: StartingOffset = StartingOffset.LATEST,
        max_versions: int | None = None,
    ):
        self._load_table = load_table
        self._storage_options_fn = storage_options_fn
        self._checkpoint = checkpoint
        self._start = start
        self._max_versions = max_versions

    def for_each_batch(
        self,
        func: Callable[[pl.DataFrame], None],
        interval: float = 30,
        stop: threading.Event | None = None,
    ) -> None:
        """Call the function with each batch of added rows until stopped.

        The offset is saved after the function returns, so the rows are
        delivered at least once.

        Parameters
        ----------
        func
            Function called with the rows of each batch.
        interval
            Seconds to wait when there are no new table versions.
        stop
            Event which stops the stream.
            If not provided, the stream runs until an exception.

        Raises
        ------
        ValueError
            If a table version deletes or updates rows, or the checkpoint
            belongs to another table.
        NotImplementedError
            If the table is partitioned or has struct columns with column
            mapping.
        """
        stop = stop or threading.Event()
        while not stop.is_set():
            if not self._process_batch(func):
                # Caught up with the table, wait for new versions
                stop.wait(interval)

    def _process_batch(self, func: Callable[[pl.DataFrame], None]) -> bool:
        """Process the next batch, return whether it had new versions."""
        offset = self._checkpoint.load()
        df, next_offset = self._read_batch(offset)
        if not df.is_empty():
            func(df)
        if next_offset == offset:
            return False
        self._checkpoint.save(next_offset)
        return True

    def _read_batch(self, offset: StreamOffset | None) -> _Batch:
        """Rows added after the offset, and the offset after them."""
        snapshot = _Snapshot(self._load_table(), self._storage_options_fn())
        if offset is None:
            df = pl.DataFrame()
            if self._start == StartingOffset.EARLIEST:
                df = snapshot.read_all()
            return df, StreamOffset(snapshot.table_id, snapshot.version)
        if offset.table_id != snapshot.table_id:
            raise ValueError(
                'The checkpoint belongs to another table, '
                'the table was probably recreated.'
            )
        return self._read_new_versions(snapshot, offset)

    def _read_new_versions(
        self, snapshot: _Snapshot, offset: StreamOffset
    ) -> _Batch:
        """Rows added by the new versions, and the offset after them."""
        start = offset.version + 1
        end = snapshot.version
        if self._max_versions is not None:
            end = min(end, offset.version + self._max_versions)

        paths = []
        for version in range(start, end + 1):
            commit = snapshot.read_commit(version)
            if commit.changes_rows:
                if version == start:
                    raise ValueError(
                        f'Version {version} deletes or updates rows, '
                        'which streams do not support.'
                    )
                # Deliver the versions before it, the next batch raises
                end = version - 1
                break
            paths += commit.paths
        return snapshot.read_files(paths), StreamOffset(snapshot.table_id, end)


class _Snapshot:
    """Latest version of a Delta table, with the settings to read it."""

    def __init__(self, table: DeltaTable, storage_options: dict[str, str]):
        self.table = table
        self.table_id = table.metadata().id
        self.version = table.version()
        self._storage_options = storage_options
        self._schema = table.schema()

        # Fail at once, before the stream reads anything
        if table.metadata().partition_columns:
            raise NotImplementedError(
                'Streams of partitioned tables are not supported.'
            )
        for field in self._schema.fields:
            # Fields of structs have physical names too, which are not mapped
            is_struct = '"struct"' in field.type.to_json()
            if _PHYSICAL_NAME in field.metadata and is_struct:
                raise NotImplementedError(
                    f'Column {field.name} contains a struct, which streams '
                    'of tables with column mapping do not support.'
                )

    @cached_property
    def _storage(self) -> DeltaStorageHandler:
        """Storage of the table, created on the first read of the Delta log."""
        return DeltaStorageHandler(self.table.table_uri, self._storage_options)

    def read_all(self) -> pl.DataFrame:
        """Read all rows of the table."""
        return pl.DataFrame(self.table.scan().read_all())

    def read_commit(self, version: int) -> _Commit:
        """Read the data changes of the table version."""
        file = self._storage.open_input_file(f'_delta_log/{version:020}.json')
        return _Commit.parse(file.read())

    def read_files(self, paths: list[str]) -> pl.DataFrame:
        """Read the data files with the column names of the table."""
        if not paths:
            return pl.DataFrame()
        types = pl.Schema(self._schema.to_arrow())
        # Column mapping stores the columns under physical names
        column_names = {
            field.metadata.get(_PHYSICAL_NAME, field.name): field.name
            for field in self._schema.fields
        }
        file_schema = {
            file_name: types[name] for file_name, name in column_names.items()
        }
        table_uri = self.table.table_uri.rstrip('/')
        return (
            pl.scan_parquet(
                [f'{table_uri}/{path}' for path in paths],
                schema=file_schema,
                # Older files miss columns added later, and keep dropped ones
                missing_columns='insert',
                extra_columns='ignore',
                storage_options=self._storage_options,
                # deltabridge supplies the credentials in the storage options
                credential_provider=None,
            )
            .rename(column_names)
            .collect()
        )


@dataclass(frozen=True)
class _Commit:
    """Data changes of a table version, from its commit in the Delta log."""

    # Paths of the files with added rows, relative to the table
    paths: list[str]
    # Whether the version deletes or updates rows
    changes_rows: bool

    @classmethod
    def parse(cls, data: bytes) -> _Commit:
        """Parse the commit, which has one JSON action per line."""
        actions = [json.loads(line) for line in data.splitlines()]
        # OPTIMIZE and compaction rewrite files without changing data
        added = [
            a['add'] for a in actions if a.get('add', {}).get('dataChange')
        ]
        removed = [a for a in actions if a.get('remove', {}).get('dataChange')]
        # Deletion vectors mark rows of the added files as deleted
        has_deletion_vectors = any(add.get('deletionVector') for add in added)
        return cls(
            paths=[unquote(add['path']) for add in added],
            changes_rows=bool(removed) or has_deletion_vectors,
        )
