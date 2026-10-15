from __future__ import annotations

import json
from enum import StrEnum
from typing import Callable
from urllib.parse import urlsplit

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.fs import DeltaStorageHandler

from deltabridge.base_checkpoint import BaseCheckpoint, StreamOffset


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

    def process_batch(self, func: Callable[[pl.DataFrame], None]) -> bool:
        """Process one batch and save its offset after the function returns.

        Parameters
        ----------
        func
            Function called with the rows of a nonempty batch.

        Returns
        -------
        bool
            Whether the checkpoint advanced, including versions with no rows.

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
            If the table is partitioned or requires reader version above 1.
        """
        rows, next_offset = self._read_batch()
        if next_offset == self._offset:
            return False
        batch = rows.collect()
        if not batch.is_empty():
            func(batch)
        self._checkpoint.save(next_offset)
        self._offset = next_offset
        return True

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
        paths = []
        if offset is None:
            if self._start == StartingOffset.EARLIEST:
                paths = table.file_uris()
        elif offset.version < end:
            if self._max_versions is not None:
                end = min(end, offset.version + self._max_versions)
            paths, end = self._read_versions(
                table, options, offset.version, end
            )

        next_offset = StreamOffset(table.metadata().id, end)
        if not paths:
            schema = pa.schema(table.schema().to_arrow())
            return pl.from_arrow(schema.empty_table()).lazy(), next_offset
        return pl.scan_parquet(
            paths,
            storage_options=options,
            credential_provider=None,
            glob=False,
        ), next_offset

    def _read_versions(
        self, table: DeltaTable, options: dict[str, str], after: int, end: int
    ) -> tuple[list[str], int]:
        storage = DeltaStorageHandler(table.table_uri, options)
        paths = []
        for version in range(after + 1, end + 1):
            with storage.open_input_file(
                f'_delta_log/{version:020}.json'
            ) as f:
                actions = [json.loads(line) for line in f.read().splitlines()]
            added = [
                a['add'] for a in actions if a.get('add', {}).get('dataChange')
            ]
            if (
                any(a.get('remove', {}).get('dataChange') for a in actions)
                or any(a.get('deletionVector') for a in added)
                or any('metaData' in a or 'protocol' in a for a in actions)
            ):
                if version == after + 1:
                    raise ValueError(
                        f'Version {version} deletes or updates rows, '
                        'or changes table metadata.'
                    )
                # Deliver earlier versions first; the next call raises.
                return paths, version - 1
            for add in added:
                path = add['path']
                if not urlsplit(path).scheme and not path.startswith('/'):
                    path = f'{table.table_uri.rstrip("/")}/{path}'
                paths.append(path)
        return paths, end
