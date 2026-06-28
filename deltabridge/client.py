from __future__ import annotations

from enum import StrEnum
from typing import Any, Callable

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import DeltaProtocolError
from deltalake.table import (
    MAX_SUPPORTED_READER_VERSION,
    NOT_SUPPORTED_READER_VERSION,
    SUPPORTED_READER_FEATURES,
)


class PartitionFilterOperator(StrEnum):
    EQUAL = '='
    NOT_EQUAL = '!='
    IN = 'in'
    NOT_IN = 'not in'


# Reader features the native Polars scanner handles: deltalake's allowlist
# plus `deletionVectors` (which polars supports but that constant omits).
# The guard matters because passing a pre-built DeltaTable to pl.scan_delta
# bypasses polars' own protocol check - most importantly, a column-mapping
# table then reads as all-null columns instead of failing.
_SUPPORTED_READER_FEATURES = frozenset(
    SUPPORTED_READER_FEATURES | {'deletionVectors'}
)


def _ensure_native_reader_support(table: DeltaTable) -> None:
    """Raise DeltaProtocolError if the native reader cannot read the table."""
    protocol = table.protocol()
    version = protocol.min_reader_version
    if version == NOT_SUPPORTED_READER_VERSION:
        raise DeltaProtocolError(
            f'The table requires reader version {version} (column mapping), '
            'which the native Polars Delta reader does not support.'
        )
    if version > MAX_SUPPORTED_READER_VERSION:
        raise DeltaProtocolError(
            f'The table requires reader version {version}, which the native '
            'Polars Delta reader does not support (only version 1 or '
            f'{MAX_SUPPORTED_READER_VERSION}).'
        )
    # Reader features are a reader-v3+ concept; like polars, only inspect them
    # at v3+ so a non-conformant lower-version table carrying a stray feature
    # list is not rejected for a feature the native reader never consults.
    if version >= 3 and protocol.reader_features:
        features = set(protocol.reader_features)
        unsupported = features - _SUPPORTED_READER_FEATURES
        if unsupported:
            raise DeltaProtocolError(
                f'The table requires reader features {sorted(unsupported)}, '
                'which the native Polars Delta reader does not support.'
            )


def _ensure_no_deletion_vectors(table: DeltaTable) -> None:
    """Raise DeltaProtocolError up front for a deletion-vector table on the
    pyarrow path.

    The pyarrow reader (delta-rs ``to_pyarrow_dataset``) does not support
    deletion vectors and rejects such tables itself - but only lazily, at
    ``collect()`` time, with a generic message. We check eagerly so the error
    surfaces at the ``load_as_polars`` call (matching the native path's guard)
    with an actionable message pointing to the native reader, which does honor
    deletion vectors.
    """
    protocol = table.protocol()
    uses_deletion_vectors = (
        protocol.min_reader_version >= 3
        and protocol.reader_features
        and 'deletionVectors' in protocol.reader_features
    )
    if uses_deletion_vectors:
        raise DeltaProtocolError(
            'The pyarrow reader used for partition filtering does not support '
            'deletion vectors, which this table enables. Read it without '
            'partition_filter (the native reader honors deletion vectors) and '
            'filter the returned LazyFrame instead.'
        )


class DeltaTableClient:
    """
    Delta table client - used for accessing tables in Delta format
    stored in a local or cloud storage.

    The client is used to load the Delta table as a Polars LazyFrame
    or as a DeltaTable object.

    Notes
    -----
    The client should never be initialized directly. Instead, use the method
    `get_table_client` of a class derived from `BaseDeltaClient` to
    get a client for a specific storage provider.

    Parameters
    ----------
    table_uri: str
        URI of the Delta table.
    storage_options_fn: Callable[[], dict[str, str]]
        Function which returns the storage options for the Delta table.
        Storage options typically include an access token for the storage
        in which the Delta table is stored.
    """

    def __init__(
        self,
        table_uri: str,
        storage_options_fn: Callable[[], dict[str, str]],
    ):
        self._table_uri = table_uri
        self._storage_options_fn = storage_options_fn
        self._storage_options = self._storage_options_fn()
        self._delta_table = self._create_delta_table(self._storage_options)

    def _create_delta_table(
        self, storage_options: dict[str, str]
    ) -> DeltaTable:
        return DeltaTable(
            self._table_uri,
            storage_options=storage_options,
        )

    def _refresh_table(self) -> None:
        refreshed_storage_options = self._storage_options_fn()
        if self._storage_options != refreshed_storage_options:
            # The storage options have changed -> recreate DeltaTable instance
            delta_table = self._create_delta_table(refreshed_storage_options)
            self._storage_options = refreshed_storage_options
            self._delta_table = delta_table
        else:
            # Update table metadata using existing token
            self._delta_table.update_incremental()

    def load_as_delta(self) -> DeltaTable:
        """Load a Delta table.

        Returns
        -------
        DeltaTable
            A DeltaTable object representing the loaded table.
        """
        self._refresh_table()
        return self._delta_table

    def load_as_polars(
        self,
        partition_filter: list[tuple[str, PartitionFilterOperator, Any]]
        | None = None,
    ) -> pl.LazyFrame:
        """Load a Delta table, with optional partition filtering.

        Without ``partition_filter`` the table is read with the native Polars
        Delta reader, which supports deletion-vector tables but does not prune
        partitions efficiently (see pola-rs/polars#20998): it skips the data of
        non-matching partitions, yet still handles per-file metadata for
        *every* partition when building the scan - a network request per file
        on object storage, so reading a few partitions of a many-partition
        table can be far slower. Passing ``partition_filter`` switches to the
        pyarrow reader, which pushes the partition predicate into delta-rs's
        file enumeration so non-matching partitions are never materialized -
        but it cannot read deletion-vector tables.

        Parameters
        ----------
        partition_filter
            Iterable of tuples containing the column name, operator and value
            to filter the table by partition columns.
            If multiple partition filters are provided, they are combined using
            the logical AND operator.
            If not provided, no partition filtering will be applied.

        Returns
        -------
        polars.LazyFrame
            A Polars LazyFrame representing the scanned Delta table.
            If partition filtering is applied, only matching rows
            are included.

        Raises
        ------
        ValueError
            If an invalid partition filter operator is provided.
        deltalake.exceptions.DeltaProtocolError
            If ``partition_filter`` is used on a table with deletion vectors,
            or - on the native path - if the table uses reader-protocol
            features the native Polars reader does not support (e.g. column
            mapping).
        """
        table = self.load_as_delta()

        if partition_filter:
            for _, operator, _ in partition_filter:
                # Raises ValueError if invalid
                PartitionFilterOperator(operator)
            # The pyarrow reader rejects deletion-vector tables; fail fast here
            # with a clear message instead of lazily at collect().
            _ensure_no_deletion_vectors(table)
            return pl.scan_delta(
                source=table,
                use_pyarrow=True,
                pyarrow_options={'partitions': partition_filter},
            )

        _ensure_native_reader_support(table)
        return pl.scan_delta(table)
