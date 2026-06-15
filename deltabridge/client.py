from __future__ import annotations

from typing import Callable

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import DeltaProtocolError

# Reader protocol versions and features the native Polars Delta scanner
# supports (mirrors polars/io/delta/_dataset.py). deltabridge passes a
# pre-built DeltaTable to pl.scan_delta, which bypasses polars' own protocol
# check; without this guard an unsupported table - most importantly one using
# column mapping - is read as all-null columns instead of failing.
_MAX_SUPPORTED_READER_VERSION = 3
_SUPPORTED_READER_FEATURES = frozenset(
    {
        'deletionVectors',
        'timestampNtz',
        'timestampNanos',
        'variantType',
        'variantType-preview',
    }
)


def _ensure_native_reader_support(table: DeltaTable) -> None:
    """Raise DeltaProtocolError if the native reader cannot read the table."""
    protocol = table.protocol()
    version = protocol.min_reader_version
    if version == 2 or version > _MAX_SUPPORTED_READER_VERSION:
        raise DeltaProtocolError(
            f'The table requires reader version {version}, which the native '
            'Polars Delta reader does not support (only versions 1 and 3). '
            'Tables using column mapping cannot be read this way.'
        )
    features = set(protocol.reader_features or ())
    unsupported = features - _SUPPORTED_READER_FEATURES
    if unsupported:
        raise DeltaProtocolError(
            f'The table requires reader features {sorted(unsupported)}, which '
            'the native Polars Delta reader does not support.'
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

    def load_as_polars(self) -> pl.LazyFrame:
        """Load a Delta table as a Polars LazyFrame.

        Returns
        -------
        polars.LazyFrame
            A Polars LazyFrame representing the scanned Delta table.

        Raises
        ------
        deltalake.exceptions.DeltaProtocolError
            If the table uses reader-protocol features the native Polars
            reader does not support (e.g. column mapping).
        """
        table = self.load_as_delta()
        _ensure_native_reader_support(table)
        return pl.scan_delta(table)
