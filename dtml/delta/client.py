from __future__ import annotations

from typing import Callable

import polars as pl
from deltalake import DeltaTable


class DeltaTableClient:
    """
    Delta table client - used for accessing tables in Delta format
    stored in a local or cloud storage.

    The client is used to load the Delta table as a Polars LazyFrame
    or as a DeltaTable object.

    Notes
    -----
    The client should never be used directly. Instead, use the method
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
        self._delta_table = self._create_delta_table()

    def _create_delta_table(self) -> DeltaTable:
        return DeltaTable(
            self._table_uri,
            storage_options=self._storage_options_fn(),
        )

    def _refresh_table(self) -> None:
        refreshed_storage_options = self._storage_options_fn()
        if self._storage_options != refreshed_storage_options:
            # The storage options have changed -> recreate DeltaTable instance
            self._delta_table = self._create_delta_table()
            self._storage_options = refreshed_storage_options
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
        partitioned_column_name: str | None = None,
        partitioned_column_value: str | None = None,
    ) -> pl.LazyFrame:
        """Load a Delta table, with optional partition filtering.

        Parameters
        ----------
        partitioned_column_name
            Name of the column used for partitioning. If not provided,
            no partition filtering will be applied.
        partitioned_column_value
            Value of the partition column to filter. Must be provided
            alongside `partitioned_column_name` for filtering
            to take effect.

        Returns
        -------
        polars.LazyFrame
            A Polars LazyFrame representing the scanned Delta table.
            If partition filtering is applied, only matching rows
            are included.

        Notes
        -----
        - If both `partitioned_column_name` and `partitioned_column_value`
        are not provided, the entire table is loaded
        without partition filtering.
        """
        table = self.load_as_delta()

        # Check if the table is partitioned
        if partitioned_column_name and partitioned_column_value:
            pyarrow_options = {
                'partitions': [
                    (
                        partitioned_column_name,
                        '=',
                        partitioned_column_value,
                    )
                ]
            }
        else:
            # No partition filter for non-partitioned tables
            pyarrow_options = {}

        return pl.scan_delta(source=table, pyarrow_options=pyarrow_options)
