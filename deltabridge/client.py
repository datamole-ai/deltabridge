from __future__ import annotations

from enum import StrEnum
from threading import Lock
from typing import Any, Callable

import polars as pl
from deltalake import DeltaTable


class PartitionFilterOperator(StrEnum):
    EQUAL = '='
    NOT_EQUAL = '!='
    IN = 'in'
    NOT_IN = 'not in'


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
        self._refresh_lock = Lock()
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
            are included. Its schema and file list are captured before this
            method returns, so subsequent client refreshes do not change it.

        Raises
        ------
        ValueError
            If an invalid partition filter operator is provided.
        """
        if partition_filter:
            for _, operator, _ in partition_filter:
                # Raises ValueError if invalid
                PartitionFilterOperator(operator)

        with self._refresh_lock:
            self._refresh_table()
            dataset = self._delta_table.to_pyarrow_dataset(
                partitions=partition_filter
            )

        return pl.scan_pyarrow_dataset(dataset)
