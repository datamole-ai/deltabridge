from __future__ import annotations

import logging
from enum import StrEnum
from typing import Any, Callable

import polars as pl
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


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

    If rebuilding the table with rotated credentials fails transiently,
    the previous table is served for that read and the rebuild is
    retried on the next call.

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

    def _refresh_table(self) -> DeltaTable:
        refreshed_storage_options = self._storage_options_fn()
        if self._storage_options == refreshed_storage_options:
            # Update table metadata using existing token
            self._delta_table.update_incremental()
        else:
            # Storage options changed -> recreate the DeltaTable.
            # Commit both attributes only after the rebuild succeeds;
            # a failed rebuild keeps the previous consistent state so
            # the next call retries, while this read is served from the
            # previous table (its token stays valid briefly after
            # rotation).
            try:
                self._delta_table = self._create_delta_table(
                    refreshed_storage_options
                )
                self._storage_options = refreshed_storage_options
            except Exception:
                logger.warning(
                    'Failed to recreate Delta table %r with refreshed '
                    'storage options; serving the previous table and '
                    'retrying on next read.',
                    self._table_uri,
                    exc_info=True,
                )
        return self._delta_table

    def load_as_delta(self) -> DeltaTable:
        """Load a Delta table.

        Returns
        -------
        DeltaTable
            A DeltaTable object representing the loaded table.
        """
        return self._refresh_table()

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
            are included.

        Raises
        ------
        ValueError
            If an invalid partition filter operator is provided.
        """
        table = self.load_as_delta()

        # Check if the table is partitioned
        if partition_filter:
            for _, operator, _ in partition_filter:
                # Raises ValueError if invalid
                PartitionFilterOperator(operator)
            pyarrow_options = {'partitions': partition_filter}
        else:
            # No partition filter for non-partitioned tables
            pyarrow_options = {}

        return pl.scan_delta(
            source=table, use_pyarrow=True, pyarrow_options=pyarrow_options
        )
