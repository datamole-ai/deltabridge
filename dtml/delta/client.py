import polars as pl
from deltalake import DeltaTable

from dtml.delta.token import TokenClient


class DeltaTableClient:
    """
    Delta table client - used for accessing tables in Delta format
    stored in Azure Blob Storage.

    Parameters
    ----------
    table_uri: str
        URI of the Delta table.
    token_client: TokenClient
        Token client used to refresh the storage access token.
    """

    def __init__(
        self,
        table_uri: str,
        token_client: TokenClient,
    ):
        self._table_uri = table_uri
        self._token_client = token_client
        self._delta_table = self._create_delta_table()

    def _create_delta_table(self) -> DeltaTable:
        return DeltaTable(
            self._table_uri,
            storage_options=self._token_client.storage_options,
        )

    def _refresh_table(self) -> None:
        if self._token_client.refresh_token():
            # There is a new token -> recreate DeltaTable instance
            self._delta_table = self._create_delta_table()
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
