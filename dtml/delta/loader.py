from datetime import datetime

import polars as pl
from azure.core.credentials import AccessToken
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable


class DeltaTableLoader:
    """DeltaTableLoader is a class that loads a Delta table from
    Azure Blob Storage into a DeltaTable object or a Polars LazyFrame.

    Parameters
    ----------
    table_uri
        The storage location URI of the Delta table to load.
    """

    __credential: DefaultAzureCredential = DefaultAzureCredential()
    __token_obj: AccessToken | None = None

    def __init__(
        self,
        table_uri: str,
    ):
        self._delta_table = DeltaTable(table_uri)

    def _refresh_table(self) -> None:
        if (
            DeltaTableLoader.__token_obj is None  # First time
            or DeltaTableLoader.__token_obj.expires_on - 60
            > datetime.now().timestamp()  # Token cl
        ):
            DeltaTableLoader.__token_obj = (
                DeltaTableLoader.__credential.get_token(
                    'https://storage.azure.com/.default'
                )
            )
            self._delta_table = DeltaTable(
                self._delta_table.table_uri,
                storage_options={
                    'azure_storage_token': DeltaTableLoader.__token_obj.token
                },
            )
        else:
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
            Value of the partition column to filter. Must be provided alongside
            `partitioned_column_name` for filtering to take effect.

        Returns
        -------
        polars.LazyFrame
            A Polars LazyFrame representing the scanned Delta table.
            If partition filtering is applied, only matching rows are included.

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
                    (partitioned_column_name, '=', partitioned_column_value)
                ]
            }
        else:
            # No partition filter for non-partitioned tables
            pyarrow_options = {}

        return pl.scan_delta(
            source=table, use_pyarrow=True, pyarrow_options=pyarrow_options
        )
