from __future__ import annotations

from datetime import datetime

import polars as pl
from azure.core.credentials import TokenCredential
from azure.identity import ChainedTokenCredential, DefaultAzureCredential
from deltalake import DeltaTable


class TokenClient:
    """Token client manges the refreshing of Azure storage access tokens.


    Parameters
    ----------
    credential
        Azure credential which is used to fetch access tokens.
        A DefaultAzureCredential is used if no credential is provided.

    Attributes
    ----------
    token_obj: AccessToken
        An access token for Azure Blob Storage.
    """

    def __init__(
        self,
        credential: TokenCredential | ChainedTokenCredential | None = None,
    ):
        self._credential = credential or DefaultAzureCredential()
        self.token_obj = self._credential.get_token(
            'https://storage.azure.com/.default'
        )

    def refresh_token(self) -> bool:
        """Refresh the token if it is expired or close to expiry.
        Returns
        -------
        bool
            True if a new token was fetched, False otherwise.
        """
        if self.token_obj.expires_on - 60 <= datetime.now().timestamp():
            self.token_obj = self._credential.get_token(
                'https://storage.azure.com/.default'
            )
            return True
        return False


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

    def __init__(self, table_uri: str, token_client: TokenClient):
        self._table_uri = table_uri
        self._token_client = token_client
        self._delta_table = DeltaTable(table_uri)

    def _refresh_table(self) -> None:
        if self._token_client.refresh_token():
            # There is a new token -> recreate DeltaTable instance
            self._delta_table = DeltaTable(
                self._table_uri,
                storage_options={
                    'azure_storage_token': self._token_client.token_obj.token  # noqa: E501
                },
            )
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
