from __future__ import annotations

from datetime import datetime
from functools import partial
from typing import Any

from azure.core import PipelineClient
from azure.core.credentials import TokenCredential
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.rest import HttpRequest
from azure.identity import ChainedTokenCredential, DefaultAzureCredential

from deltabridge.client import DeltaTableClient


class AzureDatabricksDeltaClient:
    """Databricks client reads Unity Catalog tables with vended credentials.

    Parameters
    ----------
    workspace_url
        URL of the Azure Databricks workspace.
    credential
        Azure credential which is used to authenticate to Databricks.
        A DefaultAzureCredential is used if no credential is provided.
    """

    def __init__(
        self,
        workspace_url: str,
        credential: TokenCredential | ChainedTokenCredential | None = None,
    ):
        self._api_url = f'{workspace_url.rstrip("/")}/api/2.1/unity-catalog'
        self._client = PipelineClient(
            base_url=workspace_url,
            per_retry_policies=[
                BearerTokenCredentialPolicy(
                    credential or DefaultAzureCredential(),
                    # Application ID of Azure Databricks
                    '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
                )
            ],
        )
        self._credentials: dict[str, dict[str, Any]] = {}

    def get_table_client(self, table_name: str) -> DeltaTableClient:
        """
        Get a table client for the given Unity Catalog table.

        Parameters
        ----------
        table_name: str
            Full name of the table, i.e. `catalog.schema.table`.

        Returns
        -------
        DeltaTableClient
            A table client for the given table.
        """
        table = self._request('GET', f'tables/{table_name}')
        return DeltaTableClient(
            table_uri=table['storage_location'],
            storage_options_fn=partial(
                self._get_storage_options, table['table_id']
            ),
        )

    def _get_credentials(self, table_id: str) -> dict[str, Any]:
        return self._request(
            'POST',
            'temporary-table-credentials',
            json={'table_id': table_id, 'operation': 'READ'},
        )

    def _refresh_credentials(self, table_id: str) -> None:
        """Refresh the credentials if they are expired or close to expiry."""
        credentials = self._credentials.get(table_id)
        # 5 minutes before expiry, so that a long scan started now does not
        # outlive the credentials
        if (
            credentials is None
            or credentials['expiration_time'] / 1000 - 300
            <= datetime.now().timestamp()
        ):
            self._credentials[table_id] = self._get_credentials(table_id)

    def _get_storage_options(self, table_id: str) -> dict[str, str]:
        """Get the storage options for the Delta table."""
        self._refresh_credentials(table_id)
        credentials = self._credentials[table_id]
        sas_token = credentials['azure_user_delegation_sas']['sas_token']
        return {'azure_storage_sas_key': sas_token}

    def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> dict[str, Any]:
        request = HttpRequest(method, f'{self._api_url}/{path}', **kwargs)
        response = self._client.send_request(request)
        response.raise_for_status()
        return response.json()
