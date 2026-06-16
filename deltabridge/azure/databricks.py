from __future__ import annotations

import time
from typing import Any

from azure.core import PipelineClient
from azure.core.credentials import TokenCredential
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.rest import HttpRequest
from azure.identity import ChainedTokenCredential, DefaultAzureCredential

from deltabridge.client import DeltaTableClient

# Resource ID of the Azure Databricks first-party application. A Microsoft
# Entra ID token for this scope authenticates as a bearer token to the
# Databricks REST API. It is fixed and not workspace-specific.
_DATABRICKS_AAD_SCOPE = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default'

# Refresh vended storage credentials this many seconds before they expire, so
# a scan starting just before expiry does not outlive its credential.
_EXPIRY_MARGIN_SECONDS = 300


class AzureDatabricksDeltaClient:
    """Client for reading Unity Catalog tables via credential vending.

    Reads a Unity Catalog managed (or external) table directly from its
    underlying Azure storage, using short-lived credentials vended by the
    Unity Catalog ``temporary-table-credentials`` API. No SQL warehouse or
    cluster is required.

    Parameters
    ----------
    workspace_url
        Azure Databricks workspace URL, e.g.
        ``https://adb-123.4.azuredatabricks.net``.
    credential
        Azure credential used to obtain Microsoft Entra ID tokens for the
        Databricks REST API. A ``DefaultAzureCredential`` is used if none is
        provided.

    Notes
    -----
    Prerequisites on the Databricks side:

    * the credential's identity (managed identity, service principal, ...)
      must be a workspace principal with the ``EXTERNAL USE SCHEMA`` privilege
      on the schema (or parent catalog);
    * the metastore must have external data access enabled;
    * the table must not use row filters or column masks and must not be a
      view - credential vending rejects those.
    """

    def __init__(
        self,
        workspace_url: str,
        credential: TokenCredential | ChainedTokenCredential | None = None,
    ):
        if not workspace_url:
            raise ValueError('workspace_url is required.')
        self._base_url = workspace_url.rstrip('/')
        credential = credential or DefaultAzureCredential()
        # The bearer-token policy fetches an Entra ID token for the Databricks
        # resource and attaches it as the Authorization header, caching and
        # refreshing it near expiry. As a per-retry policy it re-applies the
        # header on each attempt (e.g. after a 401 challenge).
        self._client: PipelineClient = PipelineClient(
            base_url=self._base_url,
            per_retry_policies=[
                BearerTokenCredentialPolicy(credential, _DATABRICKS_AAD_SCOPE)
            ],
        )

    def get_table_client(self, table_full_name: str) -> DeltaTableClient:
        """Get a table client for a Unity Catalog table.

        Parameters
        ----------
        table_full_name
            Fully qualified table name, ``catalog.schema.table``.

        Returns
        -------
        DeltaTableClient
            A table client reading the table's underlying storage location,
            authenticated with automatically refreshed temporary credentials.
        """
        table_id, storage_location = self._resolve_table(table_full_name)
        vend_storage_options = _TableCredentialVendor(self, table_id)
        return DeltaTableClient(
            table_uri=storage_location,
            storage_options_fn=vend_storage_options,
        )

    def _resolve_table(self, table_full_name: str) -> tuple[str, str]:
        """Resolve ``catalog.schema.table`` -> (table_id, storage_location)."""
        table = self._get(f'/api/2.1/unity-catalog/tables/{table_full_name}')
        return table['table_id'], table['storage_location']

    def _vend_credentials(self, table_id: str) -> dict[str, Any]:
        return self._post(
            '/api/2.1/unity-catalog/temporary-table-credentials',
            body={'table_id': table_id, 'operation': 'READ'},
        )

    def _get(self, path: str) -> dict[str, Any]:
        return self._request('GET', path)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request('POST', path, json=body)

    def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> dict[str, Any]:
        request = HttpRequest(method, f'{self._base_url}{path}', **kwargs)
        response = self._client.send_request(request)
        # Raises HttpResponseError on a non-success status, surfacing the
        # Databricks error message (e.g. a missing EXTERNAL USE SCHEMA grant
        # or disabled external data access).
        response.raise_for_status()
        return response.json()


class _TableCredentialVendor:
    """Callable that vends and caches storage options for a single table.

    Used as the ``storage_options_fn`` of a ``DeltaTableClient``. It calls
    the Unity Catalog ``temporary-table-credentials`` API and refreshes the
    credential shortly before it expires, so a long-running service keeps a
    valid credential without recreating the client.
    """

    def __init__(self, client: AzureDatabricksDeltaClient, table_id: str):
        self._client = client
        self._table_id = table_id
        self._storage_options: dict[str, str] = {}
        self._expires_at = 0.0

    def __call__(self) -> dict[str, str]:
        if time.time() + _EXPIRY_MARGIN_SECONDS >= self._expires_at:
            credentials = self._client._vend_credentials(self._table_id)
            self._storage_options = _to_storage_options(credentials)
            # `expiration_time` is the expiry as Unix epoch milliseconds.
            self._expires_at = credentials['expiration_time'] / 1000
        return self._storage_options


def _to_storage_options(credentials: dict[str, Any]) -> dict[str, str]:
    """Map vended Unity Catalog credentials to delta-rs storage options."""
    if 'azure_user_delegation_sas' not in credentials:
        # An Azure workspace vends an Azure SAS; anything else is an
        # unexpected response for this Azure-only client.
        raise ValueError(
            'Vended Unity Catalog credentials did not contain an Azure '
            'user-delegation SAS (azure_user_delegation_sas).'
        )
    sas_token = credentials['azure_user_delegation_sas']['sas_token']
    # `azure_storage_sas_key` is object_store's Azure SAS config key.
    # The SAS token is passed as returned (already percent-encoded,
    # without a leading '?').
    return {'azure_storage_sas_key': sas_token}
