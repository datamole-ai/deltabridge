from __future__ import annotations

from datetime import datetime

from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import ChainedTokenCredential, DefaultAzureCredential

from deltabridge.azure.checkpoint import AzureCheckpoint
from deltabridge.base_delta_client import BaseDeltaClient


class AzureDeltaClient(BaseDeltaClient):
    """Token client manges the refreshing of Azure storage access tokens.


    Parameters
    ----------
    credential
        Azure credential which is used to fetch access tokens.
        A DefaultAzureCredential is used if no credential is provided.
    """

    def __init__(
        self,
        credential: TokenCredential | ChainedTokenCredential | None = None,
    ):
        self._credential = credential or DefaultAzureCredential()
        self._token_obj = self._get_token()

    def get_checkpoint(self, uri: str) -> AzureCheckpoint:
        """
        Get a checkpoint stored in a blob in Azure Blob Storage.

        Parameters
        ----------
        uri: str
            URI of the blob, of the same form as table URIs, i.e.
            `abfss://<container>@<account>.dfs.core.windows.net/<path>`.
            The credential must be allowed to write the blob.

        Returns
        -------
        AzureCheckpoint
            A checkpoint stored in the blob.
        """
        return AzureCheckpoint(uri, self._credential)

    def _get_token(self) -> AccessToken:
        return self._credential.get_token('https://storage.azure.com/.default')

    def _refresh_token(self) -> None:
        """Refresh the token if it is expired or close to expiry."""
        if self._token_obj.expires_on - 60 <= datetime.now().timestamp():
            self._token_obj = self._get_token()

    def _get_storage_options(self) -> dict[str, str]:
        """Get the storage options for the Delta table."""
        self._refresh_token()
        return {'azure_storage_token': self._token_obj.token}
