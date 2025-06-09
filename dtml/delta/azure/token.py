from __future__ import annotations

from datetime import datetime

from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import ChainedTokenCredential, DefaultAzureCredential
from dtml.delta.token import TokenClient


class AzureTokenClient(TokenClient):
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

    __default: AzureTokenClient | None = None

    def __init__(
        self,
        credential: TokenCredential | ChainedTokenCredential | None = None,
    ):
        self._credential = credential or DefaultAzureCredential()
        self.token_obj = self._get_token()

    def _get_token(self) -> AccessToken:
        return self._credential.get_token('https://storage.azure.com/.default')

    def refresh_token(self) -> bool:
        """Refresh the token if it is expired or close to expiry.
        Returns
        -------
        bool
            True if a new token was fetched, False otherwise.
        """
        if self.token_obj.expires_on - 60 <= datetime.now().timestamp():
            self.token_obj = self._get_token()
            return True
        return False

    @property
    def storage_options(self) -> dict[str, str]:
        return {'azure_storage_token': self.token_obj.token}

    @staticmethod
    def default() -> AzureTokenClient:
        """Default token client."""
        if AzureTokenClient.__default is None:
            AzureTokenClient.__default = AzureTokenClient()
        return AzureTokenClient.__default
