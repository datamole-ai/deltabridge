from dtml.delta.azure.token import AzureTokenClient
from dtml.delta.client import DeltaTableClient
from dtml.delta.token import TokenClient


class AzureDeltaTableClient(DeltaTableClient):
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
        self, table_uri: str, token_client: TokenClient | None = None
    ):
        if token_client is None:
            token_client = AzureTokenClient.default()
        super().__init__(table_uri=table_uri, token_client=token_client)
