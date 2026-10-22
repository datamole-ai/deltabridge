from __future__ import annotations

import re

from azure.core.credentials import TokenCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ChainedTokenCredential
from azure.storage.blob import BlobClient

from deltabridge.base_checkpoint import BaseCheckpoint

# Container, account and blob path of a URI of the same form as table URIs
_URI_PATTERN = re.compile(
    r'abfss://([^@/]+)@([^./]+)\.dfs\.core\.windows\.net/(.+)'
)


class AzureCheckpoint(BaseCheckpoint):
    """Checkpoint stored in a blob in Azure Blob Storage.

    Parameters
    ----------
    uri: str
        URI of the blob, of the same form as table URIs, i.e.
        `abfss://<container>@<account>.dfs.core.windows.net/<path>`.
    credential
        Azure credential which is used to access the blob.
        It must be allowed to write the blob.
    """

    def __init__(
        self,
        uri: str,
        credential: TokenCredential | ChainedTokenCredential,
    ):
        match = _URI_PATTERN.fullmatch(uri)
        if match is None:
            raise ValueError(
                'The checkpoint URI must have the form '
                'abfss://<container>@<account>.dfs.core.windows.net/<path>.'
            )
        container, account, path = match.groups()
        # The credential refreshes its tokens, unlike a token string
        self._blob = BlobClient(
            account_url=f'https://{account}.blob.core.windows.net',
            container_name=container,
            blob_name=path,
            credential=credential,
        )

    def _read(self) -> bytes | None:
        try:
            return self._blob.download_blob().readall()
        except ResourceNotFoundError:
            return None

    def _write(self, data: bytes) -> None:
        # A single Put Blob replaces the blob atomically
        self._blob.upload_blob(data, overwrite=True)
