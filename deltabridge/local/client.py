from deltabridge.base_delta_client import BaseDeltaClient
from deltabridge.local.checkpoint import LocalCheckpoint


class LocalDeltaClient(BaseDeltaClient):
    """
    Local Delta client.
    """

    def get_checkpoint(self, path: str) -> LocalCheckpoint:
        """
        Get a checkpoint stored in a local file.

        Parameters
        ----------
        path: str
            Path of the file.

        Returns
        -------
        LocalCheckpoint
            A checkpoint stored in the file.
        """
        return LocalCheckpoint(path)

    def _get_storage_options(self) -> dict[str, str]:
        """Get the storage options for the Delta table."""
        return {}
