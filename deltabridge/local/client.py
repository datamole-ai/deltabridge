from deltabridge.base_delta_client import BaseDeltaClient


class LocalDeltaClient(BaseDeltaClient):
    """
    Local Delta client.
    """

    def _get_storage_options(self) -> dict[str, str]:
        """Get the storage options for the Delta table."""
        return {}
