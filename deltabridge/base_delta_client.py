from abc import ABC, abstractmethod

from deltabridge.client import DeltaTableClient


class BaseDeltaClient(ABC):
    """
    Interface for clients providing storage access tokens for Delta tables.
    """

    @abstractmethod
    def _get_storage_options(self) -> dict[str, str]:
        """
        Storage options which can be passed to the DeltaTable constructor.
        """
        ...

    def get_table_client(self, table_uri: str) -> DeltaTableClient:
        """
        Get a table client for the given table URI.

        Parameters
        ----------
        table_uri: str
            URI of the Delta table.

        Returns
        -------
        DeltaTableClient
            A table client for the given table URI.
        """
        return DeltaTableClient(
            table_uri=table_uri, storage_options_fn=self._get_storage_options
        )
