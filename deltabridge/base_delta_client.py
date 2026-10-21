from abc import ABC, abstractmethod

from deltabridge.base_checkpoint import BaseCheckpoint
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

    @abstractmethod
    def get_checkpoint(self, uri: str) -> BaseCheckpoint:
        """
        Get a checkpoint which stores the offset of a stream.

        Parameters
        ----------
        uri: str
            URI of the file which stores the checkpoint.

        Returns
        -------
        BaseCheckpoint
            A checkpoint stored in the file.
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
