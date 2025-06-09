from typing import Protocol


class TokenClient(Protocol):
    """
    Interface for clients providing storage access tokens for Delta tables.
    """

    def refresh_token(self) -> bool:
        """
        Refresh the storage access token.

        Returns
        -------
        bool
            True if a new token was fetched, False otherwise.
        """
        ...

    @property
    def storage_options(self) -> dict[str, str]:
        """
        Storage options which can be passed to the DeltaTable constructor.
        """
        ...
