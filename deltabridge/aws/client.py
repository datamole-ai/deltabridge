from __future__ import annotations

from boto3 import Session
from botocore.credentials import ReadOnlyCredentials
from botocore.exceptions import NoCredentialsError

from deltabridge.base_delta_client import BaseDeltaClient


class AwsDeltaClient(BaseDeltaClient):
    """Credential client manages the refreshing of AWS S3 credentials.

    Parameters
    ----------
    session
        boto3 session which is used to fetch credentials and the region.
        A default session is used if no session is provided.
    """

    def __init__(self, session: Session | None = None):
        self._session = session or Session()
        self._region = self._session.region_name
        self._credentials = self._get_credentials()

    def _get_credentials(self) -> ReadOnlyCredentials:
        credentials = self._session.get_credentials()
        if credentials is None:
            raise NoCredentialsError()
        return credentials.get_frozen_credentials()

    def _refresh_credentials(self) -> None:
        """Refresh the credentials, which botocore does close to expiry."""
        # Freezing refreshes temporary credentials (e.g. of an assumed role)
        # when needed; static credentials have no expiry and never change
        self._credentials = self._get_credentials()

    def _get_storage_options(self) -> dict[str, str]:
        """Get the storage options for the Delta table."""
        self._refresh_credentials()
        storage_options = {
            'aws_access_key_id': self._credentials.access_key,
            'aws_secret_access_key': self._credentials.secret_key,
        }
        if self._credentials.token:
            storage_options['aws_session_token'] = self._credentials.token
        if self._region:
            storage_options['aws_region'] = self._region
        return storage_options
