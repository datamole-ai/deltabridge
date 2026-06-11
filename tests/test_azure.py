from datetime import datetime, timedelta
from unittest.mock import Mock

from azure.core.credentials import AccessToken, TokenCredential

from deltabridge.azure import AzureDeltaClient


def test_token_client_token_not_expired():
    # Create a mock credential
    mock_credential = Mock(spec=TokenCredential)
    mock_credential.get_token.side_effect = [
        AccessToken(
            token='test-token',
            expires_on=int((datetime.now() + timedelta(hours=1)).timestamp()),
        ),
        AssertionError('Token should not be refreshed'),
    ]

    delta_client = AzureDeltaClient(credential=mock_credential)
    for _ in range(2):
        assert delta_client._get_storage_options() == {
            'azure_storage_token': 'test-token'
        }, 'Token should not be refreshed'


def test_token_client_token_expired():
    # Create a mock credential
    mock_credential = Mock(spec=TokenCredential)
    mock_credential.get_token.side_effect = [
        AccessToken(
            token='old-token',
            expires_on=int(datetime.now().timestamp()),
        ),
        AccessToken(
            token='new-token',
            expires_on=int((datetime.now() + timedelta(hours=1)).timestamp()),
        ),
        AssertionError('Token should not be refreshed'),
    ]

    delta_client = AzureDeltaClient(credential=mock_credential)
    for _ in range(3):
        assert delta_client._get_storage_options() == {
            'azure_storage_token': 'new-token'
        }, 'Token should be refreshed'
