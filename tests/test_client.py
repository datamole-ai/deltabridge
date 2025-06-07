from datetime import datetime, timedelta
from unittest.mock import Mock

from azure.core.credentials import AccessToken, TokenCredential

from dtml.delta.client import TokenClient


def test_token_client_token_not_expired():
    # Create a mock credential
    mock_credential = Mock(spec=TokenCredential)
    mock_credential.get_token.return_value = AccessToken(
        token='test-token',
        expires_on=int((datetime.now() + timedelta(hours=1)).timestamp()),
    )

    token_client = TokenClient(credential=mock_credential)
    assert token_client.token_obj is not None
    assert token_client.token_obj.token == 'test-token'
    assert token_client.refresh_token() is False, (
        'Token should not be refreshed'
    )
    assert token_client.token_obj.token == 'test-token'


def test_token_client_token_expired():
    # Create a mock credential
    mock_credential = Mock(spec=TokenCredential)
    mock_credential.get_token.side_effect = [
        AccessToken(
            token='old-token',
            expires_on=int(
                (datetime.now() + timedelta(seconds=10)).timestamp()
            ),
        ),
        AccessToken(
            token='new-token',
            expires_on=int((datetime.now() + timedelta(hours=1)).timestamp()),
        ),
    ]

    token_client = TokenClient(credential=mock_credential)
    assert token_client.token_obj.token == 'old-token'
    assert token_client.refresh_token() is True, 'Token should be refreshed'
    assert token_client.token_obj.token == 'new-token'
    assert token_client.refresh_token() is False, (
        'Token should not be refreshed'
    )
    assert token_client.token_obj.token == 'new-token'
