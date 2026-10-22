from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
from azure.core.credentials import AccessToken, TokenCredential
from azure.core.exceptions import ResourceNotFoundError

from deltabridge.azure import AzureDeltaClient
from deltabridge.azure.checkpoint import AzureCheckpoint
from deltabridge.base_checkpoint import StreamOffset


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


def test_azure_checkpoint_parses_uri(mocker):
    blob_client = mocker.patch('deltabridge.azure.checkpoint.BlobClient')
    mock_credential = Mock(spec=TokenCredential)

    AzureCheckpoint(
        'abfss://container@account.dfs.core.windows.net/dir/checkpoint.json',
        mock_credential,
    )

    blob_client.assert_called_once_with(
        account_url='https://account.blob.core.windows.net',
        container_name='container',
        blob_name='dir/checkpoint.json',
        credential=mock_credential,
    )


@pytest.mark.parametrize(
    'uri',
    [
        'https://account.blob.core.windows.net/container/checkpoint.json',
        'abfs://container@account.dfs.core.windows.net/checkpoint.json',
        'abfss://account.dfs.core.windows.net/checkpoint.json',
        'abfss://container@account.dfs.core.windows.net/',
        'checkpoints/checkpoint.json',
    ],
)
def test_azure_checkpoint_rejects_other_uris(uri):
    with pytest.raises(ValueError):
        AzureCheckpoint(uri, Mock(spec=TokenCredential))


def test_azure_checkpoint_save_and_load(mocker):
    blob = mocker.patch('deltabridge.azure.checkpoint.BlobClient').return_value
    blob.download_blob.side_effect = ResourceNotFoundError('BlobNotFound')
    checkpoint = AzureCheckpoint(
        'abfss://container@account.dfs.core.windows.net/checkpoint.json',
        Mock(spec=TokenCredential),
    )
    offset = StreamOffset('table-id', 42)

    assert checkpoint.load() is None
    checkpoint.save(offset)

    data = blob.upload_blob.call_args.args[0]
    blob.upload_blob.assert_called_once_with(data, overwrite=True)
    blob.download_blob.side_effect = None
    blob.download_blob.return_value.readall.return_value = data
    assert checkpoint.load() == offset
