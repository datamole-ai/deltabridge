from datetime import datetime, timedelta
from unittest.mock import Mock

from azure.core.credentials import TokenCredential

from deltabridge.azure import AzureDatabricksDeltaClient


def _credentials(sas_token: str, expires_on: datetime) -> dict:
    return {
        'azure_user_delegation_sas': {'sas_token': sas_token},
        'expiration_time': int(expires_on.timestamp() * 1000),
    }


def test_databricks_client_get_table_client(mocker):
    delta_table_client = mocker.patch(
        'deltabridge.azure.databricks.DeltaTableClient'
    )
    delta_client = AzureDatabricksDeltaClient(
        workspace_url='https://adb-123.azuredatabricks.net',
        credential=Mock(spec=TokenCredential),
    )
    request = mocker.patch.object(
        delta_client,
        '_request',
        return_value={'table_id': 'table-id', 'storage_location': 'abfss://t'},
    )

    delta_client.get_table_client('catalog.schema.table')

    request.assert_called_once_with('GET', 'tables/catalog.schema.table')
    assert delta_table_client.call_args.kwargs['table_uri'] == 'abfss://t'


def test_databricks_client_credentials_not_expired(mocker):
    delta_client = AzureDatabricksDeltaClient(
        workspace_url='https://adb-123.azuredatabricks.net',
        credential=Mock(spec=TokenCredential),
    )
    mocker.patch.object(
        delta_client,
        '_request',
        side_effect=[
            _credentials('test-sas', datetime.now() + timedelta(hours=1)),
            AssertionError('Credentials should not be refreshed'),
        ],
    )

    for _ in range(2):
        assert delta_client._get_storage_options('table-id') == {
            'azure_storage_sas_key': 'test-sas'
        }, 'Credentials should not be refreshed'


def test_databricks_client_credentials_expired(mocker):
    delta_client = AzureDatabricksDeltaClient(
        workspace_url='https://adb-123.azuredatabricks.net',
        credential=Mock(spec=TokenCredential),
    )
    request = mocker.patch.object(
        delta_client,
        '_request',
        side_effect=[
            _credentials('old-sas', datetime.now()),
            _credentials('new-sas', datetime.now() + timedelta(hours=1)),
            AssertionError('Credentials should not be refreshed'),
        ],
    )

    delta_client._get_storage_options('table-id')
    for _ in range(2):
        assert delta_client._get_storage_options('table-id') == {
            'azure_storage_sas_key': 'new-sas'
        }, 'Credentials should be refreshed'
    request.assert_called_with(
        'POST',
        'temporary-table-credentials',
        json={'table_id': 'table-id', 'operation': 'READ'},
    )
