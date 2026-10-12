from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
from azure.core.credentials import TokenCredential

from deltabridge.azure import AzureDatabricksDeltaClient


@pytest.fixture
def delta_client():
    return AzureDatabricksDeltaClient(
        workspace_url='https://adb-123.azuredatabricks.net',
        credential=Mock(spec=TokenCredential),
    )


def test_databricks_client_get_table_client(delta_client, mocker):
    delta_table_client = mocker.patch(
        'deltabridge.azure.databricks.DeltaTableClient'
    )
    request = mocker.patch.object(
        delta_client,
        '_request',
        return_value={'table_id': 'table-id', 'storage_location': 'abfss://t'},
    )

    delta_client.get_table_client('catalog.schema.table')

    request.assert_called_once_with('GET', 'tables/catalog.schema.table')
    assert delta_table_client.call_args.kwargs['table_uri'] == 'abfss://t'


def test_databricks_client_credentials_expired(delta_client, mocker):
    request = mocker.patch.object(
        delta_client,
        '_request',
        side_effect=[
            {
                'azure_user_delegation_sas': {'sas_token': 'old-sas'},
                'expiration_time': int(
                    (datetime.now() + timedelta(minutes=4)).timestamp() * 1000
                ),
            },
            {
                'azure_user_delegation_sas': {'sas_token': 'new-sas'},
                'expiration_time': int(
                    (datetime.now() + timedelta(hours=1)).timestamp() * 1000
                ),
            },
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
