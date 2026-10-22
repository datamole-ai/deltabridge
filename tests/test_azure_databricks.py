from datetime import datetime, timedelta
from unittest.mock import Mock, call

import polars as pl
import pytest
from azure.core.credentials import TokenCredential
from deltalake.writer import write_deltalake

from deltabridge.azure import AzureDatabricksDeltaClient
from deltabridge.local.checkpoint import LocalCheckpoint

TABLE_NAME = 'catalog.schema.table'
TABLE = {'table_id': 'table-id', 'storage_location': 'abfss://t'}
# The same name after DROP TABLE and CREATE TABLE
RECREATED_TABLE = {'table_id': 'new-table-id', 'storage_location': 'abfss://n'}
NOW = datetime(2026, 1, 1, 12)
# Credentials expire after an hour and are refreshed 5 minutes before
REFRESH = NOW + timedelta(minutes=55)


def credentials(sas='sas', expires=NOW + timedelta(hours=1)):
    return {
        'azure_user_delegation_sas': {'sas_token': sas},
        'expiration_time': int(expires.timestamp() * 1000),
    }


@pytest.fixture
def delta_client():
    return AzureDatabricksDeltaClient(
        workspace_url='https://adb-123.azuredatabricks.net',
        credential=Mock(spec=TokenCredential),
    )


@pytest.fixture
def now(mocker):
    """Current time of the Databricks client."""
    datetime_mock = mocker.patch('deltabridge.azure.databricks.datetime')
    datetime_mock.now.return_value = NOW
    return datetime_mock.now


def get_storage_options_fn(delta_client, mocker):
    """Storage options function of a table client for TABLE_NAME."""
    delta_table_client = mocker.patch(
        'deltabridge.azure.databricks.DeltaTableClient'
    )
    delta_client.get_table_client(TABLE_NAME)
    return delta_table_client.call_args.kwargs['storage_options_fn']


def test_databricks_client_get_table_client(delta_client, mocker):
    delta_table_client = mocker.patch(
        'deltabridge.azure.databricks.DeltaTableClient'
    )
    request = mocker.patch.object(delta_client, '_request', return_value=TABLE)

    delta_client.get_table_client(TABLE_NAME)

    request.assert_called_once_with('GET', f'tables/{TABLE_NAME}')
    assert delta_table_client.call_args.kwargs['table_uri'] == 'abfss://t'


def test_databricks_client_credentials_expired(delta_client, now, mocker):
    request = mocker.patch.object(
        delta_client,
        '_request',
        side_effect=[
            TABLE,
            credentials('old-sas'),
            TABLE,
            credentials('new-sas', expires=REFRESH + timedelta(hours=1)),
            AssertionError('Credentials should not be refreshed'),
        ],
    )
    storage_options_fn = get_storage_options_fn(delta_client, mocker)

    storage_options_fn()
    now.return_value = REFRESH - timedelta(seconds=1)
    assert storage_options_fn() == {'azure_storage_sas_key': 'old-sas'}
    now.return_value = REFRESH
    for _ in range(2):
        assert storage_options_fn() == {'azure_storage_sas_key': 'new-sas'}, (
            'Credentials should be refreshed'
        )

    get_table = call('GET', f'tables/{TABLE_NAME}')
    get_credentials = call(
        'POST',
        'temporary-table-credentials',
        json={'table_id': 'table-id', 'operation': 'READ'},
    )
    assert request.call_args_list == [
        get_table,
        get_credentials,
        get_table,
        get_credentials,
    ], 'The table name should be checked before refreshing credentials'


def test_databricks_client_recreated_table_fails(delta_client, now, mocker):
    request = mocker.patch.object(
        delta_client,
        '_request',
        side_effect=[TABLE, credentials(), RECREATED_TABLE, RECREATED_TABLE],
    )
    storage_options_fn = get_storage_options_fn(delta_client, mocker)
    storage_options_fn()

    now.return_value = REFRESH
    for _ in range(2):
        with pytest.raises(ValueError, match='now refers to another table'):
            storage_options_fn()
    assert request.call_count == 4, 'Each call after a failure should check'


def test_databricks_stream_recreated_table_fails(
    delta_client, now, mocker, tmp_path
):
    table_uri = str(tmp_path / 'table')
    write_deltalake(table_uri, pl.DataFrame({'id': [1]}))
    mocker.patch.object(
        delta_client,
        '_request',
        side_effect=[
            {'table_id': 'table-id', 'storage_location': table_uri},
            credentials(),
            RECREATED_TABLE,
        ],
    )
    table_client = delta_client.get_table_client(TABLE_NAME)
    stream = table_client.load_as_stream(
        LocalCheckpoint(str(tmp_path / 'checkpoint.json'))
    )
    stream.process_batch(Mock())

    # The dropped table stays in storage, where no new versions appear
    now.return_value = REFRESH
    with pytest.raises(ValueError, match='now refers to another table'):
        stream.process_batch(Mock())
