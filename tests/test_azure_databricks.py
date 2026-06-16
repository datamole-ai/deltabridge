import time
from unittest.mock import Mock

import pytest
from azure.core.credentials import AccessToken, TokenCredential
from azure.core.exceptions import HttpResponseError

from deltabridge.azure import AzureDatabricksDeltaClient
from deltabridge.azure.databricks import (
    _DATABRICKS_AAD_SCOPE,
    _TableCredentialVendor,
    _to_storage_options,
)

WORKSPACE_URL = 'https://example.azuredatabricks.net'
TABLE_FULL_NAME = 'main.sales.orders'
STORAGE_LOCATION = 'abfss://container@acct.dfs.core.windows.net/orders'
TABLES_URL = f'{WORKSPACE_URL}/api/2.1/unity-catalog/tables/{TABLE_FULL_NAME}'
CREDENTIALS_URL = (
    f'{WORKSPACE_URL}/api/2.1/unity-catalog/temporary-table-credentials'
)


def _mock_credential(token='aad-token'):
    credential = Mock(spec=TokenCredential)
    credential.get_token.return_value = AccessToken(
        token=token, expires_on=int(time.time() + 3600)
    )
    return credential


def _make_client(credential=None):
    return AzureDatabricksDeltaClient(
        workspace_url=WORKSPACE_URL,
        credential=credential or _mock_credential(),
    )


def _tables_body():
    return {'table_id': 'tbl-123', 'storage_location': STORAGE_LOCATION}


def _credentials_body(sas_token, expires_in_seconds=3600):
    return {
        'azure_user_delegation_sas': {'sas_token': sas_token},
        'url': STORAGE_LOCATION,
        'expiration_time': int((time.time() + expires_in_seconds) * 1000),
    }


def test_get_table_client_resolves_location_and_vends_sas(
    requests_mock, mocker
):
    requests_mock.get(TABLES_URL, json=_tables_body())
    requests_mock.post(
        CREDENTIALS_URL, json=_credentials_body('sig=abc&se=2026')
    )

    captured = {}

    def fake_delta_table_client(table_uri, storage_options_fn):
        captured['table_uri'] = table_uri
        captured['storage_options_fn'] = storage_options_fn
        return mocker.Mock()

    mocker.patch(
        'deltabridge.azure.databricks.DeltaTableClient',
        side_effect=fake_delta_table_client,
    )

    credential = _mock_credential()
    client = _make_client(credential=credential)
    client.get_table_client(TABLE_FULL_NAME)

    # The token is requested for the Azure Databricks resource scope.
    assert credential.get_token.call_args.args == (_DATABRICKS_AAD_SCOPE,)
    # The resolved storage location is used as the table URI.
    assert captured['table_uri'] == STORAGE_LOCATION
    # The vended SAS is mapped onto the correct delta-rs storage option.
    assert captured['storage_options_fn']() == {
        'azure_storage_sas_key': 'sig=abc&se=2026'
    }
    # The table is resolved via the Unity Catalog tables API, with the
    # Entra ID token sent as the bearer token.
    first_request = requests_mock.request_history[0]
    assert first_request.method == 'GET'
    assert first_request.url.endswith(f'/tables/{TABLE_FULL_NAME}')
    assert first_request.headers['Authorization'] == 'Bearer aad-token'


def test_credentials_reused_while_valid(requests_mock):
    matcher = requests_mock.post(
        CREDENTIALS_URL, json=_credentials_body('sig=valid')
    )

    client = _make_client()
    vendor = _TableCredentialVendor(client, 'tbl-123')

    first = vendor()
    second = vendor()

    assert matcher.call_count == 1, 'credential should be cached until expiry'
    assert first == second == {'azure_storage_sas_key': 'sig=valid'}


def test_credentials_refreshed_when_expired(requests_mock):
    calls = {'count': 0}

    def body(request, context):
        calls['count'] += 1
        # Already past the refresh margin -> every call re-vends.
        return _credentials_body(
            f'sig={calls["count"]}', expires_in_seconds=-10
        )

    matcher = requests_mock.post(CREDENTIALS_URL, json=body)

    client = _make_client()
    vendor = _TableCredentialVendor(client, 'tbl-123')

    first = vendor()
    second = vendor()

    assert matcher.call_count == 2, 'expired credential should be refreshed'
    assert first != second, 'refreshed credential should be used'


def test_api_error_surfaces_response_body(requests_mock):
    requests_mock.get(
        TABLES_URL,
        status_code=403,
        reason='Forbidden',
        json={
            'error_code': 'PERMISSION_DENIED',
            'message': 'User lacks EXTERNAL USE SCHEMA on the schema.',
        },
    )

    client = _make_client()
    with pytest.raises(HttpResponseError, match='EXTERNAL USE SCHEMA'):
        client.get_table_client(TABLE_FULL_NAME)


def test_to_storage_options_without_azure_sas_raises():
    with pytest.raises(ValueError):
        _to_storage_options({'aws_temp_credentials': {'access_key_id': 'x'}})


def test_empty_workspace_url_raises():
    with pytest.raises(ValueError):
        AzureDatabricksDeltaClient(workspace_url='')
