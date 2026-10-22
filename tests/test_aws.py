from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from boto3 import Session
from botocore.credentials import Credentials, RefreshableCredentials
from botocore.exceptions import NoCredentialsError

from deltabridge.aws import AwsDeltaClient


def mock_session(credentials, region_name='eu-west-1'):
    session = Mock(spec=Session)
    session.region_name = region_name
    session.get_credentials.return_value = credentials
    return session


def test_aws_client_static_credentials():
    delta_client = AwsDeltaClient(
        session=mock_session(Credentials('test-key', 'test-secret'))
    )
    assert delta_client._get_storage_options() == {
        'aws_access_key_id': 'test-key',
        'aws_secret_access_key': 'test-secret',
        'aws_region': 'eu-west-1',
    }


def test_aws_client_region_from_environment(monkeypatch):
    # boto3 ignores AWS_REGION, which delta-rs reads
    monkeypatch.setenv('AWS_REGION', 'eu-central-1')
    delta_client = AwsDeltaClient(
        session=mock_session(
            Credentials('test-key', 'test-secret'), region_name=None
        )
    )
    assert delta_client._get_storage_options()['aws_region'] == 'eu-central-1'


def test_aws_client_no_region(monkeypatch):
    monkeypatch.delenv('AWS_REGION', raising=False)
    with pytest.raises(ValueError, match='No AWS region'):
        AwsDeltaClient(
            session=mock_session(
                Credentials('test-key', 'test-secret'), region_name=None
            )
        )


def test_aws_client_credentials_expired():
    refresh = Mock(
        side_effect=[
            {
                'access_key': 'new-key',
                'secret_key': 'new-secret',
                'token': 'new-token',
                'expiry_time': (
                    datetime.now(timezone.utc) + timedelta(hours=1)
                ).isoformat(),
            },
            AssertionError('Credentials should not be refreshed'),
        ]
    )
    credentials = RefreshableCredentials(
        access_key='old-key',
        secret_key='old-secret',
        token='old-token',
        expiry_time=datetime.now(timezone.utc),
        refresh_using=refresh,
        method='test',
    )

    delta_client = AwsDeltaClient(session=mock_session(credentials))
    for _ in range(2):
        assert delta_client._get_storage_options() == {
            'aws_access_key_id': 'new-key',
            'aws_secret_access_key': 'new-secret',
            'aws_session_token': 'new-token',
            'aws_region': 'eu-west-1',
        }, 'Credentials should be refreshed'


def test_aws_client_no_credentials():
    with pytest.raises(NoCredentialsError):
        AwsDeltaClient(session=mock_session(None))
