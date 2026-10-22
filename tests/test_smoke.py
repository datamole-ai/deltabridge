import importlib
import sys

import pytest


def test_smoke():
    from deltabridge.aws.client import AwsDeltaClient  # noqa: F401
    from deltabridge.azure.client import (
        AzureDeltaClient,  # noqa: F401
    )
    from deltabridge.azure.databricks import (
        AzureDatabricksDeltaClient,  # noqa: F401
    )
    from deltabridge.base_delta_client import BaseDeltaClient  # noqa: F401
    from deltabridge.client import DeltaTableClient  # noqa: F401


@pytest.mark.parametrize(
    ('provider', 'sdk'), [('aws', 'boto3'), ('azure', 'azure.identity')]
)
def test_missing_extra(monkeypatch, provider, sdk):
    # Simulate an installation without the provider extra
    monkeypatch.setitem(sys.modules, sdk, None)
    for name in list(sys.modules):
        if name.startswith(f'deltabridge.{provider}'):
            monkeypatch.delitem(sys.modules, name)

    with pytest.raises(ModuleNotFoundError, match=rf'deltabridge\[{provider}'):
        importlib.import_module(f'deltabridge.{provider}')
