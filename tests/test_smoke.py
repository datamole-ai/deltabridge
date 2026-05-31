def test_smoke():
    from deltabridge.azure.client import (
        AzureDeltaClient,  # noqa: F401
    )
    from deltabridge.base_delta_client import BaseDeltaClient  # noqa: F401
    from deltabridge.client import DeltaTableClient  # noqa: F401
