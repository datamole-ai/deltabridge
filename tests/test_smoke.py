def test_smoke():
    from dtml.delta.azure.client import (
        AzureDeltaClient,  # noqa: F401
    )
    from dtml.delta.base_delta_client import BaseDeltaClient  # noqa: F401
    from dtml.delta.client import DeltaTableClient  # noqa: F401
