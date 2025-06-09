def test_smoke():
    from dtml.delta.azure.client import AzureDeltaTableClient  # noqa: F401
    from dtml.delta.azure.token import AzureTokenClient  # noqa: F401
    from dtml.delta.client import DeltaTableClient  # noqa: F401
    from dtml.delta.token import TokenClient  # noqa: F401
