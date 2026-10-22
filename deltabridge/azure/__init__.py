try:
    from deltabridge.azure.client import AzureDeltaClient
    from deltabridge.azure.databricks import AzureDatabricksDeltaClient
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(
        "The Azure clients require the 'azure' extra: "
        "pip install 'deltabridge[azure]'"
    ) from e
