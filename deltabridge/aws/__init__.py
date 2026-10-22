try:
    from deltabridge.aws.client import AwsDeltaClient
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(
        "AwsDeltaClient requires the 'aws' extra: "
        "pip install 'deltabridge[aws]'"
    ) from e
