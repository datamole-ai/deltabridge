import importlib.util
from pathlib import Path


def test_script_imports():
    # scripts/check_databricks_read.py is a manual developer tool, not run by
    # CI. Importing it here guards against API drift (e.g. a renamed public
    # symbol) without a live Databricks workspace; main() is __main__-guarded,
    # so importing runs no network calls.
    script = (
        Path(__file__).resolve().parent.parent
        / 'scripts'
        / 'check_databricks_read.py'
    )
    spec = importlib.util.spec_from_file_location('_check_script', script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
