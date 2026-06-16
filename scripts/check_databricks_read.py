"""Manually read a Unity Catalog table via deltabridge.

This is a developer tool, not part of the automated test suite: it reads a
Unity Catalog table from a real Azure Databricks workspace end to end (Unity
Catalog credential vending -> direct storage read with Polars) to confirm the
whole path works against a live workspace.

Usage
-----
Pass the fully qualified table name and point it at your workspace:

    export DATABRICKS_HOST='https://adb-1234567890.12.azuredatabricks.net'
    # `az login`, or set AZURE_TENANT_ID for an interactive device-code login
    uv run python scripts/check_databricks_read.py catalog.schema.table

The workspace URL defaults to $DATABRICKS_HOST and can be overridden with
--workspace-url.

Add `--verify-pruning COLUMN VALUE` to read with a `COLUMN == VALUE` filter
under POLARS_VERBOSE, to observe how many data files the predicate skips:

    uv run python scripts/check_databricks_read.py \\
        catalog.schema.table --verify-pruning location CZ

Authentication uses `DeviceCodeCredential` when AZURE_TENANT_ID is set (handy
for a personal account), otherwise the ambient `DefaultAzureCredential` chain
(`az login`, managed identity, AZURE_CLIENT_ID/SECRET, ...).

Prerequisites on the Databricks side (see README): external data access
enabled on the metastore, and the authenticating identity registered as a
workspace principal with `EXTERNAL USE SCHEMA` on the schema (or parent
catalog). Tables with row filters / column masks, views, or the
`columnMapping` reader feature are not readable this way.
"""

from __future__ import annotations

import argparse
import os

import polars as pl
from azure.identity import DefaultAzureCredential, DeviceCodeCredential

from deltabridge.azure import AzureDatabricksDeltaClient


def _build_credential():
    # A tenant id triggers an interactive device-code login; otherwise fall
    # back to the ambient credential chain (az login, managed identity, ...).
    tenant_id = os.environ.get('AZURE_TENANT_ID')
    if tenant_id:
        return DeviceCodeCredential(tenant_id=tenant_id)
    return DefaultAzureCredential()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'table',
        help='Fully qualified table name catalog.schema.table.',
    )
    parser.add_argument(
        '--workspace-url',
        default=os.environ.get('DATABRICKS_HOST'),
        help='Workspace URL (default: $DATABRICKS_HOST), e.g. '
        'https://adb-1234567890.12.azuredatabricks.net',
    )
    parser.add_argument(
        '--verify-pruning',
        nargs=2,
        metavar=('COLUMN', 'VALUE'),
        help='Read with COLUMN == VALUE under POLARS_VERBOSE and report how '
        'many data files the predicate skips.',
    )
    args = parser.parse_args()

    if not args.workspace_url:
        parser.error(
            'set DATABRICKS_HOST or pass --workspace-url, e.g. '
            'https://adb-1234567890.12.azuredatabricks.net'
        )

    print(f'Workspace: {args.workspace_url}')
    print(f'Table:     {args.table}\n')

    client = AzureDatabricksDeltaClient(
        credential=_build_credential(),
        workspace_url=args.workspace_url,
    )

    # Resolves table_id + storage location and wires up credential vending.
    table_client = client.get_table_client(args.table)
    lazy_frame = table_client.load_as_polars()

    print('Schema:')
    print(lazy_frame.collect_schema())

    if args.verify_pruning:
        column, value = args.verify_pruning
        return _verify_pruning(lazy_frame, column, value)

    head = lazy_frame.head(10).collect()
    print(f'\nFirst {head.height} row(s):')
    print(head)

    # Count is answered from file statistics, so it stays cheap.
    row_count = lazy_frame.select(pl.len()).collect().item()
    print(f'\nTotal rows: {row_count}')

    print('\nOK: read succeeded.')
    return 0


def _verify_pruning(lazy_frame: pl.LazyFrame, column: str, value: str) -> int:
    # POLARS_VERBOSE makes the engine log how many files predicate pushdown
    # skips; it is read at collect() time, so setting it here is enough.
    os.environ['POLARS_VERBOSE'] = '1'
    print(
        f'\nReading with filter {column!r} == {value!r} - watch stderr for '
        "'Predicate pushdown allows skipping ... files' messages:\n"
    )
    matched = lazy_frame.filter(pl.col(column) == value).collect()
    print(f'\nMatched {matched.height} row(s).')
    print('\nOK: filtered read succeeded.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
