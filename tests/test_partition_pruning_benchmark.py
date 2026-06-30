import tempfile
import time

import polars as pl
import pytest
from deltalake.writer import write_deltalake

from deltabridge.client import DeltaTableClient


@pytest.mark.benchmark
def test_partition_pruning_speedup():
    # Opt-in (deselected by default; run with `pytest -m benchmark -s`).
    #
    # Reads a single partition of a many-partition table two ways:
    #   * native  - pl.scan_delta + .filter(); skips non-matching partitions'
    #               data, but handles per-file metadata for every partition.
    #   * pyarrow - partition_filter pushes the predicate into delta-rs's file
    #               enumeration, so non-matching partitions are never touched.
    #
    # On a local filesystem the per-file metadata step is a cheap stat(), so
    # the gap here is modest (typically ~1.5-2x) and grows with partition
    # count. On object storage each of those is a network request per file,
    # which is where the reviewer's ~4-7s -> ~50s regression came from
    # (pola-rs/polars#20998). This benchmark documents the local trend; it
    # cannot reproduce the object-storage magnitude.
    n_part = 10_000
    df = pl.DataFrame(
        {'p': list(range(n_part)), 'v': [f'v{i}' for i in range(n_part)]},
        schema={'p': pl.Int64, 'v': pl.Utf8},
    )
    target = n_part // 2
    with tempfile.TemporaryDirectory() as tmpdir:
        write_deltalake(tmpdir, df, partition_by=['p'])
        client = DeltaTableClient(
            table_uri=tmpdir, storage_options_fn=lambda: {}
        )

        def time_read(read, repeat=5):
            best = float('inf')
            height = None
            for _ in range(repeat):
                start = time.perf_counter()
                height = read().height
                best = min(best, time.perf_counter() - start)
            return best, height

        t_native, h_native = time_read(
            lambda: (
                client.load_as_polars().filter(pl.col('p') == target).collect()
            )
        )
        t_pyarrow, h_pyarrow = time_read(
            lambda: client.load_as_polars(
                partition_filter=[('p', '=', str(target))]
            ).collect()
        )

    assert h_native == 1
    assert h_pyarrow == 1
    print(
        f'\nsingle-partition read of a {n_part}-partition table (local FS):\n'
        f'  native  (per-file metadata): {t_native * 1e3:.1f} ms\n'
        f'  pyarrow (predicate pushdown): {t_pyarrow * 1e3:.1f} ms\n'
        f'  speedup                     : {t_native / t_pyarrow:.1f}x'
    )
    # Lenient: on local FS pyarrow is reliably faster at this partition count,
    # but the gap is small and noisy - the printed numbers are the real signal.
    assert t_pyarrow < t_native
