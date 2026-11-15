"""Every FOCUS column computed from only its rule's raw columns equals the full mapping.

This is the executable proof that ``Rule.sources`` is complete, which is what makes
projection pushdown safe: under projection the reader only reads those columns.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from focus_worker.reader import ScanRequest, make_plan, new_cursor, next_batch

from .conftest import FIXTURE_SOURCES


def _read(path: str, source: str, columns: list[str]) -> pa.Table:
    plan = make_plan(ScanRequest(path, source, "1.3", 6, "auto"))
    cur = new_cursor()
    batches = []
    while (b := next_batch(plan, cur, columns)) is not None:
        batches.append(b)
    return pa.Table.from_batches(batches, schema=pa.schema([plan.schema.field(c) for c in columns]))


@pytest.mark.parametrize(("path", "source"), FIXTURE_SOURCES)
def test_each_column_alone_matches_full_mapping(path: str, source: str) -> None:
    plan = make_plan(ScanRequest(path, source, "1.3", 6, "auto"))
    names = plan.schema.names
    full = _read(path, source, names)
    for name in names:
        alone = _read(path, source, [name])
        assert alone.num_rows == full.num_rows, name
        assert alone.column(0).equals(full.column(name)), f"{source}: {name} differs when projected alone"


@pytest.mark.parametrize(("path", "source"), FIXTURE_SOURCES)
def test_no_columns_still_counts_rows(path: str, source: str) -> None:
    full = _read(path, source, ["BilledCost"])
    assert _read(path, source, []).num_rows == full.num_rows
