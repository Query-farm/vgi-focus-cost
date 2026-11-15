"""Unit tests: conversions, scanning, pushdown, resumable cursors, scalars, spec data."""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter

from focus_worker import reader, scan, services
from focus_worker.convert import ConversionError, to_decimal, to_tags, to_timestamp
from focus_worker.reader import ScanRequest, make_plan, new_cursor, next_batch, raw_filter
from focus_worker.scalars import charge_category, exact_divide, map_column, validate_value
from focus_worker.spec import SPEC_VERSIONS, column_index, columns_for

from .conftest import FIXTURES, ROOT, SAMPLES

# ------------------------------------------------------------------ conversions


def test_doubles_convert_through_shortest_text_and_round_half_even() -> None:
    arr = pa.array([0.1, 4.1667e-07, -5e-07, 2.5e-06, 1.0000005, None])
    got = to_decimal(arr, len(arr), 19, 6, what="x").to_pylist()
    assert got == [
        Decimal("0.100000"),
        Decimal("0.000000"),
        Decimal("-0.000000"),
        Decimal("0.000002"),  # half-even: 2.5 -> 2
        Decimal("1.000000"),  # 1.0000005 -> 1.000000 (half-even)
        None,
    ]


def test_strings_parse_scientific_blank_and_null_text() -> None:
    arr = pa.array(["1.5E-7", " 2.0 ", "", "NULL", None, "-3.25"])
    got = to_decimal(arr, len(arr), 19, 10, what="x").to_pylist()
    assert got == [Decimal("0.0000001500"), Decimal("2.0000000000"), None, None, None, Decimal("-3.2500000000")]


def test_bad_decimal_text_names_the_value() -> None:
    with pytest.raises(ConversionError, match="N/A"):
        to_decimal(pa.array(["1", "N/A"]), 2, 19, 6, what="BilledCost")


def test_timestamps_from_every_export_shape() -> None:
    utc = dt.UTC
    expected = dt.datetime(2026, 7, 1, 5, 0, tzinfo=utc)
    shapes = [
        ["2026-07-01T05:00:00Z"],
        ["2026-07-01T00:00:00-05:00"],
        ["2026-07-01 05:00:00"],
        ["2026-07-01 05:00:00 UTC"],  # BigQuery JSON export
    ]
    for s in shapes:
        assert to_timestamp(pa.array(s), 1, what="t").to_pylist() == [expected], s
    assert to_timestamp(pa.array(["07/31/2026"]), 1, what="t").to_pylist() == [dt.datetime(2026, 7, 31, tzinfo=utc)]
    naive = pa.array([dt.datetime(2026, 7, 1, 5)], pa.timestamp("ms"))
    assert to_timestamp(naive, 1, what="t").to_pylist() == [expected]
    with pytest.raises(ConversionError):
        to_timestamp(pa.array(["yesterday"]), 1, what="t")


def test_tags_from_json_azure_braceless_map_and_label_list() -> None:
    assert to_tags(pa.array(['{"a": "1", "b": 2}']), 1).to_pylist() == [[("a", "1"), ("b", "2")]]
    assert to_tags(pa.array(['"env": "prod","team": "x"']), 1).to_pylist() == [[("env", "prod"), ("team", "x")]]
    labels = pa.array(
        [[{"key": "team", "value": "web"}]], pa.list_(pa.struct([("key", pa.string()), ("value", pa.string())]))
    )
    assert to_tags(labels, 1).to_pylist() == [[("team", "web")]]
    assert to_tags(pa.array(["", None, "not json"]), 3).to_pylist() == [None, None, None]


# --------------------------------------------------------------------- scanning


def test_sample_scheme_and_errors() -> None:
    files = scan.resolve("sample://aws_cur2")
    assert len(files.files) == 2 and all(u.startswith("sample://aws_cur2/") for u in files.uris)
    assert set(scan.sample_names()) == {"aws_cur2", "aws_cur_legacy", "azure", "focus12", "gcp"}
    with pytest.raises(scan.ScanError, match="unknown sample"):
        scan.resolve("sample://oracle")
    with pytest.raises(scan.ScanError, match="to_focus"):
        scan.resolve("abfss://c@a.dfs.core.windows.net/x.parquet")


def test_directory_skips_manifests_and_glob_recurses() -> None:
    files = scan.resolve(str(SAMPLES / "aws_cur2"))
    assert all(f.endswith(".parquet") for f in files.files)  # the Manifest.json is skipped
    glob = scan.resolve(str(SAMPLES / "**" / "*.parquet"))
    assert len(glob.files) == 5


@pytest.fixture(scope="module")
def http_root() -> Iterator[str]:
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "fixture_server.py")], stdout=subprocess.PIPE, text=True
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)


def _all(path: str, source: str = "auto", columns: list[str] | None = None, filters: object = None) -> pa.Table:
    plan = make_plan(ScanRequest(path, source, "1.2", 6, "auto"))
    cur = new_cursor()
    names = columns or plan.schema.names
    out = []
    while (b := next_batch(plan, cur, names, filters)) is not None:
        out.append(b)
    return pa.Table.from_batches(out, schema=pa.schema([plan.schema.field(n) for n in names]))


def test_http_range_reads_match_local(http_root: str) -> None:
    local = _all(
        str(SAMPLES / "gcp" / "gcp_billing_export_v1_01A2B3_202607.parquet"), "gcp", ["BilledCost", "ChargePeriodStart"]
    )
    remote = _all(
        f"{http_root}/gcp/gcp_billing_export_v1_01A2B3_202607.parquet", "gcp", ["BilledCost", "ChargePeriodStart"]
    )
    assert remote.equals(local)
    with pytest.raises(scan.ScanError, match="HTTP 404"):
        scan.resolve(f"{http_root}/nope.parquet").filesystem.get_file_info(f"{http_root}/nope.parquet")


# ---------------------------------------------------------------------- pushdown


def _filters(*fs: object) -> object:
    return type("F", (), {"filters": fs})()


def test_raw_filter_translates_direct_columns_only() -> None:
    plan = make_plan(ScanRequest(str(SAMPLES / "aws_cur2"), "aws_cur2", "1.2", 6, "auto"))
    schema = scan.file_schema(plan.files, 0)
    ts = pa.scalar(dt.datetime(2026, 7, 10, tzinfo=dt.UTC), pa.timestamp("us", tz="UTC"))
    f = _filters(
        ConstantFilter(column_name="ChargePeriodStart", column_index=0, op=ComparisonOp.GE, value=ts),
        InFilter(column_name="SubAccountId", column_index=1, values=pa.array(["777788889999"])),
        ConstantFilter(column_name="ServiceName", column_index=2, op=ComparisonOp.EQ, value=pa.scalar("x")),
    )
    expr = raw_filter(plan, schema, f)
    assert expr is not None
    text = str(expr)
    assert "line_item_usage_start_date" in text and "line_item_usage_account_id" in text
    assert "ServiceName" not in text and "product" not in text  # derived columns are never pushed


def test_row_group_pruning_reads_fewer_units() -> None:
    plan = make_plan(ScanRequest(str(SAMPLES / "aws_cur2"), "aws_cur2", "1.2", 6, "auto"))
    frag = scan.fragment(plan.files, 0)
    every = scan.row_groups(frag, None)
    late = ds.field("line_item_usage_start_date") >= pa.scalar(dt.datetime(2026, 7, 30), pa.timestamp("ms"))
    assert len(every) > 1 and len(scan.row_groups(frag, late)) < len(every)


def test_billing_period_prunes_partitions_and_rows() -> None:
    t = _all(str(SAMPLES / "aws_cur2"), "aws_cur2", ["BillingPeriodStart"])
    plan = make_plan(ScanRequest(str(SAMPLES / "aws_cur2"), "aws_cur2", "1.2", 6, "auto", billing_period="2026-08"))
    cur = new_cursor()
    rows = 0
    while (b := next_batch(plan, cur, ["BillingPeriodStart"])) is not None:
        rows += b.num_rows
        assert {v.month for v in b.column(0).to_pylist() if v is not None} == {8}
    assert 0 < rows < t.num_rows


# -------------------------------------------------------------- resumable cursor


def test_cursor_resumes_in_a_fresh_process_state() -> None:
    """Simulate the HTTP transport: every batch is served with cold caches."""
    path = str(SAMPLES / "aws_cur2")
    expected = _all(path, "aws_cur2", ["BilledCost", "ResourceId"])
    plan = make_plan(ScanRequest(path, "aws_cur2", "1.2", 6, "auto"))
    cur = new_cursor()
    batches = []
    while True:
        scan.ITERATORS.drop(cur.scan_id)  # forget open iterators
        reader._PLANS.put(plan.request, plan)
        state = reader.ScanCursor.deserialize_from_bytes(cur.serialize_to_bytes())  # cursor round-trips as Arrow
        b = next_batch(plan, state, ["BilledCost", "ResourceId"])
        cur = state
        if b is None:
            break
        batches.append(b)
    assert len(batches) > 2
    assert pa.Table.from_batches(batches).equals(expected)


# ----------------------------------------------------------------------- scalars


def test_exact_divide() -> None:
    assert exact_divide(Decimal(10), Decimal(3)) == Decimal("3.3333333333")
    assert exact_divide(Decimal(1), Decimal(4_000_000_000)) == Decimal("0.0000000002")  # half-even
    assert exact_divide(Decimal(1), Decimal(0)) is None
    assert exact_divide(None, Decimal(1)) is None
    with pytest.raises(ValueError, match="does not fit"):
        exact_divide(Decimal(10) ** 10, Decimal(1))


def test_validate_value_and_lineage_and_charges() -> None:
    assert validate_value("ChargeCategory", "Usage") is None
    assert "not an allowed" in (validate_value("ChargeFrequency", "Usage-based") or "")
    assert validate_value("BilledCost", None) == "BilledCost must not be NULL"
    assert validate_value("ServiceProviderName", "AWS", "1.3") is None
    assert map_column("aws_cur2", "resourceTags/user:team") == "Tags"
    assert map_column("azure", "CostCenter") is None
    assert charge_category("AWS", "SavingsPlanRecurringFee") == "Purchase"
    assert charge_category("Microsoft", "UnusedSavingsPlan") == "Usage"
    assert charge_category("aws", "SomethingNew") == "Adjustment"


# ---------------------------------------------------------------------- spec data


def test_spec_versions_compiled() -> None:
    assert [len(columns_for(v)) for v in SPEC_VERSIONS] == [43, 50, 57, 65, 65]
    assert column_index("1.2")["BilledCost"].feature_level == "Mandatory"
    assert column_index("1.4").get("ProviderName") is None


def test_service_map_uses_only_allowed_values() -> None:
    spec = column_index("1.2")
    cats = set(spec["ServiceCategory"].allowed_values or ())
    parents = spec["ServiceSubcategory"].subcategory_parent or {}
    keys = set()
    for s in services.SERVICES:
        assert s.category in cats, s
        assert parents.get(s.subcategory) == s.category, s
        assert (s.provider, s.code) not in keys, s
        keys.add((s.provider, s.code))


def test_every_mandatory_column_has_a_rule_per_provider() -> None:
    from focus_worker.sources import MAPPERS

    mandatory = [c.column_name for c in columns_for("1.2") if c.feature_level == "Mandatory"]
    for token in ("aws_cur2", "azure", "azure_amortized", "gcp"):
        missing = [c for c in mandatory if c not in MAPPERS[token].rule_index and c != "ChargeClass"]
        assert missing == [], token


def test_validate_report_on_focus_sample() -> None:
    from focus_worker.validate import _accumulate, _Counter, _present, _report

    plan = make_plan(ScanRequest(str(FIXTURES / "focus_sample"), "auto", "1.2", 10, "auto"))
    cols = columns_for("1.2")
    present, custom, _case = _present(plan)
    assert custom == ["Id"]
    counts: dict[str, _Counter] = {}
    cur = new_cursor()
    while (b := next_batch(plan, cur, [c.column_name for c in cols])) is not None:
        _accumulate(b, cols, present, counts)
    report = {(r["check_name"], r["column_name"]): r for r in _report(plan, counts).to_pylist()}
    assert report[("allowed_values", "ChargeFrequency")]["failing_rows"] == 60
    assert report[("not_null", "BilledCost")]["status"] == "pass"
    assert report[("column_present", "InvoiceId")]["status"] == "not_applicable"


def test_fixture_files_exist() -> None:
    assert Path(FIXTURES / "focus_sample" / "NOTICE.md").exists()
