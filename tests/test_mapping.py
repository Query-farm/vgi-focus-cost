"""Mapping correctness: golden files plus hand-computed expectations per source.

Golden files (tests/golden/*.json) hold, per fixture, the FOCUS rows for the first
row of every distinct raw charge type plus per-category totals. Regenerate after a
deliberate mapping change with ``UPDATE_GOLDEN=1 uv run pytest tests/test_mapping.py``
and review the diff. The hand-computed tests below are independent of the goldens:
they recompute expected money from the raw fixture with Python ``decimal``.
"""

from __future__ import annotations

import json
import os
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from focus_worker.convert import pc
from focus_worker.reader import ScanRequest, make_plan, new_cursor, next_batch

from .conftest import FIXTURE_SOURCES, FIXTURES

GOLDEN = Path(__file__).resolve().parent / "golden"
KEY_COLUMNS = [
    "BilledCost",
    "EffectiveCost",
    "ListCost",
    "ContractedCost",
    "ChargeCategory",
    "ChargeClass",
    "ChargeFrequency",
    "ChargePeriodStart",
    "ChargePeriodEnd",
    "BillingPeriodStart",
    "CommitmentDiscountId",
    "CommitmentDiscountType",
    "CommitmentDiscountCategory",
    "CommitmentDiscountStatus",
    "PricingCategory",
    "PricingQuantity",
    "PricingUnit",
    "ConsumedQuantity",
    "ServiceName",
    "ServiceCategory",
    "ServiceSubcategory",
    "RegionId",
    "ResourceId",
    "SubAccountId",
    "ProviderName",
    "PublisherName",
    "InvoiceIssuerName",
    "Tags",
    "x_ChargeType",
]


def read(path: str, source: str, version: str = "1.2", scale: int = 6) -> pa.Table:
    plan = make_plan(ScanRequest(path, source, version, scale, "auto"))
    cur = new_cursor()
    batches = []
    while (b := next_batch(plan, cur, plan.schema.names)) is not None:
        batches.append(b)
    return pa.Table.from_batches(batches, schema=plan.schema)


def _jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return str(v)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, list):
        return [list(x) if isinstance(x, tuple) else x for x in v]
    return v


def snapshot(table: pa.Table) -> dict[str, Any]:
    rows = table.select([c for c in KEY_COLUMNS if c in table.schema.names]).to_pylist()
    seen: dict[str, dict[str, Any]] = {}
    for r, ct in zip(rows, table.column("x_ChargeType").to_pylist(), strict=True):
        key = f"{ct}|{r.get('CommitmentDiscountStatus')}|{r.get('ServiceCategory')}"
        seen.setdefault(key, {k: _jsonable(v) for k, v in r.items()})
    totals = (
        table.group_by(["ChargeCategory"])
        .aggregate([("BilledCost", "sum"), ("EffectiveCost", "sum"), ("BilledCost", "count")])
        .sort_by("ChargeCategory")
        .to_pylist()
    )
    return {
        "rows": table.num_rows,
        "first_by_charge_type": dict(sorted(seen.items())),
        "totals": [{k: _jsonable(v) for k, v in t.items()} for t in totals],
    }


@pytest.mark.parametrize(("path", "source"), FIXTURE_SOURCES)
def test_golden(path: str, source: str) -> None:
    name = Path(path).name.replace(".", "_") + ".json"
    got = snapshot(read(path, source))
    file = GOLDEN / name
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.mkdir(exist_ok=True)
        file.write_text(json.dumps(got, indent=1, sort_keys=True) + "\n")
    assert file.exists(), f"missing golden {file}; run with UPDATE_GOLDEN=1"
    assert got == json.loads(file.read_text())


def _q(x: Any, scale: int = 6) -> Decimal:
    return Decimal(repr(x) if isinstance(x, float) else str(x)).quantize(Decimal(1).scaleb(-scale), ROUND_HALF_EVEN)


def test_aws_amortization_matches_decimal_reference() -> None:
    """EffectiveCost per AWS formula, recomputed row by row from the raw CUR with decimal."""
    raw = pq.read_table(FIXTURES / "aws_cur2" / "acme-cur2" / "data" / "BILLING_PERIOD=2026-07").to_pylist()
    expected_billed = sum(_q(r["line_item_unblended_cost"]) for r in raw)
    expected_effective = Decimal(0)
    for r in raw:
        t = r["line_item_line_item_type"]
        if t == "SavingsPlanCoveredUsage":
            expected_effective += _q(r["savings_plan_savings_plan_effective_cost"])
        elif t == "DiscountedUsage":
            expected_effective += _q(r["reservation_effective_cost"])
        elif t == "SavingsPlanRecurringFee":
            expected_effective += _q(r["savings_plan_total_commitment_to_date"]) - _q(r["savings_plan_used_commitment"])
        elif t == "RIFee":
            expected_effective += _q(r["reservation_unused_amortized_upfront_fee_for_billing_period"]) + _q(
                r["reservation_unused_recurring_fee"]
            )
        elif t in ("SavingsPlanNegation", "SavingsPlanUpfrontFee") or (
            t == "Fee" and r["reservation_reservation_a_r_n"]
        ):
            pass
        else:
            expected_effective += _q(r["line_item_unblended_cost"])
    t = read(str(FIXTURES / "aws_cur2"), "aws_cur2")
    july = t.filter(pc.equal(pc.month(t["BillingPeriodStart"]), 7))
    assert pc.sum(july["BilledCost"]).as_py() == expected_billed
    assert pc.sum(july["EffectiveCost"]).as_py() == expected_effective


def test_aws_unused_commitment_rows() -> None:
    t = read(str(FIXTURES / "aws_cur2"), "aws_cur2", version="1.2")
    unused = t.filter(pc.equal(t["CommitmentDiscountStatus"], "Unused")).sort_by(
        [("x_ChargeType", "ascending"), ("BillingPeriodStart", "ascending")]
    )
    rows = unused.select(
        ["x_ChargeType", "ChargeCategory", "BilledCost", "EffectiveCost", "CommitmentDiscountType"]
    ).to_pylist()
    assert [r["x_ChargeType"] for r in rows] == ["RIFee (unused)"] * 2 + ["SavingsPlanRecurringFee (unused)"] * 2
    assert all(r["ChargeCategory"] == "Usage" and r["BilledCost"] == 0 for r in rows)
    assert rows[0]["EffectiveCost"] == Decimal("3.750000")  # 1.50 upfront + 2.25 recurring unused
    # July: 0.12 x 744 commitment minus 0.1142 x 744 used.
    assert rows[2]["EffectiveCost"] == Decimal("89.280000") - Decimal("84.964800")
    assert rows[2]["CommitmentDiscountType"] == "Savings Plan"


def test_aws_classification_and_correction() -> None:
    t = read(str(FIXTURES / "aws_cur2"), "aws_cur2")
    by = {r["x_ChargeType"]: r for r in t.to_pylist()}
    assert by["Refund"]["ChargeClass"] == "Correction"  # June charge billed in July
    assert by["Usage"]["ChargeClass"] is None
    assert by["DiscountedUsage"]["PricingCategory"] == "Committed"
    assert by["DiscountedUsage"]["CommitmentDiscountCategory"] == "Usage"
    assert by["SavingsPlanCoveredUsage"]["CommitmentDiscountCategory"] == "Spend"
    assert by["Tax"]["PricingQuantity"] is None and by["Tax"]["ConsumedQuantity"] is None
    assert by["Credit"]["ChargeCategory"] == "Credit"
    market = next(r for r in t.to_pylist() if r["x_ServiceCode"] == "prod-abc123marketplace")
    assert market["PublisherName"] == "Acme SaaS Vendor LLC"
    assert market["ProviderName"] == "AWS"
    assert market["ServiceName"] == "Acme Observability Pro"
    ec2 = next(r for r in t.to_pylist() if r["x_ServiceCode"] == "AmazonEC2" and r["x_ChargeType"] == "Usage")
    assert ec2["ServiceCategory"] == "Compute" and ec2["ServiceSubcategory"] == "Virtual Machines"
    assert dict(ec2["Tags"]) == {"team": "search", "env": "dev"}  # 'user_' prefix removed


def test_legacy_cur_headers() -> None:
    t = read(str(FIXTURES / "aws_cur_legacy"), "auto")
    assert t.column("x_SourceFormat")[0].as_py() == "aws_cur2"
    usage = t.filter(pc.equal(t["ChargeCategory"], "Usage"))
    assert usage.num_rows == 10
    assert pc.sum(t["BilledCost"]).as_py() == Decimal("5") * (Decimal("0.0416") + Decimal("0.526")) * 24 + Decimal(
        "1.07"
    )
    assert usage.column("RegionId")[0].as_py() == "us-east-1"
    assert sorted({str(dict(x or [])["team"]) for x in usage.column("Tags").to_pylist()}) == ["ml", "search"]


def test_sub_microdollar_costs_round_half_even_and_money_scale() -> None:
    six = read(str(FIXTURES / "aws_cur2"), "aws_cur2")
    ten = read(str(FIXTURES / "aws_cur2"), "aws_cur2", scale=10)
    lam6 = six.filter(pc.equal(six["x_ServiceCode"], "AWSLambda"))
    lam10 = ten.filter(pc.equal(ten["x_ServiceCode"], "AWSLambda"))
    assert set(lam6.column("BilledCost").to_pylist()) == {Decimal("0.000000")}  # 4.1667e-07 rounds to 0
    assert set(lam10.column("BilledCost").to_pylist()) == {Decimal("0.0000004167")}
    assert ten.schema.field("BilledCost").type == pa.decimal128(19, 10)


def test_azure_actual_vs_amortized() -> None:
    actual = read(str(FIXTURES / "azure" / "contoso-actual-cost-202607.csv"), "azure")
    amort = read(str(FIXTURES / "azure" / "contoso-amortized-cost-202607.parquet"), "azure_amortized")

    # Both describe the same month: what is not commitment-related agrees exactly.
    def ondemand(t: pa.Table) -> Decimal:
        mask = pc.and_(pc.is_null(t["CommitmentDiscountId"]), pc.equal(t["ChargeCategory"], "Usage"))
        total: Decimal = pc.sum(t.filter(mask)["BilledCost"]).as_py()
        return total

    assert ondemand(actual) == ondemand(amort)
    # Amortized EffectiveCost = on-demand + reservation/savings-plan usage + unused reservation.
    used = amort.filter(pc.equal(amort["CommitmentDiscountStatus"], "Used"))
    unused = amort.filter(pc.equal(amort["CommitmentDiscountStatus"], "Unused"))
    assert pc.sum(used["BilledCost"]).as_py() == 0
    assert pc.sum(unused["EffectiveCost"]).as_py() == Decimal("0.723000")  # 6 h x 0.1205
    assert set(used.column("CommitmentDiscountType").to_pylist()) == {"Reservation", "Savings Plan"}
    # Actual export: purchases are billed but not effective; the refund is a June correction.
    purchases = actual.filter(pc.equal(actual["ChargeCategory"], "Purchase"))
    assert pc.sum(purchases["EffectiveCost"]).as_py() == 0
    refund = actual.filter(pc.equal(actual["x_ChargeType"], "Refund")).to_pylist()[0]
    assert refund["ChargeClass"] == "Correction" and refund["BilledCost"] == Decimal("-42.500000")
    vm = actual.filter(pc.equal(actual["ResourceName"], "web-01")).to_pylist()[0]
    assert vm["RegionId"] == "eastus"
    assert vm["ResourceType"] == "microsoft.compute/virtualmachines"
    assert dict(vm["Tags"]) == {"env": "dev", "team": "web"}  # braces-less Azure tag text
    assert vm["ChargePeriodEnd"] - vm["ChargePeriodStart"] == __import__("datetime").timedelta(days=1)
    assert vm["BillingPeriodEnd"].isoformat() == "2026-08-01T00:00:00+00:00"
    market = actual.filter(pc.equal(actual["PublisherName"], "Contoso Data Inc.")).num_rows
    assert market == 1


def test_gcp_credits_and_cud() -> None:
    t = read(str(FIXTURES / "gcp" / "gcp_billing_export_v1_01A2B3_202607.parquet"), "gcp")
    raw = pq.read_table(FIXTURES / "gcp" / "gcp_billing_export_v1_01A2B3_202607.parquet").to_pylist()
    expected = sum(_q(r["cost"]) + sum((_q(c["amount"]) for c in r["credits"] or []), Decimal(0)) for r in raw)
    assert pc.sum(t["BilledCost"]).as_py() == expected
    cud = t.filter(pc.equal(t["PricingCategory"], "Committed")).to_pylist()
    assert len(cud) == 31
    assert {r["CommitmentDiscountId"] for r in cud} == {"cud-n2-ram-0001"}
    assert {r["CommitmentDiscountCategory"] for r in cud} == {"Usage"}
    tax = t.filter(pc.equal(t["ChargeCategory"], "Tax")).to_pylist()[0]
    assert tax["BillingPeriodStart"].isoformat() == "2026-07-01T00:00:00+00:00"
    adj = t.filter(pc.equal(t["x_ChargeType"], "adjustment")).to_pylist()[0]
    assert adj["ChargeClass"] == "Correction"
    vm = t.filter(pc.equal(t["ResourceName"], "web-1")).to_pylist()[0]
    assert vm["SubAccountId"] == "shop-prod-1234" and vm["AvailabilityZone"] == "us-central1-a"
    assert dict(vm["Tags"]) == {"team": "storefront", "env": "prod"}
    market = t.filter(pc.equal(t["PublisherName"], "Acme Vector Inc.")).to_pylist()[0]
    assert market["ChargeFrequency"] == "Recurring"


def test_gcp_json_matches_parquet_rows() -> None:
    pq_t = read(str(FIXTURES / "gcp" / "gcp_billing_export_v1_01A2B3_202607.parquet"), "gcp")
    js = read(str(FIXTURES / "gcp" / "gcp_billing_export_v1_01A2B3_202607.jsonl"), "gcp")
    cols = ["BilledCost", "ChargePeriodStart", "ServiceName", "SubAccountId", "CommitmentDiscountId", "Tags"]
    head = pq_t.slice(0, 20).select(cols).to_pylist()
    assert js.slice(0, 20).select(cols).to_pylist() == head


def test_native_focus_passthrough_keeps_x_columns() -> None:
    t = read(str(FIXTURES / "focus12"), "auto")
    assert "x_CostCategories" in t.schema.names and "x_Discounts" in t.schema.names
    row = t.to_pylist()[1]
    assert row["BilledCost"] == Decimal("0.000417")
    assert json.loads(row["x_CostCategories"]) == {"BusinessUnit": "Retail"}
    assert row["x_ServiceCode"] == "AmazonEC2"
    v13 = read(str(FIXTURES / "focus12"), "focus", version="1.3")
    r13 = v13.to_pylist()[0]
    assert r13["ServiceProviderName"] == "AWS" and r13["HostProviderName"] == "AWS"
    v14 = read(str(FIXTURES / "focus12"), "focus", version="1.4")
    assert "ProviderName" not in v14.schema.names


def test_focus_sample_1_0_reads_as_1_2() -> None:
    t = read(str(FIXTURES / "focus_sample"), "auto")
    assert t.num_rows == 300
    assert sorted({str(p) for p in t.column("ProviderName").to_pylist()}) == ["AWS", "Microsoft", "Oracle"]
    assert t.schema.field("ChargePeriodStart").type == pa.timestamp("us", tz="UTC")
