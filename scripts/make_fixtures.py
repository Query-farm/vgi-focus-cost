"""Generate the synthetic billing-export fixtures under test/fixtures/.

Every file is fabricated (account ids, resource ids, tags and amounts are made up)
from the providers' published export schemas, so the corpus can be published MIT.
Exports really have 60-130 columns; each fixture carries the columns the mappers
read plus the provider's other columns as realistic noise, so projection and
"unknown column" handling are exercised.

    uv run python scripts/make_fixtures.py     # deterministic; rewrites test/fixtures/

The FOCUS 1.0 sample (test/fixtures/focus_sample/) is NOT generated: it is the
first 300 rows of the FinOps Foundation's FOCUS-Sample-Data, CC BY 4.0 (see the
NOTICE file next to it).
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import json
import random
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent / "test" / "fixtures"
UTC = dt.UTC


def d(x: float | str, places: int = 10) -> float:
    """Round like a provider writing a double."""
    return float(Decimal(str(x)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_EVEN))


def days(year: int, month: int) -> list[dt.date]:
    start = dt.date(year, month, 1)
    nxt = dt.date(year + (month == 12), month % 12 + 1, 1)
    return [start + dt.timedelta(n) for n in range((nxt - start).days)]


def month_start(year: int, month: int) -> dt.datetime:
    return dt.datetime(year, month, 1)


def next_month(year: int, month: int) -> dt.datetime:
    return dt.datetime(year + (month == 12), month % 12 + 1, 1)


# --------------------------------------------------------------------- AWS CUR 2.0

CUR2_STRING = ["bill_bill_type", "bill_billing_entity", "bill_invoice_id", "bill_invoicing_entity", "bill_payer_account_id", "bill_payer_account_name", "identity_line_item_id", "identity_time_interval", "line_item_availability_zone", "line_item_blended_rate", "line_item_currency_code", "line_item_legal_entity", "line_item_line_item_description", "line_item_line_item_type", "line_item_net_unblended_rate", "line_item_operation", "line_item_product_code", "line_item_resource_id", "line_item_tax_type", "line_item_unblended_rate", "line_item_usage_account_id", "line_item_usage_account_name", "line_item_usage_type", "pricing_currency", "pricing_lease_contract_length", "pricing_offering_class", "pricing_public_on_demand_rate", "pricing_purchase_option", "pricing_rate_code", "pricing_rate_id", "pricing_term", "pricing_unit", "product_comment", "product_fee_code", "product_fee_description", "product_from_location", "product_from_location_type", "product_from_region_code", "product_instance_family", "product_instance_type", "product_instancesku", "product_location", "product_location_type", "product_operation", "product_pricing_unit", "product_product_family", "product_region_code", "product_servicecode", "product_sku", "product_to_location", "product_to_location_type", "product_to_region_code", "product_usagetype", "reservation_availability_zone", "reservation_end_time", "reservation_modification_status", "reservation_reservation_a_r_n", "reservation_start_time", "reservation_subscription_id", "savings_plan_end_time", "savings_plan_instance_type_family", "savings_plan_offering_type", "savings_plan_payment_option", "savings_plan_purchase_term", "savings_plan_region", "savings_plan_savings_plan_a_r_n", "savings_plan_start_time", "split_line_item_parent_resource_id", "capacity_reservation_capacity_reservation_a_r_n", "capacity_reservation_capacity_reservation_status", "capacity_reservation_capacity_reservation_type"]

CUR2_DOUBLE = ["discount_bundled_discount", "discount_total_discount", "line_item_blended_cost", "line_item_net_unblended_cost", "line_item_normalization_factor", "line_item_normalized_usage_amount", "line_item_unblended_cost", "line_item_usage_amount", "pricing_public_on_demand_cost", "reservation_amortized_upfront_cost_for_usage", "reservation_amortized_upfront_fee_for_billing_period", "reservation_effective_cost", "reservation_net_amortized_upfront_cost_for_usage", "reservation_net_effective_cost", "reservation_net_recurring_fee_for_usage", "reservation_net_unused_amortized_upfront_fee_for_billing_period", "reservation_net_unused_recurring_fee", "reservation_net_upfront_value", "reservation_normalized_units_per_reservation", "reservation_number_of_reservations", "reservation_recurring_fee_for_usage", "reservation_total_reserved_normalized_units", "reservation_total_reserved_units", "reservation_units_per_reservation", "reservation_unused_amortized_upfront_fee_for_billing_period", "reservation_unused_normalized_unit_quantity", "reservation_unused_quantity", "reservation_unused_recurring_fee", "reservation_upfront_value", "savings_plan_amortized_upfront_commitment_for_billing_period", "savings_plan_net_amortized_upfront_commitment_for_billing_period", "savings_plan_net_recurring_commitment_for_billing_period", "savings_plan_net_savings_plan_effective_cost", "savings_plan_recurring_commitment_for_billing_period", "savings_plan_savings_plan_effective_cost", "savings_plan_savings_plan_rate", "savings_plan_total_commitment_to_date", "savings_plan_used_commitment", "split_line_item_actual_usage", "split_line_item_net_split_cost", "split_line_item_net_unused_cost", "split_line_item_reserved_usage", "split_line_item_split_cost", "split_line_item_split_usage", "split_line_item_split_usage_ratio", "split_line_item_unused_cost"]

CUR2_TS = ["bill_billing_period_end_date", "bill_billing_period_start_date", "line_item_usage_end_date", "line_item_usage_start_date"]

CUR2_MAP = ["product", "resource_tags", "cost_category"]
CUR2_DISCOUNT_MAP = ["discount"]

PAYER = ("111122223333", "Acme Payer")
PROD = ("444455556666", "acme-prod")
DEV = ("777788889999", "acme-dev")
SP_ARN = "arn:aws:savingsplans::111122223333:savingsplan/0a1b2c3d-sp-compute-0001"
RI_ARN = "arn:aws:rds:us-east-1:444455556666:ri:acme-rds-ri-0001"

AWS_PRODUCTS = {
    "AmazonEC2": "Amazon Elastic Compute Cloud",
    "AmazonS3": "Amazon Simple Storage Service",
    "AmazonRDS": "Amazon Relational Database Service",
    "AWSLambda": "AWS Lambda",
    "AmazonCloudWatch": "AmazonCloudWatch",
    "AWSDataTransfer": "AWS Data Transfer",
    "AmazonDynamoDB": "Amazon DynamoDB",
}


def cur2_rows(year: int, month: int, rng: random.Random) -> list[dict[str, Any]]:
    bp_start, bp_end = month_start(year, month), next_month(year, month)
    period = f"{year}-{month:02d}"
    rows: list[dict[str, Any]] = []
    seq = [0]

    def row(**kw: Any) -> None:
        seq[0] += 1
        account = kw.pop("account", PROD)
        start = kw.pop("start")
        end = kw.pop("end")
        r: dict[str, Any] = {
            "bill_bill_type": "Anniversary",
            "bill_billing_entity": "AWS",
            "bill_invoice_id": f"EUINUS{year}{month:02d}0001",
            "bill_invoicing_entity": "Amazon Web Services, Inc.",
            "bill_payer_account_id": PAYER[0],
            "bill_payer_account_name": PAYER[1],
            "bill_billing_period_start_date": bp_start,
            "bill_billing_period_end_date": bp_end,
            "identity_line_item_id": f"li-{period}-{seq[0]:05d}",
            "identity_time_interval": f"{start:%Y-%m-%dT%H:%M:%SZ}/{end:%Y-%m-%dT%H:%M:%SZ}",
            "line_item_currency_code": "USD",
            "line_item_legal_entity": "Amazon Web Services, Inc.",
            "line_item_usage_account_id": account[0],
            "line_item_usage_account_name": account[1],
            "line_item_usage_start_date": start,
            "line_item_usage_end_date": end,
            "pricing_currency": "USD",
        }
        r.update(kw)
        code = r.get("line_item_product_code")
        if code and "product" not in r:
            r["product"] = {
                "product_name": AWS_PRODUCTS.get(code, code),
                "region": r.get("product_region_code") or "",
                "servicename": AWS_PRODUCTS.get(code, code),
            }
        rows.append(r)

    for day in days(year, month):
        s = dt.datetime.combine(day, dt.time())
        e = s + dt.timedelta(days=1)
        # EC2 on-demand in dev, with a tiny jitter.
        hrs = 24.0
        rate = 0.0816
        for idx, (account, team) in enumerate(((DEV, "search"), (DEV, "payments"))):
            cost = d(hrs * rate)
            row(
                account=account,
                start=s,
                end=e,
                line_item_line_item_type="Usage",
                line_item_product_code="AmazonEC2",
                line_item_usage_type="USE1-BoxUsage:m7g.large",
                line_item_operation="RunInstances",
                line_item_resource_id=f"i-0dev{idx:013d}",
                line_item_availability_zone="us-east-1a",
                line_item_usage_amount=hrs,
                line_item_unblended_rate=str(rate),
                line_item_unblended_cost=cost,
                line_item_blended_cost=cost,
                line_item_net_unblended_cost=cost,
                line_item_line_item_description=f"${rate} per On Demand Linux m7g.large Instance Hour",
                pricing_public_on_demand_rate=str(rate),
                pricing_public_on_demand_cost=cost,
                pricing_term="OnDemand",
                pricing_unit="Hrs",
                pricing_rate_id="2187263457",
                product_sku="3DG6WFZ5QW4JAAHJ",
                product_region_code="us-east-1",
                product_location="US East (N. Virginia)",
                product_usagetype="USE1-BoxUsage:m7g.large",
                product_instance_type="m7g.large",
                product_product_family="Compute Instance",
                resource_tags={"user_team": team, "user_env": "dev"},
            )
        # EC2 covered by the Compute Savings Plan in prod: covered usage + negation.
        od = d(24 * 0.1632)
        eff = d(24 * 0.1142)
        row(
            start=s,
            end=e,
            line_item_line_item_type="SavingsPlanCoveredUsage",
            line_item_product_code="AmazonEC2",
            line_item_usage_type="USE1-BoxUsage:m7g.xlarge",
            line_item_operation="RunInstances",
            line_item_resource_id="i-0prod000000000001",
            line_item_availability_zone="us-east-1b",
            line_item_usage_amount=24.0,
            line_item_unblended_rate="0.1632",
            line_item_unblended_cost=od,
            line_item_line_item_description="USD 0.1142 per hour for Compute Savings Plan",
            pricing_public_on_demand_rate="0.1632",
            pricing_public_on_demand_cost=od,
            pricing_term="OnDemand",
            pricing_unit="Hrs",
            product_sku="7AJJ9WTR3YE8TFBY",
            product_region_code="us-east-1",
            product_location="US East (N. Virginia)",
            product_usagetype="USE1-BoxUsage:m7g.xlarge",
            savings_plan_savings_plan_a_r_n=SP_ARN,
            savings_plan_savings_plan_effective_cost=eff,
            savings_plan_savings_plan_rate=0.1142,
            resource_tags={"user_team": "checkout", "user_env": "prod"},
        )
        row(
            start=s,
            end=e,
            line_item_line_item_type="SavingsPlanNegation",
            line_item_product_code="AmazonEC2",
            line_item_usage_type="USE1-BoxUsage:m7g.xlarge",
            line_item_operation="RunInstances",
            line_item_resource_id="i-0prod000000000001",
            line_item_usage_amount=24.0,
            line_item_unblended_cost=-od,
            line_item_line_item_description="SavingsPlanNegation used by AccountId",
            pricing_public_on_demand_cost=0.0,
            savings_plan_savings_plan_a_r_n=SP_ARN,
            product_region_code="us-east-1",
            resource_tags={"user_team": "checkout", "user_env": "prod"},
        )
        # RDS covered by a Reserved Instance.
        row(
            start=s,
            end=e,
            line_item_line_item_type="DiscountedUsage",
            line_item_product_code="AmazonRDS",
            line_item_usage_type="USE1-InstanceUsage:db.r6g.large",
            line_item_operation="CreateDBInstance:0014",
            line_item_resource_id="arn:aws:rds:us-east-1:444455556666:db:orders",
            line_item_usage_amount=24.0,
            line_item_unblended_rate="0",
            line_item_unblended_cost=0.0,
            line_item_line_item_description="USD 0.0 hourly fee per PostgreSQL, db.r6g.large instance",
            pricing_public_on_demand_rate="0.26",
            pricing_public_on_demand_cost=d(24 * 0.26),
            pricing_term="Reserved",
            pricing_unit="Hrs",
            product_sku="QFKGJ3VCYV8S8TSQ",
            product_region_code="us-east-1",
            product_location="US East (N. Virginia)",
            reservation_reservation_a_r_n=RI_ARN,
            reservation_effective_cost=d(24 * 0.1712),
            resource_tags={"user_team": "payments", "user_env": "prod"},
        )
        # S3 storage and requests: request rows have sub-microcent costs.
        gb = 1200 + day.day * 3.5
        row(
            start=s,
            end=e,
            line_item_line_item_type="Usage",
            line_item_product_code="AmazonS3",
            line_item_usage_type="TimedStorage-ByteHrs",
            line_item_operation="StandardStorage",
            line_item_resource_id="acme-data-lake",
            line_item_usage_amount=d(gb / 31, 8),
            line_item_unblended_rate="0.023",
            line_item_unblended_cost=d(gb / 31 * 0.023),
            line_item_line_item_description="$0.023 per GB - first 50 TB / month of storage used",
            pricing_public_on_demand_rate="0.023",
            pricing_public_on_demand_cost=d(gb / 31 * 0.023),
            pricing_term="OnDemand",
            pricing_unit="GB-Mo",
            product_sku="WP9ANXZGBYYSGJEA",
            product_region_code="us-east-1",
            product_location="US East (N. Virginia)",
            resource_tags={"user_team": "data", "user_env": "prod"},
        )
        row(
            start=s,
            end=e,
            line_item_line_item_type="Usage",
            line_item_product_code="AmazonS3",
            line_item_usage_type="Requests-Tier1",
            line_item_operation="PutObject",
            line_item_resource_id="acme-data-lake",
            line_item_usage_amount=7.0,
            line_item_unblended_rate="0.000005",
            line_item_unblended_cost=d(7 * 0.000005),
            line_item_line_item_description="$0.005 per 1,000 PUT, COPY, POST, or LIST requests",
            pricing_public_on_demand_rate="0.000005",
            pricing_public_on_demand_cost=d(7 * 0.000005),
            pricing_term="OnDemand",
            pricing_unit="Requests",
            product_sku="JDCMHPTKFNNWQY7E",
            product_region_code="us-east-1",
            resource_tags={"user_team": "data"},
        )
        # Lambda: one GB-second line with a cost below one microdollar.
        row(
            account=DEV,
            start=s,
            end=e,
            line_item_line_item_type="Usage",
            line_item_product_code="AWSLambda",
            line_item_usage_type="USE1-Lambda-GB-Second",
            line_item_operation="Invoke",
            line_item_resource_id="arn:aws:lambda:us-east-1:777788889999:function:thumbnailer",
            line_item_usage_amount=0.025,
            line_item_unblended_rate="0.0000166667",
            line_item_unblended_cost=4.1667e-07,
            line_item_line_item_description="AWS Lambda - Total Compute - US East (N. Virginia)",
            pricing_public_on_demand_rate="0.0000166667",
            pricing_public_on_demand_cost=4.1667e-07,
            pricing_term="OnDemand",
            pricing_unit="Lambda-GB-Second",
            product_region_code="us-east-1",
        )
        # Data transfer out (no resource tags).
        row(
            start=s,
            end=e,
            line_item_line_item_type="Usage",
            line_item_product_code="AWSDataTransfer",
            line_item_usage_type="USE1-DataTransfer-Out-Bytes",
            line_item_operation="",
            line_item_usage_amount=d(rng.uniform(5, 40), 6),
            line_item_unblended_rate="0.09",
            line_item_unblended_cost=0.0,
            pricing_unit="GB",
            pricing_term="OnDemand",
            product_region_code="us-east-1",
            product_from_location="US East (N. Virginia)",
            product_to_location="External",
        )
        rows[-1]["line_item_unblended_cost"] = d(rows[-1]["line_item_usage_amount"] * 0.09)
        rows[-1]["pricing_public_on_demand_cost"] = rows[-1]["line_item_unblended_cost"]
    # Monthly lines.
    s, e = bp_start, bp_end
    commitment = d(0.12 * 24 * len(days(year, month)))
    used = d(0.1142 * 24 * len(days(year, month)))
    row(
        account=PAYER,
        start=s,
        end=e,
        line_item_line_item_type="SavingsPlanRecurringFee",
        line_item_product_code="ComputeSavingsPlans",
        line_item_usage_type="ComputeSP:1yrNoUpfront",
        line_item_usage_amount=744.0,
        line_item_unblended_cost=commitment,
        line_item_line_item_description="1 year No Upfront Compute Savings Plan",
        savings_plan_savings_plan_a_r_n=SP_ARN,
        savings_plan_total_commitment_to_date=commitment,
        savings_plan_used_commitment=used,
        savings_plan_recurring_commitment_for_billing_period=commitment,
        pricing_unit="Hrs",
    )
    row(
        start=s,
        end=e,
        line_item_line_item_type="RIFee",
        line_item_product_code="AmazonRDS",
        line_item_usage_type="USE1-HeavyUsage:db.r6g.large",
        line_item_operation="CreateDBInstance:0014",
        line_item_usage_amount=744.0,
        line_item_unblended_rate="0.0712",
        line_item_unblended_cost=d(0.0712 * 744),
        line_item_line_item_description="USD 0.0712 hourly fee per PostgreSQL (Multi-AZ), db.r6g.large",
        reservation_reservation_a_r_n=RI_ARN,
        reservation_unused_amortized_upfront_fee_for_billing_period=1.5,
        reservation_unused_recurring_fee=2.25,
        reservation_amortized_upfront_fee_for_billing_period=74.4,
        pricing_unit="Hrs",
    )
    if month == 7:
        row(
            start=s,
            end=s + dt.timedelta(seconds=1),
            line_item_line_item_type="Fee",
            line_item_product_code="AmazonRDS",
            line_item_usage_type="USE1-HeavyUsage:db.r6g.large",
            line_item_usage_amount=1.0,
            line_item_unblended_cost=876.0,
            line_item_line_item_description="Sign up charge for subscription: 0001, planId: RDS RI",
            reservation_reservation_a_r_n=RI_ARN,
        )
        # A refund for June, billed in July: ChargeClass = Correction.
        row(
            start=dt.datetime(2026, 6, 1),
            end=dt.datetime(2026, 7, 1),
            line_item_line_item_type="Refund",
            line_item_product_code="AmazonEC2",
            line_item_unblended_cost=-3.0,
            line_item_line_item_description="Refund for June service disruption",
        )
    row(
        account=PROD,
        start=s,
        end=e,
        line_item_line_item_type="Tax",
        line_item_product_code="AmazonEC2",
        line_item_tax_type="Sales Tax",
        line_item_unblended_cost=12.34,
        line_item_line_item_description="Tax for product code AmazonEC2",
    )
    row(
        account=DEV,
        start=s,
        end=e,
        line_item_line_item_type="Credit",
        line_item_product_code="AmazonEC2",
        line_item_unblended_cost=-25.0,
        line_item_line_item_description="AWS Activate credits",
    )
    row(
        account=PROD,
        start=s,
        end=e,
        line_item_line_item_type="EdpDiscount",
        line_item_product_code="AmazonEC2",
        line_item_unblended_cost=-7.77,
        line_item_line_item_description="EDP discount",
    )
    row(
        account=PROD,
        start=s,
        end=e,
        line_item_line_item_type="Usage",
        line_item_product_code="prod-abc123marketplace",
        bill_billing_entity="AWS Marketplace",
        line_item_legal_entity="Acme SaaS Vendor LLC",
        line_item_usage_type="SaaS-Seats",
        line_item_usage_amount=10.0,
        line_item_unblended_cost=150.0,
        line_item_line_item_description="Acme Observability Pro, 10 seats",
        pricing_unit="Units",
        product={"product_name": "Acme Observability Pro"},
    )
    return rows


def cur2_schema() -> pa.Schema:
    fields = []
    for n in sorted(CUR2_STRING + CUR2_DOUBLE + CUR2_TS + CUR2_MAP + CUR2_DISCOUNT_MAP):
        if n in CUR2_DOUBLE:
            t: pa.DataType = pa.float64()
        elif n in CUR2_TS:
            t = pa.timestamp("ms")
        elif n in CUR2_MAP:
            t = pa.map_(pa.string(), pa.string())
        elif n in CUR2_DISCOUNT_MAP:
            t = pa.map_(pa.string(), pa.float64())
        else:
            t = pa.string()
        fields.append(pa.field(n, t))
    return pa.schema(fields)


def write_cur2() -> None:
    rng = random.Random(20260701)
    schema = cur2_schema()
    base = ROOT / "aws_cur2" / "acme-cur2"
    for month in (7, 8):
        rows = cur2_rows(2026, month, rng)
        cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
        for k in CUR2_MAP:
            cols[k] = [list(v.items()) if v else None for v in cols[k]]
        cols["discount"] = [None] * len(rows)
        table = pa.table(cols, schema=schema)
        out = base / "data" / f"BILLING_PERIOD=2026-{month:02d}"
        out.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, out / "acme-cur2-00001.snappy.parquet", row_group_size=150, compression="snappy")
    meta = base / "metadata" / "BILLING_PERIOD=2026-07"
    meta.mkdir(parents=True, exist_ok=True)
    (meta / "acme-cur2-Manifest.json").write_text(json.dumps({"assemblyId": "fixture", "dataFiles": []}) + "\n")


def write_cur_legacy() -> None:
    """A few days of legacy CUR (lineItem/... headers) as CSV.gz."""
    rng = random.Random(7)
    header = [
        "identity/LineItemId",
        "identity/TimeInterval",
        "bill/InvoiceId",
        "bill/BillingEntity",
        "bill/BillType",
        "bill/PayerAccountId",
        "bill/BillingPeriodStartDate",
        "bill/BillingPeriodEndDate",
        "lineItem/UsageAccountId",
        "lineItem/LineItemType",
        "lineItem/UsageStartDate",
        "lineItem/UsageEndDate",
        "lineItem/ProductCode",
        "lineItem/UsageType",
        "lineItem/Operation",
        "lineItem/AvailabilityZone",
        "lineItem/ResourceId",
        "lineItem/UsageAmount",
        "lineItem/NormalizationFactor",
        "lineItem/CurrencyCode",
        "lineItem/UnblendedRate",
        "lineItem/UnblendedCost",
        "lineItem/BlendedRate",
        "lineItem/BlendedCost",
        "lineItem/LineItemDescription",
        "lineItem/TaxType",
        "product/ProductName",
        "product/instanceType",
        "product/location",
        "product/region",
        "product/sku",
        "pricing/publicOnDemandCost",
        "pricing/publicOnDemandRate",
        "pricing/term",
        "pricing/unit",
        "reservation/ReservationARN",
        "savingsPlan/SavingsPlanARN",
        "resourceTags/user:team",
        "resourceTags/user:env",
    ]
    out = ROOT / "aws_cur_legacy"
    out.mkdir(parents=True, exist_ok=True)
    with gzip.open(out / "acme-legacy-00001.csv.gz", "wt", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        n = 0
        for day in range(1, 6):
            s = f"2026-05-{day:02d}T00:00:00Z"
            e = f"2026-05-{day + 1:02d}T00:00:00Z"
            for team, rate in (("search", "0.0416"), ("ml", "0.526")):
                n += 1
                qty = 24
                cost = str(Decimal(rate) * qty)
                w.writerow(
                    [
                        f"legacy-{n}",
                        f"{s}/{e}",
                        "EUINUS202605",
                        "AWS",
                        "Anniversary",
                        PAYER[0],
                        "2026-05-01T00:00:00Z",
                        "2026-06-01T00:00:00Z",
                        DEV[0],
                        "Usage",
                        s,
                        e,
                        "AmazonEC2",
                        "USE1-BoxUsage:t3.medium",
                        "RunInstances",
                        "us-east-1c",
                        f"i-legacy{team}",
                        qty,
                        "2",
                        "USD",
                        rate,
                        cost,
                        rate,
                        cost,
                        f"${rate} per On Demand Linux Instance Hour",
                        "",
                        "Amazon Elastic Compute Cloud",
                        "t3.medium",
                        "US East (N. Virginia)",
                        "us-east-1",
                        "SKU123",
                        cost,
                        rate,
                        "OnDemand",
                        "Hrs",
                        "",
                        "",
                        team,
                        "dev",
                    ]
                )
        w.writerow(
            [
                "legacy-tax",
                "",
                "EUINUS202605",
                "AWS",
                "Anniversary",
                PAYER[0],
                "2026-05-01T00:00:00Z",
                "2026-06-01T00:00:00Z",
                DEV[0],
                "Tax",
                "2026-05-01T00:00:00Z",
                "2026-06-01T00:00:00Z",
                "AmazonEC2",
                "",
                "",
                "",
                "",
                "",
                "",
                "USD",
                "",
                "1.07",
                "",
                "1.07",
                "Tax for product code AmazonEC2",
                "Sales Tax",
                "Amazon Elastic Compute Cloud",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
    del rng


# ---------------------------------------------------------------------- Azure

AZURE_COLUMNS = ["InvoiceSectionName", "AccountName", "AccountOwnerId", "SubscriptionId", "SubscriptionName", "ResourceGroup", "ResourceLocation", "Date", "ProductName", "MeterCategory", "MeterSubCategory", "MeterId", "MeterName", "MeterRegion", "UnitOfMeasure", "Quantity", "EffectivePrice", "CostInBillingCurrency", "CostCenter", "ConsumedService", "ResourceId", "Tags", "OfferId", "AdditionalInfo", "ServiceInfo1", "ServiceInfo2", "ResourceName", "ReservationId", "ReservationName", "UnitPrice", "ProductOrderId", "ProductOrderName", "Term", "PublisherType", "PublisherName", "ChargeType", "Frequency", "PricingModel", "AvailabilityZone", "BillingAccountId", "BillingAccountName", "BillingCurrencyCode", "BillingPeriodStartDate", "BillingPeriodEndDate", "BillingProfileId", "BillingProfileName", "InvoiceSectionId", "IsAzureCreditEligible", "PartNumber", "PayGPrice", "PlanName", "ServiceFamily", "CostAllocationRuleName", "benefitId", "benefitName", "CostInPricingCurrency", "CostInUsd", "ExchangeRatePricingToBilling", "ExchangeRateDate", "PaygCostInBillingCurrency", "PaygCostInUsd", "PricingCurrencyCode", "ProductId", "ResellerName", "ResellerMpnId", "InvoiceId", "PreviousInvoiceId"]

SUB_PROD = ("/subscriptions/00000000-0000-0000-0000-00000000a001", "contoso-prod")
SUB_DEV = ("/subscriptions/00000000-0000-0000-0000-00000000a002", "contoso-dev")
RES_ID = "/providers/Microsoft.Capacity/reservationOrders/11111111-ro/reservations/22222222-res"
SP_ID = "/providers/Microsoft.BillingBenefits/savingsPlanOrders/33333333-spo/savingsPlans/44444444-sp"


def azure_rows(amortized: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    year, month = 2026, 7

    def base(sub: tuple[str, str], day: dt.date, **kw: Any) -> dict[str, Any]:
        r = {
            "InvoiceSectionName": "Platform",
            "AccountName": "Contoso",
            "SubscriptionId": sub[0].rsplit("/", 1)[-1],
            "SubscriptionName": sub[1],
            "Date": f"{day:%m/%d/%Y}",
            "BillingAccountId": "contoso-ba-0001",
            "BillingAccountName": "Contoso Ltd",
            "BillingCurrencyCode": "USD",
            "PricingCurrencyCode": "USD",
            "BillingPeriodStartDate": "07/01/2026",
            "BillingPeriodEndDate": "07/31/2026",
            "BillingProfileId": "BP-01",
            "BillingProfileName": "Contoso Billing",
            "InvoiceSectionId": "IS-01",
            "IsAzureCreditEligible": "True",
            "ExchangeRatePricingToBilling": "1",
            "ExchangeRateDate": "07/01/2026",
            "Frequency": "UsageBased",
            "PricingModel": "OnDemand",
            "ChargeType": "Usage",
            "PublisherType": "Azure",
            "PublisherName": "Microsoft",
            "InvoiceId": "" if day.month == month else "",
            "CostCenter": "",
            "ResellerName": "",
            "ResellerMpnId": "",
            "PreviousInvoiceId": "",
        }
        r.update(kw)
        return r

    def money(x: Decimal) -> str:
        return str(x.quantize(Decimal("0.0000000001")))

    for day in days(year, month):
        vm_od = Decimal("0.192") * 24
        rows.append(
            base(
                SUB_DEV,
                day,
                ResourceGroup="rg-web",
                ResourceLocation="EastUS",
                ProductName="Virtual Machines Dsv5 Series - D4s v5 - US East",
                MeterCategory="Virtual Machines",
                MeterSubCategory="Dsv5 Series",
                MeterId="mtr-d4sv5",
                MeterName="D4s v5",
                MeterRegion="US East",
                UnitOfMeasure="1 Hour",
                Quantity="24",
                EffectivePrice="0.192",
                UnitPrice="0.192",
                PayGPrice="0.192",
                CostInBillingCurrency=money(vm_od),
                CostInPricingCurrency=money(vm_od),
                CostInUsd=money(vm_od),
                PaygCostInBillingCurrency=money(vm_od),
                PaygCostInUsd=money(vm_od),
                ConsumedService="Microsoft.Compute",
                ResourceId=f"{SUB_DEV[0]}/resourceGroups/rg-web/providers/Microsoft.Compute/virtualMachines/web-01",
                ResourceName="web-01",
                Tags='"env": "dev","team": "web"',
                ServiceFamily="Compute",
                ProductId="DZH318Z0BQ4L",
                AdditionalInfo='{"ServiceType":"Standard_D4s_v5"}',
            )
        )
        # Reservation-covered VM in prod.
        ri_cost = Decimal("0.1205") * 24 if amortized else Decimal(0)
        rows.append(
            base(
                SUB_PROD,
                day,
                ResourceGroup="rg-db",
                ResourceLocation="EastUS",
                ProductName="Virtual Machines Esv5 Series - E8s v5 - US East",
                MeterCategory="Virtual Machines",
                MeterSubCategory="Esv5 Series",
                MeterId="mtr-e8sv5",
                MeterName="E8s v5",
                MeterRegion="US East",
                UnitOfMeasure="1 Hour",
                Quantity="24",
                EffectivePrice=str(Decimal("0.1205") if amortized else 0),
                UnitPrice="0.504",
                PayGPrice="0.504",
                CostInBillingCurrency=money(ri_cost),
                PaygCostInBillingCurrency=money(Decimal("0.504") * 24),
                PricingModel="Reservation",
                ReservationId=RES_ID,
                ReservationName="VM_RI_E8s",
                benefitId=RES_ID,
                benefitName="VM_RI_E8s",
                ConsumedService="Microsoft.Compute",
                ResourceId=f"{SUB_PROD[0]}/resourceGroups/rg-db/providers/Microsoft.Compute/virtualMachines/db-01",
                ResourceName="db-01",
                Tags='"env": "prod","team": "data"',
                ServiceFamily="Compute",
                ProductId="DZH318Z0BQ5R",
            )
        )
        # Savings-plan covered App Service.
        sp_cost = Decimal("0.21") * 24 if amortized else Decimal(0)
        rows.append(
            base(
                SUB_PROD,
                day,
                ResourceGroup="rg-api",
                ResourceLocation="westeurope",
                ProductName="Azure App Service Premium v3 Plan - P1 v3 - EU West",
                MeterCategory="Azure App Service",
                MeterSubCategory="Premium v3 Plan",
                MeterId="mtr-p1v3",
                MeterName="P1 v3",
                MeterRegion="EU West",
                UnitOfMeasure="1 Hour",
                Quantity="24",
                UnitPrice="0.30",
                PayGPrice="0.30",
                CostInBillingCurrency=money(sp_cost),
                PaygCostInBillingCurrency=money(Decimal("0.30") * 24),
                PricingModel="SavingsPlan",
                benefitId=SP_ID,
                benefitName="Compute SP 1y",
                ConsumedService="Microsoft.Web",
                ResourceId=f"{SUB_PROD[0]}/resourceGroups/rg-api/providers/Microsoft.Web/serverFarms/api-plan",
                ResourceName="api-plan",
                Tags='"env": "prod","team": "api"',
                ServiceFamily="Compute",
                ProductId="DZH317F1HKN0",
            )
        )
        # Storage with tiny per-transaction costs.
        rows.append(
            base(
                SUB_PROD,
                day,
                ResourceGroup="rg-data",
                ResourceLocation="EastUS",
                ProductName="General Block Blob v2 - Hot LRS - US East",
                MeterCategory="Storage",
                MeterSubCategory="General Block Blob v2",
                MeterId="mtr-blob-hot",
                MeterName="Hot LRS Data Stored",
                MeterRegion="US East",
                UnitOfMeasure="1 GB/Month",
                Quantity=str(Decimal("2048") / 31),
                UnitPrice="0.0184",
                PayGPrice="0.0208",
                CostInBillingCurrency=money(Decimal("2048") / 31 * Decimal("0.0184")),
                PaygCostInBillingCurrency=money(Decimal("2048") / 31 * Decimal("0.0208")),
                ConsumedService="Microsoft.Storage",
                ResourceId=f"{SUB_PROD[0]}/resourceGroups/rg-data/providers/Microsoft.Storage/storageAccounts/contosolake",
                ResourceName="contosolake",
                Tags='"env": "prod","team": "data"',
                ServiceFamily="Storage",
                ProductId="DZH318Z0BNXX",
            )
        )
        rows.append(
            base(
                SUB_PROD,
                day,
                ResourceGroup="rg-data",
                ResourceLocation="EastUS",
                ProductName="Bandwidth - Data Transfer Out",
                MeterCategory="Bandwidth",
                MeterSubCategory="",
                MeterId="mtr-bw-out",
                MeterName="Standard Data Transfer Out",
                MeterRegion="Zone 1",
                UnitOfMeasure="1 GB",
                Quantity="3.217",
                UnitPrice="0.087",
                PayGPrice="0.087",
                CostInBillingCurrency=money(Decimal("3.217") * Decimal("0.087")),
                ConsumedService="Microsoft.Network",
                ResourceId=f"{SUB_PROD[0]}/resourceGroups/rg-data/providers/Microsoft.Storage/storageAccounts/contosolake",
                Tags="",
                ServiceFamily="Networking",
                ProductId="DZH318Z0BNVX",
            )
        )
    first = dt.date(year, month, 1)
    if amortized:
        rows.append(
            base(
                SUB_PROD,
                dt.date(year, month, 31),
                ChargeType="UnusedReservation",
                PricingModel="Reservation",
                MeterCategory="Virtual Machines",
                MeterSubCategory="Esv5 Series",
                MeterName="E8s v5",
                ProductName="Virtual Machines Esv5 Series - E8s v5 - US East",
                UnitOfMeasure="1 Hour",
                Quantity="6",
                UnitPrice="0.504",
                CostInBillingCurrency=money(Decimal("0.1205") * 6),
                ReservationId=RES_ID,
                ReservationName="VM_RI_E8s",
                benefitId=RES_ID,
                benefitName="VM_RI_E8s",
                ResourceLocation="EastUS",
                ServiceFamily="Compute",
            )
        )
    else:
        rows.append(
            base(
                SUB_PROD,
                first,
                ChargeType="Purchase",
                PricingModel="Reservation",
                Frequency="Recurring",
                MeterCategory="Virtual Machines",
                ProductName="Reserved VM Instance, E8s v5, US East, 1 Year",
                UnitOfMeasure="1 Hour",
                Quantity="744",
                UnitPrice="0.1205",
                CostInBillingCurrency=money(Decimal("0.1205") * 744),
                ReservationId=RES_ID,
                ReservationName="VM_RI_E8s",
                benefitId=RES_ID,
                benefitName="VM_RI_E8s",
                ServiceFamily="Compute",
                ProductOrderName="Reserved VM Instance",
                Term="12",
            )
        )
        rows.append(
            base(
                SUB_PROD,
                first,
                ChargeType="Purchase",
                PricingModel="SavingsPlan",
                Frequency="Recurring",
                MeterCategory="Azure App Service",
                ProductName="Savings Plan, Compute, 1 Year",
                UnitOfMeasure="1 Hour",
                Quantity="744",
                UnitPrice="0.21",
                CostInBillingCurrency=money(Decimal("0.21") * 744),
                benefitId=SP_ID,
                benefitName="Compute SP 1y",
                ServiceFamily="Compute",
                Term="12",
            )
        )
        rows.append(
            base(
                SUB_PROD,
                dt.date(2026, 6, 15),
                ChargeType="Refund",
                PricingModel="Reservation",
                Frequency="OneTime",
                MeterCategory="Virtual Machines",
                ProductName="Reserved VM Instance refund",
                UnitOfMeasure="1 Hour",
                Quantity="1",
                CostInBillingCurrency="-42.5",
                ReservationId=RES_ID,
                ServiceFamily="Compute",
            )
        )
    rows.append(
        base(
            SUB_DEV,
            first,
            ChargeType="Usage",
            MeterCategory="Log Analytics",
            MeterSubCategory="",
            ProductName="Contoso Data Insights - Pro",
            PublisherType="Marketplace",
            PublisherName="Contoso Data Inc.",
            UnitOfMeasure="1 Month",
            Quantity="1",
            UnitPrice="99",
            CostInBillingCurrency="99",
            Frequency="Recurring",
            ServiceFamily="Management and Governance",
        )
    )
    rows.append(
        base(
            SUB_DEV,
            first,
            ChargeType="RoundingAdjustment",
            MeterCategory="",
            ProductName="Rounding adjustment",
            Quantity="0",
            CostInBillingCurrency="-0.01",
            Frequency="OneTime",
        )
    )
    return rows


def write_azure() -> None:
    out = ROOT / "azure"
    out.mkdir(parents=True, exist_ok=True)
    actual = azure_rows(amortized=False)
    with (out / "contoso-actual-cost-202607.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=AZURE_COLUMNS, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in actual:
            w.writerow({k: r.get(k, "") for k in AZURE_COLUMNS})
    amort = azure_rows(amortized=True)
    cols = {k: [r.get(k) or None for r in amort] for k in AZURE_COLUMNS}
    pq.write_table(pa.table(cols), out / "contoso-amortized-cost-202607.parquet")


# ------------------------------------------------------------------------ GCP


def gcp_schema() -> pa.Schema:
    kv = pa.struct([("key", pa.string()), ("value", pa.string())])
    return pa.schema(
        [
            ("billing_account_id", pa.string()),
            ("service", pa.struct([("id", pa.string()), ("description", pa.string())])),
            ("sku", pa.struct([("id", pa.string()), ("description", pa.string())])),
            ("usage_start_time", pa.timestamp("us", tz="UTC")),
            ("usage_end_time", pa.timestamp("us", tz="UTC")),
            (
                "project",
                pa.struct(
                    [
                        ("id", pa.string()),
                        ("number", pa.string()),
                        ("name", pa.string()),
                        ("labels", pa.list_(kv)),
                        ("ancestry_numbers", pa.string()),
                    ]
                ),
            ),
            ("labels", pa.list_(kv)),
            ("system_labels", pa.list_(kv)),
            (
                "location",
                pa.struct(
                    [
                        ("location", pa.string()),
                        ("country", pa.string()),
                        ("region", pa.string()),
                        ("zone", pa.string()),
                    ]
                ),
            ),
            ("resource", pa.struct([("name", pa.string()), ("global_name", pa.string())])),
            (
                "tags",
                pa.list_(
                    pa.struct(
                        [
                            ("key", pa.string()),
                            ("value", pa.string()),
                            ("inherited", pa.bool_()),
                            ("namespace", pa.string()),
                        ]
                    )
                ),
            ),
            (
                "price",
                pa.struct(
                    [
                        ("effective_price", pa.float64()),
                        ("tier_start_amount", pa.float64()),
                        ("unit", pa.string()),
                        ("pricing_unit_quantity", pa.float64()),
                    ]
                ),
            ),
            ("subscription", pa.struct([("instance_id", pa.string())])),
            ("transaction_type", pa.string()),
            ("seller_name", pa.string()),
            ("export_time", pa.timestamp("us", tz="UTC")),
            ("cost", pa.float64()),
            ("currency", pa.string()),
            ("currency_conversion_rate", pa.float64()),
            (
                "usage",
                pa.struct(
                    [
                        ("amount", pa.float64()),
                        ("unit", pa.string()),
                        ("amount_in_pricing_units", pa.float64()),
                        ("pricing_unit", pa.string()),
                    ]
                ),
            ),
            (
                "credits",
                pa.list_(
                    pa.struct(
                        [
                            ("name", pa.string()),
                            ("amount", pa.float64()),
                            ("full_name", pa.string()),
                            ("id", pa.string()),
                            ("type", pa.string()),
                        ]
                    )
                ),
            ),
            ("invoice", pa.struct([("month", pa.string()), ("publisher_type", pa.string())])),
            ("cost_type", pa.string()),
            (
                "adjustment_info",
                pa.struct(
                    [("id", pa.string()), ("description", pa.string()), ("mode", pa.string()), ("type", pa.string())]
                ),
            ),
            ("cost_at_list", pa.float64()),
        ]
    )


def gcp_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ba = "01A2B3-C4D5E6-F7A8B9"
    shop = {
        "id": "shop-prod-1234",
        "number": "123456789012",
        "name": "shop-prod",
        "labels": [{"key": "cost-center", "value": "cc-42"}],
        "ancestry_numbers": "/111/123456789012/",
    }
    ml = {
        "id": "ml-sandbox-5678",
        "number": "567856785678",
        "name": "ml-sandbox",
        "labels": [],
        "ancestry_numbers": "/111/567856785678/",
    }

    def r(**kw: Any) -> None:
        start = kw.pop("start")
        hours = kw.pop("hours", 24)
        row = {
            "billing_account_id": ba,
            "usage_start_time": start,
            "usage_end_time": start + dt.timedelta(hours=hours),
            "export_time": start + dt.timedelta(hours=hours + 6),
            "currency": "USD",
            "currency_conversion_rate": 1.0,
            "invoice": {"month": f"{start:%Y%m}", "publisher_type": "GOOGLE"},
            "cost_type": "regular",
            "transaction_type": "GOOGLE",
            "labels": [],
            "system_labels": [],
            "credits": [],
            "tags": [],
        }
        row.update(kw)
        rows.append(row)

    for day in days(2026, 7):
        s = dt.datetime.combine(day, dt.time(), tzinfo=UTC)
        cost = d(24 * 0.134)
        r(
            start=s,
            service={"id": "6F81-5844-456A", "description": "Compute Engine"},
            sku={"id": "CF4E-A0C7-E3BF", "description": "N2 Instance Core running in Americas"},
            project=shop,
            labels=[{"key": "team", "value": "storefront"}, {"key": "env", "value": "prod"}],
            location={"location": "us-central1", "country": "US", "region": "us-central1", "zone": "us-central1-a"},
            resource={
                "name": "web-1",
                "global_name": "//compute.googleapis.com/projects/shop-prod-1234/zones/us-central1-a/instances/1001",
            },
            price={"effective_price": 0.134, "tier_start_amount": 0.0, "unit": "hour", "pricing_unit_quantity": 1.0},
            cost=cost,
            cost_at_list=cost,
            usage={"amount": 86400.0, "unit": "seconds", "amount_in_pricing_units": 24.0, "pricing_unit": "hour"},
            credits=[
                {
                    "name": "Sustained Usage Discount",
                    "amount": -d(cost * 0.2),
                    "full_name": "Sustained Usage Discount",
                    "id": "",
                    "type": "SUSTAINED_USAGE_DISCOUNT",
                }
            ],
        )
        cud_cost = d(24 * 0.31)
        r(
            start=s,
            service={"id": "6F81-5844-456A", "description": "Compute Engine"},
            sku={"id": "2E27-4F75-95CD", "description": "N2 Instance Ram running in Americas"},
            project=shop,
            labels=[{"key": "team", "value": "storefront"}],
            location={"location": "us-central1", "country": "US", "region": "us-central1", "zone": "us-central1-b"},
            resource={
                "name": "db-1",
                "global_name": "//compute.googleapis.com/projects/shop-prod-1234/zones/us-central1-b/instances/1002",
            },
            cost=cud_cost,
            cost_at_list=cud_cost,
            usage={
                "amount": 3.0e12,
                "unit": "byte-seconds",
                "amount_in_pricing_units": 24.0,
                "pricing_unit": "gibibyte hour",
            },
            credits=[
                {
                    "name": "Committed use discount: RAM",
                    "amount": -d(cud_cost * 0.37),
                    "full_name": "Committed use discount: N2 RAM, 1 year",
                    "id": "cud-n2-ram-0001",
                    "type": "COMMITTED_USAGE_DISCOUNT",
                }
            ],
        )
        gb = 5000 + day.day * 10
        r(
            start=s,
            service={"id": "95FF-2EF5-5EA1", "description": "Cloud Storage"},
            sku={"id": "E5F0-6A5D-7BAD", "description": "Standard Storage US Multi-region"},
            project=shop,
            labels=[{"key": "team", "value": "data"}],
            location={"location": "us", "country": "US", "region": None, "zone": None},
            resource={"name": "shop-assets", "global_name": "//storage.googleapis.com/projects/_/buckets/shop-assets"},
            cost=d(gb / 31 * 0.026),
            cost_at_list=d(gb / 31 * 0.026),
            usage={
                "amount": gb * 2.6e12,
                "unit": "byte-seconds",
                "amount_in_pricing_units": d(gb / 31, 8),
                "pricing_unit": "gibibyte month",
            },
        )
        r(
            start=s,
            service={"id": "24E6-581D-38E5", "description": "BigQuery"},
            sku={"id": "3362-E469-6BEF", "description": "Analysis"},
            project=ml,
            location={"location": "US", "country": "US", "region": None, "zone": None},
            cost=d(0.000001 * day.day),
            cost_at_list=d(0.000001 * day.day),
            usage={
                "amount": 1.6e8 * day.day,
                "unit": "bytes",
                "amount_in_pricing_units": d(1.6e8 * day.day / 2**40, 12),
                "pricing_unit": "tebibyte",
            },
            credits=[
                {
                    "name": "Free Tier",
                    "amount": -d(0.000001 * day.day),
                    "full_name": "Free Tier",
                    "id": "",
                    "type": "FREE_TIER",
                }
            ],
        )
    s = dt.datetime(2026, 7, 1, tzinfo=UTC)
    r(
        start=s,
        hours=744,
        cost_type="tax",
        service={"id": "", "description": "Invoice"},
        sku={"id": "", "description": "Tax"},
        project=None,
        cost=18.75,
        cost_at_list=None,
        usage={"amount": 0.0, "unit": None, "amount_in_pricing_units": 0.0, "pricing_unit": None},
    )
    r(
        start=dt.datetime(2026, 6, 10, tzinfo=UTC),
        cost_type="adjustment",
        invoice={"month": "202607", "publisher_type": "GOOGLE"},
        service={"id": "6F81-5844-456A", "description": "Compute Engine"},
        sku={"id": "CF4E-A0C7-E3BF", "description": "N2 Instance Core running in Americas"},
        project=shop,
        cost=-4.2,
        cost_at_list=-4.2,
        adjustment_info={
            "id": "adj-0001",
            "description": "Billing correction",
            "mode": "COMPLETE_NEGATION",
            "type": "USAGE_CORRECTION",
        },
    )
    r(
        start=s,
        hours=744,
        service={"id": "MKT-1", "description": "Acme Vector DB"},
        sku={"id": "MKT-SKU-1", "description": "Acme Vector DB Pro plan"},
        project=ml,
        transaction_type="THIRD_PARTY_RESELLER",
        seller_name="Acme Vector Inc.",
        cost=300.0,
        cost_at_list=300.0,
        subscription={"instance_id": "sub-acme-1"},
        invoice={"month": "202607", "publisher_type": "MARKETPLACE"},
        usage={"amount": 1.0, "unit": "month", "amount_in_pricing_units": 1.0, "pricing_unit": "month"},
    )
    return rows


def write_gcp() -> None:
    out = ROOT / "gcp"
    out.mkdir(parents=True, exist_ok=True)
    rows = gcp_rows()
    schema = gcp_schema()
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, out / "gcp_billing_export_v1_01A2B3_202607.parquet", row_group_size=60)
    with (out / "gcp_billing_export_v1_01A2B3_202607.jsonl").open("w") as fh:
        for row in rows[:20] + rows[-3:]:
            fh.write(json.dumps(row, default=lambda o: o.strftime("%Y-%m-%d %H:%M:%S UTC")) + "\n")


# ------------------------------------------------------------- native FOCUS 1.2


def write_focus12() -> None:
    """An AWS-style native FOCUS 1.2 Parquet export with x_ provider columns."""
    rows = []
    for i, day in enumerate(days(2026, 7)[:10]):
        s = dt.datetime.combine(day, dt.time(), tzinfo=UTC)
        cost = Decimal("1.958400") if i % 2 == 0 else Decimal("0.000417")
        rows.append(
            {
                "AvailabilityZone": "us-east-1a",
                "BilledCost": cost,
                "BillingAccountId": PAYER[0],
                "BillingAccountName": PAYER[1],
                "BillingAccountType": "Management Account",
                "BillingCurrency": "USD",
                "BillingPeriodEnd": dt.datetime(2026, 8, 1, tzinfo=UTC),
                "BillingPeriodStart": dt.datetime(2026, 7, 1, tzinfo=UTC),
                "CapacityReservationId": None,
                "CapacityReservationStatus": None,
                "ChargeCategory": "Usage",
                "ChargeClass": None,
                "ChargeDescription": "$0.0816 per On Demand Linux m7g.large Instance Hour",
                "ChargeFrequency": "Usage-Based",
                "ChargePeriodEnd": s + dt.timedelta(days=1),
                "ChargePeriodStart": s,
                "CommitmentDiscountCategory": None,
                "CommitmentDiscountId": None,
                "CommitmentDiscountName": None,
                "CommitmentDiscountQuantity": None,
                "CommitmentDiscountStatus": None,
                "CommitmentDiscountType": None,
                "CommitmentDiscountUnit": None,
                "ConsumedQuantity": Decimal("24"),
                "ConsumedUnit": "Hours",
                "ContractedCost": cost,
                "ContractedUnitPrice": Decimal("0.0816"),
                "EffectiveCost": cost,
                "InvoiceId": "EUINUS2026070001",
                "InvoiceIssuerName": "Amazon Web Services, Inc.",
                "ListCost": cost,
                "ListUnitPrice": Decimal("0.0816"),
                "PricingCategory": "Standard",
                "PricingCurrency": "USD",
                "PricingCurrencyContractedUnitPrice": Decimal("0.0816"),
                "PricingCurrencyEffectiveCost": cost,
                "PricingCurrencyListUnitPrice": Decimal("0.0816"),
                "PricingQuantity": Decimal("24"),
                "PricingUnit": "Hours",
                "ProviderName": "AWS",
                "PublisherName": "AWS",
                "RegionId": "us-east-1",
                "RegionName": "US East (N. Virginia)",
                "ResourceId": f"arn:aws:ec2:us-east-1:{DEV[0]}:instance/i-0f{i:04d}",
                "ResourceName": None,
                "ResourceType": "Instance",
                "ServiceCategory": "Compute",
                "ServiceName": "Amazon Elastic Compute Cloud",
                "ServiceSubcategory": "Virtual Machines",
                "SkuId": "3DG6WFZ5QW4JAAHJ",
                "SkuMeter": "BoxUsage",
                "SkuPriceDetails": json.dumps({"InstanceType": "m7g.large"}),
                "SkuPriceId": "3DG6WFZ5QW4JAAHJ.JRTCKXETXF.6YS6EN2CT7",
                "SubAccountId": DEV[0],
                "SubAccountName": DEV[1],
                "SubAccountType": "Member Account",
                "Tags": [("team", "search"), ("env", "dev")],
                "x_CostCategories": [("BusinessUnit", "Retail")],
                "x_Discounts": None,
                "x_Operation": "RunInstances",
                "x_ServiceCode": "AmazonEC2",
                "x_UsageType": "USE1-BoxUsage:m7g.large",
            }
        )
    money = pa.decimal128(38, 10)
    types = {
        "BilledCost": money,
        "ContractedCost": money,
        "EffectiveCost": money,
        "ListCost": money,
        "PricingCurrencyEffectiveCost": money,
        "ContractedUnitPrice": money,
        "ListUnitPrice": money,
        "PricingCurrencyContractedUnitPrice": money,
        "PricingCurrencyListUnitPrice": money,
        "ConsumedQuantity": money,
        "PricingQuantity": money,
        "CommitmentDiscountQuantity": money,
        "BillingPeriodEnd": pa.timestamp("ms", tz="UTC"),
        "BillingPeriodStart": pa.timestamp("ms", tz="UTC"),
        "ChargePeriodEnd": pa.timestamp("ms", tz="UTC"),
        "ChargePeriodStart": pa.timestamp("ms", tz="UTC"),
        "Tags": pa.map_(pa.string(), pa.string()),
        "x_CostCategories": pa.map_(pa.string(), pa.string()),
        "x_Discounts": pa.map_(pa.string(), pa.float64()),
    }
    schema = pa.schema([(k, types.get(k, pa.string())) for k in rows[0]])
    out = ROOT / "focus12"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out / "aws-focus-1.2-00001.parquet")


def main() -> None:
    write_cur2()
    write_cur_legacy()
    write_azure()
    write_gcp()
    write_focus12()
    print(f"fixtures written under {ROOT}")


if __name__ == "__main__":
    main()
