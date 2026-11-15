"""Google Cloud BigQuery billing export (standard or detailed) -> FOCUS.

Read the export after dumping it from BigQuery to Parquet or newline-delimited
JSON (both keep the nested ``service``, ``sku``, ``project``, ``location``,
``usage``, ``invoice`` structs and the ``credits`` / ``labels`` arrays). Flattened
dumps whose columns are named ``service_description``, ``usage_amount`` ... work
too; a CSV dump cannot carry ``credits``, so its BilledCost excludes credits.

``BilledCost`` and ``EffectiveCost`` are ``cost`` plus the sum of the row's
``credits.amount`` (credits are negative), which is what the invoice totals.
Committed-use-discount credits identify the commitment
(``CommitmentDiscountId`` = the credit id).
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from typing import Any

import pyarrow as pa

from .. import services
from ..convert import Arr, lookup, map_values, pc, to_decimal
from .base import Rule, Src, isin, money_add, when, zero_money
from .common import MapOptions, Mapper

COST_TYPES = {
    "regular": "Usage",
    "tax": "Tax",
    "adjustment": "Adjustment",
    "rounding_error": "Adjustment",
    "rounding error": "Adjustment",
}
CUD_TYPES = {"COMMITTED_USAGE_DISCOUNT": "Usage", "COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE": "Spend"}

RULES: tuple[Rule, ...] = (
    Rule("BilledCost", ("cost", "credits"), "cost + sum(credits.amount), exact decimal"),
    Rule("EffectiveCost", ("cost", "credits"), "cost + sum(credits.amount), exact decimal"),
    Rule("ListCost", ("cost_at_list", "cost"), "cost_at_list, else cost"),
    Rule("ContractedCost", ("cost",), "cost (negotiated price, before credits)"),
    Rule("ContractedUnitPrice", ("price.effective_price",), "price.effective_price (detailed export)"),
    Rule("BillingAccountId", ("billing_account_id",), "billing_account_id", direct="billing_account_id"),
    Rule("BillingAccountName", (), "NULL", "The BigQuery billing export does not carry the billing account name."),
    Rule("BillingAccountType", (), "constant 'Billing Account'"),
    Rule("BillingCurrency", ("currency",), "currency"),
    Rule("BillingPeriodStart", ("invoice.month",), "first day of invoice.month (YYYYMM), UTC"),
    Rule("BillingPeriodEnd", ("invoice.month",), "first day of the month after invoice.month, UTC"),
    Rule("ChargePeriodStart", ("usage_start_time",), "usage_start_time", direct="usage_start_time"),
    Rule("ChargePeriodEnd", ("usage_end_time",), "usage_end_time", direct="usage_end_time"),
    Rule("ChargeCategory", ("cost_type",), "cost_type through charge_type_map (regular -> Usage, tax -> Tax ...)"),
    Rule(
        "ChargeClass",
        ("usage_start_time", "invoice.month"),
        "'Correction' when the charge period starts before the billing period, else NULL",
    ),
    Rule("ChargeDescription", ("sku.description",), "sku.description"),
    Rule(
        "ChargeFrequency",
        ("cost_type", "subscription.instance_id"),
        "'Recurring' for subscription rows, else 'Usage-Based'",
    ),
    Rule("CommitmentDiscountId", ("credits",), "id of the row's committed-use-discount credit"),
    Rule("CommitmentDiscountName", ("credits",), "full_name (else name) of that credit"),
    Rule("CommitmentDiscountType", ("credits",), "'Committed Use Discount' when a CUD credit applies"),
    Rule("CommitmentDiscountCategory", ("credits",), "'Usage' (resource-based CUD) or 'Spend' (spend-based CUD)"),
    Rule("CommitmentDiscountStatus", ("credits",), "'Used' when a CUD credit applies"),
    Rule(
        "PricingCategory",
        ("credits", "sku.description", "cost_type"),
        "'Committed' with a CUD credit, 'Dynamic' for Spot/Preemptible SKUs, else 'Standard' (NULL for tax)",
    ),
    Rule("PricingQuantity", ("usage.amount_in_pricing_units",), "usage.amount_in_pricing_units"),
    Rule("PricingUnit", ("usage.pricing_unit",), "usage.pricing_unit"),
    Rule("ConsumedQuantity", ("usage.amount",), "usage.amount on Usage rows"),
    Rule("ConsumedUnit", ("usage.unit",), "usage.unit on Usage rows"),
    Rule("PricingCurrency", ("currency",), "currency"),
    Rule("InvoiceIssuerName", (), "constant 'Google Cloud'"),
    Rule("ProviderName", (), "constant 'Google Cloud'"),
    Rule("PublisherName", ("seller_name",), "seller_name (Marketplace), else 'Google Cloud'"),
    Rule("RegionId", ("location.region",), "location.region"),
    Rule("RegionName", ("location.region",), "location.region"),
    Rule("AvailabilityZone", ("location.zone",), "location.zone"),
    Rule(
        "ResourceId",
        ("resource.global_name", "resource.name"),
        "resource.global_name, else resource.name (detailed export)",
    ),
    Rule("ResourceName", ("resource.name",), "resource.name"),
    Rule("ServiceName", ("service.description",), "service.description, else 'Google Cloud'"),
    Rule("ServiceCategory", ("service.description",), "service_map on service.description, else 'Other'"),
    Rule("ServiceSubcategory", ("service.description",), "service_map on service.description, else 'Other (Other)'"),
    Rule("SkuId", ("sku.id",), "sku.id"),
    Rule("SkuMeter", ("sku.description",), "sku.description"),
    Rule("SubAccountId", ("project.id",), "project.id"),
    Rule("SubAccountName", ("project.name",), "project.name"),
    Rule("SubAccountType", (), "constant 'Project'"),
    Rule("Tags", ("labels",), "labels (array of {key, value}) as a map"),
    Rule("x_ServiceCode", ("service.id",), "service.id"),
    Rule("x_UsageType", ("sku.description",), "sku.description"),
    Rule("x_Operation", (), "NULL"),
    Rule("x_ChargeType", ("cost_type",), "cost_type"),
)


def _month_start(v: Any) -> dt.datetime | None:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) < 6 or not s[:6].isdigit():
        return None
    return dt.datetime(int(s[:4]), int(s[4:6]), 1, tzinfo=dt.UTC)


def _next_month(v: Any) -> dt.datetime | None:
    d = _month_start(v)
    if d is None:
        return None
    return d.replace(year=d.year + (d.month == 12), month=d.month % 12 + 1)


def _credit_rows(src: Src, n: int) -> list[list[dict[str, Any]] | None]:
    arr = src.get("credits")
    if arr is None or pa.types.is_null(arr.type):
        return [None] * n
    if pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
        out: list[list[dict[str, Any]] | None] = []
        for v in arr.to_pylist():
            try:
                out.append(json.loads(v) if v else None)
            except json.JSONDecodeError:
                out.append(None)
        return out
    return arr.to_pylist()  # type: ignore[no-any-return]


def map_gcp(src: Src, opts: MapOptions) -> dict[str, Arr]:
    """Map one billing-export batch to canonical FOCUS columns."""
    s = opts.money_scale
    n = src.n
    cost = src.money(s, "cost")
    credit_rows = _credit_rows(src, n)
    totals: list[str | None] = []
    cud_id: list[str | None] = []
    cud_name: list[str | None] = []
    cud_cat: list[str | None] = []
    for row in credit_rows:
        total = Decimal(0)
        cid = cname = ccat = None
        for c in row or ():
            if not c:
                continue
            amt = c.get("amount")
            if amt is not None:
                total += Decimal(repr(amt)) if isinstance(amt, float) else Decimal(str(amt))
            ctype = c.get("type")
            if cid is None and ctype in CUD_TYPES:
                cid = c.get("id") or c.get("name")
                cname = c.get("full_name") or c.get("name")
                ccat = CUD_TYPES[ctype]
        totals.append(format(total, "f"))
        cud_id.append(cid)
        cud_name.append(cname)
        cud_cat.append(ccat)
    credit_total = to_decimal(pa.array(totals, pa.string()), n, 19, s, what="credits.amount")
    billed = money_add(pc.coalesce(cost, zero_money(n, s)), credit_total, s)
    billed = when(pc.is_null(cost), src.null(billed.type), billed)
    cid_arr = pa.array(cud_id, pa.string())
    has_cud = pc.is_valid(cid_arr)
    cost_type = src.string("cost_type")
    category = lookup(cost_type, COST_TYPES, pa.string(), "Usage")
    sku_desc = src.string("sku.description")
    spot = pc.fill_null(pc.match_substring_regex(sku_desc, "(?i)spot|preemptible"), False)
    none = src.null(pa.string())
    pricing = when(
        isin(category, ("Usage", "Purchase")),
        when(has_cud, src.const("Committed"), when(spot, src.const("Dynamic"), src.const("Standard"))),
        none,
    )
    subscription = pc.is_valid(src.string("subscription.instance_id"))
    month = src.get("invoice.month")
    month_arr = month if month is not None else src.null(pa.string())
    service = src.string("service.description")
    return {
        "BilledCost": billed,
        "EffectiveCost": billed,
        "ListCost": pc.coalesce(src.money(s, "cost_at_list"), cost),
        "ContractedCost": cost,
        "ContractedUnitPrice": src.price("price.effective_price"),
        "BillingAccountId": src.string("billing_account_id"),
        "BillingAccountType": src.const("Billing Account"),
        "BillingCurrency": src.string("currency"),
        "BillingPeriodStart": map_values(month_arr, _month_start, pa.timestamp("us", tz="UTC")),
        "BillingPeriodEnd": map_values(month_arr, _next_month, pa.timestamp("us", tz="UTC")),
        "ChargePeriodStart": src.timestamp("usage_start_time"),
        "ChargePeriodEnd": src.timestamp("usage_end_time"),
        "ChargeCategory": category,
        "ChargeDescription": sku_desc,
        "ChargeFrequency": when(subscription, src.const("Recurring"), src.const("Usage-Based")),
        "CommitmentDiscountId": cid_arr,
        "CommitmentDiscountName": pa.array(cud_name, pa.string()),
        "CommitmentDiscountType": when(has_cud, src.const("Committed Use Discount"), none),
        "CommitmentDiscountCategory": pa.array(cud_cat, pa.string()),
        "CommitmentDiscountStatus": when(has_cud, src.const("Used"), none),
        "PricingCategory": pricing,
        "PricingQuantity": src.quantity("usage.amount_in_pricing_units"),
        "PricingUnit": src.string("usage.pricing_unit"),
        "ConsumedQuantity": src.quantity("usage.amount"),
        "ConsumedUnit": src.string("usage.unit"),
        "PricingCurrency": src.string("currency"),
        "InvoiceIssuerName": src.const("Google Cloud"),
        "ProviderName": src.const("Google Cloud"),
        "PublisherName": pc.coalesce(src.string("seller_name"), src.const("Google Cloud")),
        "RegionId": src.string("location.region"),
        "RegionName": src.string("location.region"),
        "AvailabilityZone": src.string("location.zone"),
        "ResourceId": pc.coalesce(src.string("resource.global_name"), src.string("resource.name")),
        "ResourceName": src.string("resource.name"),
        "ServiceName": pc.coalesce(service, src.const("Google Cloud")),
        "ServiceCategory": map_values(service, lambda v: services.category("gcp", v), pa.string()),
        "ServiceSubcategory": map_values(service, lambda v: services.subcategory("gcp", v), pa.string()),
        "SkuId": src.string("sku.id"),
        "SkuMeter": sku_desc,
        "SubAccountId": src.string("project.id"),
        "SubAccountName": src.string("project.name"),
        "SubAccountType": src.const("Project"),
        "Tags": src.tags("labels"),
        "x_ServiceCode": src.string("service.id"),
        "x_UsageType": sku_desc,
        "x_ChargeType": cost_type,
    }


MAPPER = Mapper(
    token="gcp",
    provider="gcp",
    title="Google Cloud BigQuery billing export (standard or detailed), dumped to Parquet / JSON / CSV",
    rules=RULES,
    signature=("billing_account_id", "usage_start_time"),
    map_fn=map_gcp,
)
