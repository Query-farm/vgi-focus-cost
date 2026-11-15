"""AWS Cost and Usage Report 2.0 (Data Exports) -> FOCUS.

Reads CUR 2.0 Parquet/CSV (``line_item_unblended_cost`` style names, ``product``,
``resource_tags`` and ``cost_category`` maps) and legacy CUR / Athena CUR
(``lineItem/UnblendedCost`` headers or flattened ``resource_tags_user_*``
columns), which are renamed to CUR 2.0 names first.

Amortization follows AWS's documented amortized-cost formula: Savings Plan and
Reserved Instance covered usage carries the commitment's effective cost, upfront
fees are zeroed (they are spread into usage), and the unused part of a recurring
commitment fee becomes a separate ``Usage`` row with
``CommitmentDiscountStatus = 'Unused'`` (FOCUS's representation of waste).
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from .. import services
from ..convert import lookup, map_values, to_string
from .base import Rule, Src, isin, money_sub, when, zero_money
from .common import MapOptions, Mapper

# line_item_line_item_type -> (ChargeCategory, ChargeFrequency)
LINE_ITEM_TYPES: dict[str, tuple[str, str]] = {
    "Usage": ("Usage", "Usage-Based"),
    "DiscountedUsage": ("Usage", "Usage-Based"),
    "SavingsPlanCoveredUsage": ("Usage", "Usage-Based"),
    "SavingsPlanNegation": ("Usage", "Usage-Based"),
    "Tax": ("Tax", "Usage-Based"),
    "Fee": ("Purchase", "One-Time"),
    "RIFee": ("Purchase", "Recurring"),
    "SavingsPlanUpfrontFee": ("Purchase", "One-Time"),
    "SavingsPlanRecurringFee": ("Purchase", "Recurring"),
    "Credit": ("Credit", "One-Time"),
    "Refund": ("Adjustment", "One-Time"),
    "BundledDiscount": ("Credit", "Usage-Based"),
    "EdpDiscount": ("Credit", "Usage-Based"),
    "PrivateRateDiscount": ("Credit", "Usage-Based"),
    "Discount": ("Credit", "Usage-Based"),
    "DistributorDiscount": ("Credit", "Usage-Based"),
}
COMMITMENT_USED = ("DiscountedUsage", "SavingsPlanCoveredUsage")
RECURRING_COMMITMENT = ("RIFee", "SavingsPlanRecurringFee")

_UNBLENDED = "line_item_unblended_cost"
_TYPE = "line_item_line_item_type"
_SP_ARN = "savings_plan_savings_plan_a_r_n"
_RI_ARN = "reservation_reservation_a_r_n"

RULES: tuple[Rule, ...] = (
    Rule("BilledCost", (_UNBLENDED,), "line_item_unblended_cost, exact decimal",
         "Savings Plan covered usage is billed at on-demand and offset by SavingsPlanNegation rows, so sums match the invoice."),
    Rule("EffectiveCost", (_UNBLENDED, _TYPE, "savings_plan_savings_plan_effective_cost", "reservation_effective_cost",
                           "savings_plan_total_commitment_to_date", "savings_plan_used_commitment",
                           "reservation_unused_amortized_upfront_fee_for_billing_period", "reservation_unused_recurring_fee",
                           _RI_ARN),
         "SavingsPlanCoveredUsage -> savings_plan_savings_plan_effective_cost; DiscountedUsage -> reservation_effective_cost; "
         "SavingsPlanNegation, SavingsPlanUpfrontFee, RI upfront Fee, RIFee and SavingsPlanRecurringFee -> 0 "
         "(their unused part becomes a separate Unused row); everything else -> unblended cost",
         "AWS's amortized-cost formula."),
    Rule("ListCost", ("pricing_public_on_demand_cost", _UNBLENDED), "pricing_public_on_demand_cost, else BilledCost"),
    Rule("ContractedCost", (_UNBLENDED,), "line_item_unblended_cost (the negotiated rate times usage)"),
    Rule("ListUnitPrice", ("pricing_public_on_demand_rate",), "pricing_public_on_demand_rate"),
    Rule("ContractedUnitPrice", ("line_item_unblended_rate",), "line_item_unblended_rate"),
    Rule("BillingAccountId", ("bill_payer_account_id",), "bill_payer_account_id", direct="bill_payer_account_id"),
    Rule("BillingAccountName", ("bill_payer_account_name",), "bill_payer_account_name"),
    Rule("BillingAccountType", (), "constant 'Management Account'"),
    Rule("BillingCurrency", ("line_item_currency_code",), "line_item_currency_code"),
    Rule("BillingPeriodStart", ("bill_billing_period_start_date",), "bill_billing_period_start_date as UTC",
         direct="bill_billing_period_start_date"),
    Rule("BillingPeriodEnd", ("bill_billing_period_end_date",), "bill_billing_period_end_date as UTC"),
    Rule("ChargePeriodStart", ("line_item_usage_start_date",), "line_item_usage_start_date as UTC",
         direct="line_item_usage_start_date"),
    Rule("ChargePeriodEnd", ("line_item_usage_end_date",), "line_item_usage_end_date as UTC",
         direct="line_item_usage_end_date"),
    Rule("ChargeCategory", (_TYPE,), "line_item_line_item_type through charge_type_map",
         "Unknown line item types map to 'Adjustment' and keep their raw value in x_ChargeType."),
    Rule("ChargeClass", ("line_item_usage_start_date", "bill_billing_period_start_date"),
         "'Correction' when the charge period starts before the billing period, else NULL"),
    Rule("ChargeDescription", ("line_item_line_item_description",), "line_item_line_item_description"),
    Rule("ChargeFrequency", (_TYPE,), "line_item_line_item_type through charge_type_map"),
    Rule("CommitmentDiscountId", (_SP_ARN, _RI_ARN), "savings_plan_savings_plan_a_r_n, else reservation_reservation_a_r_n"),
    Rule("CommitmentDiscountType", (_SP_ARN, _RI_ARN), "'Savings Plan' or 'Reserved Instance' by which ARN is set"),
    Rule("CommitmentDiscountCategory", (_SP_ARN, _RI_ARN), "'Spend' for Savings Plans, 'Usage' for Reserved Instances"),
    Rule("CommitmentDiscountStatus", (_TYPE,),
         "'Used' for DiscountedUsage/SavingsPlanCoveredUsage, 'Unused' on the synthetic unused-commitment row"),
    Rule("PricingCategory", (_TYPE, "pricing_term", "line_item_usage_type"),
         "'Committed' for commitment-covered usage and SavingsPlanNegation, 'Dynamic' for Spot, 'Standard' for other usage and purchases"),
    Rule("PricingQuantity", ("line_item_usage_amount",), "line_item_usage_amount (NULL for Tax, Credit and SavingsPlanNegation)"),
    Rule("PricingUnit", ("pricing_unit",), "pricing_unit (NULL for Tax, Credit and SavingsPlanNegation)"),
    Rule("ConsumedQuantity", ("line_item_usage_amount",), "line_item_usage_amount for Usage rows that are not Unused or SavingsPlanNegation"),
    Rule("ConsumedUnit", ("pricing_unit",), "pricing_unit for Usage rows that are not Unused or SavingsPlanNegation"),
    Rule("PricingCurrency", ("pricing_currency", "line_item_currency_code"), "pricing_currency, else line_item_currency_code"),
    Rule("InvoiceId", ("bill_invoice_id",), "bill_invoice_id"),
    Rule("InvoiceIssuerName", ("bill_invoicing_entity",), "bill_invoicing_entity, else 'AWS'"),
    Rule("ProviderName", (), "constant 'AWS'"),
    Rule("PublisherName", ("bill_billing_entity", "line_item_legal_entity"),
         "line_item_legal_entity for AWS Marketplace rows, else 'AWS'"),
    Rule("RegionId", ("product_region_code", "product_region", "product"), "product_region_code (legacy product/region), else product['region']"),
    Rule("RegionName", ("product_location", "product"), "product_location, else product['location']"),
    Rule("AvailabilityZone", ("line_item_availability_zone",), "line_item_availability_zone"),
    Rule("ResourceId", ("line_item_resource_id",), "line_item_resource_id"),
    Rule("ServiceName", ("product", "product_product_name", "line_item_product_code"),
         "product['product_name'] (legacy product_product_name), else service_map name for the product code, else the code"),
    Rule("ServiceCategory", ("line_item_product_code",), "service_map on line_item_product_code, else 'Other'"),
    Rule("ServiceSubcategory", ("line_item_product_code",), "service_map on line_item_product_code, else 'Other (Other)'"),
    Rule("SkuId", ("product_sku",), "product_sku"),
    Rule("SkuPriceId", ("pricing_rate_id",), "pricing_rate_id"),
    Rule("SkuMeter", ("product_usagetype", "line_item_usage_type"), "product_usagetype, else line_item_usage_type"),
    Rule("SubAccountId", ("line_item_usage_account_id",), "line_item_usage_account_id", direct="line_item_usage_account_id"),
    Rule("SubAccountName", ("line_item_usage_account_name",), "line_item_usage_account_name"),
    Rule("SubAccountType", (), "constant 'Member Account'"),
    Rule("CapacityReservationId", ("capacity_reservation_capacity_reservation_a_r_n",),
         "capacity_reservation_capacity_reservation_a_r_n"),
    Rule("Tags", ("resource_tags", "resource_tags_*"),
         "resource_tags map (CUR 2.0) or resource_tags_* columns (legacy); 'user_'/'user:' key prefixes removed"),
    Rule("x_ServiceCode", ("line_item_product_code",), "line_item_product_code", direct="line_item_product_code"),
    Rule("x_UsageType", ("line_item_usage_type",), "line_item_usage_type"),
    Rule("x_Operation", ("line_item_operation",), "line_item_operation"),
    Rule("x_ChargeType", (_TYPE,), "line_item_line_item_type ('<type> (unused)' on synthetic unused rows)"),
)


def _strip_user(key: str) -> str:
    for prefix in ("user_", "user:"):
        if key.startswith(prefix):
            return key[len(prefix):]
    return key


def _map_field(src: Src, column: str, key: str) -> pa.Array | None:
    arr = src.get(column)
    if arr is None or not pa.types.is_map(arr.type):
        return None
    value = pc.map_lookup(arr, pa.scalar(key, arr.type.key_type), "first")
    return to_string(value, src.n)


def _tags(src: Src) -> pa.Array:
    arr = src.get("resource_tags")
    if arr is not None and not pa.types.is_null(arr.type):
        return src.tags("resource_tags", key_transform=_strip_user)
    names = [n for n in src.batch.schema.names if n.lower().startswith("resource_tags_")]
    if not names:
        return src.null(pa.map_(pa.string(), pa.string()))
    cols = {n[len("resource_tags_"):]: src.string(n).to_pylist() for n in names}
    rows: list[list[tuple[str, str]] | None] = []
    for i in range(src.n):
        pairs = [(_strip_user(k), v[i]) for k, v in cols.items() if v[i] is not None]
        rows.append(pairs or None)
    return pa.array(rows, pa.map_(pa.string(), pa.string()))


def map_aws(src: Src, opts: MapOptions) -> dict[str, pa.Array]:
    """Map one CUR batch to canonical FOCUS columns (before unused-row expansion)."""
    s = opts.money_scale
    n = src.n
    lit = src.string(_TYPE)
    unblended = src.money(s, _UNBLENDED)
    sp_eff = src.money(s, "savings_plan_savings_plan_effective_cost")
    ri_eff = src.money(s, "reservation_effective_cost")
    sp_arn = src.string(_SP_ARN)
    ri_arn = src.string(_RI_ARN)
    zero = zero_money(n, s)
    is_ri_upfront = pc.and_(pc.fill_null(pc.equal(lit, "Fee"), False), pc.is_valid(ri_arn))
    zeroed = pc.or_(isin(lit, ("SavingsPlanNegation", "SavingsPlanUpfrontFee", *RECURRING_COMMITMENT)), is_ri_upfront)
    effective = when(pc.equal(lit, "SavingsPlanCoveredUsage"), pc.coalesce(sp_eff, unblended),
                     when(pc.equal(lit, "DiscountedUsage"), pc.coalesce(ri_eff, unblended),
                          when(zeroed, zero, unblended)))
    category = lookup(lit, {k: v[0] for k, v in LINE_ITEM_TYPES.items()}, pa.string(), "Adjustment")
    frequency = lookup(lit, {k: v[1] for k, v in LINE_ITEM_TYPES.items()}, pa.string(), "One-Time")
    cd_id = pc.coalesce(sp_arn, ri_arn)
    cd_type = when(pc.is_valid(sp_arn), src.const("Savings Plan"), when(pc.is_valid(ri_arn), src.const("Reserved Instance"), src.null(pa.string())))
    cd_cat = when(pc.is_valid(sp_arn), src.const("Spend"), when(pc.is_valid(ri_arn), src.const("Usage"), src.null(pa.string())))
    used = isin(lit, COMMITMENT_USED)
    negation = pc.fill_null(pc.equal(lit, "SavingsPlanNegation"), False)
    cd_status = when(used, src.const("Used"), src.null(pa.string()))
    term = src.string("pricing_term")
    usage_type = src.string("line_item_usage_type")
    spot = pc.or_(pc.fill_null(pc.equal(term, "Spot"), False),
                  pc.fill_null(pc.match_substring(usage_type, "SpotUsage"), False))
    usage_or_purchase = isin(category, ("Usage", "Purchase"))
    pricing_cat = when(pc.or_(used, negation), src.const("Committed"),
                       when(spot, src.const("Dynamic"),
                            when(usage_or_purchase, src.const("Standard"), src.null(pa.string()))))
    # Negation rows offset covered usage; their quantity would double count it.
    no_qty = pc.or_(isin(category, ("Tax", "Credit")), negation)
    qty = src.quantity("line_item_usage_amount")
    unit = src.string("pricing_unit")
    code = src.string("line_item_product_code")
    product_name = _map_field(src, "product", "product_name")
    service_name = pc.coalesce(
        *(a for a in (product_name, src.string("product_product_name")) if a is not None),
        map_values(code, lambda c: (services.find("aws", c).name if services.find("aws", c) else c), pa.string()),
    )
    billing_entity = src.string("bill_billing_entity")
    marketplace = pc.fill_null(pc.equal(billing_entity, "AWS Marketplace"), False)
    region = src.string("product_region_code", "product_region")
    region_from_map = _map_field(src, "product", "region")
    location = src.string("product_location")
    location_from_map = _map_field(src, "product", "location")
    currency = src.string("line_item_currency_code")
    return {
        "BilledCost": unblended,
        "EffectiveCost": effective,
        "ListCost": pc.coalesce(src.money(s, "pricing_public_on_demand_cost"), unblended),
        "ContractedCost": unblended,
        "ListUnitPrice": src.price("pricing_public_on_demand_rate"),
        "ContractedUnitPrice": src.price("line_item_unblended_rate"),
        "BillingAccountId": src.string("bill_payer_account_id"),
        "BillingAccountName": src.string("bill_payer_account_name"),
        "BillingAccountType": src.const("Management Account"),
        "BillingCurrency": currency,
        "BillingPeriodStart": src.timestamp("bill_billing_period_start_date"),
        "BillingPeriodEnd": src.timestamp("bill_billing_period_end_date"),
        "ChargePeriodStart": src.timestamp("line_item_usage_start_date"),
        "ChargePeriodEnd": src.timestamp("line_item_usage_end_date"),
        "ChargeCategory": category,
        "ChargeDescription": src.string("line_item_line_item_description"),
        "ChargeFrequency": frequency,
        "CommitmentDiscountId": cd_id,
        "CommitmentDiscountType": cd_type,
        "CommitmentDiscountCategory": cd_cat,
        "CommitmentDiscountStatus": cd_status,
        "PricingCategory": pricing_cat,
        "PricingQuantity": when(no_qty, src.null(qty.type), qty),
        "PricingUnit": when(no_qty, src.null(pa.string()), unit),
        "ConsumedQuantity": when(no_qty, src.null(qty.type), qty),
        "ConsumedUnit": when(no_qty, src.null(pa.string()), unit),
        "PricingCurrency": pc.coalesce(src.string("pricing_currency"), currency),
        "InvoiceId": src.string("bill_invoice_id"),
        "InvoiceIssuerName": pc.coalesce(src.string("bill_invoicing_entity"), src.const("AWS")),
        "ProviderName": src.const("AWS"),
        "PublisherName": when(marketplace, pc.coalesce(src.string("line_item_legal_entity"), src.const("AWS Marketplace")), src.const("AWS")),
        "RegionId": pc.coalesce(*(a for a in (region, region_from_map) if a is not None)),
        "RegionName": pc.coalesce(*(a for a in (location, location_from_map) if a is not None)),
        "AvailabilityZone": src.string("line_item_availability_zone"),
        "ResourceId": src.string("line_item_resource_id"),
        "ServiceName": service_name,
        "ServiceCategory": map_values(code, lambda c: services.category("aws", c), pa.string(), skip_null=False),
        "ServiceSubcategory": map_values(code, lambda c: services.subcategory("aws", c), pa.string(), skip_null=False),
        "SkuId": src.string("product_sku"),
        "SkuPriceId": src.string("pricing_rate_id"),
        "SkuMeter": pc.coalesce(src.string("product_usagetype"), usage_type),
        "SubAccountId": src.string("line_item_usage_account_id"),
        "SubAccountName": src.string("line_item_usage_account_name"),
        "SubAccountType": src.const("Member Account"),
        "CapacityReservationId": src.string("capacity_reservation_capacity_reservation_a_r_n"),
        "Tags": _tags(src),
        "x_ServiceCode": code,
        "x_UsageType": usage_type,
        "x_Operation": src.string("line_item_operation"),
        "x_ChargeType": lit,
    }


def unused_rows(src: Src, mapped: dict[str, pa.Array], opts: MapOptions) -> dict[str, pa.Array] | None:
    """Synthetic 'Unused' usage rows for the unused part of recurring commitment fees.

    A RIFee / SavingsPlanRecurringFee row is a Purchase whose EffectiveCost is 0;
    the waste is emitted as an extra Usage row with BilledCost 0 and EffectiveCost
    equal to the unused amount, so EffectiveCost still sums to AWS's amortized total.
    """
    s = opts.money_scale
    lit = mapped["x_ChargeType"]
    sp_unused = money_sub(src.money(s, "savings_plan_total_commitment_to_date"), src.money(s, "savings_plan_used_commitment"), s)
    ri_unused_upfront = src.money(s, "reservation_unused_amortized_upfront_fee_for_billing_period")
    ri_unused_recurring = src.money(s, "reservation_unused_recurring_fee")
    ri_unused = pc.cast(pc.add(pc.coalesce(ri_unused_upfront, zero_money(src.n, s)),
                               pc.coalesce(ri_unused_recurring, zero_money(src.n, s))), pa.decimal128(19, s))
    unused = when(pc.equal(lit, "SavingsPlanRecurringFee"), sp_unused,
                  when(pc.equal(lit, "RIFee"), ri_unused, src.null(pa.decimal128(19, s))))
    mask = pc.fill_null(pc.not_equal(unused, pa.scalar(0, pa.decimal128(19, s))), False)
    if not pc.any(mask).as_py():
        return None
    extra = {k: pc.filter(v, mask) for k, v in mapped.items()}
    m = len(extra["BilledCost"])
    amount = pc.filter(unused, mask)
    zero = zero_money(m, s)
    extra.update(
        BilledCost=zero,
        ContractedCost=zero,
        ListCost=zero,
        EffectiveCost=amount,
        ChargeCategory=pa.array(["Usage"] * m, pa.string()),
        ChargeFrequency=pa.array(["Usage-Based"] * m, pa.string()),
        CommitmentDiscountStatus=pa.array(["Unused"] * m, pa.string()),
        PricingCategory=pa.array(["Committed"] * m, pa.string()),
        x_ChargeType=pc.binary_join_element_wise(extra["x_ChargeType"], pa.array([" (unused)"] * m), ""),
    )
    return extra


MAPPER = Mapper(
    token="aws_cur2",
    provider="aws",
    title="AWS Cost and Usage Report 2.0 / Data Exports (and legacy CUR)",
    rules=RULES,
    signature=("line_item_unblended_cost", "line_item_line_item_type"),
    map_fn=map_aws,
    expand_fn=unused_rows,
    rename_fn=None,
    row_sources=(_TYPE, "savings_plan_total_commitment_to_date", "savings_plan_used_commitment",
                 "reservation_unused_amortized_upfront_fee_for_billing_period", "reservation_unused_recurring_fee"),
)
