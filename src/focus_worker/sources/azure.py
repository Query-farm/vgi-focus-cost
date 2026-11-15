"""Azure Cost Management exports (actual or amortized cost) -> FOCUS.

Covers the EA / MCA "Cost and usage details" export schema
(``CostInBillingCurrency``, ``ChargeType``, ``PricingModel``, ``MeterCategory``,
``Tags`` ...). Column names match case-insensitively, and the older ``Cost`` /
``PreTaxCost`` / ``BillingCurrency`` spellings are accepted. Azure's own FOCUS
export should be read with ``source := 'focus'``.

Azure publishes billed and amortized cost as two different exports:

* ``azure`` (actual cost): ``BilledCost`` is the row cost. Commitment purchases
  are not spread, so ``EffectiveCost`` equals ``BilledCost`` except that a
  reservation/savings-plan purchase (or refund) gets ``EffectiveCost = 0`` and is
  not double counted. Use the amortized export when you need amortized ``EffectiveCost``.
* ``azure_amortized``: ``EffectiveCost`` is the row cost; rows covered by a
  commitment (and ``UnusedReservation``/``UnusedSavingsPlan`` rows) are not
  billed again, so their ``BilledCost`` is 0.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa

from .. import services
from ..convert import Arr, lookup, map_values, pc
from .base import Rule, Src, isin, money_mul, when, zero_money
from .common import MapOptions, Mapper

COST = ("CostInBillingCurrency", "Cost", "PreTaxCost")
CURRENCY = ("BillingCurrencyCode", "BillingCurrency", "Currency")

# ChargeType -> ChargeCategory
CHARGE_TYPES: dict[str, str] = {
    "Usage": "Usage",
    "Purchase": "Purchase",
    "Refund": "Purchase",
    "Tax": "Tax",
    "UnusedReservation": "Usage",
    "UnusedSavingsPlan": "Usage",
    "RoundingAdjustment": "Adjustment",
    "Credit": "Credit",
    "Adjustment": "Adjustment",
}
FREQUENCIES = {"UsageBased": "Usage-Based", "OneTime": "One-Time", "Recurring": "Recurring"}
PRICING_MODELS = {"OnDemand": "Standard", "Spot": "Dynamic", "Reservation": "Committed", "SavingsPlan": "Committed"}
COMMITMENT_MODELS = ("Reservation", "SavingsPlan")
UNUSED_TYPES = ("UnusedReservation", "UnusedSavingsPlan")

_SHARED_RULES: tuple[Rule, ...] = (
    Rule(
        "ListCost",
        ("PaygCostInBillingCurrency", "PayGPrice", "Quantity", *COST),
        "PaygCostInBillingCurrency, else PayGPrice x Quantity, else the row cost",
    ),
    Rule(
        "ContractedCost",
        ("UnitPrice", "Quantity", "ExchangeRatePricingToBilling", *COST),
        "UnitPrice x Quantity x ExchangeRatePricingToBilling (exact, half-even), else the row cost",
    ),
    Rule("ListUnitPrice", ("PayGPrice", "MarketPrice"), "PayGPrice, else MarketPrice"),
    Rule("ContractedUnitPrice", ("UnitPrice",), "UnitPrice"),
    Rule("BillingAccountId", ("BillingAccountId",), "BillingAccountId", direct="BillingAccountId"),
    Rule("BillingAccountName", ("BillingAccountName",), "BillingAccountName"),
    Rule("BillingAccountType", (), "constant 'Billing Account'"),
    Rule("BillingCurrency", CURRENCY, "BillingCurrencyCode (or BillingCurrency / Currency)"),
    Rule("BillingPeriodStart", ("BillingPeriodStartDate",), "BillingPeriodStartDate at 00:00 UTC"),
    Rule("BillingPeriodEnd", ("BillingPeriodEndDate",), "BillingPeriodEndDate + 1 day (Azure's end date is inclusive)"),
    Rule("ChargePeriodStart", ("Date", "UsageDateTime"), "Date (MM/DD/YYYY or ISO) at 00:00 UTC"),
    Rule("ChargePeriodEnd", ("Date", "UsageDateTime"), "ChargePeriodStart + 1 day (daily granularity)"),
    Rule(
        "ChargeCategory",
        ("ChargeType",),
        "ChargeType through charge_type_map",
        "UnusedReservation/UnusedSavingsPlan are Usage rows with CommitmentDiscountStatus 'Unused'.",
    ),
    Rule(
        "ChargeClass",
        ("Date", "BillingPeriodStartDate"),
        "'Correction' when the charge period starts before the billing period, else NULL",
    ),
    Rule("ChargeDescription", ("ProductName", "MeterName"), "ProductName, else MeterName"),
    Rule(
        "ChargeFrequency", ("Frequency",), "Frequency: UsageBased/OneTime/Recurring -> Usage-Based/One-Time/Recurring"
    ),
    Rule(
        "CommitmentDiscountId",
        ("benefitId", "ReservationId", "PricingModel", "ChargeType"),
        "benefitId, else ReservationId, on commitment rows",
    ),
    Rule(
        "CommitmentDiscountName",
        ("benefitName", "ReservationName", "PricingModel", "ChargeType"),
        "benefitName, else ReservationName, on commitment rows",
    ),
    Rule("CommitmentDiscountType", ("PricingModel",), "'Reservation' or 'Savings Plan' from PricingModel"),
    Rule("CommitmentDiscountCategory", ("PricingModel",), "'Usage' for reservations, 'Spend' for savings plans"),
    Rule(
        "CommitmentDiscountStatus",
        ("ChargeType", "PricingModel"),
        "'Unused' for UnusedReservation/UnusedSavingsPlan, 'Used' for commitment-priced Usage",
    ),
    Rule(
        "PricingCategory",
        ("PricingModel", "ChargeType"),
        "OnDemand -> Standard, Spot -> Dynamic, Reservation/SavingsPlan -> Committed; NULL for Tax",
    ),
    Rule("PricingQuantity", ("Quantity",), "Quantity"),
    Rule("PricingUnit", ("UnitOfMeasure",), "UnitOfMeasure"),
    Rule("ConsumedQuantity", ("Quantity",), "Quantity on Usage rows that are not Unused"),
    Rule("ConsumedUnit", ("UnitOfMeasure",), "UnitOfMeasure on Usage rows that are not Unused"),
    Rule("PricingCurrency", ("PricingCurrencyCode",), "PricingCurrencyCode"),
    Rule("InvoiceId", ("InvoiceId",), "InvoiceId"),
    Rule("InvoiceIssuerName", ("PartnerName",), "PartnerName (CSP), else 'Microsoft'"),
    Rule("ProviderName", (), "constant 'Microsoft'"),
    Rule("PublisherName", ("PublisherName",), "PublisherName, else 'Microsoft'"),
    Rule("RegionId", ("ResourceLocation",), "ResourceLocation lower-cased without spaces ('East US' -> 'eastus')"),
    Rule("RegionName", ("MeterRegion", "ResourceLocation"), "MeterRegion, else ResourceLocation"),
    Rule("AvailabilityZone", ("AvailabilityZone",), "AvailabilityZone"),
    Rule("ResourceId", ("ResourceId", "InstanceId"), "ResourceId (or InstanceId)"),
    Rule(
        "ResourceName",
        ("ResourceName", "ResourceId", "InstanceId"),
        "ResourceName, else the last segment of ResourceId",
    ),
    Rule("ResourceType", ("ResourceId", "InstanceId"), "'<namespace>/<type>' parsed from the ResourceId"),
    Rule(
        "ServiceName",
        ("MeterCategory", "ConsumedService", "ProductName", "ServiceFamily"),
        "service_map name for MeterCategory, else MeterCategory, ConsumedService, ProductName, "
        "ServiceFamily, 'Microsoft'",
        "Rows with no meter (rounding adjustments) fall back so ServiceName is never NULL.",
    ),
    Rule(
        "ServiceCategory",
        ("MeterCategory", "ServiceFamily"),
        "service_map on MeterCategory, else on ServiceFamily, else 'Other'",
    ),
    Rule(
        "ServiceSubcategory",
        ("MeterCategory", "ServiceFamily"),
        "service_map on MeterCategory, else 'Other (<category>)'",
    ),
    Rule("SkuId", ("ProductId",), "ProductId"),
    Rule("SkuPriceId", ("MeterId",), "MeterId"),
    Rule("SkuMeter", ("MeterName",), "MeterName"),
    Rule("SubAccountId", ("SubscriptionId", "SubscriptionGuid"), "SubscriptionId", direct="SubscriptionId"),
    Rule("SubAccountName", ("SubscriptionName",), "SubscriptionName"),
    Rule("SubAccountType", (), "constant 'Subscription'"),
    Rule("Tags", ("Tags",), "Tags JSON object (braces optional, as older exports omit them)"),
    Rule("x_ServiceCode", ("MeterCategory",), "MeterCategory", direct="MeterCategory"),
    Rule("x_UsageType", ("MeterSubCategory",), "MeterSubCategory"),
    Rule("x_Operation", (), "NULL (Azure has no operation column)"),
    Rule("x_ChargeType", ("ChargeType",), "ChargeType"),
)

ACTUAL_RULES = (
    Rule("BilledCost", COST, "CostInBillingCurrency (actual-cost export)"),
    Rule(
        "EffectiveCost",
        (*COST, "ChargeType", "PricingModel"),
        "the row cost; 0 for reservation/savings-plan Purchase and Refund rows (not amortized in an "
        "actual-cost export)",
    ),
    *_SHARED_RULES,
)
AMORTIZED_RULES = (
    Rule(
        "BilledCost",
        (*COST, "ChargeType", "PricingModel"),
        "the row cost, but 0 for commitment-covered Usage and Unused* rows (billed through the purchase)",
    ),
    Rule(
        "EffectiveCost",
        (*COST, "ChargeType", "PricingModel"),
        "CostInBillingCurrency (amortized-cost export); 0 for commitment Purchase and Refund rows",
    ),
    *_SHARED_RULES,
)


def _known(service: str | None, attr: str) -> str | None:
    """Return the service_map ``attr`` for a known Azure MeterCategory, else NULL."""
    hit = services.find("azure", service) if service else None
    return None if hit is None else str(getattr(hit, attr))


def _region_id(v: str | None) -> str | None:
    return None if v is None else v.replace(" ", "").lower()


def _resource_type(rid: str | None) -> str | None:
    if rid is None:
        return None
    parts = rid.strip("/").split("/")
    low = [p.lower() for p in parts]
    if "providers" not in low:
        return None
    i = len(low) - 1 - low[::-1].index("providers")
    tail = parts[i + 1 :]
    if len(tail) < 2:
        return None
    types = [tail[0], *tail[1::2]]
    return "/".join(types).lower()


def _last_segment(rid: str | None) -> str | None:
    if rid is None:
        return None
    seg = rid.rstrip("/").rsplit("/", 1)[-1]
    return seg or None


def _plus_one_day(ts: Arr) -> Arr:
    return pc.add(ts, pa.scalar(dt.timedelta(days=1), pa.duration("us")))


def _map(src: Src, opts: MapOptions, amortized: bool) -> dict[str, Arr]:
    s = opts.money_scale
    n = src.n
    cost = src.money(s, *COST)
    ctype = src.string("ChargeType")
    model = src.string("PricingModel")
    zero = zero_money(n, s)
    commitment = isin(model, COMMITMENT_MODELS)
    unused = isin(ctype, UNUSED_TYPES)
    purchase = isin(ctype, ("Purchase", "Refund"))
    usage = pc.fill_null(pc.equal(ctype, "Usage"), False)
    commit_purchase = pc.and_(purchase, commitment)
    if amortized:
        billed = when(pc.or_(pc.and_(usage, commitment), unused), zero, cost)
        effective = when(commit_purchase, zero, cost)
    else:
        billed = cost
        effective = when(commit_purchase, zero, cost)
    qty = src.quantity("Quantity")
    payg_price = src.price("PayGPrice", "MarketPrice")
    payg_cost = src.money(s, "PaygCostInBillingCurrency")
    unit_price = src.price("UnitPrice")
    fx = src.get("ExchangeRatePricingToBilling")
    fx_arr = src.quantity("ExchangeRatePricingToBilling") if fx is not None else None
    if fx_arr is not None:
        fx_arr = pc.coalesce(fx_arr, pa.scalar(1, fx_arr.type))
    list_cost = pc.coalesce(payg_cost, money_mul(payg_price, qty, s), cost)
    contracted = pc.coalesce(money_mul(unit_price, qty, s, fx_arr), cost)
    start = src.timestamp("Date", "UsageDateTime")
    bp_start = src.timestamp("BillingPeriodStartDate")
    bp_end_raw = src.timestamp("BillingPeriodEndDate")
    on_commit_row = pc.or_(pc.and_(usage, commitment), pc.or_(unused, commit_purchase))
    none = src.null(pa.string())
    cd_id = when(on_commit_row, pc.coalesce(src.string("benefitId"), src.string("ReservationId")), none)
    cd_name = when(on_commit_row, pc.coalesce(src.string("benefitName"), src.string("ReservationName")), none)
    is_ri = pc.fill_null(pc.equal(model, "Reservation"), False)
    is_sp = pc.fill_null(pc.equal(model, "SavingsPlan"), False)
    cd_type = when(
        on_commit_row, when(is_ri, src.const("Reservation"), when(is_sp, src.const("Savings Plan"), none)), none
    )
    cd_cat = when(on_commit_row, when(is_ri, src.const("Usage"), when(is_sp, src.const("Spend"), none)), none)
    cd_status = when(unused, src.const("Unused"), when(pc.and_(usage, commitment), src.const("Used"), none))
    category = lookup(ctype, CHARGE_TYPES, pa.string(), "Adjustment")
    pricing_cat = when(
        isin(category, ("Usage", "Purchase")),
        pc.coalesce(lookup(model, PRICING_MODELS, pa.string(), "Other"), src.const("Standard")),
        none,
    )
    meter_cat = src.string("MeterCategory")
    family = src.string("ServiceFamily")
    service_key = pc.coalesce(meter_cat, family)

    def _svc_name(v: str | None) -> str | None:
        hit = services.find("azure", v) if v else None
        return hit.name if hit else v

    def _cat(v: str | None) -> str | None:
        return services.category("azure", v) if v else "Other"

    def _subcat(v: str | None) -> str | None:
        return services.subcategory("azure", v) if v else "Other (Other)"

    category_by_meter = map_values(
        meter_cat,
        lambda v: _known(v, "category"),
        pa.string(),
    )
    sub_by_meter = map_values(
        meter_cat,
        lambda v: _known(v, "subcategory"),
        pa.string(),
    )
    rid = src.string("ResourceId", "InstanceId")
    location = src.string("ResourceLocation")
    return {
        "BilledCost": billed,
        "EffectiveCost": effective,
        "ListCost": list_cost,
        "ContractedCost": contracted,
        "ListUnitPrice": payg_price,
        "ContractedUnitPrice": unit_price,
        "BillingAccountId": src.string("BillingAccountId"),
        "BillingAccountName": src.string("BillingAccountName"),
        "BillingAccountType": src.const("Billing Account"),
        "BillingCurrency": src.string(*CURRENCY),
        "BillingPeriodStart": bp_start,
        "BillingPeriodEnd": _plus_one_day(bp_end_raw),
        "ChargePeriodStart": start,
        "ChargePeriodEnd": _plus_one_day(start),
        "ChargeCategory": category,
        "ChargeDescription": pc.coalesce(src.string("ProductName"), src.string("MeterName")),
        "ChargeFrequency": lookup(src.string("Frequency"), FREQUENCIES, pa.string(), None),
        "CommitmentDiscountId": cd_id,
        "CommitmentDiscountName": cd_name,
        "CommitmentDiscountType": cd_type,
        "CommitmentDiscountCategory": cd_cat,
        "CommitmentDiscountStatus": cd_status,
        "PricingCategory": pricing_cat,
        "PricingQuantity": qty,
        "PricingUnit": src.string("UnitOfMeasure"),
        "ConsumedQuantity": qty,
        "ConsumedUnit": src.string("UnitOfMeasure"),
        "PricingCurrency": src.string("PricingCurrencyCode"),
        "InvoiceId": src.string("InvoiceId"),
        "InvoiceIssuerName": pc.coalesce(src.string("PartnerName"), src.const("Microsoft")),
        "ProviderName": src.const("Microsoft"),
        "PublisherName": pc.coalesce(src.string("PublisherName"), src.const("Microsoft")),
        "RegionId": map_values(location, _region_id, pa.string()),
        "RegionName": pc.coalesce(src.string("MeterRegion"), location),
        "AvailabilityZone": src.string("AvailabilityZone"),
        "ResourceId": rid,
        "ResourceName": pc.coalesce(src.string("ResourceName"), map_values(rid, _last_segment, pa.string())),
        "ResourceType": map_values(rid, _resource_type, pa.string()),
        "ServiceName": pc.coalesce(
            map_values(meter_cat, _svc_name, pa.string()),
            src.string("ConsumedService"),
            src.string("ProductName"),
            family,
            src.const("Microsoft"),
        ),
        "ServiceCategory": pc.coalesce(category_by_meter, map_values(family, _cat, pa.string(), skip_null=False)),
        "ServiceSubcategory": pc.coalesce(sub_by_meter, map_values(service_key, _subcat, pa.string(), skip_null=False)),
        "SkuId": src.string("ProductId"),
        "SkuPriceId": src.string("MeterId"),
        "SkuMeter": src.string("MeterName"),
        "SubAccountId": src.string("SubscriptionId", "SubscriptionGuid"),
        "SubAccountName": src.string("SubscriptionName"),
        "SubAccountType": src.const("Subscription"),
        "Tags": src.tags("Tags"),
        "x_ServiceCode": meter_cat,
        "x_UsageType": src.string("MeterSubCategory"),
        "x_ChargeType": ctype,
    }


def map_actual(src: Src, opts: MapOptions) -> dict[str, Arr]:
    """Map an actual-cost export batch."""
    return _map(src, opts, amortized=False)


def map_amortized(src: Src, opts: MapOptions) -> dict[str, Arr]:
    """Map an amortized-cost export batch."""
    return _map(src, opts, amortized=True)


_SIGNATURE = ("metercategory", "chargetype")

ACTUAL = Mapper(
    token="azure",
    provider="azure",
    title="Azure Cost Management actual-cost export (EA / MCA cost details)",
    rules=ACTUAL_RULES,
    signature=_SIGNATURE,
    map_fn=map_actual,
)
AMORTIZED = Mapper(
    token="azure_amortized",
    provider="azure",
    title="Azure Cost Management amortized-cost export (EA / MCA cost details)",
    rules=AMORTIZED_RULES,
    signature=_SIGNATURE,
    map_fn=map_amortized,
)
