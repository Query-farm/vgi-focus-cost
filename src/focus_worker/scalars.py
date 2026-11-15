"""Scalar functions: classification, spec lookups and exact decimal division."""

from __future__ import annotations

import json
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from . import services
from .meta import object_tags
from .sources import MAPPERS, aws, azure, gcp
from .spec import DEFAULT_SPEC_VERSION, SPEC_VERSIONS, column_index

RATIO_TYPE = pa.decimal128(19, 10)
WIDE_TYPE = pa.decimal128(38, 10)

_PROVIDER_DOC = "Cloud provider: 'aws', 'azure' or 'gcp' (also 'AWS', 'Microsoft', 'Google Cloud'). NULL in, NULL out."
_SERVICE_DOC = (
    "Provider service key: AWS product code ('AmazonEC2'), Azure MeterCategory ('Virtual Machines'), GCP service "
    "description ('Compute Engine'); display names also match. Case-insensitive."
)


def _executable(*items: tuple[str, str, list[list[Any]]]) -> str:
    return json.dumps([{"description": d, "sql": s, "expected_result": r} for d, s, r in items])


def _pairs(a: pa.Array, b: pa.Array) -> list[tuple[Any, Any]]:
    return list(zip(a.to_pylist(), b.to_pylist(), strict=True))


def _pairwise(a: pa.Array, b: pa.Array, fn: Any, out_type: pa.DataType) -> pa.Array:
    """Apply ``fn`` once per distinct (a, b) pair."""
    memo: dict[tuple[Any, Any], Any] = {}
    out = []
    for pair in _pairs(a, b):
        if pair not in memo:
            memo[pair] = fn(*pair)
        out.append(memo[pair])
    return pa.array(out, out_type)


class ServiceCategory(ScalarFunction):
    """``focus_service_category(provider, service)``."""

    class Meta:
        """Function metadata."""

        name = "focus_service_category"
        description = "FOCUS ServiceCategory for a provider service, e.g. ('aws', 'AmazonEC2') -> 'Compute'"
        tags = {
            **object_tags(
                title="Service Category",
                category="classification",
                keywords="service category, focus, classify service, aws product code, azure meter category",
                doc_llm=(
                    "Classify a cloud service into its FOCUS ServiceCategory using the worker's service_map: "
                    "focus_service_category('aws', 'AmazonEC2') = 'Compute', ('azure', 'Storage') = 'Storage', "
                    "('gcp', 'BigQuery') = 'Databases'. Unknown services return 'Other'; NULL provider or service "
                    "returns NULL. Vectorized, so it works over a column of service codes."
                ),
                doc_md="## focus_service_category(provider, service)\n\nFOCUS ServiceCategory; `Other` when unknown.",
            ),
            "vgi.executable_examples": _executable(
                ("AWS, Azure and GCP services",
                 "SELECT focus.main.focus_service_category(p, s) AS c FROM (VALUES ('aws', 'AmazonRDS', 1), "
                 "('azure', 'Virtual Machines', 2), ('gcp', 'Cloud Storage', 3), ('aws', 'NotAService', 4)) t(p, s, i) "
                 "ORDER BY i",
                 [["Databases"], ["Compute"], ["Storage"], ["Other"]]),
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT focus.main.focus_service_category('aws', 'AmazonEC2') AS category",
                description="Classify EC2",
            )
        ]

    @classmethod
    def compute(
        cls,
        provider: Annotated[pa.StringArray, Param(doc=_PROVIDER_DOC)],
        service: Annotated[pa.StringArray, Param(doc=_SERVICE_DOC)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Look each (provider, service) up in the service map."""
        return _pairwise(provider, service, services.category, pa.string())  # type: ignore[return-value]


class ServiceSubcategory(ScalarFunction):
    """``focus_service_subcategory(provider, service)``."""

    class Meta:
        """Function metadata."""

        name = "focus_service_subcategory"
        description = "FOCUS ServiceSubcategory for a provider service, e.g. ('aws', 'AmazonRDS') -> 'Relational Databases'"
        tags = {
            **object_tags(
                title="Service Subcategory",
                category="classification",
                keywords="service subcategory, focus, classify service",
                doc_llm=(
                    "Classify a cloud service into its FOCUS 1.1+ ServiceSubcategory: "
                    "focus_service_subcategory('aws', 'AmazonRDS') = 'Relational Databases'. Unknown services return "
                    "'Other (Other)'; NULL in, NULL out."
                ),
                doc_md="## focus_service_subcategory(provider, service)\n\nFOCUS ServiceSubcategory.",
            ),
            "vgi.executable_examples": _executable(
                ("Subcategories", "SELECT focus.main.focus_service_subcategory('aws', 'AmazonRDS') AS a, "
                 "focus.main.focus_service_subcategory('gcp', 'Cloud Run') AS b",
                 [["Relational Databases", "Serverless Compute"]]),
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT focus.main.focus_service_subcategory('azure', 'Azure Cosmos DB') AS subcategory",
                description="Classify Cosmos DB",
            )
        ]

    @classmethod
    def compute(
        cls,
        provider: Annotated[pa.StringArray, Param(doc=_PROVIDER_DOC)],
        service: Annotated[pa.StringArray, Param(doc=_SERVICE_DOC)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Look each (provider, service) up in the service map."""
        return _pairwise(provider, service, services.subcategory, pa.string())  # type: ignore[return-value]


_CHARGE_TABLES: dict[str, dict[str, str]] = {
    "aws": {k: v[0] for k, v in aws.LINE_ITEM_TYPES.items()},
    "azure": azure.CHARGE_TYPES,
    "gcp": gcp.COST_TYPES,
}


def charge_category(provider: str | None, charge_type: str | None) -> str | None:
    """FOCUS ChargeCategory for a raw charge type, as the readers derive it."""
    p = services.normalize_provider(provider)
    if p is None or charge_type is None:
        return None
    return _CHARGE_TABLES[p].get(charge_type.strip(), "Usage" if p == "gcp" else "Adjustment")


class ChargeCategory(ScalarFunction):
    """``focus_charge_category(provider, charge_type)``."""

    class Meta:
        """Function metadata."""

        name = "focus_charge_category"
        description = "FOCUS ChargeCategory for a raw provider charge type, e.g. ('aws', 'RIFee') -> 'Purchase'"
        tags = {
            **object_tags(
                title="Charge Category",
                category="classification",
                keywords="charge category, line item type, charge type, cost type",
                doc_llm=(
                    "Map a provider's raw charge type to FOCUS ChargeCategory (Usage, Purchase, Tax, Credit, "
                    "Adjustment) exactly as focus() does: AWS line_item_line_item_type ('DiscountedUsage' -> Usage, "
                    "'RIFee' -> Purchase), Azure ChargeType ('UnusedReservation' -> Usage), GCP cost_type ('tax' -> "
                    "Tax). Unknown AWS/Azure types return 'Adjustment', unknown GCP types 'Usage'; NULL in, NULL out."
                ),
                doc_md="## focus_charge_category(provider, charge_type)\n\nSee `charge_type_map` for every rule.",
            ),
            "vgi.executable_examples": _executable(
                ("One per provider",
                 "SELECT focus.main.focus_charge_category(p, t) AS c FROM (VALUES ('aws', 'SavingsPlanCoveredUsage', 1), "
                 "('azure', 'Purchase', 2), ('gcp', 'tax', 3)) v(p, t, i) ORDER BY i",
                 [["Usage"], ["Purchase"], ["Tax"]]),
            ),
        }
        examples = [
            FunctionExample(sql="SELECT focus.main.focus_charge_category('aws', 'RIFee') AS category",
                            description="AWS reserved-instance fee")
        ]

    @classmethod
    def compute(
        cls,
        provider: Annotated[pa.StringArray, Param(doc=_PROVIDER_DOC)],
        charge_type: Annotated[pa.StringArray, Param(doc="Raw charge type, e.g. 'RIFee', 'UnusedReservation', 'tax'.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Look each (provider, charge type) up."""
        return _pairwise(provider, charge_type, charge_category, pa.string())  # type: ignore[return-value]


def map_column(source: str | None, raw: str | None) -> str | None:
    """Comma-separated FOCUS columns that read raw column ``raw`` for ``source``."""
    if source is None or raw is None:
        return None
    m = MAPPERS.get(source.strip())
    if m is None:
        return None
    key = m.canonical_name(raw.strip()).lower()
    hits = sorted(
        r.focus_column
        for r in m.rules
        if any(s.lower() == key or ("*" in s and key.startswith(s.lower().rstrip("*"))) for s in r.sources)
    )
    return ", ".join(hits) or None


class MapColumn(ScalarFunction):
    """``focus_map_column(source, source_column)``."""

    class Meta:
        """Function metadata."""

        name = "focus_map_column"
        description = "Which FOCUS columns a raw export column feeds, e.g. ('aws_cur2', 'line_item_unblended_cost')"
        tags = {
            **object_tags(
                title="Map Raw Column",
                category="reference",
                keywords="column mapping, lineage, raw column, export column",
                doc_llm=(
                    "Given a source token ('aws_cur2', 'azure', 'azure_amortized', 'gcp', 'focus') and a raw export "
                    "column name, return the comma-separated FOCUS columns derived from it (alphabetical), or NULL "
                    "when the reader ignores that column. Legacy CUR headers ('lineItem/UnblendedCost') are accepted. "
                    "Use it to find export columns the mapping does not use."
                ),
                doc_md="## focus_map_column(source, source_column)\n\nLineage from a raw export column to FOCUS columns.",
            ),
            "vgi.executable_examples": _executable(
                ("AWS unblended cost",
                 "SELECT focus.main.focus_map_column('aws_cur2', 'lineItem/UnblendedCost') AS cols",
                 [["BilledCost, ContractedCost, EffectiveCost, ListCost"]]),
                ("An unused column", "SELECT focus.main.focus_map_column('azure', 'CostCenter') IS NULL AS unused",
                 [[True]]),
            ),
        }
        examples = [
            FunctionExample(sql="SELECT focus.main.focus_map_column('gcp', 'cost_at_list') AS focus_columns",
                            description="Where GCP list cost goes")
        ]

    @classmethod
    def compute(
        cls,
        source: Annotated[pa.StringArray, Param(doc="Source token: aws_cur2, azure, azure_amortized, gcp or focus.")],
        source_column: Annotated[pa.StringArray, Param(doc="Raw export column name, case-insensitive.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Find the rules that read each column."""
        return _pairwise(source, source_column, map_column, pa.string())  # type: ignore[return-value]


def validate_value(column: str | None, value: str | None, version: str = DEFAULT_SPEC_VERSION) -> str | None:
    """NULL when ``value`` is legal for the FOCUS column, else the violation."""
    if column is None:
        return None
    spec = column_index(version).get(column)
    if spec is None:
        return f"{column} is not a FOCUS {version} column"
    if value is None:
        return None if spec.allows_nulls else f"{column} must not be NULL"
    if spec.data_type == "String" and spec.allowed_values and value not in spec.allowed_values:
        return f"{value!r} is not an allowed {column} value; expected one of: {', '.join(spec.allowed_values)}"
    if spec.data_type == "Decimal":
        try:
            Decimal(value)
        except InvalidOperation:
            return f"{value!r} is not a decimal number"
    if column in ("BillingCurrency", "PricingCurrency") and not (len(value) == 3 and value.isalpha() and value.isupper()):
        return f"{value!r} is not an ISO 4217 currency code"
    return None


class ValidateValue(ScalarFunction):
    """``focus_validate_value(column_name, value)``."""

    class Meta:
        """Function metadata."""

        name = "focus_validate_value"
        description = "NULL if a value is legal for a FOCUS 1.2 column, else the violation text"
        tags = {
            **object_tags(
                title="Validate a FOCUS Value",
                category="validation",
                keywords="validate, allowed values, conformance, focus spec",
                doc_llm=(
                    "Check one value against a FOCUS 1.2 column's rules: allowed values (ChargeCategory, "
                    "ChargeFrequency, PricingCategory, ServiceCategory ...), NULL allowed or not, decimal syntax, "
                    "ISO currency codes. Returns NULL when the value is legal, otherwise a message. Pass the value as "
                    "VARCHAR (cast numbers). Use focus_validate_value(column, value, spec_version) for another version."
                ),
                doc_md="## focus_validate_value(column_name, value)\n\nNULL = legal; otherwise the violation.",
            ),
            "vgi.executable_examples": _executable(
                ("A legal and an illegal ChargeCategory",
                 "SELECT focus.main.focus_validate_value('ChargeCategory', 'Usage') IS NULL AS ok, "
                 "focus.main.focus_validate_value('ChargeCategory', 'Fee') LIKE '%not an allowed%' AS bad",
                 [[True, True]]),
            ),
        }
        examples = [
            FunctionExample(sql="SELECT focus.main.focus_validate_value('ChargeFrequency', 'Monthly') AS problem",
                            description="An invalid ChargeFrequency")
        ]

    @classmethod
    def compute(
        cls,
        column_name: Annotated[pa.StringArray, Param(doc="FOCUS column name, e.g. 'ChargeCategory'.")],
        value: Annotated[pa.StringArray, Param(doc="The value to check, as text; NULL checks nullability.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Validate each value against FOCUS 1.2."""
        return _pairwise(column_name, value, validate_value, pa.string())  # type: ignore[return-value]


class ValidateValueVersion(ScalarFunction):
    """``focus_validate_value(column_name, value, spec_version)``."""

    class Meta:
        """Function metadata."""

        name = "focus_validate_value"
        description = "NULL if a value is legal for a FOCUS column in the given spec version, else the violation"
        tags = {
            **object_tags(
                title="Validate a FOCUS Value (versioned)",
                category="validation",
                keywords="validate, allowed values, focus version",
                doc_llm=(
                    "Like focus_validate_value(column_name, value) but against a chosen FOCUS version: spec_version "
                    "is '1.0', '1.1', '1.2', '1.3' or '1.4'. A column that does not exist in that version is reported."
                ),
                doc_md="## focus_validate_value(column_name, value, spec_version)\n\nVersioned value check.",
            ),
            "vgi.executable_examples": _executable(
                ("ServiceProviderName only exists from 1.3",
                 "SELECT focus.main.focus_validate_value('ServiceProviderName', 'AWS', '1.2') AS v12, "
                 "focus.main.focus_validate_value('ServiceProviderName', 'AWS', '1.3') AS v13",
                 [["ServiceProviderName is not a FOCUS 1.2 column", None]]),
            ),
        }
        examples = [
            FunctionExample(sql="SELECT focus.main.focus_validate_value('ChargeCategory', 'Usage', '1.0') AS problem",
                            description="Validate against FOCUS 1.0")
        ]

    @classmethod
    def compute(
        cls,
        column_name: Annotated[pa.StringArray, Param(doc="FOCUS column name.")],
        value: Annotated[pa.StringArray, Param(doc="The value to check, as text.")],
        spec_version: Annotated[str, ConstParam("FOCUS version: '1.0' to '1.4'.", choices=list(SPEC_VERSIONS))],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Validate each value against the given version."""
        return _pairwise(column_name, value, lambda c, v: validate_value(c, v, spec_version), pa.string())  # type: ignore[return-value]


class SpecVersion(ScalarFunction):
    """``focus_spec_version()``."""

    class Meta:
        """Function metadata."""

        name = "focus_spec_version"
        description = "The FOCUS version focus() and to_focus() emit by default ('1.2')"
        tags = {
            **object_tags(
                title="Default FOCUS Version",
                category="reference",
                keywords="focus version, specification version",
                doc_llm=(
                    "Return the FOCUS specification version this worker emits by default (spec_version := '1.2'). "
                    "Versions 1.0 to 1.4 are available through the spec_version argument and focus_columns."
                ),
                doc_md="## focus_spec_version()\n\nThe default FOCUS version: `1.2`.",
            ),
            "vgi.executable_examples": _executable(
                ("Default version", "SELECT focus.main.focus_spec_version() AS v", [["1.2"]]),
            ),
        }
        examples = [FunctionExample(sql="SELECT focus.main.focus_spec_version() AS version", description="Default version")]

    @classmethod
    def compute(cls) -> Annotated[pa.StringArray, Returns()]:
        """The default version."""
        return pa.array([DEFAULT_SPEC_VERSION], pa.string())  # type: ignore[return-value]


def exact_divide(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    """numerator / denominator rounded half-to-even to 10 places; NULL for NULL or zero denominator."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    with localcontext() as ctx:
        ctx.prec = 80
        q = (numerator / denominator).quantize(Decimal("1E-10"), rounding=ROUND_HALF_EVEN)
    if abs(q) >= Decimal(10) ** 9:
        raise ValueError(f"ratio {q} does not fit DECIMAL(19,10) (|ratio| must be below 1e9)")
    return q


def _divide_arrays(n: pa.Array, d: pa.Array) -> pa.Array:
    out = [exact_divide(a, b) for a, b in zip(n.to_pylist(), d.to_pylist(), strict=True)]
    return pa.array(out, RATIO_TYPE)


class Divide(ScalarFunction):
    """``focus_divide(numerator, denominator)``."""

    class Meta:
        """Function metadata."""

        name = "focus_divide"
        description = "Exact decimal division to DECIMAL(19,10), half-even; NULL on a zero or NULL denominator"
        tags = {
            **object_tags(
                title="Exact Decimal Divide",
                category="arithmetic",
                keywords="exact division, decimal, ratio, coverage, utilization, unit cost, no double",
                doc_llm=(
                    "Divide two DECIMAL values exactly and return DECIMAL(19,10), rounded half-to-even. In DuckDB "
                    "`DECIMAL / DECIMAL` returns DOUBLE, which silently loses cents in cost ratios; use "
                    "focus_divide(sum(EffectiveCost), sum(BilledCost)) instead. Zero or NULL denominator returns "
                    "NULL. Results must be below 1e9 in magnitude. The result multiplies with a DECIMAL(19,6) money "
                    "column into DECIMAL(38,16) without overflow."
                ),
                doc_md="## focus_divide(numerator, denominator)\n\nExact ratio, DECIMAL(19,10); `/` would give DOUBLE.",
            ),
            "vgi.executable_examples": _executable(
                ("Ten over three, and a zero denominator",
                 "SELECT focus.main.focus_divide(10, 3) AS r, focus.main.focus_divide(1, 0) AS z",
                 [["3.3333333333", None]]),
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT focus.main.focus_divide(sum(EffectiveCost), sum(ListCost)) AS effective_over_list "
                "FROM focus.main.focus('/data/billing/')",
                description="Effective-to-list cost ratio without DOUBLE rounding",
            )
        ]

    @classmethod
    def compute(
        cls,
        numerator: Annotated[pa.Decimal128Array, Param(WIDE_TYPE, doc="Dividend, any DECIMAL or integer (cast to DECIMAL(38,10)).")],
        denominator: Annotated[pa.Decimal128Array, Param(WIDE_TYPE, doc="Divisor; 0 or NULL gives NULL.")],
    ) -> Annotated[pa.Decimal128Array, Returns(RATIO_TYPE)]:
        """Divide row by row with Python decimal."""
        return _divide_arrays(numerator, denominator)  # type: ignore[return-value]


class EffectiveRate(ScalarFunction):
    """``focus_effective_rate(cost, quantity)``."""

    class Meta:
        """Function metadata."""

        name = "focus_effective_rate"
        description = "Exact unit rate cost / quantity as DECIMAL(19,10); NULL when quantity is 0 or NULL"
        tags = {
            **object_tags(
                title="Effective Unit Rate",
                category="arithmetic",
                keywords="unit rate, effective rate, cost per unit, price per hour, exact division",
                doc_llm=(
                    "Effective price per pricing unit: focus_effective_rate(EffectiveCost, PricingQuantity) returns "
                    "DECIMAL(19,10), exact and half-even, NULL for zero or NULL quantity. Use it instead of "
                    "EffectiveCost / PricingQuantity, which DuckDB evaluates in DOUBLE."
                ),
                doc_md="## focus_effective_rate(cost, quantity)\n\nExact cost per unit, DECIMAL(19,10).",
            ),
            "vgi.executable_examples": _executable(
                ("Hourly rate of 24 hours costing 2.7408",
                 "SELECT focus.main.focus_effective_rate(2.7408, 24) AS rate", [["0.1142000000"]]),
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT ResourceId, focus.main.focus_effective_rate(sum(EffectiveCost), sum(PricingQuantity)) AS rate "
                "FROM focus.main.focus('/data/billing/') WHERE ChargeCategory = 'Usage' GROUP BY ResourceId",
                description="Effective hourly rate per resource",
            )
        ]

    @classmethod
    def compute(
        cls,
        cost: Annotated[pa.Decimal128Array, Param(WIDE_TYPE, doc="Cost, e.g. EffectiveCost.")],
        quantity: Annotated[pa.Decimal128Array, Param(WIDE_TYPE, doc="Quantity, e.g. PricingQuantity; 0 gives NULL.")],
    ) -> Annotated[pa.Decimal128Array, Returns(RATIO_TYPE)]:
        """Divide row by row with Python decimal."""
        return _divide_arrays(cost, quantity)  # type: ignore[return-value]


SCALARS: list[type[Any]] = [
    ServiceCategory,
    ServiceSubcategory,
    ChargeCategory,
    MapColumn,
    ValidateValue,
    ValidateValueVersion,
    SpecVersion,
    Divide,
    EffectiveRate,
]
