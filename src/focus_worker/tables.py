"""Reference tables: the FOCUS spec and this worker's mapping rules, as data.

``focus_columns`` is compiled from the FOCUS specification (CC BY 4.0, see NOTICE).
``provider_map``, ``service_map`` and ``charge_type_map`` are generated from the
same rule objects the mappers run, so the published mapping is the mapping.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from . import services
from .meta import column, object_tags, schema_columns_json
from .sources import MAPPERS, aws, azure, gcp
from .spec import all_columns, arrow_type, duckdb_type_name


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Reference tables take no arguments."""


def _constraints(table: str, key: list[str]) -> tuple[Any, ...]:
    records = [
        constraint_record(["main"], table, f"{k}_not_null", "NOT_NULL", [[k]], enforcement="ENFORCED", validation="ALL_ROWS")
        for k in key
    ]
    records.append(
        constraint_record(["main"], table, "pk", "PRIMARY_KEY", [[k] for k in key], enforcement="ENFORCED", validation="ALL_ROWS")
    )
    return tuple(records)


def _comments(schema: pa.Schema) -> dict[str, str]:
    return {f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in schema}


# ----------------------------------------------------------------- focus_columns

COLUMNS_SCHEMA = pa.schema(
    [
        column("spec_version", pa.string(), "FOCUS version, '1.0' to '1.4'.", nullable=False),
        column("column_name", pa.string(), "FOCUS Column ID, e.g. 'BilledCost'.", nullable=False),
        column("display_name", pa.string(), "The spec's display name, e.g. 'Billed Cost'.", nullable=False),
        column("description", pa.string(), "The spec's one-sentence description of the column.", nullable=False),
        column("column_type", pa.string(), "'Dimension' or 'Metric'.", nullable=False),
        column("feature_level", pa.string(), "'Mandatory', 'Conditional' or 'Recommended'.", nullable=False),
        column("allows_nulls", pa.bool_(), "Whether the spec allows NULL in this column.", nullable=False),
        column("data_type", pa.string(), "The spec's data type: String, Decimal, Date/Time or JSON.", nullable=False),
        column("duckdb_type", pa.string(), "The DuckDB type focus() and to_focus() emit for this column.", nullable=False),
        column("value_format", pa.string(), "The spec's value format, e.g. 'Numeric Format', 'Allowed Values'.", nullable=False),
        column("allowed_values", pa.list_(pa.string()), "The closed set of allowed values, or NULL when any value is allowed."),
        column("introduced", pa.string(), "The FOCUS version that introduced the column.", nullable=False),
    ]
)


@bind_fixed_schema
@init_single_worker
class FocusColumnsSource(TableFunctionGenerator[NoArgs, None]):
    """Rows of ``focus_columns``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COLUMNS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "focus_columns_source"
        description = "Backing rows for the focus_columns table"
        tags = object_tags(
            title="FOCUS Columns (scan)",
            category="reference",
            keywords="focus spec, columns",
            doc_llm="Backing scan of the focus_columns table; query the table instead.",
            doc_md="Backing scan of `focus_columns`.",
        )

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit every (version, column) record."""
        cols = all_columns()
        data: dict[str, list[Any]] = {
            "spec_version": [c.spec_version for c in cols],
            "column_name": [c.column_name for c in cols],
            "display_name": [c.display_name for c in cols],
            "description": [c.description for c in cols],
            "column_type": [c.column_type for c in cols],
            "feature_level": [c.feature_level for c in cols],
            "allows_nulls": [c.allows_nulls for c in cols],
            "data_type": [c.data_type for c in cols],
            "duckdb_type": [duckdb_type_name(arrow_type(c)) for c in cols],
            "value_format": [c.value_format for c in cols],
            "allowed_values": [list(c.allowed_values) if c.allowed_values else None for c in cols],
            "introduced": [c.introduced for c in cols],
        }
        batch = pa.RecordBatch.from_pydict(data, schema=COLUMNS_SCHEMA)
        out.emit(batch.select(params.output_schema.names))
        out.finish()


def _table_tags(title: str, keywords: str, doc_llm: str, doc_md: str, schema: pa.Schema, examples: list[dict[str, Any]]) -> dict[str, str]:
    return {
        **object_tags(title=title, category="reference", keywords=keywords, doc_llm=doc_llm, doc_md=doc_md),
        "vgi.result_columns_schema": json.dumps(schema_columns_json(schema)),
        "vgi.executable_examples": json.dumps(examples),
        "vgi.example_queries": json.dumps([{"description": e["description"], "sql": e["sql"]} for e in examples]),
    }


FOCUS_COLUMNS = Table(
    name="focus_columns",
    function=FocusColumnsSource,
    comment="The FOCUS specification's Cost and Usage columns for versions 1.0 to 1.4 (CC BY 4.0, FinOps Foundation).",
    column_comments=_comments(COLUMNS_SCHEMA),
    constraints=_constraints("focus_columns", ["spec_version", "column_name"]),
    tags=_table_tags(
        "FOCUS Specification Columns",
        "focus spec, finops, column reference, mandatory columns, allowed values, data dictionary",
        "One row per column per FOCUS version (1.0, 1.1, 1.2, 1.3, 1.4) of the Cost and Usage dataset, compiled from "
        "the FinOps Foundation specification: column_name, display_name, description, column_type, feature_level "
        "(Mandatory/Conditional/Recommended), allows_nulls, data_type, duckdb_type (what this worker emits), "
        "value_format, allowed_values (VARCHAR[] or NULL) and introduced. Filter on spec_version. Use it to list "
        "mandatory columns, look up allowed values (e.g. ChargeCategory) or diff versions.",
        "## focus_columns\n\nThe FOCUS spec as a table: one row per column per version (1.0–1.4).\n\n"
        "Column definitions © FOCUS contributors, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).",
        COLUMNS_SCHEMA,
        [
            {
                "description": "Allowed ChargeCategory values in FOCUS 1.2",
                "sql": "SELECT unnest(allowed_values) AS v FROM focus.main.focus_columns "
                "WHERE spec_version = '1.2' AND column_name = 'ChargeCategory' ORDER BY v",
                "expected_result": [["Adjustment"], ["Credit"], ["Purchase"], ["Tax"], ["Usage"]],
            },
            {
                "description": "Columns added in FOCUS 1.3",
                "sql": "SELECT column_name FROM focus.main.focus_columns WHERE spec_version = '1.3' "
                "AND column_name NOT IN (SELECT column_name FROM focus.main.focus_columns WHERE spec_version = '1.2') "
                "ORDER BY column_name",
                "expected_result": [["AllocatedMethodDetails"], ["AllocatedMethodId"], ["AllocatedResourceId"],
                                    ["AllocatedResourceName"], ["AllocatedTags"], ["ContractApplied"],
                                    ["HostProviderName"], ["ServiceProviderName"]],
            },
        ],
    ),
)

# ------------------------------------------------------------------ provider_map

PROVIDER_MAP_SCHEMA = pa.schema(
    [
        column("source", pa.string(), "Source token for focus()/to_focus(): aws_cur2, azure, azure_amortized, gcp, focus.", nullable=False),
        column("provider", pa.string(), "Cloud: aws, azure, gcp, or 'any' for native FOCUS files.", nullable=False),
        column("focus_column", pa.string(), "The FOCUS (or x_ extension) column produced.", nullable=False),
        column("source_columns", pa.list_(pa.string()), "Raw export columns read to produce it (empty for constants).", nullable=False),
        column("transform", pa.string(), "How the value is derived, in words.", nullable=False),
        column("notes", pa.string(), "Caveats and provider-specific detail; NULL when none."),
        column("filter_pushdown", pa.bool_(), "True when a comparison on this column is pushed into the Parquet scan.", nullable=False),
    ]
)


def provider_map_rows() -> list[dict[str, Any]]:
    """Every mapping rule of every source."""
    rows = []
    for token, m in MAPPERS.items():
        for r in m.rules:
            rows.append({
                "source": token,
                "provider": m.provider,
                "focus_column": r.focus_column,
                "source_columns": list(r.sources),
                "transform": r.transform,
                "notes": r.notes or None,
                "filter_pushdown": r.direct is not None,
            })
    return rows


@bind_fixed_schema
@init_single_worker
class ProviderMapSource(TableFunctionGenerator[NoArgs, None]):
    """Rows of ``provider_map``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = PROVIDER_MAP_SCHEMA

    class Meta:
        """Function metadata."""

        name = "provider_map_source"
        description = "Backing rows for the provider_map table"
        tags = object_tags(
            title="Provider Map (scan)",
            category="reference",
            keywords="mapping rules",
            doc_llm="Backing scan of the provider_map table; query the table instead.",
            doc_md="Backing scan of `provider_map`.",
        )

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit every rule."""
        batch = pa.RecordBatch.from_pylist(provider_map_rows(), schema=PROVIDER_MAP_SCHEMA)
        out.emit(batch.select(params.output_schema.names))
        out.finish()


PROVIDER_MAP = Table(
    name="provider_map",
    function=ProviderMapSource,
    comment="How every FOCUS column is derived from each provider's export: the auditable mapping rules.",
    column_comments=_comments(PROVIDER_MAP_SCHEMA),
    constraints=_constraints("provider_map", ["source", "focus_column"]),
    tags=_table_tags(
        "Provider Mapping Rules",
        "mapping, lineage, aws cur, azure export, gcp billing export, audit, column mapping",
        "One row per (source, focus_column): which raw export columns feed each FOCUS column and the exact transform, "
        "for sources aws_cur2, azure, azure_amortized, gcp and focus. filter_pushdown says whether a WHERE comparison "
        "on that column is pushed into the Parquet reader. Use it to audit or explain a FOCUS value, e.g. "
        "WHERE source = 'aws_cur2' AND focus_column = 'EffectiveCost'.",
        "## provider_map\n\nThe mapping rules the readers run, as data: source → FOCUS column, raw columns, transform.",
        PROVIDER_MAP_SCHEMA,
        [
            {
                "description": "Where AWS EffectiveCost comes from",
                "sql": "SELECT len(source_columns) AS inputs, filter_pushdown FROM focus.main.provider_map "
                "WHERE source = 'aws_cur2' AND focus_column = 'EffectiveCost'",
                "expected_result": [[9, False]],
            },
            {
                "description": "Columns pushed into the Parquet scan per source",
                "sql": "SELECT source, count(*) FILTER (WHERE filter_pushdown) AS pushed FROM focus.main.provider_map "
                "WHERE source <> 'focus' GROUP BY source ORDER BY source",
                "expected_result": [["aws_cur2", 6], ["azure", 3], ["azure_amortized", 3], ["gcp", 3]],
            },
        ],
    ),
)

# ------------------------------------------------------------------- service_map

SERVICE_MAP_SCHEMA = pa.schema(
    [
        column("provider", pa.string(), "aws, azure or gcp.", nullable=False),
        column("service_code", pa.string(), "Provider service key: AWS product code, Azure MeterCategory, GCP service description.", nullable=False),
        column("service_name", pa.string(), "The ServiceName emitted for it.", nullable=False),
        column("service_category", pa.string(), "FOCUS ServiceCategory.", nullable=False),
        column("service_subcategory", pa.string(), "FOCUS ServiceSubcategory (1.1+).", nullable=False),
    ]
)


@bind_fixed_schema
@init_single_worker
class ServiceMapSource(TableFunctionGenerator[NoArgs, None]):
    """Rows of ``service_map``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SERVICE_MAP_SCHEMA

    class Meta:
        """Function metadata."""

        name = "service_map_source"
        description = "Backing rows for the service_map table"
        tags = object_tags(
            title="Service Map (scan)",
            category="reference",
            keywords="service classification",
            doc_llm="Backing scan of the service_map table; query the table instead.",
            doc_md="Backing scan of `service_map`.",
        )

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit the curated service classification."""
        rows = [
            {"provider": s.provider, "service_code": s.code, "service_name": s.name, "service_category": s.category,
             "service_subcategory": s.subcategory}
            for s in services.SERVICES
        ]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=SERVICE_MAP_SCHEMA).select(params.output_schema.names))
        out.finish()


SERVICE_MAP = Table(
    name="service_map",
    function=ServiceMapSource,
    comment="Curated provider service -> FOCUS ServiceName, ServiceCategory and ServiceSubcategory.",
    column_comments=_comments(SERVICE_MAP_SCHEMA),
    constraints=_constraints("service_map", ["provider", "service_code"]),
    tags=_table_tags(
        "Service Classification Map",
        "service category, service subcategory, aws product code, azure meter category, gcp service",
        "One row per known provider service: provider (aws/azure/gcp), service_code (AWS line_item_product_code, "
        "Azure MeterCategory, GCP service.description), and the FOCUS service_name, service_category and "
        "service_subcategory the readers emit. Services not listed map to 'Other' / 'Other (Other)'.",
        "## service_map\n\nProvider services and their FOCUS categories. Unlisted services become `Other`.",
        SERVICE_MAP_SCHEMA,
        [
            {
                "description": "Database services per provider",
                "sql": "SELECT provider, count(*) AS services FROM focus.main.service_map "
                "WHERE service_category = 'Databases' GROUP BY provider ORDER BY provider",
                "expected_result": [["aws", 6], ["azure", 6], ["gcp", 7]],
            },
        ],
    ),
)

# --------------------------------------------------------------- charge_type_map

CHARGE_MAP_SCHEMA = pa.schema(
    [
        column("source", pa.string(), "Source token: aws_cur2, azure (both Azure tokens) or gcp.", nullable=False),
        column("charge_type", pa.string(), "The provider's raw charge type (line_item_line_item_type, ChargeType, cost_type).", nullable=False),
        column("charge_category", pa.string(), "FOCUS ChargeCategory it maps to.", nullable=False),
        column("charge_frequency", pa.string(), "FOCUS ChargeFrequency where the charge type decides it, else NULL."),
    ]
)


def charge_rows() -> list[dict[str, Any]]:
    """The charge-type tables the mappers use."""
    rows = [{"source": "aws_cur2", "charge_type": k, "charge_category": c, "charge_frequency": f}
            for k, (c, f) in aws.LINE_ITEM_TYPES.items()]
    rows += [{"source": "azure", "charge_type": k, "charge_category": c, "charge_frequency": None}
             for k, c in azure.CHARGE_TYPES.items()]
    rows += [{"source": "gcp", "charge_type": k, "charge_category": c, "charge_frequency": None}
             for k, c in gcp.COST_TYPES.items()]
    return rows


@bind_fixed_schema
@init_single_worker
class ChargeTypeMapSource(TableFunctionGenerator[NoArgs, None]):
    """Rows of ``charge_type_map``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CHARGE_MAP_SCHEMA

    class Meta:
        """Function metadata."""

        name = "charge_type_map_source"
        description = "Backing rows for the charge_type_map table"
        tags = object_tags(
            title="Charge Type Map (scan)",
            category="reference",
            keywords="charge category",
            doc_llm="Backing scan of the charge_type_map table; query the table instead.",
            doc_md="Backing scan of `charge_type_map`.",
        )

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit the charge-type rules."""
        out.emit(pa.RecordBatch.from_pylist(charge_rows(), schema=CHARGE_MAP_SCHEMA).select(params.output_schema.names))
        out.finish()


CHARGE_TYPE_MAP = Table(
    name="charge_type_map",
    function=ChargeTypeMapSource,
    comment="Provider charge types -> FOCUS ChargeCategory (and ChargeFrequency for AWS).",
    column_comments=_comments(CHARGE_MAP_SCHEMA),
    constraints=_constraints("charge_type_map", ["source", "charge_type"]),
    tags=_table_tags(
        "Charge Type Map",
        "charge category, line item type, charge type, cost type, savings plan, reservation",
        "One row per provider charge type: AWS line_item_line_item_type (Usage, DiscountedUsage, "
        "SavingsPlanCoveredUsage, RIFee, Tax, Credit ...), Azure ChargeType (Usage, Purchase, UnusedReservation ...) "
        "and GCP cost_type (regular, tax, adjustment), with the FOCUS charge_category and, for AWS, "
        "charge_frequency. Charge types not listed map to 'Adjustment'.",
        "## charge_type_map\n\nHow provider charge types become FOCUS ChargeCategory.",
        CHARGE_MAP_SCHEMA,
        [
            {
                "description": "AWS line item types that are FOCUS purchases",
                "sql": "SELECT charge_type, charge_frequency FROM focus.main.charge_type_map "
                "WHERE source = 'aws_cur2' AND charge_category = 'Purchase' ORDER BY charge_type",
                "expected_result": [["Fee", "One-Time"], ["RIFee", "Recurring"],
                                    ["SavingsPlanRecurringFee", "Recurring"], ["SavingsPlanUpfrontFee", "One-Time"]],
            },
        ],
    ),
)

TABLES = [FOCUS_COLUMNS, PROVIDER_MAP, SERVICE_MAP, CHARGE_TYPE_MAP]
TABLE_FUNCTIONS: list[type[Any]] = [FocusColumnsSource, ProviderMapSource, ServiceMapSource, ChargeTypeMapSource]
