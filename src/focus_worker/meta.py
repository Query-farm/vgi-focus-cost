"""Discovery metadata: catalog/schema docs, categories and shared helpers.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these tags.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "focus"

SOURCE_URL = "https://github.com/Query-farm/vgi-focus-cost"
FOCUS_SPEC_URL = "https://focus.finops.org/focus-specification/"

SCHEMA_CATEGORIES = [
    {"name": "ingest", "title": "Ingest", "description": "Read billing export files as FOCUS rows."},
    {"name": "transform", "title": "Transform", "description": "Map rows DuckDB already has into FOCUS."},
    {"name": "validation", "title": "Validation", "description": "Check FOCUS data against the specification."},
    {"name": "reference", "title": "Reference", "description": "The FOCUS spec and the mapping rules as tables."},
    {"name": "classification", "title": "Classification", "description": "Classify provider services and charges."},
    {"name": "arithmetic", "title": "Exact Arithmetic", "description": "Exact decimal ratios for money columns."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def schema_columns_json(schema: pa.Schema) -> list[dict[str, str]]:
    """``vgi.result_columns_schema`` entries for an Arrow schema."""
    from .spec import duckdb_type_name

    out = []
    for f in schema:
        comment = (f.metadata or {}).get(b"comment", b"").decode()
        out.append({"name": f.name, "type": duckdb_type_name(f.type), "description": comment or f.name})
    return out


def focus_result_columns() -> list[dict[str, str]]:
    """Return Result columns of ``focus()`` / ``to_focus()`` at the default version and scale."""
    from .sources import output_schema
    from .spec import DEFAULT_MONEY_SCALE, DEFAULT_SPEC_VERSION

    return schema_columns_json(output_schema(DEFAULT_SPEC_VERSION, DEFAULT_MONEY_SCALE))


CATALOG_DESCRIPTION = (
    "Normalize AWS, Azure and Google Cloud billing exports into the FinOps FOCUS 1.x schema, so one DuckDB query "
    "covers every cloud, with exact decimal money and conformance checks."
)

CATALOG_DOC_LLM = (
    "FinOps cost data in DuckDB. `focus.main.focus(path, source := 'auto')` reads billing export files the user "
    "already has (AWS CUR 2.0 / Data Exports, Azure cost-details exports, GCP BigQuery billing export dumps, or any "
    "native FOCUS file; local paths, globs, https URLs or s3:// URIs) and returns FinOps Open Cost and Usage "
    "Specification (FOCUS) rows: BilledCost/EffectiveCost/ListCost/ContractedCost as exact DECIMAL(19,6), "
    "ChargePeriodStart/End as TIMESTAMPTZ, ChargeCategory, ServiceName, ServiceCategory, RegionId, ResourceId, "
    "SubAccountId, Tags as a MAP, commitment discount columns and x_ provider columns. "
    "`focus.main.to_focus(TABLE (...))` does the same mapping for rows DuckDB has already read (for example from "
    "Azure Blob or BigQuery). `focus.main.focus_validate(path)` reports conformance (required columns, nulls, "
    "allowed values) as rows. Reference tables: `focus_columns` (the spec, versions 1.0 to 1.4), `provider_map` "
    "(how every FOCUS column is derived per source), `service_map`, `charge_type_map`. Scalars classify services "
    "and charges and do exact decimal division (`focus_divide`) because `/` on DECIMAL returns DOUBLE in DuckDB. "
    "No cloud credentials are needed: everything reads exported files."
)

CATALOG_DOC_MD = (
    "# FOCUS cost normalization\n\n"
    "Turn AWS, Azure and Google Cloud billing exports into the FinOps Foundation's "
    "[FOCUS](https://focus.finops.org) schema inside DuckDB: one query, every cloud.\n\n"
    "```sql\nSELECT ProviderName, ServiceCategory, sum(EffectiveCost) AS cost\n"
    "FROM focus.main.focus('exports/**/*') GROUP BY ALL ORDER BY cost DESC;\n```\n\n"
    "- **Readers** for AWS CUR 2.0, Azure cost exports, GCP billing export dumps and native FOCUS files.\n"
    "- **Exact money**: DECIMAL(19,6), never DOUBLE, rounded half-to-even.\n"
    "- **Auditable**: every mapping rule is a row in `provider_map`.\n"
    "- **Conformance**: `focus_validate` checks files against the FOCUS spec.\n\n"
    "FOCUS column definitions are © FinOps Open Cost and Usage Specification (FOCUS) Series contributors, "
    "used under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)."
)

SCHEMA_DOC_LLM = (
    "The `main` schema of the focus catalog. Table functions: `focus(path, source := 'auto', spec_version := "
    "'1.2', billing_period := NULL, money_scale := 6, file_format := 'auto')` reads export files as FOCUS rows; "
    "`to_focus(TABLE (SELECT ...), source := 'auto', spec_version := '1.2', money_scale := 6)` maps rows from any "
    "DuckDB query; `focus_validate(path, source := 'auto', spec_version := '1.2')` returns one row per conformance "
    "check (status pass/fail/skip, failing row count, sample value). Tables: `focus_columns`, `provider_map`, "
    "`service_map`, `charge_type_map`. Scalars: `focus_service_category(provider, service)`, "
    "`focus_service_subcategory(provider, service)`, `focus_charge_category(provider, charge_type)`, "
    "`focus_map_column(source, column)`, `focus_validate_value(column, value)`, `focus_spec_version()`, "
    "`focus_divide(numerator, denominator)` and `focus_effective_rate(cost, quantity)`. Provider arguments accept "
    "'aws', 'azure' or 'gcp' (and 'AWS', 'Microsoft', 'Google Cloud'). Sum EffectiveCost for amortized spend and "
    "BilledCost for invoice totals."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "- **focus**, **to_focus**: billing data → FOCUS rows.\n"
    "- **focus_validate**: FOCUS conformance report.\n"
    "- **focus_columns**, **provider_map**, **service_map**, **charge_type_map**: reference tables.\n"
    "- Scalars for service/charge classification and exact decimal division."
)

AGENT_TEST_TASKS = [
    {
        "name": "aws_effective_cost_inputs",
        "prompt": "Which raw AWS CUR 2.0 columns does the focus worker read to compute EffectiveCost?",
    },
    {
        "name": "ec2_category",
        "prompt": "Which FOCUS service category and subcategory does the AWS product code 'AmazonRDS' map to?",
    },
    {
        "name": "mandatory_columns_1_2",
        "prompt": "How many columns does FOCUS version 1.2 define as Mandatory?",
    },
    {
        "name": "exact_ratio",
        "prompt": "Divide 10 by 3 exactly as a FOCUS money ratio, without DuckDB turning it into a DOUBLE.",
    },
]

CATALOG_TAGS = {
    "vgi.title": "FOCUS Cloud Cost Normalization",
    "vgi.subtitle": "AWS, Azure and GCP billing exports as one FinOps FOCUS schema in DuckDB.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "MIT-0",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "focus, finops, cloud cost, aws cur, azure cost management, gcp billing export, multi-cloud, chargeback"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
