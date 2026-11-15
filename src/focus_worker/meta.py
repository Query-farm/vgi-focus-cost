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


def schema_columns_json(schema: pa.Schema, max_description: int = 400) -> list[dict[str, str]]:
    """``vgi.result_columns_schema`` entries for an Arrow schema."""
    from .spec import duckdb_type_name

    out = []
    for f in schema:
        comment = (f.metadata or {}).get(b"comment", b"").decode() or f.name
        if len(comment) > max_description:  # keep wide schemas inside agent-tool context limits
            comment = comment[: max_description - 3].rsplit(" ", 1)[0] + "..."
        out.append({"name": f.name, "type": duckdb_type_name(f.type), "description": comment})
    return out


def focus_dynamic_columns_md() -> str:
    """Return the ``vgi.result_dynamic_columns_md`` table for ``focus()`` / ``to_focus()``.

    The columns depend on ``spec_version`` (and money columns on ``money_scale``), so
    every column of every supported version is listed with the versions it is in.
    """
    from .spec import EXTENSION_COLUMNS, all_columns, arrow_type, duckdb_type_name

    by_name: dict[str, list[Any]] = {}
    for c in all_columns():
        by_name.setdefault(c.column_name, []).append(c)
    rows = [
        "Columns of the chosen spec_version (alphabetical), then the x_ columns. Cost columns are shown at the "
        "default money_scale 6; money_scale := s makes them DECIMAL(19, s). Native FOCUS files add their own "
        "x_ columns.",
        "",
        "| Name | Type | Description |",
        "|---|---|---|",
    ]
    for name in sorted(by_name):
        cols = by_name[name]
        first, last = cols[0].spec_version, cols[-1].spec_version
        span = f"{first}+" if last == "1.4" else f"{first}-{last}"
        t = duckdb_type_name(arrow_type(cols[-1])).replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ")
        rows.append(f"| {name} | {t} | {cols[-1].feature_level} {span} |")
    rows += [f"| {name} | VARCHAR | extension |" for name, _ in EXTENSION_COLUMNS]
    return "\n".join(rows)


def focus_result_columns() -> list[dict[str, str]]:
    """Return Result columns of ``focus()`` / ``to_focus()`` at the default version and scale."""
    from .sources import output_schema
    from .spec import DEFAULT_MONEY_SCALE, DEFAULT_SPEC_VERSION

    return schema_columns_json(output_schema(DEFAULT_SPEC_VERSION, DEFAULT_MONEY_SCALE), max_description=60)


CATALOG_DESCRIPTION = (
    "Normalize AWS, Azure and Google Cloud billing exports into the FinOps FOCUS 1.x schema, so one DuckDB query "
    "covers every cloud, with exact decimal money and conformance checks."
)

CATALOG_DOC_LLM = (
    "FinOps cloud cost data in DuckDB, in the FinOps Open Cost and Usage Specification (FOCUS) schema. The readers "
    "take billing export files the user already has, with no cloud credentials: AWS Cost and Usage Report 2.0 / Data "
    "Exports (Parquet or CSV, legacy CUR too), Azure Cost Management exports (actual or amortized), Google Cloud "
    "BigQuery billing exports dumped to Parquet or JSON, and any provider's native FOCUS files. Paths can be local "
    "files, globs, https URLs or s3:// URIs, and `sample://aws_cur2`, `sample://azure`, `sample://gcp`, "
    "`sample://focus12` and `sample://aws_cur_legacy` name synthetic exports bundled with the worker for trying "
    "things out. Every reader emits the same FOCUS columns, so results from different clouds UNION ALL directly: "
    "money (BilledCost, EffectiveCost, ListCost, ContractedCost) as exact `DECIMAL(19,6)`, charge and billing "
    "periods as `TIMESTAMPTZ` in UTC, Tags as `MAP(VARCHAR, VARCHAR)`, commitment-discount columns, and x_ columns "
    "with provider detail. Sum EffectiveCost for amortized spend and BilledCost for invoice totals. A conformance "
    "report checks files against the spec. Reference tables publish the spec itself (versions 1.0 to 1.4) and "
    "every mapping rule, so any value can be traced to the raw export columns it came from. Exact-division scalars "
    "exist because `/` on `DECIMAL` returns `DOUBLE` in DuckDB."
)

CATALOG_DOC_MD = (
    "# FOCUS cost normalization\n\n"
    "Turn AWS, Azure and Google Cloud billing exports into the FinOps Foundation's "
    "[FOCUS](https://focus.finops.org) schema inside DuckDB: one query, every cloud.\n\n"
    "- **Readers** for AWS CUR 2.0, Azure cost exports, GCP billing export dumps and native FOCUS files.\n"
    "- **Exact money**: `DECIMAL(19,6)`, never `DOUBLE`, rounded half-to-even.\n"
    "- **Auditable**: every mapping rule is a row you can query.\n"
    "- **Conformance**: check any FOCUS file against the spec.\n\n"
    "FOCUS column definitions are © FinOps Open Cost and Usage Specification (FOCUS) Series contributors, "
    "used under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)."
)

SCHEMA_DOC_LLM = (
    "The `main` schema holds everything: table functions that read billing export files or map rows from any "
    "DuckDB query into FOCUS, a conformance report, reference tables for the FOCUS spec and the mapping rules, and "
    "scalars that classify provider services and charge types or divide decimals exactly. Readers take a positional "
    "path plus named arguments: source ('auto' detects the export format per file; 'aws_cur2', 'azure', "
    "'azure_amortized', 'gcp' or 'focus' force it), spec_version ('1.0' to '1.4', default '1.2'), billing_period "
    "('YYYY-MM') and money_scale (decimal places on cost columns). Pass a subquery, not a table name, to the "
    "mapping function. Provider arguments of the scalars are 'aws', 'azure' or 'gcp'. Use the bundled "
    "`sample://` exports to explore before pointing at real data."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Readers turn billing exports into FOCUS rows; a mapping function does the same for rows DuckDB has already "
    "read; a validator reports spec conformance.\n\n"
    "Reference tables publish the FOCUS spec (1.0-1.4) and every mapping rule. Scalars classify services and "
    "charges and divide money exactly.\n\n"
    "Try it on the bundled synthetic exports: `sample://aws_cur2`, `sample://azure`, `sample://gcp`, "
    "`sample://focus12`."
)

AGENT_TEST_TASKS = [
    {
        "name": "aws_effective_by_category",
        "prompt": "Using the focus worker's bundled AWS CUR 2.0 sample export (sample://aws_cur2), what is the total "
        "effective (amortized) cost per FOCUS ChargeCategory for billing period 2026-07?",
    },
    {
        "name": "azure_amortized_by_service_category",
        "prompt": "The bundled Azure sample sample://azure/contoso-amortized-cost-202607.parquet is an amortized-cost "
        "export. What is its effective cost per FOCUS ServiceCategory?",
    },
    {
        "name": "validate_focus12_as_1_3",
        "prompt": "Validate the bundled native FOCUS 1.2 export sample://focus12 against FOCUS 1.3. Which checks are "
        "not passing? Give check name, column and status.",
    },
    {
        "name": "map_one_cur_row",
        "prompt": "A raw AWS CUR 2.0 line item has line_item_line_item_type 'DiscountedUsage', "
        "line_item_unblended_cost 0, reservation_effective_cost 4.1088 and reservation_reservation_a_r_n "
        "'arn:aws:rds:us-east-1:1:ri:x'. Map it to FOCUS: what are its BilledCost, EffectiveCost and "
        "CommitmentDiscountType?",
    },
    {
        "name": "mandatory_columns_1_2",
        "prompt": "How many columns does FOCUS version 1.2 define as Mandatory?",
    },
    {
        "name": "aws_effective_cost_inputs",
        "prompt": "Which raw AWS CUR 2.0 columns does the focus worker read to compute EffectiveCost?",
    },
    {
        "name": "azure_database_services",
        "prompt": "How many Azure services in the focus worker's service classification map to the Databases category?",
    },
    {
        "name": "aws_credit_line_items",
        "prompt": "Which AWS line item types does the focus worker classify as the FOCUS Credit charge category?",
    },
    {
        "name": "rds_category",
        "prompt": "Which FOCUS service category and subcategory does the AWS product code 'AmazonRDS' map to?",
    },
    {
        "name": "unused_reservation_category",
        "prompt": "What FOCUS ChargeCategory does an Azure 'UnusedReservation' charge type map to?",
    },
    {
        "name": "gcp_list_cost_lineage",
        "prompt": "Which FOCUS columns does the focus worker derive from the GCP billing export column cost_at_list?",
    },
    {
        "name": "validate_charge_frequency",
        "prompt": "Is 'Usage-based' (lower-case b) a valid FOCUS 1.2 ChargeFrequency value? "
        "Show the validation result.",
    },
    {
        "name": "exact_ratio",
        "prompt": "Divide 10 by 3 exactly as a FOCUS money ratio, without DuckDB turning it into a DOUBLE.",
    },
    {
        "name": "sp_effective_rate",
        "prompt": "In the bundled AWS sample sample://aws_cur2, what is the exact effective hourly rate (effective "
        "cost per pricing quantity) of Savings Plan covered usage in billing period 2026-07?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "FOCUS Cloud Cost Normalization",
    "vgi.subtitle": "Normalizes AWS, Azure and Google Cloud billing exports into the FinOps FOCUS schema.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "focus, finops, cloud cost, aws cur, azure cost management, gcp billing export, multi-cloud, chargeback"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
