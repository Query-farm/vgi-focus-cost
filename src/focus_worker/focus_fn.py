"""``focus(path, source := 'auto', ...)``: billing export files as typed FOCUS rows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

from . import scan
from .meta import focus_result_columns, object_tags
from .reader import ScanCursor, ScanRequest, make_plan, new_cursor, next_batch
from .sources import SOURCE_CHOICES
from .spec import DEFAULT_MONEY_SCALE, DEFAULT_SPEC_VERSION, SPEC_VERSIONS

PATH_DOC = (
    "Export file, directory (read recursively), glob ('exports/**/*.parquet'), http(s) URL, or s3:// / gs:// URI. "
    "Parquet, CSV (optionally .gz) and newline-delimited JSON are read by extension."
)
SOURCE_DOC = (
    "Export format: 'auto' (detect from the column names), 'aws_cur2' (AWS CUR 2.0 / Data Exports, also legacy CUR), "
    "'azure' (Azure actual-cost export), 'azure_amortized' (Azure amortized-cost export), 'gcp' (BigQuery billing "
    "export dump) or 'focus' (any provider's native FOCUS file)."
)
SPEC_DOC = "FOCUS version whose column set is returned: '1.0', '1.1', '1.2' (default), '1.3' or '1.4'."
PERIOD_DOC = (
    "Only rows whose BillingPeriodStart falls in this month, 'YYYY-MM'. Prunes BILLING_PERIOD=YYYY-MM directories "
    "and Parquet row groups before reading."
)
SCALE_DOC = (
    "Decimal places kept on cost columns, 2 to 10 (default 6): BilledCost etc. are DECIMAL(19, money_scale). "
    "Values are rounded half-to-even; raise it to keep sub-microcent line items exactly."
)
FORMAT_DOC = "File format override: 'auto' (by extension), 'parquet', 'csv' or 'json'."


@dataclass(slots=True, frozen=True, kw_only=True)
class FocusArgs:
    """Arguments for ``focus(path, ...)``."""

    path: Annotated[str, Arg(0, doc=PATH_DOC)]
    source: Annotated[str, Arg("source", default="auto", doc=SOURCE_DOC, choices=list(SOURCE_CHOICES))]
    spec_version: Annotated[
        str, Arg("spec_version", default=DEFAULT_SPEC_VERSION, doc=SPEC_DOC, choices=list(SPEC_VERSIONS))
    ]
    billing_period: Annotated[
        str | None, Arg("billing_period", default=None, doc=PERIOD_DOC, pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    ]
    money_scale: Annotated[int, Arg("money_scale", default=DEFAULT_MONEY_SCALE, doc=SCALE_DOC, ge=2, le=10)]
    file_format: Annotated[str, Arg("file_format", default="auto", doc=FORMAT_DOC, choices=list(scan.FORMATS))]


def request_secret(params: BindParams[Any], path: str) -> dict[str, Any] | None:
    """Ask DuckDB for the ``s3`` (or ``gcs``) secret whose scope covers ``path``."""
    s = scan.scheme_of(path)
    if s in ("s3", "s3a", "r2"):
        return params.secrets.get("s3" if s != "r2" else "r2", scope=path)
    if s in ("gs", "gcs"):
        return params.secrets.get("gcs", scope=path)
    return None


def store_from_resolved(secrets: Any, path: str) -> scan.ObjectStoreConfig | None:
    """Return the object-store settings for ``path`` from already-resolved secrets."""
    s = scan.scheme_of(path)
    if s not in ("s3", "s3a", "r2", "gs", "gcs"):
        return None
    kind = {"s3": "s3", "s3a": "s3", "r2": "r2", "gs": "gcs", "gcs": "gcs"}[s]
    found = secrets.for_scope_of_type(path, kind) if hasattr(secrets, "for_scope_of_type") else None
    return scan.object_store_config(found)


def scan_request(args: Any, store: scan.ObjectStoreConfig | None) -> ScanRequest:
    """Build the plan key from the literal arguments."""
    return ScanRequest(
        path=args.path,
        source=args.source,
        spec_version=args.spec_version,
        money_scale=args.money_scale,
        file_format=args.file_format,
        billing_period=args.billing_period,
        store=store,
    )


_DOC_LLM = (
    "Read cloud billing export files and return them as FOCUS (FinOps Open Cost and Usage Specification) rows, so one "
    "SQL query covers AWS, Azure and GCP. `path` (positional) is a file, directory, glob, https URL or s3:// URI of "
    "the export the user already has: AWS CUR 2.0 / Data Exports Parquet or CSV, Azure cost-details CSV/Parquet "
    "(actual or amortized), a GCP BigQuery billing export dumped to Parquet/JSON, or any native FOCUS file. "
    "`source := 'auto'` detects the format; pass 'aws_cur2', 'azure', 'azure_amortized', 'gcp' or 'focus' to force "
    "it (use 'azure_amortized' for an Azure amortized export, since it cannot be told apart from the actual one). "
    "`spec_version` picks the column set ('1.2' default). `billing_period := 'YYYY-MM'` restricts to one billing "
    "month. Money columns (BilledCost, EffectiveCost, ListCost, ContractedCost) are exact DECIMAL(19,6); "
    "ChargePeriodStart/End and BillingPeriodStart/End are TIMESTAMPTZ in UTC; Tags is MAP(VARCHAR, VARCHAR) "
    "(read a tag with Tags['team']). x_ columns carry provider detail (x_ServiceCode, x_UsageType, x_ChargeType, "
    "x_SourceFile). Use EffectiveCost for amortized spend and BilledCost for invoice totals. Filters on "
    "ChargePeriodStart, BillingAccountId and SubAccountId are pushed into Parquet reads."
)

_DOC_MD = (
    "## focus(path, source := 'auto', spec_version := '1.2', billing_period := NULL, money_scale := 6, "
    "file_format := 'auto')\n\n"
    "Billing export files → typed **FOCUS** rows. One query, every cloud:\n\n"
    "```sql\nSELECT ProviderName, ServiceCategory, sum(EffectiveCost)\n"
    "FROM focus.main.focus('exports/**/*.parquet')\nGROUP BY ALL;\n```\n\n"
    "| source | reads |\n|---|---|\n"
    "| `aws_cur2` | AWS CUR 2.0 / Data Exports (Parquet, CSV), legacy CUR headers |\n"
    "| `azure` / `azure_amortized` | Azure cost-details exports (actual / amortized) |\n"
    "| `gcp` | BigQuery billing export dumped to Parquet / JSON / CSV |\n"
    "| `focus` | native FOCUS 1.0-1.4 files from any provider |\n\n"
    "Files stream one row group at a time; projection and filters on ChargePeriodStart, BillingPeriodStart, "
    "BillingAccountId and SubAccountId reach the Parquet reader. The mapping per source is in "
    "`focus.main.provider_map`."
)


@init_single_worker
class Focus(TableFunctionGenerator[FocusArgs, ScanCursor]):
    """Stream billing export files as FOCUS rows."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "focus"
        description = "Read AWS, Azure, GCP or native FOCUS billing export files as typed FOCUS 1.x rows"
        projection_pushdown = True
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Billing Exports as FOCUS Rows",
                category="ingest",
                keywords="focus, finops, cloud cost, billing export, aws cur, cur 2.0, azure cost "
                "export, gcp billing export, "
                "multi-cloud, cost and usage",
                doc_llm=_DOC_LLM,
                doc_md=_DOC_MD,
            ),
            "vgi.result_columns_schema": json.dumps(focus_result_columns()),
        }
        examples = [
            FunctionExample(
                sql="SELECT ProviderName, ServiceCategory, sum(EffectiveCost) AS effective_cost "
                "FROM focus.main.focus('/data/billing/**/*.parquet') GROUP BY ALL ORDER BY effective_cost DESC",
                description="Amortized spend by cloud and service category across every export under a folder",
            ),
            FunctionExample(
                sql="SELECT Tags['team'] AS team, sum(BilledCost) AS billed FROM focus.main.focus("
                "'s3://my-cur-bucket/cur2/data/', source := 'aws_cur2', billing_period := '2026-07') "
                "GROUP BY team ORDER BY billed DESC",
                description="July invoice cost by team tag, read straight from the CUR 2.0 bucket",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FocusArgs]) -> BindResponse:
        """Resolve the files and source, and declare the FOCUS output schema."""
        secret = request_secret(params, params.args.path)
        if params.secrets.needs_resolution:
            return BindResponse(output_schema=pa.schema([pa.field("pending", pa.string())]))
        store = scan.object_store_config(secret) if scan.scheme_of(params.args.path) else None
        plan = make_plan(scan_request(params.args, store))
        return BindResponse(output_schema=plan.schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[FocusArgs]) -> ScanCursor:
        """Start at the first file."""
        return new_cursor()

    @classmethod
    def process(cls, params: ProcessParams[FocusArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Emit the next mapped batch, or finish."""
        plan = make_plan(scan_request(params.args, store_from_resolved(params.secrets, params.args.path)))
        batch = next_batch(plan, state, params.output_schema.names, params.current_pushdown_filters)
        if batch is None:
            out.finish()
            return
        out.emit(pa.RecordBatch.from_arrays(list(batch.columns), schema=params.output_schema))
