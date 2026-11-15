"""``focus_validate(path, ...)``: FOCUS conformance checks over export files.

Every check is computed from the specification data in ``focus_columns`` (feature
level, nullability, allowed values, ServiceSubcategory parents) plus a few
cross-column rules from the spec's requirement text. The scan is resumable: the
running counts travel in the cursor, so a long validation survives the HTTP
transport like any other scan.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, OutputCollector, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi_rpc import ArrowSerializableDataclass

from . import scan
from .convert import Arr, pc
from .focus_fn import FORMAT_DOC, PATH_DOC, SPEC_DOC, request_secret, store_from_resolved
from .meta import column, object_tags, schema_columns_json
from .reader import Plan, ScanCursor, ScanRequest, make_plan, next_batch
from .sources import SOURCE_CHOICES
from .spec import DEFAULT_SPEC_VERSION, SPEC_VERSIONS, ColumnSpec, all_column_names, columns_for

TIME_BUDGET_SECONDS = 5.0

RESULT_SCHEMA = pa.schema(
    [
        column(
            "check_name", pa.string(), "Check id, e.g. 'not_null', 'allowed_values', 'column_present'.", nullable=False
        ),
        column("column_name", pa.string(), "FOCUS column checked; NULL for file-level checks."),
        column(
            "severity", pa.string(), "'error' (spec MUST), 'warning' (SHOULD / likely wrong) or 'info'.", nullable=False
        ),
        column("status", pa.string(), "'pass', 'fail' or 'not_applicable'.", nullable=False),
        column("rows_checked", pa.int64(), "Rows the check looked at.", nullable=False),
        column("failing_rows", pa.int64(), "Rows (or columns, for file-level checks) that failed.", nullable=False),
        column("sample_value", pa.string(), "One offending value, for diagnosis; NULL when passing."),
        column("detail", pa.string(), "What the rule requires.", nullable=False),
    ]
)

_CURRENCY = re.compile(r"^[A-Z]{3}$")
_SOURCE_DOC = "Export format, as for focus(): 'auto', 'aws_cur2', 'azure', 'azure_amortized', 'gcp' or 'focus'."


@dataclass(slots=True, frozen=True, kw_only=True)
class ValidateArgs:
    """Arguments for ``focus_validate(path, ...)``."""

    path: Annotated[str, Arg(0, doc=PATH_DOC)]
    source: Annotated[str, Arg("source", default="auto", doc=_SOURCE_DOC, choices=list(SOURCE_CHOICES))]
    spec_version: Annotated[
        str, Arg("spec_version", default=DEFAULT_SPEC_VERSION, doc=SPEC_DOC, choices=list(SPEC_VERSIONS))
    ]
    file_format: Annotated[str, Arg("file_format", default="auto", doc=FORMAT_DOC, choices=list(scan.FORMATS))]


@dataclass(kw_only=True)
class ValidateState(ArrowSerializableDataclass):
    """Scan cursor plus the running counts (JSON) of every check."""

    scan_id: str = ""
    file_idx: int = 0
    unit_pos: int = 0
    batch_idx: int = 0
    counts: str = "{}"
    done: bool = False


@dataclass
class _Counter:
    rows: int = 0
    fails: int = 0
    sample: str | None = None

    def add(self, rows: int, mask: Arr, values: Arr) -> None:
        self.rows += rows
        bad = int(pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py() or 0)
        self.fails += bad
        if bad and self.sample is None:
            hit = pc.filter(values, pc.fill_null(mask, False))
            v = hit[0].as_py() if len(hit) else None
            self.sample = None if v is None else str(v)[:200]


def _key(check: str, col: str | None) -> str:
    return f"{check}|{col or ''}"


def _present(plan: Plan) -> tuple[set[str], list[str], list[str]]:
    """(FOCUS columns the source provides, non-FOCUS columns without x_, case mismatches)."""
    if plan.mapper.token == "focus":
        names = scan.file_schema(plan.files, 0).names
        focus_names = all_column_names()
        lower = {n.lower(): n for n in focus_names}
        present = {lower[n.lower()] for n in names if n.lower() in lower}
        custom = [n for n in names if n.lower() not in lower and not n.lower().startswith("x_")]
        case = [n for n in names if n.lower() in lower and n not in focus_names]
        bridged = {
            "ServiceProviderName": "PublisherName",
            "HostProviderName": "ProviderName",
            "PublisherName": "ServiceProviderName",
            "ProviderName": "HostProviderName",
        }
        present |= {k for k, v in bridged.items() if v in present}
        return present, custom, case
    provided = set(plan.mapper.rule_index) | {"ChargeClass", "ServiceProviderName", "HostProviderName"}
    return provided, [], []


def _checks_for(cols: tuple[ColumnSpec, ...]) -> list[tuple[str, str | None, str, str]]:
    """(check, column, severity, detail) in report order."""
    out: list[tuple[str, str | None, str, str]] = []
    for c in cols:
        level = c.feature_level
        out.append(
            (
                "column_present",
                c.column_name,
                "error" if level == "Mandatory" else "info",
                f"{c.column_name} is {level} in FOCUS {c.spec_version}",
            )
        )
    for c in cols:
        if not c.allows_nulls:
            out.append(("not_null", c.column_name, "error", f"{c.column_name} MUST NOT be null"))
    for c in cols:
        if c.allowed_values and c.data_type == "String":
            out.append(
                (
                    "allowed_values",
                    c.column_name,
                    "error",
                    f"{c.column_name} MUST be one of: {', '.join(c.allowed_values)}",
                )
            )
    names = {c.column_name for c in cols}
    if {"ServiceCategory", "ServiceSubcategory"} <= names:
        out.append(
            (
                "subcategory_matches_category",
                "ServiceSubcategory",
                "error",
                "ServiceSubcategory MUST belong to the row's ServiceCategory",
            )
        )
    out.append(("charge_period_order", "ChargePeriodEnd", "error", "ChargePeriodEnd MUST be after ChargePeriodStart"))
    out.append(
        ("billing_period_order", "BillingPeriodEnd", "error", "BillingPeriodEnd MUST be after BillingPeriodStart")
    )
    for cur in ("BillingCurrency", "PricingCurrency"):
        if cur in names:
            out.append(("currency_code", cur, "error", f"{cur} MUST be an ISO 4217 alphabetic code"))
    out.append(
        (
            "consumed_quantity_usage_only",
            "ConsumedQuantity",
            "warning",
            "ConsumedQuantity MUST be null unless ChargeCategory is 'Usage'",
        )
    )
    out.append(
        (
            "commitment_status_requires_id",
            "CommitmentDiscountStatus",
            "error",
            "CommitmentDiscountStatus MUST be null when CommitmentDiscountId is null",
        )
    )
    out.append(("custom_column_prefix", None, "warning", "Columns that are not FOCUS columns SHOULD use the x_ prefix"))
    out.append(("column_name_case", None, "warning", "FOCUS column names are case-sensitive (PascalCase)"))
    return out


def _accumulate(
    batch: pa.RecordBatch, cols: tuple[ColumnSpec, ...], present: set[str], counts: dict[str, _Counter]
) -> None:
    n = batch.num_rows

    def get(name: str) -> Arr | None:
        return batch.column(name) if name in batch.schema.names and name in present else None

    for c in cols:
        arr = get(c.column_name)
        if arr is None:
            continue
        if not c.allows_nulls:
            counts.setdefault(_key("not_null", c.column_name), _Counter()).add(n, pc.is_null(arr), arr)
        if c.allowed_values and c.data_type == "String":
            bad = pc.and_(pc.is_valid(arr), pc.invert(pc.is_in(arr, value_set=pa.array(list(c.allowed_values)))))
            counts.setdefault(_key("allowed_values", c.column_name), _Counter()).add(n, bad, arr)
        if c.column_name == "ServiceSubcategory" and c.subcategory_parent and get("ServiceCategory") is not None:
            parent_map = c.subcategory_parent
            parents = pa.array([parent_map.get(v) if v is not None else None for v in arr.to_pylist()], pa.string())
            service_cat = batch.column("ServiceCategory")
            bad = pc.and_(pc.and_(pc.is_valid(parents), pc.is_valid(service_cat)), pc.not_equal(parents, service_cat))
            label = pc.binary_join_element_wise(arr, pc.fill_null(service_cat, ""), " under ")
            counts.setdefault(_key("subcategory_matches_category", "ServiceSubcategory"), _Counter()).add(n, bad, label)
        if c.column_name in ("BillingCurrency", "PricingCurrency"):
            ok = pc.match_substring_regex(arr, _CURRENCY.pattern)
            bad = pc.and_(pc.is_valid(arr), pc.invert(pc.fill_null(ok, False)))
            counts.setdefault(_key("currency_code", c.column_name), _Counter()).add(n, bad, arr)
    for start, end, check in (
        ("ChargePeriodStart", "ChargePeriodEnd", "charge_period_order"),
        ("BillingPeriodStart", "BillingPeriodEnd", "billing_period_order"),
    ):
        s, e = get(start), get(end)
        if s is not None and e is not None:
            counts.setdefault(_key(check, end), _Counter()).add(n, pc.less_equal(e, s), pc.cast(e, pa.string()))
    cq, cat = get("ConsumedQuantity"), get("ChargeCategory")
    if cq is not None and cat is not None:
        bad = pc.and_(pc.is_valid(cq), pc.not_equal(pc.fill_null(cat, ""), "Usage"))
        counts.setdefault(_key("consumed_quantity_usage_only", "ConsumedQuantity"), _Counter()).add(n, bad, cat)
    st, cid = get("CommitmentDiscountStatus"), get("CommitmentDiscountId")
    if st is not None and cid is not None:
        bad = pc.and_(pc.is_valid(st), pc.is_null(cid))
        counts.setdefault(_key("commitment_status_requires_id", "CommitmentDiscountStatus"), _Counter()).add(n, bad, st)


def _report(plan: Plan, counts: dict[str, _Counter]) -> pa.RecordBatch:
    cols = columns_for(plan.request.spec_version)
    present, custom, case = _present(plan)
    total_rows = max((c.rows for k, c in counts.items() if k.startswith("not_null|")), default=0)
    rows: list[dict[str, Any]] = []
    for check, col, severity, detail in _checks_for(cols):
        if check == "column_present":
            ok = col in present
            spec = next(c for c in cols if c.column_name == col)
            status = "pass" if ok else ("fail" if spec.feature_level == "Mandatory" else "not_applicable")
            rows.append(
                {
                    "check_name": check,
                    "column_name": col,
                    "severity": severity,
                    "status": status,
                    "rows_checked": total_rows,
                    "failing_rows": total_rows if status == "fail" else 0,
                    "sample_value": None,
                    "detail": detail + ("" if ok else "; the source does not provide it"),
                }
            )
            continue
        if check in ("custom_column_prefix", "column_name_case"):
            bad = custom if check == "custom_column_prefix" else case
            applicable = plan.mapper.token == "focus"
            rows.append(
                {
                    "check_name": check,
                    "column_name": col,
                    "severity": severity,
                    "status": ("fail" if bad else "pass") if applicable else "not_applicable",
                    "rows_checked": total_rows,
                    "failing_rows": len(bad),
                    "sample_value": ", ".join(bad[:10]) or None,
                    "detail": detail,
                }
            )
            continue
        counter = counts.get(_key(check, col))
        if counter is None:
            rows.append(
                {
                    "check_name": check,
                    "column_name": col,
                    "severity": severity,
                    "status": "not_applicable",
                    "rows_checked": 0,
                    "failing_rows": 0,
                    "sample_value": None,
                    "detail": detail + "; column not present",
                }
            )
            continue
        rows.append(
            {
                "check_name": check,
                "column_name": col,
                "severity": severity,
                "status": "fail" if counter.fails else "pass",
                "rows_checked": counter.rows,
                "failing_rows": counter.fails,
                "sample_value": counter.sample,
                "detail": detail,
            }
        )
    return pa.RecordBatch.from_pylist(rows, schema=RESULT_SCHEMA)


@init_single_worker
class FocusValidate(TableFunctionGenerator[ValidateArgs, ValidateState]):
    """Conformance report for billing export files."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = RESULT_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "focus_validate"
        description = "Check billing export files against the FOCUS spec: required columns, nulls, allowed values"
        tags = {
            **object_tags(
                title="FOCUS Conformance Report",
                category="validation",
                keywords="focus validation, conformance, data quality, allowed values, mandatory columns, finops",
                doc_llm=(
                    "Validate billing data against a FOCUS version. Reads the same paths and sources as focus() "
                    "(native FOCUS files are checked as-is; provider exports are checked after mapping) and returns "
                    "one row per check: check_name (column_present, not_null, allowed_values, "
                    "subcategory_matches_category, charge_period_order, billing_period_order, currency_code, "
                    "consumed_quantity_usage_only, commitment_status_requires_id, custom_column_prefix, "
                    "column_name_case), column_name, severity (error/warning/info), status (pass/fail/"
                    "not_applicable), rows_checked, failing_rows, sample_value and detail. Filter "
                    "WHERE status = 'fail' for the problems. spec_version defaults to '1.2'."
                ),
                doc_md=(
                    "## focus_validate(path, source := 'auto', spec_version := '1.2', file_format := 'auto')\n\n"
                    "One row per conformance check, with the failing row count and a sample offending value. "
                    "Filter on `status = 'fail'` for the problems."
                ),
            ),
            "vgi.result_columns_schema": json.dumps(schema_columns_json(RESULT_SCHEMA)),
        }
        examples = [
            FunctionExample(
                sql="SELECT check_name, column_name, status FROM focus.main.focus_validate("
                "'sample://focus12', spec_version := '1.3') WHERE status <> 'pass' ORDER BY check_name, column_name",
                description="Checks a native FOCUS 1.2 export does not pass as FOCUS 1.3",
            ),
            FunctionExample(
                sql="SELECT check_name, column_name, failing_rows, sample_value FROM focus.main.focus_validate("
                "'sample://gcp/gcp_billing_export_v1_01A2B3_202607.parquet') WHERE severity = 'error' "
                "ORDER BY failing_rows DESC, check_name",
                description="Error-level checks of the mapped GCP billing export sample",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ValidateArgs]) -> BindResponse:
        """Resolve the files early so a bad path fails at bind."""
        secret = request_secret(params, params.args.path)
        if params.secrets.needs_resolution:
            return BindResponse(output_schema=RESULT_SCHEMA)
        store = scan.object_store_config(secret) if scan.scheme_of(params.args.path) else None
        make_plan(_request(params.args, store))
        return BindResponse(output_schema=RESULT_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ValidateArgs]) -> ValidateState:
        """Start at the first file with empty counts."""
        import uuid

        return ValidateState(scan_id=uuid.uuid4().hex)

    @classmethod
    def process(cls, params: ProcessParams[ValidateArgs], state: ValidateState, out: OutputCollector) -> None:
        """Scan for up to a few seconds per call; emit the report at the end."""
        if state.done:
            out.finish()
            return
        plan = make_plan(_request(params.args, store_from_resolved(params.secrets, params.args.path)))
        cols = columns_for(plan.request.spec_version)
        present, _, _ = _present(plan)
        counts = {k: _Counter(**v) for k, v in json.loads(state.counts).items()}
        cursor = ScanCursor(
            scan_id=state.scan_id, file_idx=state.file_idx, unit_pos=state.unit_pos, batch_idx=state.batch_idx
        )
        wanted = [c.column_name for c in cols]
        deadline = time.monotonic() + TIME_BUDGET_SECONDS
        finished = False
        while time.monotonic() < deadline:
            batch = next_batch(plan, cursor, wanted)
            if batch is None:
                finished = True
                break
            _accumulate(batch, cols, present, counts)
        state.file_idx, state.unit_pos, state.batch_idx = cursor.file_idx, cursor.unit_pos, cursor.batch_idx
        state.counts = json.dumps(
            {k: {"rows": c.rows, "fails": c.fails, "sample": c.sample} for k, c in counts.items()}
        )
        if not finished:
            out.emit(pa.RecordBatch.from_pylist([], schema=RESULT_SCHEMA).select(params.output_schema.names))
            return
        state.done = True
        out.emit(_report(plan, counts).select(params.output_schema.names))


def _request(args: ValidateArgs, store: scan.ObjectStoreConfig | None) -> ScanRequest:
    return ScanRequest(
        path=args.path,
        source=args.source,
        spec_version=args.spec_version,
        money_scale=10,
        file_format=args.file_format,
        store=store,
    )
