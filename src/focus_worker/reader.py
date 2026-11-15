"""Plan and run a scan of billing export files into FOCUS batches.

Shared by ``focus()`` (rows) and ``focus_validate()`` (conformance checks). The
plan (file list, detected source, output schema) is rebuilt deterministically
from the arguments in whichever process serves a call, so only the small cursor
travels between calls.
"""

from __future__ import annotations

import datetime as dt
import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter
from vgi_rpc import ArrowSerializableDataclass

from . import scan
from .sources import MAPPERS, MapOptions, Mapper, map_batch, output_schema, resolve_source
from .sources.focus import extra_fields


@dataclass(frozen=True)
class ScanRequest:
    """The literal arguments that define a scan."""

    path: str
    source: str
    spec_version: str
    money_scale: int
    file_format: str
    billing_period: str | None = None
    store: scan.ObjectStoreConfig | None = None


@dataclass(frozen=True)
class Plan:
    """Everything derived from a ``ScanRequest``."""

    request: ScanRequest
    files: scan.FileSet
    mapper: Mapper
    schema: pa.Schema  # full output schema


@dataclass(kw_only=True)
class ScanCursor(ArrowSerializableDataclass):
    """Resumable scan position: file, unit (row group) and batch within the unit."""

    scan_id: str = ""
    file_idx: int = 0
    unit_pos: int = 0
    batch_idx: int = 0


class _Lru[K, V]:
    def __init__(self, capacity: int) -> None:
        self._d: OrderedDict[K, V] = OrderedDict()
        self._cap = capacity
        self._lock = threading.Lock()

    def get(self, key: K) -> V | None:
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
                return self._d[key]
            return None

    def put(self, key: K, value: V) -> None:
        with self._lock:
            self._d[key] = value
            while len(self._d) > self._cap:
                self._d.popitem(last=False)


_PLANS: _Lru[ScanRequest, Plan] = _Lru(16)
_UNITS: _Lru[tuple[str, str], list[int]] = _Lru(256)


def make_plan(req: ScanRequest) -> Plan:
    """Resolve files, detect the source and build the output schema (cached per process)."""
    cached = _PLANS.get(req)
    if cached is not None:
        return cached
    files = scan.resolve(req.path, req.file_format, req.store)
    first = scan.file_schema(files, 0)
    token = resolve_source(req.source, first.names)
    mapper = MAPPERS[token]
    extras = extra_fields(first) if token == "focus" else []
    plan = Plan(req, files, mapper, output_schema(req.spec_version, req.money_scale, extras))
    _PLANS.put(req, plan)
    return plan


def new_cursor() -> ScanCursor:
    """A cursor at the start of the scan."""
    return ScanCursor(scan_id=uuid.uuid4().hex)


def month_bounds(billing_period: str) -> tuple[dt.datetime, dt.datetime]:
    """``YYYY-MM`` -> [first instant, first instant of next month) in UTC."""
    y, m = int(billing_period[:4]), int(billing_period[5:7])
    if not 1 <= m <= 12:
        raise ValueError(f"billing_period must be YYYY-MM, got {billing_period!r}")
    start = dt.datetime(y, m, 1, tzinfo=dt.UTC)
    end = dt.datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=dt.UTC)
    return start, end


def _raw_literal(value: Any, raw_type: pa.DataType) -> pa.Scalar[Any] | None:
    if pa.types.is_timestamp(raw_type):
        if not isinstance(value, dt.datetime):
            return None
        if value.tzinfo is not None:
            value = value.astimezone(dt.UTC)
        if raw_type.tz is None:
            value = value.replace(tzinfo=None)
        elif value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return pa.scalar(value, type=raw_type)
    if pa.types.is_string(raw_type) or pa.types.is_large_string(raw_type):
        return pa.scalar(value, type=raw_type) if isinstance(value, str) else None
    return None


_OPS = {
    ComparisonOp.EQ: lambda f, v: f == v,
    ComparisonOp.GT: lambda f, v: f > v,
    ComparisonOp.GE: lambda f, v: f >= v,
    ComparisonOp.LT: lambda f, v: f < v,
    ComparisonOp.LE: lambda f, v: f <= v,
}


def raw_filter(plan: Plan, file_schema: pa.Schema, filters: Any) -> ds.Expression | None:
    """Translate pushed-down FOCUS predicates into a predicate on raw Parquet columns.

    Only exact, order-preserving copies (``Rule.direct``) of typed Parquet columns
    are translated: timestamp comparisons (row-group pruning by statistics) and
    string equality / IN. The SDK re-applies every pushed predicate to the
    mapped rows, so this only ever narrows what is read, never the answer.
    """
    mapper = plan.mapper
    by_name = {plan.mapper.canonical_name(f.name).lower(): f for f in file_schema}
    parts: list[ds.Expression] = []

    def direct_field(focus_col: str) -> pa.Field[Any] | None:
        rule = mapper.rule_index.get(focus_col)
        if rule is None or rule.direct is None:
            return None
        return by_name.get(rule.direct.lower())

    for f in getattr(filters, "filters", ()) or ():
        raw = direct_field(f.column_name)
        if raw is None:
            continue
        if isinstance(f, ConstantFilter) and f.op in _OPS:
            lit = _raw_literal(f.value.as_py(), raw.type)
            if lit is not None and (f.op == ComparisonOp.EQ or pa.types.is_timestamp(raw.type)):
                parts.append(_OPS[f.op](ds.field(raw.name), lit))
        elif isinstance(f, InFilter) and pa.types.is_string(raw.type):
            values = [v for v in f.values.to_pylist() if isinstance(v, str)]
            if values and len(values) == len(f.values):
                parts.append(ds.field(raw.name).isin(values))
    bp = plan.request.billing_period
    if bp:
        raw = direct_field("BillingPeriodStart")
        if raw is not None and pa.types.is_timestamp(raw.type):
            lo, hi = month_bounds(bp)
            lo_s, hi_s = _raw_literal(lo, raw.type), _raw_literal(hi, raw.type)
            if lo_s is not None and hi_s is not None:
                parts.append((ds.field(raw.name) >= lo_s) & (ds.field(raw.name) < hi_s))
    if not parts:
        return None
    expr = parts[0]
    for p in parts[1:]:
        expr = expr & p
    return expr


def _skip_file(plan: Plan, i: int) -> bool:
    bp = plan.request.billing_period
    if not bp:
        return False
    parts = scan.hive_partitions(plan.files.uris[i])
    value = parts.get("billing_period")
    return value is not None and value[:7] != bp


def next_batch(
    plan: Plan,
    cursor: ScanCursor,
    wanted: list[str],
    filters: Any = None,
) -> pa.RecordBatch | None:
    """Map and return the next non-empty batch of ``wanted`` columns, advancing ``cursor``.

    Returns ``None`` when every file has been read.
    """
    schema = plan.schema
    internal_names = list(dict.fromkeys([*wanted, *(["BillingPeriodStart"] if plan.request.billing_period else [])]))
    internal = pa.schema([schema.field(n) for n in internal_names])
    out_schema = pa.schema([schema.field(n) for n in wanted])
    files = plan.files
    while cursor.file_idx < len(files.files):
        i = cursor.file_idx
        if _skip_file(plan, i):
            cursor.file_idx, cursor.unit_pos, cursor.batch_idx = i + 1, 0, 0
            continue
        frag = scan.fragment(files, i)
        fschema = frag.physical_schema
        expr = raw_filter(plan, fschema, filters)
        units_key = (files.uris[i], str(expr))
        units = _UNITS.get(units_key)
        if units is None:
            units = scan.row_groups(frag, expr)
            _UNITS.put(units_key, units)
        if cursor.unit_pos >= len(units):
            cursor.file_idx, cursor.unit_pos, cursor.batch_idx = i + 1, 0, 0
            continue
        position = (i, cursor.unit_pos, cursor.batch_idx)
        raw_names = plan.mapper.raw_columns_for(internal_names, fschema.names)
        it = scan.ITERATORS.take(cursor.scan_id, position)
        if it is None:
            it = scan.iter_unit(frag, units[cursor.unit_pos], raw_names, expr)
            for _ in range(cursor.batch_idx):
                if next(it, None) is None:
                    break
        raw = next(it, None)
        if raw is None:
            cursor.unit_pos, cursor.batch_idx = cursor.unit_pos + 1, 0
            continue
        cursor.batch_idx += 1
        scan.ITERATORS.put(cursor.scan_id, (i, cursor.unit_pos, cursor.batch_idx), it)
        if raw.num_rows == 0:
            continue
        opts = MapOptions(plan.request.money_scale, plan.mapper.token, files.uris[i])
        mapped = map_batch(plan.mapper, raw, internal, opts)
        if plan.request.billing_period:
            lo, hi = month_bounds(plan.request.billing_period)
            col = mapped.column("BillingPeriodStart")
            keep = pc.and_(pc.greater_equal(col, pa.scalar(lo, col.type)), pc.less(col, pa.scalar(hi, col.type)))
            mapped = mapped.filter(pc.fill_null(keep, False))
        if mapped.num_rows == 0:
            continue
        return mapped.select(out_schema.names) if internal_names != list(wanted) else mapped
    scan.ITERATORS.drop(cursor.scan_id)
    return None
