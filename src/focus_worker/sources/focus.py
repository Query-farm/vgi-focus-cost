"""Native FOCUS files (any provider, FOCUS 1.0 to 1.4) -> typed FOCUS rows.

Provider-generated FOCUS exports (AWS Data Exports "FOCUS 1.0/1.2", Azure's
"FOCUS cost" export, the GCP FOCUS BigQuery view, OCI FOCUS reports, the FinOps
Foundation sample data) already use FOCUS column names. This mapper matches them
case-insensitively, converts each to the worker's FOCUS type (exact decimals,
UTC timestamps, Tags as a MAP) and bridges columns renamed between versions.
Columns with the ``x_`` prefix are carried through unchanged.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ..convert import to_string
from ..spec import EXTENSION_NAMES, MAP_COLUMNS, MONEY_COLUMNS, UNIT_PRICE_COLUMNS, all_columns
from .base import Rule, Src
from .common import MapOptions, Mapper

_TYPES = {c.column_name: c.data_type for c in all_columns()}


_BRIDGE = {
    "ServiceProviderName": ("PublisherName",),
    "HostProviderName": ("ProviderName",),
    "PublisherName": ("ServiceProviderName",),
    "ProviderName": ("HostProviderName",),
}


def _rules() -> tuple[Rule, ...]:
    out = []
    for name in sorted(_TYPES):
        dt = _TYPES[name]
        if name in MONEY_COLUMNS:
            how = "same column, exact DECIMAL(19, money_scale)"
        elif name in UNIT_PRICE_COLUMNS:
            how = "same column, DECIMAL(19,10)"
        elif name in MAP_COLUMNS:
            how = "same column, JSON object text -> MAP(VARCHAR, VARCHAR)"
        elif dt == "Decimal":
            how = "same column, DECIMAL(28,10)"
        elif dt == "Date/Time":
            how = "same column, TIMESTAMPTZ in UTC"
        else:
            how = "same column, VARCHAR"
        notes = ""
        if name == "ServiceProviderName":
            notes = "From PublisherName when the file is FOCUS 1.2 or earlier."
        elif name == "HostProviderName":
            notes = "From ProviderName when the file is FOCUS 1.2 or earlier."
        elif name == "PublisherName":
            notes = "From ServiceProviderName when the file is FOCUS 1.3 or later."
        elif name == "ProviderName":
            notes = "From HostProviderName when the file is FOCUS 1.3 or later."
        sources = (name, *_BRIDGE.get(name, ()))
        out.append(Rule(name, sources, how, notes, direct=name))
    out.append(Rule("x_*", ("x_*",), "every x_ extension column in the file, carried through with its own type"))
    return tuple(out)


RULES = _rules()


def map_focus(src: Src, opts: MapOptions) -> dict[str, pa.Array]:
    """Convert each FOCUS column present in the batch to its worker type."""
    s = opts.money_scale
    cols: dict[str, Any] = {}
    for name, dt in _TYPES.items():
        if not src.has(name):
            continue
        if name in MONEY_COLUMNS:
            cols[name] = src.money(s, name)
        elif name in UNIT_PRICE_COLUMNS:
            cols[name] = src.price(name)
        elif name in MAP_COLUMNS:
            cols[name] = src.tags(name)
        elif dt == "Decimal":
            cols[name] = src.quantity(name)
        elif dt == "Date/Time":
            cols[name] = src.timestamp(name)
        else:
            cols[name] = src.string(name)
    lower = {n.lower(): n for n in src.batch.schema.names}
    for ext in EXTENSION_NAMES:
        if ext.lower() in lower:
            cols[ext] = to_string(src.get(lower[ext.lower()]), src.n)
    for name in src.batch.schema.names:
        if name.lower().startswith("x_") and name not in cols and name.lower() not in {e.lower() for e in EXTENSION_NAMES}:
            cols[name] = _plain(src.get(name))
    return cols


def _plain(arr: pa.Array | None) -> pa.Array | None:
    """Nested / binary extension values as JSON text; others unchanged."""
    if arr is None:
        return None
    t = arr.type.value_type if pa.types.is_dictionary(arr.type) else arr.type
    if pa.types.is_nested(t) or pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return pa.array([None if v is None else json.dumps(v, default=str) for v in arr.to_pylist()], pa.string())
    return arr


def extra_fields(schema: pa.Schema) -> list[pa.Field[Any]]:
    """The file's own ``x_`` columns, appended to the output schema at bind time.

    Nested or binary types are emitted as text; everything else keeps its type.
    """
    taken = {e.lower() for e in EXTENSION_NAMES}
    out: list[pa.Field[Any]] = []
    seen: set[str] = set()
    for f in schema:
        low = f.name.lower()
        if not low.startswith("x_") or low in taken or low in seen:
            continue
        seen.add(low)
        t = f.type
        if pa.types.is_dictionary(t):
            t = t.value_type
        if pa.types.is_nested(t) or pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_null(t):
            t = pa.string()
        if pa.types.is_large_string(t) or pa.types.is_string_view(t):
            t = pa.string()
        out.append(pa.field(f.name, t, nullable=True,
                            metadata={b"comment": b"Provider extension column carried through from the source file."}))
    return out


MAPPER = Mapper(
    token="focus",
    provider="any",
    title="Native FOCUS 1.0-1.4 files from any provider (AWS, Azure, GCP, OCI, SaaS)",
    rules=RULES,
    signature=("billedcost", "chargeperiodstart"),
    map_fn=map_focus,
)
