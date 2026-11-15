"""Vectorized conversions from raw export columns to FOCUS-typed Arrow arrays.

Every function takes whatever Arrow type an export happens to use (CSV strings,
Parquet doubles, decimals, timestamps with or without a zone, nested structs) and
returns one well-defined type. Money is exact: doubles are read through their
shortest round-trip decimal text (what the provider wrote), strings are parsed as
decimals, and the result is rounded half-to-even to the target scale. No value
ever passes through binary floating point arithmetic.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

UTC_TS = pa.timestamp("us", tz="UTC")
TAGS_TYPE = pa.map_(pa.string(), pa.string())
_WIDE = pa.decimal128(38, 18)


class ConversionError(ValueError):
    """A source value could not be converted to its FOCUS type."""


def nulls(n: int, type_: pa.DataType) -> pa.Array:
    """An all-NULL array of ``n`` values."""
    return pa.nulls(n, type=type_)


def _blank_to_null(arr: pa.Array) -> pa.Array:
    """Trim strings and turn empty strings into NULL."""
    trimmed = pc.utf8_trim_whitespace(arr)
    return pc.if_else(pc.equal(trimmed, ""), pa.scalar(None, pa.string()), trimmed)


def to_string(arr: pa.Array | pa.ChunkedArray | None, n: int) -> pa.Array:
    """Any column as VARCHAR; blank strings become NULL."""
    if arr is None:
        return nulls(n, pa.string())
    arr = _combine(arr)
    if pa.types.is_dictionary(arr.type):
        arr = arr.cast(arr.type.value_type)
    if pa.types.is_null(arr.type):
        return nulls(n, pa.string())
    if pa.types.is_large_string(arr.type) or pa.types.is_string_view(arr.type):
        arr = arr.cast(pa.string())
    if not pa.types.is_string(arr.type):
        arr = pc.cast(arr, pa.string())
    return _blank_to_null(arr)


def _combine(arr: pa.Array | pa.ChunkedArray) -> pa.Array:
    if isinstance(arr, pa.ChunkedArray):
        return arr.combine_chunks() if arr.num_chunks != 1 else arr.chunk(0)
    return arr


def to_decimal(arr: pa.Array | pa.ChunkedArray | None, n: int, precision: int, scale: int, *, what: str) -> pa.Array:
    """Exact conversion of a numeric or textual column to ``DECIMAL(precision, scale)``.

    Doubles are converted through their shortest round-trip text, so ``0.1`` stays
    ``0.1``; values are then rounded half-to-even to ``scale`` digits.
    """
    target = pa.decimal128(precision, scale)
    if arr is None:
        return nulls(n, target)
    arr = _combine(arr)
    if pa.types.is_dictionary(arr.type):
        arr = arr.cast(arr.type.value_type)
    t = arr.type
    if pa.types.is_null(t):
        return nulls(n, target)
    try:
        if pa.types.is_integer(t):
            wide = pc.cast(arr, _WIDE)
        elif pa.types.is_decimal(t):
            wide = pc.cast(arr, pa.decimal128(38, min(t.scale, 18)), safe=False) if t.scale > 18 else arr
        else:
            text = _blank_to_null(pc.cast(arr, pa.string())) if pa.types.is_floating(t) else to_string(arr, n)
            wide = pc.cast(text, _WIDE, safe=False)
        rounded = pc.round(wide, ndigits=scale, round_mode="half_to_even")
        return pc.cast(rounded, target)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as err:
        sample = _first_bad_decimal(arr)
        raise ConversionError(f"{what}: cannot convert {sample!r} to DECIMAL({precision},{scale}): {err}") from err


def _first_bad_decimal(arr: pa.Array) -> Any:
    from decimal import Decimal, InvalidOperation

    for v in arr.to_pylist():
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        try:
            d = Decimal(str(v).strip())
            if abs(d) >= Decimal(10) ** 20 or not d.is_finite():
                return v
        except InvalidOperation:
            return v
    return None


_TS_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%Y%m%d",
)


def to_timestamp(arr: pa.Array | pa.ChunkedArray | None, n: int, *, what: str) -> pa.Array:
    """Convert dates, timestamps (any zone, any unit) or text to ``TIMESTAMPTZ`` in UTC.

    Text with an explicit offset is honoured; text without one is taken as UTC.
    Accepted text shapes: ISO 8601 (``T`` or space separated, optional ``Z`` or
    offset, optional fraction), ``YYYY-MM-DD``, ``MM/DD/YYYY`` and ``YYYYMMDD``.
    """
    if arr is None:
        return nulls(n, UTC_TS)
    arr = _combine(arr)
    if pa.types.is_dictionary(arr.type):
        arr = arr.cast(arr.type.value_type)
    t = arr.type
    if pa.types.is_null(t):
        return nulls(n, UTC_TS)
    if pa.types.is_timestamp(t):
        if t.tz is None:
            return pc.assume_timezone(pc.cast(arr, pa.timestamp("us")), "UTC")
        return pc.cast(arr, UTC_TS)
    if pa.types.is_date(t):
        return pc.assume_timezone(pc.cast(pc.cast(arr, pa.date32()), pa.timestamp("us")), "UTC")
    text = pc.replace_substring_regex(to_string(arr, n), r"\s*(UTC|GMT)$", "")  # BigQuery JSON: '... UTC'
    for attempt in (_ts_iso_tz, _ts_iso_naive, *(_strptime(f) for f in _TS_FORMATS)):
        try:
            return attempt(text)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError):
            continue
    sample = next((v for v in text.to_pylist() if v is not None), None)
    raise ConversionError(f"{what}: cannot parse {sample!r} as a timestamp")


def _ts_iso_tz(text: pa.Array) -> pa.Array:
    return pc.cast(text, UTC_TS)


def _ts_iso_naive(text: pa.Array) -> pa.Array:
    return pc.assume_timezone(pc.cast(text, pa.timestamp("us")), "UTC")


def _strptime(fmt: str) -> Callable[[pa.Array], pa.Array]:
    def parse(text: pa.Array) -> pa.Array:
        parsed = pc.strptime(text, format=fmt, unit="us", error_is_null=False)
        return pc.assume_timezone(parsed, "UTC")

    return parse


def map_values(
    arr: pa.Array, fn: Callable[[Any], Any], out_type: pa.DataType, *, skip_null: bool = True
) -> pa.Array:
    """Apply a Python function to each *distinct* value, then expand by index.

    Export columns such as charge types, service codes and tag strings have few
    distinct values, so this costs one call per distinct value, not per row.
    """
    arr = _combine(arr)
    if len(arr) == 0:
        return pa.array([], type=out_type)
    if pa.types.is_null(arr.type):
        return pa.array([None if skip_null else fn(None)] * len(arr), type=out_type)
    enc = arr if pa.types.is_dictionary(arr.type) else pc.dictionary_encode(arr)
    enc = cast("pa.DictionaryArray", enc)
    mapped = [fn(v) for v in enc.dictionary.to_pylist()]
    values = pa.array(mapped, type=out_type)
    taken = values.take(enc.indices)
    if not skip_null and enc.indices.null_count:
        fill = fn(None)
        if fill is not None:
            taken = pc.fill_null(taken, pa.scalar(fill, out_type))
    return taken


def lookup(arr: pa.Array, table: Mapping[str, Any], out_type: pa.DataType, default: Any = None) -> pa.Array:
    """Map distinct string values through ``table``; unmatched values get ``default``."""
    return map_values(to_string(arr, len(arr)), lambda v: table.get(v, default) if v is not None else default,
                      out_type, skip_null=False)


def _tags_from_text(value: str | None) -> list[tuple[str, str]] | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if not s.startswith("{"):
        s = "{" + s + "}"  # Azure legacy exports omit the braces: "k": "v","k2": "v2"
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    return [(str(k), v if isinstance(v, str) else json.dumps(v)) for k, v in obj.items()]


def to_tags(arr: pa.Array | pa.ChunkedArray | None, n: int, *, key_transform: Callable[[str], str] | None = None) -> pa.Array:
    """Convert a tag column to ``MAP(VARCHAR, VARCHAR)``.

    Accepts a JSON object string (FOCUS, Azure, with or without braces), an Arrow
    map, a list of ``{key, value}`` structs (GCP labels) or a struct.
    """
    if arr is None:
        return nulls(n, TAGS_TYPE)
    arr = _combine(arr)
    t = arr.type
    if pa.types.is_null(t):
        return nulls(n, TAGS_TYPE)
    rows: list[list[tuple[str, str]] | None]
    if pa.types.is_map(t):
        rows = [None if v is None else [(str(k), _tag_value(x)) for k, x in v] for v in arr.to_pylist()]
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        rows = []
        for v in arr.to_pylist():
            if v is None:
                rows.append(None)
                continue
            rows.append([(str(e.get("key")), _tag_value(e.get("value"))) for e in v if e and e.get("key") is not None])
    elif pa.types.is_struct(t):
        rows = [None if v is None else [(k, _tag_value(x)) for k, x in v.items() if x is not None] for v in arr.to_pylist()]
    else:
        text = to_string(arr, n)
        enc = cast("pa.DictionaryArray", pc.dictionary_encode(text))
        parsed = [_tags_from_text(v) for v in enc.dictionary.to_pylist()]
        idx = enc.indices.to_pylist()
        rows = [None if i is None else parsed[i] for i in idx]
    if key_transform is not None:
        rows = [None if r is None else [(key_transform(k), v) for k, v in r] for r in rows]
    return pa.array(rows, type=TAGS_TYPE)


def _tag_value(v: Any) -> str:
    if v is None:
        return ""
    return v if isinstance(v, str) else json.dumps(v)


def coalesce(*arrays: pa.Array) -> pa.Array:
    """First non-NULL value across same-typed arrays."""
    present = [a for a in arrays if a is not None]
    if len(present) == 1:
        return present[0]
    return pc.coalesce(*present)


def const(value: Any, n: int, type_: pa.DataType = pa.string()) -> pa.Array:
    """A constant column."""
    return pa.array([value] * n, type=type_) if value is not None else nulls(n, type_)
