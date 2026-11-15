"""Mapper registry, source detection and the version-aware output assembly."""

from __future__ import annotations

import fnmatch
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from ..convert import TAGS_TYPE, UTC_TS, Arr, pc
from ..convert import nulls as pa_nulls
from ..spec import EXTENSION_COLUMNS, EXTENSION_NAMES, arrow_type, column_comment, columns_for
from .base import Rule, Src, snake


@dataclass(frozen=True, slots=True)
class MapOptions:
    """Per-call mapping options."""

    money_scale: int = 6
    source_format: str = ""
    source_file: str | None = None


@dataclass(frozen=True)
class Mapper:
    """One export format's mapping to FOCUS."""

    token: str
    provider: str
    title: str
    rules: tuple[Rule, ...]
    signature: tuple[str, ...]  # raw columns (lower case) whose presence identifies the format
    map_fn: Callable[[Src, MapOptions], dict[str, Arr]]
    expand_fn: Callable[[Src, dict[str, Arr], MapOptions], dict[str, Arr] | None] | None = None
    rename_fn: Callable[[str], str] | None = None
    # Raw columns that decide which rows exist (e.g. AWS synthetic unused-commitment rows).
    row_sources: tuple[str, ...] = ()
    rule_index: dict[str, Rule] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        """Index rules by FOCUS column."""
        self.rule_index.update({r.focus_column: r for r in self.rules})

    def raw_columns_for(self, focus_columns: Iterable[str], available: Iterable[str]) -> list[str]:
        """Return the raw columns (as named in the file) needed to produce ``focus_columns``.

        Columns that influence other columns through generic rules (charge type,
        periods) are always included. Patterns such as ``resource_tags_*`` match
        case-insensitively; nested references (``project.id``) read their root.
        """
        wanted: set[str] = {s.lower() for s in self.row_sources}
        cols = list(focus_columns)
        cols += [b for c in cols for b in BRIDGE.get(c, ())]
        for col in (*cols, *_ALWAYS):
            rule = self.rule_index.get(col)
            if rule is not None:
                wanted.update(s.lower().partition(".")[0] for s in rule.sources)
            elif col.lower().startswith("x_"):
                wanted.add(col.lower())  # a provider extension column carried through by name
        out = []
        for name in available:
            key = self.canonical_name(name).lower()
            flat_match = any(key == w or key.startswith(w + "_") for w in wanted if "*" not in w)
            if key in wanted or flat_match or any("*" in w and fnmatch.fnmatchcase(key, w) for w in wanted):
                out.append(name)
        return out

    def canonical_name(self, name: str) -> str:
        """Raw header -> the name the rules use (legacy CUR ``lineItem/UsageType`` -> snake)."""
        if self.rename_fn is not None:
            return self.rename_fn(name)
        return snake(name) if "/" in name else name


# Columns renamed between FOCUS versions: either can be derived from the other.
BRIDGE = {
    "ServiceProviderName": ("PublisherName",),
    "HostProviderName": ("ProviderName",),
    "PublisherName": ("ServiceProviderName",),
    "ProviderName": ("HostProviderName",),
}

# Rules that other columns depend on (generic post-processing reads them).
_ALWAYS = ("ChargeCategory", "ChargePeriodStart", "BillingPeriodStart", "CommitmentDiscountStatus", "x_ChargeType")


def output_schema(version: str, money_scale: int, extras: list[pa.Field[Any]] | None = None) -> pa.Schema:
    """Return the FOCUS output schema for one spec version, plus the x_ extension columns."""
    fields = [
        pa.field(
            c.column_name, arrow_type(c, money_scale), nullable=True, metadata={b"comment": column_comment(c).encode()}
        )
        for c in columns_for(version)
    ]
    fields += [
        pa.field(name, pa.string(), nullable=True, metadata={b"comment": doc.encode()})
        for name, doc in EXTENSION_COLUMNS
    ]
    fields += extras or []
    return pa.schema(fields)


def _bridge_versions(cols: dict[str, Arr], n: int) -> None:
    """Fill renamed columns in both directions so any version can be emitted.

    FOCUS 1.3 replaced ProviderName/PublisherName with HostProviderName (who hosts
    the resources) and ServiceProviderName (who makes the service available).
    """
    if cols.get("ServiceProviderName") is None and cols.get("PublisherName") is not None:
        cols["ServiceProviderName"] = cols["PublisherName"]
    if cols.get("HostProviderName") is None and cols.get("ProviderName") is not None:
        cols["HostProviderName"] = cols["ProviderName"]
    if cols.get("PublisherName") is None and cols.get("ServiceProviderName") is not None:
        cols["PublisherName"] = cols["ServiceProviderName"]
    if cols.get("ProviderName") is None and cols.get("HostProviderName") is not None:
        cols["ProviderName"] = cols["HostProviderName"]


def apply_generic_rules(cols: dict[str, Arr], n: int) -> None:
    """FOCUS rules every provider mapping shares.

    * ``ChargeClass`` is 'Correction' when the charge period starts before the
      billing period it was billed in (a correction to a closed period).
    * ``ConsumedQuantity``/``ConsumedUnit`` are NULL unless the row is Usage and
      not an unused-commitment row (FOCUS ConsumedQuantity requirements).
    """
    if cols.get("ChargeClass") is None and "ChargePeriodStart" in cols and "BillingPeriodStart" in cols:
        corr = pc.fill_null(pc.less(cols["ChargePeriodStart"], cols["BillingPeriodStart"]), False)
        cols["ChargeClass"] = pc.if_else(corr, pa.scalar("Correction"), pa.scalar(None, pa.string()))
    cat = cols.get("ChargeCategory")
    status = cols.get("CommitmentDiscountStatus")
    if cat is not None:
        keep = pc.fill_null(pc.equal(cat, "Usage"), False)
        if status is not None:
            keep = pc.and_(keep, pc.invert(pc.fill_null(pc.equal(status, "Unused"), False)))
        for name in ("ConsumedQuantity", "ConsumedUnit"):
            if cols.get(name) is not None:
                arr = cols[name]
                cols[name] = pc.if_else(keep, arr, pa.scalar(None, arr.type))


def assemble(cols: dict[str, Arr], schema: pa.Schema, n: int) -> pa.RecordBatch:
    """Cast the canonical columns to ``schema`` (NULL for anything not produced)."""
    _bridge_versions(cols, n)
    if len(schema) == 0:  # e.g. count(*): no columns, but the row count still matters
        return pa.RecordBatch.from_arrays([pa_nulls(n, pa.null())], names=["_"]).select([])
    arrays = []
    for f in schema:
        arr = cols.get(f.name)
        if arr is None:
            arrays.append(pa_nulls(n, f.type))
            continue
        if isinstance(arr, pa.ChunkedArray):
            arr = arr.combine_chunks()
        if not arr.type.equals(f.type):
            arr = _cast(arr, f.type, f.name)
        arrays.append(arr)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _cast(arr: Arr, t: pa.DataType, name: str) -> Arr:
    if pa.types.is_null(arr.type):
        return pa_nulls(len(arr), t)
    if t.equals(UTC_TS) or t.equals(TAGS_TYPE):
        return arr.cast(t)
    try:
        return pc.cast(arr, t)
    except pa.ArrowInvalid as err:
        raise ValueError(f"{name}: cannot cast {arr.type} to {t}: {err}") from err


def extension_names() -> frozenset[str]:
    """Return the x_ columns every mapped row carries."""
    return EXTENSION_NAMES
