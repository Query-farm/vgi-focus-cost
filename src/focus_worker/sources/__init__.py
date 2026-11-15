"""Export-format mappers and the batch mapping pipeline."""

from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa

from . import aws, azure, focus, gcp
from .base import Rule, Src
from .common import MapOptions, Mapper, apply_generic_rules, assemble, output_schema

MAPPERS: dict[str, Mapper] = {m.token: m for m in (aws.MAPPER, azure.ACTUAL, azure.AMORTIZED, gcp.MAPPER, focus.MAPPER)}
SOURCE_TOKENS = tuple(MAPPERS)
SOURCE_CHOICES = ("auto", *SOURCE_TOKENS)


def detect(names: Iterable[str]) -> str | None:
    """Identify the export format from its column names (case-insensitive).

    FOCUS columns win (provider FOCUS exports also carry provider x_ columns);
    then AWS CUR (2.0 or legacy), GCP billing export, Azure cost details.
    """
    low = {n.lower() for n in names}
    low |= {aws.MAPPER.canonical_name(n).lower() for n in names if "/" in n}
    roots = {n.partition(".")[0] for n in low}
    for token in ("focus", "aws_cur2", "gcp", "azure"):
        if all(sig in low or sig in roots for sig in MAPPERS[token].signature):
            return token
    if "costinbillingcurrency" in low or ("pretaxcost" in low and "metercategory" in low):
        return "azure"
    return None


def resolve_source(source: str, names: Iterable[str]) -> str:
    """Return the concrete source token: ``source`` itself, or the detected one for 'auto'."""
    if source != "auto":
        if source not in MAPPERS:
            raise ValueError(f"unknown source {source!r}; expected one of {', '.join(SOURCE_CHOICES)}")
        return source
    names = list(names)
    found = detect(names)
    if found is None:
        preview = ", ".join(names[:12]) + (" ..." if len(names) > 12 else "")
        raise ValueError(
            "could not recognise the export format from its columns "
            f"({preview}); pass source := 'aws_cur2' | 'azure' | 'azure_amortized' | 'gcp' | 'focus'"
        )
    return found


def map_batch(
    mapper: Mapper,
    raw: pa.RecordBatch,
    schema: pa.Schema,
    opts: MapOptions,
) -> pa.RecordBatch:
    """Map one raw export batch to a FOCUS batch with exactly ``schema``."""
    names = [mapper.canonical_name(n) for n in raw.schema.names]
    if names != raw.schema.names:
        raw = pa.RecordBatch.from_arrays(list(raw.columns), names=names)
    src = Src(raw)
    cols = mapper.map_fn(src, opts)
    n = raw.num_rows
    if mapper.expand_fn is not None and n:
        extra = mapper.expand_fn(src, cols, opts)
        if extra is not None:
            m = len(next(iter(extra.values())))
            for k, v in cols.items():
                add = extra.get(k)
                cols[k] = pa.concat_arrays([v, add if add is not None else pa.nulls(m, v.type)])
            n += m
    if mapper.token != "focus":
        apply_generic_rules(cols, n)
    if "x_SourceFormat" not in cols or mapper.token != "focus":
        cols["x_SourceFormat"] = pa.array([mapper.token] * n, pa.string())
    if opts.source_file is not None and "x_SourceFile" not in cols:
        cols["x_SourceFile"] = pa.array([opts.source_file] * n, pa.string())
    return assemble(cols, schema, n)


__all__ = [
    "MAPPERS",
    "SOURCE_CHOICES",
    "SOURCE_TOKENS",
    "MapOptions",
    "Mapper",
    "Rule",
    "detect",
    "map_batch",
    "output_schema",
    "resolve_source",
]
