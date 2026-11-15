"""Shared machinery for the per-provider mappers.

A mapper turns one Arrow batch of a provider's raw export into the canonical FOCUS
columns. Each mapper also publishes its rules as data (``Rule``): the FOCUS column,
the raw columns it reads and the transform in words. That list feeds the
``provider_map`` table, decides which raw columns are read for a projection, and
decides which filters can be pushed into the file scan, so the documentation and
the behaviour cannot drift apart.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from ..convert import const, nulls, to_decimal, to_string, to_tags, to_timestamp
from ..spec import QUANTITY_TYPE, UNIT_PRICE_TYPE


@dataclass(frozen=True, slots=True)
class Rule:
    """How one FOCUS column is derived from one provider's export."""

    focus_column: str
    sources: tuple[str, ...]
    transform: str
    notes: str = ""
    # Raw column copied verbatim (so a comparison filter on the FOCUS column can be
    # pushed into the file scan unchanged). Only set for exact, order-preserving copies.
    direct: str | None = None


@dataclass
class Src:
    """Case-insensitive, nesting-aware access to the raw columns of one batch.

    ``get("project.id")`` finds a top-level column literally named ``project.id``,
    else the ``id`` field of a struct column ``project``, else a flattened
    ``project_id`` column. Missing columns return ``None`` so mappers degrade to
    NULL for optional export columns. Under projection only the raw columns named
    by the requested columns' rules are read, so a column computed from columns
    that were not read comes out wrong; that is harmless because it is dropped,
    and ``tests/test_projection.py`` proves every column's rule lists all it reads.
    """

    batch: pa.RecordBatch
    _index: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Index column names case-insensitively."""
        for i, name in enumerate(self.batch.schema.names):
            self._index.setdefault(name.lower(), i)

    @property
    def n(self) -> int:
        """Rows in the batch."""
        return self.batch.num_rows

    def has(self, name: str) -> bool:
        """Whether the raw column (or nested field) exists in this batch."""
        return self._resolve(name) is not None

    def _resolve(self, name: str) -> pa.Array | None:
        key = name.lower()
        if key in self._index:
            return self._col(self._index[key])
        if "." in key:
            head, _, rest = key.partition(".")
            if head in self._index:
                arr = self._col(self._index[head])
                for part in rest.split("."):
                    if not pa.types.is_struct(arr.type):
                        return None
                    names = [arr.type.field(i).name.lower() for i in range(arr.type.num_fields)]
                    if part not in names:
                        return None
                    arr = pc.struct_field(arr, names.index(part))
                return arr
            flat = key.replace(".", "_")
            if flat in self._index:
                return self._col(self._index[flat])
        return None

    def _col(self, i: int) -> pa.Array:
        return self.batch.column(i)

    def get(self, *names: str) -> pa.Array | None:
        """The first of ``names`` that exists, or ``None``."""
        for name in names:
            arr = self._resolve(name)
            if arr is not None:
                return arr
        return None

    # Typed helpers -------------------------------------------------------------
    def string(self, *names: str) -> pa.Array:
        """First existing column as VARCHAR (blank -> NULL), else all NULL."""
        return to_string(self.get(*names), self.n)

    def money(self, scale: int, *names: str) -> pa.Array:
        """First existing column as DECIMAL(19, scale), else all NULL."""
        return to_decimal(self.get(*names), self.n, 19, scale, what="/".join(names))

    def price(self, *names: str) -> pa.Array:
        """First existing column as a unit price, DECIMAL(19,10)."""
        return to_decimal(self.get(*names), self.n, UNIT_PRICE_TYPE.precision, UNIT_PRICE_TYPE.scale, what="/".join(names))

    def quantity(self, *names: str) -> pa.Array:
        """First existing column as a quantity, DECIMAL(28,10)."""
        return to_decimal(self.get(*names), self.n, QUANTITY_TYPE.precision, QUANTITY_TYPE.scale, what="/".join(names))

    def timestamp(self, *names: str) -> pa.Array:
        """First existing column as TIMESTAMPTZ (UTC)."""
        return to_timestamp(self.get(*names), self.n, what="/".join(names))

    def tags(self, *names: str, key_transform: Callable[[str], str] | None = None) -> pa.Array:
        """First existing column as MAP(VARCHAR, VARCHAR)."""
        return to_tags(self.get(*names), self.n, key_transform=key_transform)

    def const(self, value: Any, type_: pa.DataType | None = None) -> pa.Array:
        """A constant column."""
        return const(value, self.n, type_ or pa.string())

    def null(self, type_: pa.DataType) -> pa.Array:
        """An all-NULL column."""
        return nulls(self.n, type_)


def when(cond: pa.Array, then: pa.Array, otherwise: pa.Array) -> pa.Array:
    """Vectorized IF with NULL conditions treated as false."""
    return pc.if_else(pc.fill_null(cond, False), then, otherwise)


def isin(arr: pa.Array, values: Iterable[str]) -> pa.Array:
    """Boolean mask: value in the set (NULL -> false)."""
    return pc.fill_null(pc.is_in(arr, value_set=pa.array(list(values), pa.string())), False)


def zero_money(n: int, scale: int) -> pa.Array:
    """A column of exact zeros."""
    from decimal import Decimal

    return pa.repeat(pa.scalar(Decimal(0), pa.decimal128(19, scale)), n)


def money_add(a: pa.Array, b: pa.Array, scale: int) -> pa.Array:
    """Exact a + b as DECIMAL(19, scale)."""
    return pc.cast(pc.add(a, b), pa.decimal128(19, scale))


def money_sub(a: pa.Array, b: pa.Array, scale: int) -> pa.Array:
    """Exact a - b as DECIMAL(19, scale)."""
    return pc.cast(pc.subtract(a, b), pa.decimal128(19, scale))


def money_mul(a: pa.Array, b: pa.Array, scale: int, factor: pa.Array | None = None) -> pa.Array:
    """Exact a * b (* factor), rounded half-to-even to DECIMAL(19, scale).

    Done with Python ``decimal`` because the product of two wide decimals exceeds
    Arrow's 38-digit limit; only rows where every operand is non-NULL are computed.
    """
    from decimal import ROUND_HALF_EVEN, Decimal, localcontext

    q = Decimal(1).scaleb(-scale)
    av, bv = a.to_pylist(), b.to_pylist()
    fv = factor.to_pylist() if factor is not None else [Decimal(1)] * len(av)
    out: list[Decimal | None] = []
    with localcontext() as ctx:
        ctx.prec = 80
        for x, y, f in zip(av, bv, fv, strict=True):
            if x is None or y is None or f is None:
                out.append(None)
            else:
                out.append((x * y * f).quantize(q, rounding=ROUND_HALF_EVEN))
    return pa.array(out, pa.decimal128(19, scale))


_CAMEL = re.compile(r"(?<!^)(?=[A-Z])")


def snake(name: str) -> str:
    """AWS legacy CUR header -> Athena/CUR 2.0 column name.

    ``lineItem/UnblendedCost`` -> ``line_item_unblended_cost``;
    ``reservation/ReservationARN`` -> ``reservation_reservation_a_r_n``.
    """
    return "_".join(_CAMEL.sub("_", part).lower() for part in name.split("/"))
