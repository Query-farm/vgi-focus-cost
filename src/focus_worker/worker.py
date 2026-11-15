"""Assemble the focus catalog and run the worker.

vgi-focus-worker                      # stdio: what DuckDB launches by default
vgi-focus-worker --http --port 8000   # HTTP: for Docker / hosted deploys

ATTACH 'focus' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-focus-worker');
"""

from __future__ import annotations

import json
from typing import Any

from vgi import Worker
from vgi.catalog import Catalog, Schema

from .focus_fn import Focus
from .meta import (
    CATALOG_DESCRIPTION,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .scalars import SCALARS
from .tables import TABLE_FUNCTIONS, TABLES
from .to_focus import ToFocus
from .validate import FocusValidate

FUNCTIONS: list[type[Any]] = [Focus, ToFocus, FocusValidate, *SCALARS]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries`` (overloads merged per name)."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class FocusWorker(Worker):
    """The worker process: one catalog, one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DESCRIPTION,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="FOCUS readers, mapping, validation, reference tables and exact-arithmetic scalars.",
                functions=FUNCTIONS,
                tables=TABLES,
                tags={
                    "vgi.title": "FOCUS Functions and Reference Tables",
                    "domain": "finops",
                    "vgi.keywords": keywords_array(
                        "focus, finops, cloud billing, cost and usage, aws cur, azure cost export, gcp billing export"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_ALL_EXAMPLES),
                    "vgi.source_url": SOURCE_URL,
                },
            )
        ],
    )


_ = TABLE_FUNCTIONS  # backing scans are reached through TABLES


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    FocusWorker.main()


if __name__ == "__main__":
    main()
