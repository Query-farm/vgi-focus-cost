"""Shared fixtures: a worker client and the fixture file paths."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from vgi.client import Client

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures"
SAMPLES = ROOT / "src" / "focus_worker" / "samples"

# (path, source) for every fixture export, as focus() is called on it.
FIXTURE_SOURCES = [
    (str(SAMPLES / "aws_cur2"), "aws_cur2"),
    (str(SAMPLES / "aws_cur_legacy"), "auto"),
    (str(SAMPLES / "azure" / "contoso-actual-cost-202607.csv"), "azure"),
    (str(SAMPLES / "azure" / "contoso-amortized-cost-202607.parquet"), "azure_amortized"),
    (str(SAMPLES / "gcp" / "gcp_billing_export_v1_01A2B3_202607.parquet"), "gcp"),
    (str(SAMPLES / "gcp" / "gcp_billing_export_v1_01A2B3_202607.jsonl"), "gcp"),
    (str(SAMPLES / "focus12"), "focus"),
    (str(FIXTURES / "focus_sample"), "focus"),
]


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "focus_worker.worker"]) as c:
        yield c
