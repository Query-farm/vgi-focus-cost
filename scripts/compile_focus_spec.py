"""Compile the FOCUS column reference from the published specification.

Downloads the per-column Markdown files of the FinOps Open Cost and Usage
Specification (FOCUS) at each release tag and writes
``src/focus_worker/data/focus_spec.json``: one record per (version, column) with
the column ID, display name, short description, column type, feature level,
nullability, data type, value format, allowed values and the version it was
introduced in.

The FOCUS specification is (c) Joint Development Foundation Projects, LLC,
FinOps Open Cost and Usage Specification (FOCUS) Series and its contributors,
published under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/).
Only column names, types, constraint values and the one-sentence column
descriptions are compiled; see NOTICE for the attribution.

    uv run --no-project python scripts/compile_focus_spec.py            # all versions
    uv run --no-project python scripts/compile_focus_spec.py --check    # exit 1 if stale

Uses only the standard library. Network access to github.com is required.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path

REPO = "FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec"
# tag -> directory holding the Cost and Usage dataset's column files.
VERSIONS = {
    "1.0": ("v1.0", "specification/columns"),
    "1.1": ("v1.1", "specification/columns"),
    "1.2": ("v1.2", "specification/columns"),
    "1.3": ("v1.3", "specification/datasets/cost_and_usage/columns"),
    "1.4": ("v1.4", "specification/datasets/cost_and_usage/columns"),
}
OUT = Path(__file__).resolve().parent.parent / "src" / "focus_worker" / "data" / "focus_spec.json"


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "focus-spec-compiler"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data: bytes = resp.read()
        return data


def _list_files(tag: str, directory: str) -> list[str]:
    tree = json.loads(_get(f"https://api.github.com/repos/{REPO}/git/trees/{tag}?recursive=1"))
    return sorted(
        e["path"]
        for e in tree["tree"]
        if e["path"].startswith(directory + "/") and e["path"].endswith(".md") and e["path"].count("/") == directory.count("/") + 1
    )


def _section(md: str, title: str) -> str:
    m = re.search(rf"^##\s+{re.escape(title)}\s*$(.*?)(?=^##\s|\Z)", md, re.M | re.S | re.I)
    return m.group(1).strip() if m else ""


def _clean(text: str) -> str:
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)  # links -> text
    text = text.replace("*", "").replace("\\<", "<")
    return re.sub(r"\s+", " ", text).strip()


def _tables(md: str) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    cur: list[list[str]] = []
    for line in md.splitlines():
        s = line.strip()
        if s.startswith("|"):
            cells = [c.strip() for c in s.strip("|").split("|")]
            if all(re.fullmatch(r":?-{2,}:?", c) for c in cells if c):
                continue
            cur.append(cells)
        elif cur:
            tables.append(cur)
            cur = []
    if cur:
        tables.append(cur)
    return tables


def _parse(md: str, version: str) -> dict[str, object]:
    col_id = _clean(_section(md, "Column ID"))
    constraints: dict[str, str] = {}
    allowed: list[str] = []
    parents: dict[str, str] = {}
    for table in _tables(md):
        header = [h.lower() for h in table[0]]
        if header[:2] == ["constraint", "value"]:
            for row in table[1:]:
                if len(row) >= 2:
                    constraints[_clean(row[0]).lower()] = _clean(row[1])
        elif "service subcategory" in header:
            i, p = header.index("service subcategory"), header.index("service category")
            for row in table[1:]:
                allowed.append(_clean(row[i]))
                parents[_clean(row[i])] = _clean(row[p])
        elif len(header) >= 2 and (header[0] == "value" or header[1] == "description") and col_id:
            allowed.extend(_clean(row[0]) for row in table[1:] if row and _clean(row[0]))
    fmt = constraints.get("value format", "")
    fmt = re.sub(r"\s*recommended$", "", fmt).strip()
    nulls = constraints.get("allows nulls", "").lower()
    return {
        "spec_version": version,
        "column_name": col_id,
        "display_name": _clean(_section(md, "Display Name")),
        "description": _clean(_section(md, "Description")),
        "column_type": constraints.get("column type", "").replace(" / ", " and "),
        "feature_level": constraints.get("feature level", ""),
        "allows_nulls": nulls == "true",
        "data_type": constraints.get("data type", ""),
        "value_format": "<not specified>" if "not specified" in fmt else fmt,
        "allowed_values": allowed or None,
        "subcategory_parent": parents or None,
        "introduced": _clean(_section(md, "Introduced (version)")),
    }


def compile_spec() -> list[dict[str, object]]:
    """Download and parse every column file of every configured version."""
    records: list[dict[str, object]] = []
    for version, (tag, directory) in VERSIONS.items():
        for path in _list_files(tag, directory):
            md = _get(f"https://raw.githubusercontent.com/{REPO}/{tag}/{path}").decode()
            rec = _parse(md, version)
            if rec["column_name"]:
                records.append(rec)
        print(f"FOCUS {version}: {sum(1 for r in records if r['spec_version'] == version)} columns", file=sys.stderr)
    return records


def main() -> int:
    """Write (or with --check, verify) the compiled column reference."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if the committed file differs")
    args = ap.parse_args()
    text = json.dumps(compile_spec(), indent=1, sort_keys=True) + "\n"
    if args.check:
        same = OUT.exists() and OUT.read_text() == text
        print("up to date" if same else "STALE: rerun scripts/compile_focus_spec.py", file=sys.stderr)
        return 0 if same else 1
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text)
    print(f"wrote {OUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
