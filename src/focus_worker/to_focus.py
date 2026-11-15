"""``to_focus(TABLE (...), source := 'auto')``: map rows DuckDB already has to FOCUS.

For exports this worker does not read directly (Azure Blob / ADLS, BigQuery
through the ``bigquery`` extension, a database table of CUR rows), read them with
DuckDB and pass the relation in. The mapping is the same one ``focus()`` uses,
streamed batch by batch.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi import Arg
from vgi.arguments import TableInput
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import OutputCollector, TableInOutGenerator

from .focus_fn import SCALE_DOC, SPEC_DOC
from .meta import focus_result_columns, object_tags
from .sources import MAPPERS, SOURCE_CHOICES, MapOptions, map_batch, output_schema, resolve_source
from .sources.focus import extra_fields
from .spec import DEFAULT_MONEY_SCALE, DEFAULT_SPEC_VERSION, SPEC_VERSIONS

_SOURCE_DOC = (
    "Format of the input rows: 'auto' (detect from the column names), 'aws_cur2', 'azure', 'azure_amortized', "
    "'gcp' or 'focus'."
)


@dataclass(slots=True, frozen=True, kw_only=True)
class ToFocusArgs:
    """Arguments for ``to_focus(TABLE (...), ...)``."""

    data: Annotated[TableInput, Arg(0, doc="Raw billing rows: TABLE (SELECT * FROM read_parquet(...)).")]
    source: Annotated[str, Arg("source", default="auto", doc=_SOURCE_DOC, choices=list(SOURCE_CHOICES))]
    spec_version: Annotated[
        str, Arg("spec_version", default=DEFAULT_SPEC_VERSION, doc=SPEC_DOC, choices=list(SPEC_VERSIONS))
    ]
    money_scale: Annotated[int, Arg("money_scale", default=DEFAULT_MONEY_SCALE, doc=SCALE_DOC, ge=2, le=10)]


def _schema_and_token(params: BindParams[ToFocusArgs] | ProcessParams[ToFocusArgs], input_schema: pa.Schema) -> tuple[str, pa.Schema]:
    token = resolve_source(params.args.source, input_schema.names)
    extras = extra_fields(input_schema) if token == "focus" else []
    return token, output_schema(params.args.spec_version, params.args.money_scale, extras)


class ToFocus(TableInOutGenerator[ToFocusArgs]):
    """Map each input batch to FOCUS."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "to_focus"
        description = "Map raw AWS, Azure, GCP or FOCUS billing rows from any DuckDB query to typed FOCUS rows"
        tags = {
            **object_tags(
                title="Map Rows to FOCUS",
                category="transform",
                keywords="focus, normalize, cloud cost, billing rows, azure blob, bigquery, transform",
                doc_llm=(
                    "Table in-out function: pass raw billing rows that DuckDB has already read, as "
                    "TABLE (SELECT * FROM read_parquet('az://...')) or TABLE (SELECT * FROM my_cur_table), and get "
                    "FOCUS rows back with the same mapping, types and x_ columns as focus(path). `source := 'auto'` "
                    "detects the format from the column names; force 'azure_amortized' for Azure amortized exports. "
                    "Use it for storage this worker cannot read directly (Azure Blob, BigQuery via the bigquery "
                    "extension, any database). The relation must keep the export's original column names."
                ),
                doc_md=(
                    "## to_focus(TABLE (...), source := 'auto', spec_version := '1.2', money_scale := 6)\n\n"
                    "```sql\nSELECT ServiceName, sum(EffectiveCost)\nFROM focus.main.to_focus(TABLE (\n"
                    "  SELECT * FROM read_csv('az://billing/export/*.csv', all_varchar = true)))\nGROUP BY ALL;\n```"
                ),
            ),
            "vgi.result_columns_schema": json.dumps(focus_result_columns()),
        }
        examples = [
            FunctionExample(
                sql="SELECT ServiceCategory, sum(EffectiveCost) AS cost FROM focus.main.to_focus(TABLE ("
                "SELECT * FROM read_parquet('az://costexports/contoso/*.parquet')), source := 'azure_amortized') "
                "GROUP BY ServiceCategory ORDER BY cost DESC",
                description="Amortized Azure spend by category from an export DuckDB reads over the azure extension",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ToFocusArgs]) -> BindResponse:
        """Detect the source from the input columns and declare the FOCUS schema."""
        schema = params.bind_call.input_schema
        if schema is None:
            raise ValueError("to_focus needs a relation: to_focus(TABLE (SELECT ...))")
        _, out = _schema_and_token(params, schema)
        return BindResponse(output_schema=out)

    @classmethod
    def process(
        cls, params: ProcessParams[ToFocusArgs], state: None, batch: pa.RecordBatch, out: OutputCollector
    ) -> None:
        """Map one input batch."""
        token, full = _schema_and_token(params, batch.schema)
        mapper = MAPPERS[token]
        schema = pa.schema([full.field(n) for n in params.output_schema.names])
        mapped = map_batch(mapper, batch, schema, MapOptions(params.args.money_scale, token, None))
        out.emit(pa.RecordBatch.from_arrays(list(mapped.columns), schema=params.output_schema))
