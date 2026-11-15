<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Normalize AWS, Azure and Google Cloud Billing Exports to FinOps FOCUS in DuckDB

> **vgi-focus-cost** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-focus-cost/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-focus-cost/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Every cloud bills in its own dialect: AWS writes `line_item_unblended_cost`, Azure writes
`CostInBillingCurrency`, GCP writes a nested `cost` plus a `credits` array. The FinOps Foundation's
[FOCUS](https://focus.finops.org) specification (FinOps Open Cost and Usage Specification) defines one
schema for all of them. This worker reads the billing **export files you already have** and returns
FOCUS rows, so one SQL query covers every cloud:

```sql
SELECT ProviderName, ServiceCategory, sum(EffectiveCost) AS cost
FROM focus.main.focus('/exports/**/*.parquet')        -- AWS, Azure, GCP and FOCUS files, mixed
GROUP BY ALL ORDER BY cost DESC;
```

- **No cloud credentials** for the core: it reads exports from disk, globs, `https://` URLs (HTTP range
  reads) or `s3://` (with a DuckDB `s3` secret).
- **Exact money.** Costs are `DECIMAL(19,6)`, never `DOUBLE`. Provider doubles are read through their
  shortest decimal text and rounded half-to-even.
- **Streaming.** Parquet is read one row group at a time, only the raw columns the selected FOCUS columns
  need are read, and filters on periods and accounts prune row groups. A multi-GB CUR never sits in memory.
- **Auditable.** Every mapping rule is a row in `focus.main.provider_map`; any FOCUS value traces back to
  the raw export columns it came from.
- **Conformance.** `focus_validate(path)` checks a file against FOCUS 1.0–1.4: required columns, nulls,
  allowed values, cross-column rules.

## Quick start

Workers are not published to PyPI; run this one straight from a release tag:

```bash
uvx --from git+https://github.com/Query-farm/vgi-focus-cost@v0.1.0 vgi-focus-worker --help
```

In DuckDB 2.0 (or [Haybarn](https://query.farm/products/haybarn) 2.0):

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
ATTACH 'focus' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-focus-cost@v0.1.0 vgi-focus-worker');
```

(In Haybarn: `INSTALL vgi FROM community; LOAD vgi;`.) For a local checkout, `make build` and use
`LOCATION '/abs/path/to/vgi-focus-cost/.venv/bin/vgi-focus-worker'`.

The worker ships **synthetic sample exports** so everything below runs with no billing account:
`sample://aws_cur2`, `sample://aws_cur_legacy`, `sample://azure`, `sample://gcp`, `sample://focus12`.

```sql
-- July 2026 AWS CUR 2.0 sample: invoice cost vs amortized cost per charge category
SELECT ChargeCategory, sum(BilledCost) AS billed, sum(EffectiveCost) AS effective
FROM focus.main.focus('sample://aws_cur2', billing_period := '2026-07')
GROUP BY ChargeCategory ORDER BY ChargeCategory;
-- Adjustment  -3.000000     -3.000000
-- Credit      -32.770000    -32.770000
-- Purchase    1018.252800   0.000000      <- RI / Savings Plan fees, spread into usage
-- Tax         12.340000     12.340000
-- Usage       361.671976    582.074776

-- Three clouds, one schema
SELECT ProviderName, ServiceCategory, sum(EffectiveCost) AS effective_cost FROM (
  SELECT * FROM focus.main.focus('sample://aws_cur2')
  UNION ALL SELECT * FROM focus.main.focus('sample://azure/contoso-actual-cost-202607.csv')
  UNION ALL SELECT * FROM focus.main.focus('sample://gcp/gcp_billing_export_v1_01A2B3_202607.parquet'))
GROUP BY ALL ORDER BY effective_cost DESC;

-- Tags are a MAP
SELECT Tags['team'] AS team, sum(BilledCost) FROM focus.main.focus('sample://aws_cur2')
WHERE Tags['team'] IS NOT NULL GROUP BY team ORDER BY team;
```

## What it reads

| `source :=` | Export | Formats |
|---|---|---|
| `aws_cur2` | AWS Data Exports **CUR 2.0** (and legacy CUR / Athena CUR: `lineItem/UnblendedCost` headers, `resource_tags_user_*` columns) | Parquet, CSV(.gz) |
| `azure` | Azure Cost Management **actual-cost** export (EA / MCA cost details) | CSV, Parquet |
| `azure_amortized` | Azure **amortized-cost** export (same columns, so name it explicitly) | CSV, Parquet |
| `gcp` | Google Cloud **BigQuery billing export** (standard or detailed), dumped to files | Parquet, NDJSON (nested structs and `credits` kept), flattened CSV |
| `focus` | **Native FOCUS 1.0–1.4** files from any provider: AWS FOCUS export, Azure FOCUS export, GCP FOCUS view, OCI, the FinOps Foundation sample data | Parquet, CSV, JSON |

`source := 'auto'` (default) detects the format **per file** from its column names, so one glob can mix
clouds. The provider's own FOCUS exports are already FOCUS-shaped; this worker types them (exact decimals,
UTC timestamps, `Tags` as a `MAP`), bridges columns renamed between versions, and carries every `x_`
extension column through.

Paths: a file, a directory (recursive; export manifests are skipped), a glob (`*`, `**`), an `http(s)://`
URL (Range requests, so Parquet is never downloaded whole), `s3://bucket/prefix/` (and `gs://` through
GCS's S3-compatible endpoint with a `gcs` HMAC secret). **Azure Blob / ADLS and BigQuery are not read
directly**: read them with DuckDB's own extensions and map the rows with `to_focus`:

```sql
SELECT ServiceCategory, sum(EffectiveCost)
FROM focus.main.to_focus((SELECT * FROM read_parquet('az://costexports/contoso/*.parquet')),
                         source := 'azure_amortized')
GROUP BY ALL;
```

### Credentials for s3://

`focus()` asks DuckDB for the `s3` secret whose scope covers the path, exactly like `read_parquet` does,
and passes `KEY_ID`, `SECRET`, `SESSION_TOKEN`, `REGION`, `ENDPOINT`, `URL_STYLE` and `USE_SSL` to pyarrow's
S3 client. Without a secret the bucket is read anonymously. Nothing is read from environment variables.

```sql
CREATE SECRET cur (TYPE s3, KEY_ID '...', SECRET '...', REGION 'us-east-1', SCOPE 's3://my-cur-bucket');
SELECT sum(BilledCost) FROM focus.main.focus('s3://my-cur-bucket/cur2/data/', billing_period := '2026-07');
```

## Functions and tables

| Object | Kind | What it does |
|---|---|---|
| `focus(path, source := 'auto', spec_version := '1.2', billing_period := NULL, money_scale := 6, file_format := 'auto')` | table function | Export files → FOCUS rows |
| `to_focus(subquery, source := 'auto', spec_version := '1.2', money_scale := 6)` | table in-out | Rows from any DuckDB query → FOCUS rows (same mapping) |
| `focus_validate(path, source := 'auto', spec_version := '1.2', file_format := 'auto')` | table function | One row per conformance check: `check_name, column_name, severity, status, rows_checked, failing_rows, sample_value, detail` |
| `focus_columns` | table | The FOCUS spec: every column of 1.0, 1.1, 1.2, 1.3, 1.4 with feature level, nullability, type, allowed values |
| `provider_map` | table | Every mapping rule: source → FOCUS column, raw columns read, transform, notes, whether filters push down |
| `service_map` | table | Provider service → FOCUS ServiceName / ServiceCategory / ServiceSubcategory |
| `charge_type_map` | table | Provider charge type → FOCUS ChargeCategory (and ChargeFrequency) |
| `focus_service_category(provider, service)`, `focus_service_subcategory(...)` | scalar | Classify a service (`'aws', 'AmazonRDS'` → `Databases` / `Relational Databases`) |
| `focus_charge_category(provider, charge_type)` | scalar | `'aws', 'RIFee'` → `Purchase` |
| `focus_map_column(source, column)` | scalar | Which FOCUS columns a raw export column feeds |
| `focus_validate_value(column, value [, spec_version])` | scalar | `NULL` if legal, else the violation |
| `focus_divide(n, d)`, `focus_effective_rate(cost, qty)` | scalar | Exact `DECIMAL(19,10)` ratios, half-even |

`spec_version` picks the column set: 43 columns for 1.0, 50 for 1.1, 57 for 1.2 (default), 65 for 1.3
and 1.4, followed by six `x_` columns: `x_SourceFormat`, `x_SourceFile`, `x_ServiceCode`, `x_UsageType`,
`x_Operation`, `x_ChargeType`.

## Types and the arithmetic contract

| Columns | DuckDB type |
|---|---|
| BilledCost, EffectiveCost, ListCost, ContractedCost, PricingCurrencyEffectiveCost | `DECIMAL(19,6)` (`money_scale := s` gives `DECIMAL(19,s)`, s = 2..10) |
| ListUnitPrice, ContractedUnitPrice, PricingCurrency*UnitPrice | `DECIMAL(19,10)` |
| PricingQuantity, ConsumedQuantity, CommitmentDiscountQuantity | `DECIMAL(28,10)` |
| ChargePeriodStart/End, BillingPeriodStart/End | `TIMESTAMP WITH TIME ZONE` (UTC) |
| Tags (and AllocatedTags in 1.3+) | `MAP(VARCHAR, VARCHAR)` |
| everything else, JSON columns as text | `VARCHAR` |

`DECIMAL(19,6) * DECIMAL(19,6)` is `DECIMAL(38,12)` in DuckDB: exact, no overflow. `SUM` widens to
`DECIMAL(38,6)`, exact. **`DECIMAL / DECIMAL` is `DOUBLE`** in DuckDB, which is why `focus_divide` exists:
`focus_divide(sum(EffectiveCost), sum(ListCost))` stays exact, and its `DECIMAL(19,10)` result times a
money column is `DECIMAL(38,16)`, again without overflow.

Six decimal places round away sub-microdollar line items (an AWS Lambda line of `4.1667e-07` becomes
`0.000000`). Use `money_scale := 10` when those matter; the limit is then $999,999,999 per line item.

All output columns are nullable: a non-conforming export still loads, and `focus_validate` tells you what
is wrong with it rather than the read failing.

## How the mapping works

The full rule list is `SELECT * FROM focus.main.provider_map`. The interesting parts:

**AWS CUR 2.0.** `BilledCost` is the unblended cost. `EffectiveCost` follows AWS's amortized-cost
formula: Savings Plan covered usage carries `savings_plan_savings_plan_effective_cost`, RI covered usage
carries `reservation_effective_cost`, `SavingsPlanNegation`, upfront fees and recurring commitment fees
are zeroed, and the **unused** part of a recurring RI / Savings Plan fee becomes a separate synthetic
`Usage` row with `CommitmentDiscountStatus = 'Unused'` (FOCUS's representation of commitment waste;
`x_ChargeType = 'RIFee (unused)'`). Billed and effective totals both reconcile. `ServiceName` comes from
the `product` map, `Tags` from `resource_tags` with the `user_` prefix removed, Marketplace rows get the
seller as `PublisherName`.

**Azure.** Actual and amortized exports have the same columns but different meanings. With `azure`,
`BilledCost` is the row cost and commitment purchases get `EffectiveCost = 0` (an actual export cannot
amortize). With `azure_amortized`, `EffectiveCost` is the row cost and commitment-covered usage and
`UnusedReservation` / `UnusedSavingsPlan` rows get `BilledCost = 0`. `ContractedCost` is
`UnitPrice × Quantity × ExchangeRatePricingToBilling` computed exactly. Dates are daily (`MM/DD/YYYY` or
ISO); `BillingPeriodEnd` is Azure's inclusive end date plus one day.

**GCP.** `BilledCost` = `cost` + sum(`credits.amount`), which is what the invoice totals. A committed-use
discount credit sets `CommitmentDiscountId`/`Category` and `PricingCategory = 'Committed'`. The billing
period is `invoice.month`.

**Every provider.** `ChargeClass = 'Correction'` when a charge period starts before the billing period
it was billed in. `ConsumedQuantity` is NULL unless the row is Usage and not an unused-commitment row.
Unknown services become `ServiceCategory = 'Other'` (look for them by `x_ServiceCode`).

## Pushdown

- **Projection**: only the raw columns behind the selected FOCUS columns are read (the 125-column CUR
  sample queried for `sum(BilledCost)` reads 8 of them; all 59 FOCUS columns need 61). `tests/test_projection.py` proves every column computed
  from only its declared inputs equals the full mapping.
- **Filters**: comparisons on `ChargePeriodStart`, `ChargePeriodEnd`, `BillingPeriodStart`,
  `BillingAccountId`, `SubAccountId` (and `x_ServiceCode` for AWS/Azure) are translated to the raw Parquet
  column and prune row groups by their statistics; DuckDB's predicate is then re-applied exactly.
- **`billing_period := 'YYYY-MM'`** skips `BILLING_PERIOD=YYYY-MM` directories (the CUR 2.0 layout) and
  prunes row groups.
- **Resumable scans**: the cursor is `(file, row group, batch)`, so scans resume in any worker process,
  which is what the HTTP transport needs.

## Conformance

```sql
SELECT check_name, column_name, failing_rows, sample_value
FROM focus.main.focus_validate('sample://focus12', spec_version := '1.3') WHERE status <> 'pass';
```

Checks are generated from the spec data in `focus_columns`: `column_present` (Mandatory missing = error),
`not_null`, `allowed_values`, `subcategory_matches_category`, `charge_period_order`,
`billing_period_order`, `currency_code`, `consumed_quantity_usage_only`,
`commitment_status_requires_id`, `custom_column_prefix` (non-FOCUS columns without `x_`) and
`column_name_case`. Run against the FinOps Foundation's own FOCUS 1.0 sample as 1.2 it finds real
problems: `ChargeFrequency = 'Usage-based'`, `ServiceCategory = 'Others'`, NULL `ContractedCost`, an
unprefixed `Id` column.

## Scope and limits

Built: readers for three clouds plus native FOCUS, `to_focus`, validation, the spec and mapping tables,
classification and exact-division scalars.

Not built (from the original design brief, deliberately deferred): commitment amortization over arbitrary
FOCUS rows, coverage/utilization, multi-pass cost allocation, invoice reconciliation, unit cost,
allocation-rule validation, AWS manifest/assembly-id cursors, OCI cost reports, live billing API pulls
(use [vgi-azure-cost](https://github.com/Query-farm/vgi-azure-cost) for the Azure Cost Management API) and
a drift canary against the providers' published schemas. Native FOCUS files' extension columns come from
the first file of a glob.

Mapping caveats are listed per rule in `provider_map.notes`. The fixtures are synthetic, built from the
providers' published export schemas, and have not been checked against a real customer export.

## Development

```bash
make build                    # uv sync
make test-unit                # pytest: mapping goldens, decimal reference, projection proof, units
make test-sql TRANSPORT=stdio # test/sql/*.test through Haybarn 2.0 (also launch, http)
make lint-explain             # vgi-lint metadata gate (fail_on = info)
make check                    # everything CI runs
uv run python scripts/make_fixtures.py       # regenerate the synthetic samples
uv run --no-project python scripts/compile_focus_spec.py [--check]   # recompile focus_spec.json
```

`test/fixtures/focus_sample/` holds 300 rows of the FinOps Foundation's FOCUS 1.0 sample data
(CC BY 4.0, see its NOTICE); it is used by the tests and does not ship in the package.
Golden files live in `tests/golden/`; regenerate after a deliberate mapping change with
`UPDATE_GOLDEN=1 uv run pytest tests/test_mapping.py` and review the diff.

Releases: push a tag `vX.Y.Z`; the release workflow builds and signs the wheel and sdist. Users install
with `uvx --from git+https://github.com/Query-farm/vgi-focus-cost@vX.Y.Z vgi-focus-worker`.

## FOCUS attribution

The FOCUS column definitions in `focus_columns` (column names, constraint values and one-sentence
descriptions, versions 1.0 to 1.4) are compiled by `scripts/compile_focus_spec.py` from the
[FOCUS specification](https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec), © Joint Development
Foundation Projects, LLC, FinOps Open Cost and Usage Specification (FOCUS) Series and its contributors,
licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) (license verified 2026-11-15 on
the v1.2 tag's `license.md`). See [NOTICE.md](NOTICE.md). FOCUS is a trademark of the FinOps Foundation;
this worker is not affiliated with or endorsed by it.

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). The FOCUS specification data and the FOCUS
sample fixture keep their CC BY 4.0 license; see [NOTICE.md](NOTICE.md).
