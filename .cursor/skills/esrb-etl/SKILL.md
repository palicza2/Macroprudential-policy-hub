---
name: esrb-etl
description: >-
  Parses ESRB Excel (CCyB, SyRB, BBM, O-SII, reciprocation) into silver parquet.
  Use when changing etl.py, column mapping, country ISO codes, rate extraction,
  bronze downloads, schema, or processed_*.parquet.
---

# ESRB ETL

Bronze Excel is downloaded; silver parquet is parsed. **Never hand-edit** `data/processed_*.parquet` or `latest_*.parquet`. Fix the parser or replace bronze, then re-run.

## Bronze sources (`config.py` `URLS` / `FILES`)

| Workbook | Typical sheets | Produces |
|----------|----------------|----------|
| `esrb.ccybd_CCyB_data.xlsx` | first sheet | CCyB history |
| `esrb.measures_overview_macroprudential_measures.xlsx` | SRB/Systemic, BoBM, reciprocation | SyRB, BBM, reciprocity |
| `esrb.measures_overview_capital-based_measures.xlsx` | Overview of measures | O-SII / G-SII |

Downloads: `ETLPipeline.download_bronze()` via `download_file_safely` (identical bytes not overwritten).

Curated (not ESRB Excel): `data/institutional_setup.json`, DTI gold CSV. Do not treat those as parse output.

## Parse rules

`etl.py` maps columns by **substring on cleaned headers**, not fixed Excel column letters. ESRB layouts change; keep fuzzy maps.

Detect header row with `find_header_row` / first row containing `country` or `reference of measure`. `clean_columns` before mapping.

If a required column is missing, return an **empty DataFrame** (do not invent columns):

- CCyB: need `rate` (and country/date)
- SyRB / BBM: need `country`

### Sheet selection

- SyRB: sheet name contains `SRB` or `Systemic`
- BBM: sheet name contains `BoBM`
- CCyB: sheet 0
- O-SII: capital workbook `"Overview of measures"` (hierarchical country / bank rows)

### Country hygiene

- `country_converter` (`coco.convert(..., to='iso2')`) on the country name column.
- CCyB also stores `iso3`.
- Drop rows with missing `country` (CCyB also drops missing `date`).
- BBM gold later maps **GB → UK**. Silver CCyB/SyRB may still be `GB`. Do not “fix” gold UK rows back to GB in ETL.

### Dates and status

- Prefer **measure becomes active on** as `date`; revocation from **date of revocation**.
- BBM `active_status`: Inactive if status text has deactivated/revoked/expired, or revocation date ≤ now; else Active.
- SyRB trends must honor activation **and** revocation (see existing trend logic; do not count revoked measures as active).

### Rates

- CCyB: `utils.extract_rate` on the CCyB rate column (exclude buffer **guide** columns).
- SyRB: parse `%` from **description** first; optional numeric `rate` column. Watch **percent vs fraction** and GDP `%` false positives. Values `> 10` are suspicious; prefer description if the column looks like 50 meaning 50% vs 0.5%. Cap/log high rates; do not silently scale without evidence.
- Reasonable policy buffers are typically 0–20%, often ≤ 3–5% for CCyB.

Credit gap: first column with `gap` in the name, excluding `additional`.

## Skip granularity

`DataStage` can skip **per workbook** (`skip.ccyb` / `skip.measures` / `skip.capital`) when bronze hash + parser fingerprint match. Measures skip covers SyRB **and** BBM **and** reciprocation together.

Parser fingerprint: `etl.py`, `capital_overall.py`, `reciprocation.py`, `utils/dataframe.py`.

`capital_overall.py` builds the stacked buffer view (`ccob_rate=2.5`). Changing it invalidates parser skip.

## Silver outputs

Written by `_persist_silver`: `processed_{ccyb,syrb,bbm,osii}.parquet`, `latest_*`, `trend_*`, plus reciprocation parquet/json.

`load_silver()` is the skip path. If latest snapshots are empty, it recomputes from processed frames.

## Reciprocation

`reciprocation.py` reads the measures overview workbook (not a separate URL). Output: `data/reciprocation_measures.parquet`, `reciprocation_matrix.parquet`, `reciprocation_meta.json`.

## When Excel layout breaks

1. Inspect sheet names and header row in the new file.
2. Extend substring maps; do not hard-code column indexes unless the sheet is the hierarchical O-SII overview.
3. Add a small fixture test under `scripts/tests/` rather than only a live download.
4. Do not patch numbers in parquet to “hotfix” a dashboard.

## Code map

- `etl.py` — parse, download, persist, trends
- `pipeline/stages/data_stage.py` — skip plan → `run_pipeline` / `load_silver`
- `utils.py` / `utils/dataframe.py` — headers, `extract_rate`
- `config.py` — URLs and `FILES`
