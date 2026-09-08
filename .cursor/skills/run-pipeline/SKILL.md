---
name: run-pipeline
description: >-
  Runs and modifies the Macroprudential Hub batch pipeline (DataStage, Visualization,
  BBM, AI, profiles, render). Use when changing pipeline stages, skip/cache behavior,
  main.py, orchestrator, FORCE_REBUILD, or generating index.html.
---

# Run Pipeline

Batch DAG, not an agent. Entry: `python main.py` → `pipeline/orchestrator.py`.

## Stage order

1. **DataStage** — download ESRB Excel, parse to parquet (or load silver if skipped)
2. **VisualizationStage** — Plotly HTML + PNG figures
3. **BBMStage** — gold LTV/DTI tables + delta report (LLM only on text change)
4. **AIStage** — Gemini chart/section/executive text; grounding only if `RUN_GROUNDING=true`
5. **ProfileStage** — country profiles; skip AI copy when LLM is skipped
6. **RenderStage** — `index.html` + `reports/`

Knowledge-graph build and KG analysis are **disabled**. Do not re-enable without an explicit flag and a performance check.

Canonical country-profile shape is `country_profiles/profile_mapper.py` (`canonicalize_profile`). Pipeline, Supabase merge, and frontend must share that shape.

## How to run

```bash
python main.py
```

Optional Docker: `docker-compose run --rm pipeline`. Monthly GitHub Action: **Build Dashboard**.

Requires `.env` with `GOOGLE_API_KEY`. Do not commit `.env`.

## Env flags (`config.py`)

| Variable | Default | Effect |
|----------|---------|--------|
| `FORCE_REBUILD` | off | Disables all manifest skips |
| `RUN_GROUNDING` | off | Claim verify + optional Google Search (costly) |
| `BBM_GOLD_DELTA_AI` | on | LLM match/conflict on changed ESRB text only |
| `NEWS_TTL_DAYS` | 7 | News cache TTL; not tied to Excel hash |
| `ENABLE_SUPABASE` | off | Upsert gold to Postgres |
| `USE_SUPABASE_FOR_RENDER` | off | Frontend/render reads Supabase |

There is no CLI `--skip-*`. Skips are computed from `data/pipeline_manifest.json`.

## Manifest skips

`pipeline/manifest.py` hashes bronze Excel + parser files, then silver + viz/LLM code + figures.

- Unchanged CCyB Excel + parser → skip CCyB parse
- Unchanged measures Excel + parser → skip SyRB/BBM/reciprocation parse
- Unchanged capital Excel + parser → skip O-SII parse
- Unchanged silver + `visualizer.py` + existing plots → skip viz
- Unchanged silver + figures + LLM code + `analyses_cache.json` + `countries_data.json` → skip Gemini
- News skip uses TTL only

**LLM fingerprint files** (changing any invalidates LLM skip): `llm_analysis.py`, `prompts.py`, `llm_tasks.py`, `ccyb.py`, `syrb.py`, `country_profiles/profile_generator.py`, `country_profiles/data_aggregators.py`.

**Parser fingerprint files:** `etl.py`, `capital_overall.py`, `reciprocation.py`, `utils/dataframe.py`.

Prompt-level cache lives in `cache/` (gitignored). Stage-level reuse is `data/analyses_cache.json`.

## Do not

- Hand-edit `data/processed_*.parquet` or `latest_*.parquet`. Fix bronze or the parser and re-run.
- Insert a LangGraph/agent loop into `PipelineOrchestrator`. Keep a deterministic DAG.
- Call Gemini on every BBM row. Gold CSVs are the dashboard source of truth.
- Run `FORCE_REBUILD=true` unless you need a full refresh (downloads still happen; parse/viz/LLM all rerun).

## After code changes

- Parser/ETL change → expect silver re-parse on next run (manifest sees parser fingerprint).
- Prompt/LLM change → expect Gemini rerun; do not delete gold CSVs.
- UI-only (`assets/`, templates) → full pipeline often unnecessary; edit render/partials and verify in the browser.

## Tests

```bash
python -m unittest scripts.tests.test_pipeline_manifest
python -m unittest scripts.tests.test_bbm_gold_delta
```

Broader BBM/extraction scripts: `scripts/tests/`.

## Layout

Logical medallion (files still flat under `data/`): see `docs/architecture/MEDALLION_LAYOUT.md`.
