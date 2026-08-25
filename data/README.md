# `data/` — medallion mapping

Logical layers (files are still flat in this folder). Full DAG: [`docs/architecture/MEDALLION_LAYOUT.md`](../docs/architecture/MEDALLION_LAYOUT.md).

| Layer | What belongs here |
|-------|-------------------|
| **Bronze** | `esrb.*.xlsx`, `institutional_setup.json`, `BBM táblázatok.xlsx` (optional DTI import) |
| **Silver** | `processed_ccyb.parquet`, `processed_syrb.parquet`, `processed_bbm.parquet`, `processed_osii.parquet` |
| **Gold** | `latest_*.parquet`, `trend_*.parquet`, `dti_expert_table.csv`, `ltv_gold.csv`, `bbm_gold_state.json`, `pipeline_manifest.json`, `analyses_cache.json` |

`bbm_delta_report.json` is the review queue when an ESRB description changes. `pipeline_manifest.json` stores bronze/parser/silver hashes so unchanged Excel does not re-parse, re-plot, or re-call Gemini. News uses `NEWS_TTL_DAYS` (default 7), not the Excel hash. Set `FORCE_REBUILD=true` to ignore skips.

Do not edit `processed_*.parquet` by hand. Re-run `python main.py` (or the GitHub Action) after a bronze refresh.
