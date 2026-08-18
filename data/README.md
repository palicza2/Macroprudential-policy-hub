# `data/` — medallion mapping

Logical layers (files are still flat in this folder). Full DAG: [`docs/architecture/MEDALLION_LAYOUT.md`](../docs/architecture/MEDALLION_LAYOUT.md).

| Layer | What belongs here |
|-------|-------------------|
| **Bronze** | `esrb.*.xlsx`, `institutional_setup.json`, `BBM táblázatok.xlsx`, expert DTI CSV |
| **Silver** | `processed_ccyb.parquet`, `processed_syrb.parquet`, `processed_bbm.parquet`, `processed_osii.parquet` |
| **Gold** | `latest_*.parquet`, `dti_lti_rules.csv` (structured product) |

Do not edit `processed_*.parquet` by hand. Re-run `python main.py` (or the GitHub Action) after a bronze refresh.
