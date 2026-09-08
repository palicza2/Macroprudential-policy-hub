---
name: bbm-gold
description: >-
  Maintains BBM gold tables (LTV, DTI/LTI) as the dashboard source of truth.
  Use when editing LTV/DTI extraction, expert corrections, dti_expert_table.csv,
  ltv_gold.csv, gold delta, BBMStage, or borrower-based measure display.
---

# BBM Gold

Dashboard LTV and DTI/LTI tables come from **curated gold CSVs**, not from live LLM extraction. ESRB text is hashed; the model only reviews rows whose description changed, appeared, or has no gold match.

## Source of truth

| Artifact | Path | Role |
|----------|------|------|
| DTI/LTI gold | `data/dti_expert_table.csv` | Dashboard DTI/LTI rows |
| LTV gold | `data/ltv_gold.csv` | Dashboard LTV rows (bootstrapped once if missing) |
| Hash state | `data/bbm_gold_state.json` | Approved description hashes |
| Review queue | `data/bbm_delta_report.json` | Changed / new / withdrawn |
| Proposals | `data/bbm_gold_proposals.json` | LLM extracts awaiting human append |
| Optional Excel import | `data/BBM táblázatok.xlsx` or `BBM_EXCEL_PATH` | Import into DTI gold; not the live path |

`BBMStage` (`pipeline/stages/bbm_stage.py`) loads gold, classifies ESRB items, writes the delta report, and **renders gold**. It does not overwrite DTI gold with model output.

## Edit gold, not the overlay

Country-specific facts belong **in the CSV**:

- **SK:** DTI all outstanding debt, **net** income, standard **8.0x**, preferential **9.0x (under 35 / FTB)**, notes that limits decrease with age. Gold may also show a 3–8 range in structured extracts.
- **IE:** LTI **3.5x** standard, **4.0x FTB**, requested-mortgage numerator, 15% volume speed limit.
- **UK:** gold country code is **UK**. ESRB parquet may use **GB**. Use `normalize_iso2` / `esrb_iso_aliases` in `bbm/gold.py` (`GB`/`UK` → `UK`).

`apply_expert_corrections()` in `bbm/dti_lti_builder.py` is **deprecated** for the dashboard path. Do not add new country hacks there; patch the gold CSV.

## Delta kinds and verdicts

Classification (`bbm/gold.py`): `unchanged` | `changed` | `new` | `withdrawn`.

Changed rows: if `BBM_GOLD_DELTA_AI` is on, `bbm/delta_checker.py` returns `match` | `conflict` | `unclear`.

- **match** — gold richer than ESRB is fine; do not strip gold fields ESRB omitted.
- **conflict** — keep gold on the dashboard; record `conflicting_fields`, `evidence_excerpt`, `proposed_patch` in the report. A human updates the CSV.
- **unclear** — do not advance the approved hash.

New countries: extract at most **8** per family (`_MAX_NEW_EXTRACT`). Output goes to **proposals**, not straight into gold. DTI action string: append to `dti_expert_table.csv` after review. LTV: append to `ltv_gold.csv` after review.

Exception: empty `ltv_gold.csv` triggers a one-time bootstrap extract that **does** write `data/ltv_gold.csv` and should be reviewed.

`BBM_GOLD_DELTA_AI=false` still hashes; it only skips the LLM verdict (all changed → `unclear`).

## DSTI vs DTI/LTI vs LTV

Keep families separate in UI and gold:

- **LTV** — % of value (`ltv_gold.csv`, `bbm/ltv_model.py`). Dashboard columns are **FTB / OOO**, **SSB / BTL**, and **Other limits** — not standard/FTB/BTL. Unlabeled residential caps are OOO. Green/secondary-home/FX go in Other. Volume shares are exception quotas, not LTV caps.
- **DTI/LTI** — income multiple (`dti_expert_table.csv`)
- **DSTI** — debt-service % of income; different tool. Do not merge DSTI rows into DTI gold.

ESRB `measure_type` on silver BBM parquet is free text (`type of measure`). Gold tables are structured expert rows.

## LLM rules for this path

- Do not call Gemini per active BBM row on a normal run.
- Require an **evidence excerpt** from the ESRB description when proposing a patch.
- Prefer regex extractors (`bbm/ltv_extractor.py`, `bbm/dti_lti_extractor.py`) plus gold; LLM is for delta/new review.
- Keyword cleaning of the **recent decisions** table is separate (short tags), not gold mutation.

## Tests

```bash
python -m unittest scripts.tests.test_bbm_gold_delta
python scripts/tests/test_bbm_dti_lti.py
```

## Code map

- Stage: `pipeline/stages/bbm_stage.py`
- Gold I/O + GB/UK: `bbm/gold.py`
- Delta LLM: `bbm/delta_checker.py`
- DTI Excel import: `bbm/dti_excel_loader.py`
- Structured extract (debug / new countries): `bbm/dti_lti_builder.py`, `bbm/ltv_builder.py`
