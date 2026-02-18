# Improvement Suggestions: Macroprudential Hub

**Date:** 2025-02-17  
**Purpose:** Saved for further use. Analysis from three perspectives: Financial Stability Expert, AI Engineer, Data Engineer.

---

## 1. Financial Stability Expert Perspective

### Policy & Indicators
- **Credit gap vs CCyB alignment:** Add a “CCyB vs credit gap” or “guide compliance” view (BCBS/ESRB guide: 0% when gap ≤ 2%, up to 2.5% when gap > 2%); flag countries where announced CCyB is clearly off.
- **Reciprocity / cross-border CCyB:** Model which countries reciprocate to which; add a reciprocity matrix or list per jurisdiction.
- **O-SII vs G-SII split:** Expose O-SII vs G-SII separately in country profiles and capital stack (tooltips or columns), not only the max.
- **BBM clarity (DSTI vs DTI/LTI):** Ensure DSTI (cap %) is clearly defined and separated from DTI/LTI in the BBM overview for cross-country comparability.
- **SyRB sectoral timeline:** Show when each sectoral SyRB became active and whether it is still active (e.g. first activation date per country/sector).
- **Data disclaimer:** Add a short “Data & methodology” note: source (ESRB), extraction date, definition of “latest” (e.g. latest decision date), and limitations.

### Risk Narrative
- **Executive summary structure:** Optionally structure global summary into cyclical (CCyB), structural (SyRB/O-SII), and sectoral (BBM) risk paragraphs.
- **Country peer comparison:** Enrich peer groups (e.g. by region, buffer level, or “similar policy mix”) for “who is like us” and stance comparison.

---

## 2. AI Engineer Perspective

### Reliability & Cost
- **Caching:** Use LLM cache for all LLM calls (chart analyses, section summaries, executive summary, BBM validation, grounding). Version cache by pipeline/schema if needed.
- **Retries:** Add exponential backoff and rate-limit handling (e.g. 429); distinguish transient vs permanent errors.
- **Structured outputs:** Use provider JSON/structured-output mode where available to reduce parsing failures.

### Validation & Quality
- **Grounding:** Consider a “light” grounding by default (numeric claims vs tables only); keep full search-based grounding optional.
- **Numeric guardrails:** Before rendering, check AI-generated numbers (e.g. “X countries raised CCyB”) against data; auto-correct or flag if beyond a threshold.
- **Evaluation:** Add a small eval set (fixed snapshots + gold summaries/keywords) and run after prompt/model changes to catch regressions.

### Prompts & Context
- **System context:** Add one line: “If a number is in the DATA table, use that number; do not estimate or round differently.”
- **Knowledge graph:** If re-enabled, keep behind a flag or as a separate “deep dive” run for performance and cost.

---

## 3. Data Engineer Perspective

### ETL Robustness & Schema
- **Schema contract:** Define expected columns and dtypes for CCyB, SyRB, BBM, O-SII outputs; validate after each `_process_*` and fail fast if ESRB layout changed.
- **Idempotency:** Ensure “same inputs ⇒ same outputs” (no unversioned cache or date-dependent logic that changes results).
- **Incremental / backfill:** If ESRB provides incremental or point-in-time files, consider incremental ingestion and a backfill procedure.

### Data Quality & Lineage
- **Data quality checks:** Post-ETL checks: no duplicate (country, date) for CCyB; rates in [0,5] (CCyB) and [0,10] (SyRB); required ISO2 present; critical columns non-null. Log or fail on critical violations.
- **Lineage:** Record per run: input URLs (and hashes/modified dates if possible), pipeline version (e.g. git commit), output paths; store in a small JSON or Supabase.

### Supabase & Testing
- **Supabase writer:** Move transformers out of `archive/supabase_migration` into main codebase (e.g. `supabase/` or `pipeline/writers/`) so schema and transforms are in one place and testable.
- **Testing:** ETL unit tests: fixture with 2–3 rows for `_process_ccyb` / `_process_syrb` (assert columns/dtypes); tests for `ccyb_change_only_points`, `get_latest_quarter_end`; capital stack aggregation with minimal df. Integration test with a small Excel fixture in repo.
- **Config:** Keep all URLs and env keys in `config.py` / `.env`; support a “dry run” (skip download, use local files) for CI and offline runs.

---

## Summary Table

| Area | Financial stability | AI engineer | Data engineer |
|------|---------------------|------------|----------------|
| **High impact** | CCyB–credit gap guide view; reciprocity; data disclaimer | Cache all LLM calls; numeric guardrails; lightweight grounding by default | Schema validation after ETL; data quality checks; ETL unit tests |
| **Medium impact** | O-SII/G-SII split; BBM DSTI vs DTI/LTI; SyRB timeline | Structured outputs; retry/backoff; eval set | Lineage/versioning; move Supabase transformers out of archive; idempotency |
| **Lower effort** | Executive summary structure (cyclical/structural/sectoral); peer groups | One-line prompt tweak (“use DATA numbers”) | Dry-run mode; document “same inputs = same outputs” |
