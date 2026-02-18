# Reciprocity Section: Data Review & Implementation Analysis

**Source:** `esrb.measures_overview_macroprudential_measures.xlsx`  
**Sheets of interest:** "Matrix of reciprocation", "Reciprocation (recognition)"  
**Date:** 2025

---

## 1. Context: Why Reciprocity Matters (Financial Stability)

- **Reciprocation** = when a *reciprocating* Member State applies the same or equivalent macroprudential measure as the *activating* Member State, so that institutions in both jurisdictions are treated similarly for the targeted risk.
- It extends the reach of measures beyond the activating country (branches + direct cross-border exposures), supports **level playing field** and **effectiveness** of macroprudential policy in the EU.
- **ESRB Recommendation ESRB/2015/2** (voluntary reciprocity): activating country requests → ESRB recommends → other countries “act or explain” (reciprocate or justify non-reciprocation).
- For a **reciprocity section** in the hub, the main questions are:
  - Which measures are **currently recommended for reciprocation** (and by which country)?
  - For each such measure, **which countries have reciprocated** vs not (or partially)?
  - Optional: time dimension (when requested, when reciprocated) and link to recommendation documents.

Your idea—**show currently applicable measures that were requested to be reciprocated, and for each country whether they have reciprocated or not**—aligns well with this and is the right level of granularity for a dashboard.

---

## 2. Financial Stability Expert Perspective

### What to present

1. **Table: “Measures recommended for reciprocation” (current)**
   - Activating country, measure type (e.g. CCyB, SyRB, BBM), short description/rate, date of ESRB recommendation, status (active/revoked if in data).
   - Filter: only “currently applicable” (e.g. measure still active in the activating country and recommendation still in force).

2. **Reciprocation matrix / list (your idea)**
   - Rows: each **measure** (or measure × activating country).
   - Columns: **all EEA countries** (or a clear subset).
   - Cell (or list entry): **Reciprocated / Not reciprocated / Partial / Not applicable** (e.g. activating country itself), with optional tooltip or link to source.
   - This directly supports: “Who has reciprocated which request?” and “Which countries have not yet reciprocated this measure?”

3. **Insights to highlight**
   - **Coverage gap:** Measures with many non-reciprocating countries (potential level-playing-field or effectiveness concerns).
   - **Activating vs reciprocating:** Which countries are most often “requesters” vs “reciprocators”.
   - **Recency:** Recently recommended measures with still few reciprocations (implementation lag).
   - **“Act or explain”:** Link or short note where non-reciprocation is explained (if the Excel or a companion source provides it).

### Data needs from the Excel

- Identifiers: **activating country**, **measure** (type + e.g. rate/description), **recommendation date** (and optionally end date if revoked).
- For each **reciprocating country**: **status** (reciprocated / not / partial) and ideally **date** of reciprocation.
- Distinction between “measure no longer active” vs “recommendation still active but country has not reciprocated”.

---

## 3. Data Engineer Perspective

### Excel structure to confirm (run inspector script)

The file is at `data/esrb.measures_overview_macroprudential_measures.xlsx` (after pipeline download). You need to verify:

- **Sheet names** (exact spelling): e.g. `Matrix of reciprocation`, `Reciprocation (recognition)`.
- **Header row** and column mapping:
  - “Matrix” sheet: likely **countries as rows and/or columns**, or **measures as rows** and **countries as columns**; cells = reciprocation status.
  - “Reciprocation (recognition)” sheet: often **one row per measure** (or per measure × country) with columns for activating country, measure description, recommendation date, and then one column per reciprocating country (or a long-format: country, status).
- **Data types:** dates (recommendation, reciprocation), status (text/categorical), country codes (ISO2/names).
- **Multi-row headers / merged cells:** common in ESRB files; need a robust `header_row` detection and column cleanup (as in existing SyRB/BBM ETL).

### ETL design

1. **Dedicated loader** (e.g. `etl.py` or `reciprocation.py`):
   - Open the same workbook used for SyRB/BBM (`measures_overview_source`).
   - Select sheet by name (substring match: e.g. `"reciproc"` in sheet name for matrix, `"Reciprocation"` or `"recognition"` for the other).
   - `find_header_row()` + `clean_columns()` (reuse existing utils).
   - Map columns to a canonical schema (see below).
2. **Canonical schema (suggested)**
   - **Request/measure table:** `activating_country_iso2`, `measure_type` (CCyB/SyRB/BBM/other), `measure_description`, `recommendation_date`, `effective_date`, `status` (e.g. active/revoked), `esrb_recommendation_id` (if present).
   - **Reciprocation table:** `activating_country_iso2`, `measure_id` or measure key, `reciprocating_country_iso2`, `reciprocation_status` (reciprocated / not_reciprocated / partial / n_a), `reciprocation_date` (if available).
   - If the Excel is a single matrix: one row per measure (or per activating country), one column per reciprocating country → melt to long format for the reciprocation table.
3. **Idempotency & quality**
   - Normalise country names to ISO2 (reuse `country_converter`).
   - Validate: no duplicate (measure, reciprocating_country); dates parseable; status in allowed set.
   - Output: e.g. `data/processed_reciprocation.parquet` and optionally `data/latest_reciprocation.parquet` for “current” view.
4. **Integration**
   - Add reciprocation to the pipeline data dict; optionally write to Supabase if you have a `reciprocation` or `reciprocation_status` table.

### Risks

- **Sheet layout changes:** ESRB may rename or restructure sheets; use defensive name matching and log unmapped columns.
- **Mixed formats:** “Matrix” vs “Reciprocation (recognition)” may need two different parsers; then join by (activating country, measure) or similar.

---

## 4. AI Engineer Perspective

### How AI can help

1. **Summaries**
   - Short narrative: “X measures are currently recommended for reciprocation; Y countries have fully reciprocated at least one measure; Z measures show incomplete reciprocation.”
   - Per-measure or per-country one-liners (e.g. “DE SyRB 0.5%: 12 countries reciprocated, 5 not yet.”).

2. **Consistency checks**
   - Cross-check with SyRB/CCyB/BBM tables: e.g. “Recommendation says country A activated SyRB 2%” vs “SyRB sheet shows A with 2%” to catch mismatches.

3. **“Act or explain”**
   - If the Excel or linked documents contain non-reciprocation reasons, LLM can summarise or tag (e.g. “de minimis”, “different risk assessment”) for display in the dashboard.

4. **Anomaly / delay detection**
   - Flag measures recommended > N months ago with very few reciprocations (possible implementation delay or materiality threshold issues).

### Implementation

- Reuse existing pattern: **structured inputs** (reciprocation tables + optional text) → **prompts** (e.g. “Summarise reciprocation status for the following measures and country matrix”) → **cleaned HTML** for a “Reciprocation” summary box.
- Prefer **table-driven** facts; use LLM for short narrative and labels, not for primary status (reciprocated yes/no should come from ETL).

---

## 5. Suggested Analysis & Presentation (Dashboard)

### Primary view (your idea, refined)

1. **Section: “Reciprocation of macroprudential measures”**
   - Short intro (ESRB voluntary framework, act-or-explain).
   - **Table 1 – Measures currently recommended for reciprocation**
     - Columns: Activating country, Measure type, Short description / rate, Recommendation date, (optional) Link to ESRB recommendation.
     - Rows: only measures that are “currently applicable” (active in source sheet / not revoked).
   - **Table 2 – Reciprocation status by country**
     - Rows: same measures (or measure × activating country).
     - Columns: all EEA countries (or “Reciprocating countries”).
     - Cell: **Reciprocated** (e.g. green) / **Not reciprocated** (e.g. red or grey) / **Partial** / **N/A** (e.g. activating country), with tooltip for date or source if available.
   - Optional: **Download** (e.g. Excel/CSV) of the same data.

2. **Filters**
   - By **activating country** (show only measures requested by DE, BE, etc.).
   - By **measure type** (CCyB, SyRB, BBM).
   - By **reciprocation status** (e.g. “Show only measures with at least one non-reciprocating country”).

3. **Insight cards or sidebar**
   - “X measures currently recommended for reciprocation.”
   - “Y countries have not yet reciprocated at least one measure.”
   - “Most recent recommendation: [Country] – [Date].”
   - Optional: one short AI-generated paragraph (from table-driven input) summarising the current picture.

### Secondary views (later)

- **Country profile:** In the country page, add “Reciprocation” tab or block: “Measures this country has requested for reciprocation” + “Measures this country has reciprocated (from others).”
- **Time dimension:** “Reciprocation over time” (recommendation date vs reciprocation date per country) if data allows.

---

## 6. Next Steps (Implementation)

1. **Inspect the Excel**  
   Run the script below (when the xlsx is present) to list sheets and sample the two reciprocation-related sheets:
   ```bash
   python scripts/inspect_reciprocation_sheets.py
   ```
   Then map columns to the canonical schema above.

2. **ETL**  
   Add `_process_reciprocation()` (or similar) in `etl.py` (or a dedicated module); output `processed_reciprocation.parquet` and a “latest” view; integrate into pipeline data dict.

3. **Render**  
   Add a “Reciprocation” subsection in the report template; add a partial that renders Table 1 + Table 2 from the processed data (and optional AI summary).

4. **Frontend**  
   Reuse existing patterns (table with country flags, filters, download link); optional JS for filters if you want client-side only.

5. **AI (optional)**  
   Add a small “reciprocation summary” task that takes the reciprocation tables and returns 2–4 sentences for the new section.

---

## 7. Script: Inspect Reciprocation Sheets

Save as `scripts/inspect_reciprocation_sheets.py` and run after downloading the ESRB file (e.g. run the pipeline once so `data/esrb.measures_overview_macroprudential_measures.xlsx` exists). It will print sheet names and the first rows of any sheet whose name contains “reciproc” or “recognition”.

See the script content in the repo at `scripts/inspect_reciprocation_sheets.py`.
