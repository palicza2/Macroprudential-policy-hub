---
name: llm-grounding
description: >-
  Guides Gemini summaries, BBM field extraction, claim grounding, and LLM cache
  for this hub. Use when editing prompts, llm_tasks, llm_analysis, llm_runner,
  grounding_validator, RUN_GROUNDING, analyses, or keyword/rate extraction.
---

# LLM Grounding

The pipeline uses **versioned tasks** (prompt in, text out), not a tool-calling agent. Do not replace `run_tasks` / `AIStage` with an autonomous LangGraph loop.

## Style contract (`prompts.py`)

`SYSTEM_CONTEXT_LAST_12M`:

- Role: financial analyst; last **12 months** unless older context is briefly needed
- Emphasize **country objectives and risks addressed**
- **Do not** explain what CCyB/SyRB/LTV are or how tools transmit
- **DATA tables are the source of truth for numbers and rates** — do not estimate or round differently than the table

Task prompts in `llm_tasks.py` repeat: strong topic sentence, one paragraph, avoid tool mechanics.

If a number appears in the DATA table, copy that number.

## Analysis DAG (`LLMAnalyzer.run_analysis`)

1. Independent chart/table tasks (`build_chart_tasks`) — may attach PNG from `figures/`
2. O-SII task if data exists
3. Section summaries from chart results (`build_section_tasks`)
4. Executive summary (`build_global_task`) — 4–5 paragraphs, bold topic sentences

`llm_runner.run_tasks` is sequential. Chart tasks do not depend on each other; do not add hidden cross-task mutation. Bounded parallelism is OK; changing skip/cache keys is not.

Model: `gemini-2.5-flash-lite` (`config.LLM_CONFIG`). Temperatures live on `LLMTask` (typically 0.2–0.3). Extraction should stay near `0.0`.

## Two caches

| Cache | Where | Key / when |
|-------|--------|------------|
| Prompt cache | `cache/llm/` via `llm/cache.py` | MD5 of prompt + data + img_key + model + temperature |
| Stage cache | `data/analyses_cache.json`, `countries_data.json` | Manifest: silver + figures + LLM fingerprint |

Changing `prompts.py` / `llm_tasks.py` / `llm_analysis.py` (and other files in `LLM_RELATIVE`) **invalidates** the stage skip. Prompt-cache hits still apply if the hashed prompt is unchanged.

Retries: `utils.retry.retry_with_backoff` in `llm_runner.py`. Failed tasks become `"N/A"` — do not crash the pipeline.

## Grounding (`grounding_validator.py`)

Off unless `RUN_GROUNDING` is `1`/`true`/`yes`/`on`.

Fixed graph (not an agent): extract claims → verify vs tables/charts → optional Google CSE on **allowed domains** → revise text. Writes `data/validation_report.json` (gitignored).

Search: `SEARCH_CONFIG` / `CUSTOM_SEARCH_API_KEY` + `GOOGLE_CSE_ID`. Citations look like `(Source: URL)`.

Prefer **cheap numeric checks** against parquet over enabling search-based grounding by default.

## Extraction vs narrative

- **Narrative** (charts, sections, exec): free text, then `_clean_text` (strip `#`/`$`, bold → `<b>`, global → `<p>`/`<ul>`).
- **Structured** (keywords, LTV/DTI fields, delta verdicts): JSON via `utils/json_parser.py`. Prefer Gemini JSON / structured output over regex-repair when adding new extractors. Require evidence quotes for limits.

BBM dashboard rows still come from gold CSVs; see skill `bbm-gold`.

## Knowledge graph

Orchestrator passes empty graph data. `analyses['knowledge_graph_analysis']` is a stub. `KnowledgeGraphRAG` exists but is unused in the live DAG. Do not add a second long LLM essay. If re-enabled, keep it behind a flag and use graph stats as **retrieval context** for the existing global summary.

## Institutional copy

Country profile descriptions use a **confidence 0–1** badge plus grounding notes and source list. `1.0` = structured data only, `0.5` = partial inference, `0.0` = no data. Do not invent NMA/NDA names.

## Do not

- Hallucinate country counts or buffer rates
- Enable `RUN_GROUNDING` in CI/docs examples as if it were free
- Duplicate `_safe_json_loads` — use `utils/json_parser.py`
- Put secrets in prompts or commit `cache/`

## Code map

- Tasks: `llm_tasks.py`, `prompts.py`
- Runner + cache: `llm_runner.py`, `llm/cache.py`
- Analyzer: `llm_analysis.py`
- Stage: `pipeline/stages/ai_stage.py`
- Grounding: `grounding_validator.py`
