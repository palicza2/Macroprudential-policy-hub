"""
BBM (Borrower-Based Measures) processing stage.

Dashboard tables come from gold CSVs. ESRB text is hashed; the LLM is only
used when a description changes, a country appears, or a gold row has no
matching active measure.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from pipeline.writers.supabase_writer import SupabaseWriter

from bbm import build_bbm_matrix_html
from bbm.dti_lti.items_builder import build_dti_lti_items
from bbm.dti_lti_builder import build_dti_lti_comparison_df_structured
from bbm.ltv_builder import build_ltv_comparison_df_structured, build_ltv_items
from bbm.delta_checker import check_gold_deltas
from bbm.gold import (
    FAMILY_DTI,
    FAMILY_LTV,
    GoldPaths,
    KIND_CHANGED,
    KIND_NEW,
    apply_state_updates,
    build_gold_list_html,
    classify_family,
    dti_gold_index,
    dti_gold_to_structured,
    esrb_iso_aliases,
    load_dti_gold,
    load_ltv_gold,
    load_state,
    ltv_gold_index,
    save_json,
    save_ltv_gold,
    save_state,
    should_skip_new_extract,
    summarize_report,
)
from bbm.dti_excel_loader import load_dti_expert_table

logger = logging.getLogger(__name__)

# Safety valve: do not re-run full extraction for a large set of "new" ESRB hits.
_MAX_NEW_EXTRACT = 8


def build_dti_lti_comparison_df(bbm_full: pd.DataFrame, analyzer, search_config=None) -> pd.DataFrame:
    """Wrapper kept for scripts that still import this name."""
    from config import SEARCH_CONFIG
    return build_dti_lti_comparison_df_structured(
        bbm_full,
        analyzer,
        validate_with_ai=True,
        final_validation_with_search=False,
        search_config=search_config or SEARCH_CONFIG,
    )


def _filter_countries(bbm_full: pd.DataFrame, countries: List[str]) -> pd.DataFrame:
    aliases = set()
    for c in countries:
        aliases |= esrb_iso_aliases(c)
    if "iso2" not in bbm_full.columns:
        return bbm_full.iloc[0:0]
    return bbm_full[bbm_full["iso2"].astype(str).str.upper().isin(aliases)].copy()


def _unique(seq: List[str]) -> List[str]:
    return list(dict.fromkeys(seq))


def _extract_new_dti(bbm_full, analyzer, search_config, countries: List[str]) -> pd.DataFrame:
    subset = _filter_countries(bbm_full, countries)
    if subset.empty:
        return pd.DataFrame()
    logger.info("   -> Extracting new DTI/LTI countries for review: %s", ", ".join(sorted(set(countries))))
    return build_dti_lti_comparison_df_structured(
        subset,
        analyzer,
        validate_with_ai=bool(analyzer),
        final_validation_with_search=False,
        search_config=search_config,
    )


def _extract_ltv(bbm_full, analyzer, search_config, countries: Optional[List[str]] = None) -> pd.DataFrame:
    subset = bbm_full if not countries else _filter_countries(bbm_full, countries)
    if subset.empty:
        return pd.DataFrame()
    return build_ltv_comparison_df_structured(
        subset,
        analyzer,
        validate_with_ai=bool(analyzer),
        final_validation_with_search=False,
        search_config=search_config,
    )


class BBMStage:
    """Processes BBM data: matrix, decisions, gold tables, delta review."""

    def __init__(self, analyzer, search_config=None, supabase_writer: SupabaseWriter = None, paths: GoldPaths = None):
        self.analyzer = analyzer
        self.search_config = search_config
        self.supabase_writer = supabase_writer or SupabaseWriter()
        self.paths = paths or GoldPaths.from_config()
        self.skip_llm = False

    def process(self, bbm_full: pd.DataFrame, skip_llm: bool = False, skip_supabase: bool = False) -> Dict[str, Any]:
        from config import BBM_GOLD_DELTA_AI, SEARCH_CONFIG

        if self.search_config is None:
            self.search_config = SEARCH_CONFIG

        empty = {
            "active_bbm": pd.DataFrame(),
            "bbm_decisions": pd.DataFrame(),
            "bbm_pivot_html": "",
            "bbm_ref_date": "",
            "ltv_table": pd.DataFrame(),
            "ltv_ref_date": "",
            "dti_lti_compare": pd.DataFrame(),
            "dti_expert_table": load_dti_expert_table(),
            "dti_lti_eu_list_html": "",
            "bbm_delta_report": {},
        }

        if bbm_full is None or bbm_full.empty:
            return empty

        logger.info("   -> BBM processing (gold + delta)...")
        self.skip_llm = skip_llm
        active_bbm = bbm_full[bbm_full["active_status"] == "Active"].copy()
        bbm_pivot_html, bbm_ref_date = build_bbm_matrix_html(bbm_full)
        bbm_decisions = self._recent_decisions(bbm_full)

        dti_gold = load_dti_gold(self.paths)
        ltv_gold = load_ltv_gold(self.paths)
        state = load_state(self.paths.state_json)

        dti_esrb = build_dti_lti_items(bbm_full)
        ltv_esrb = build_ltv_items(bbm_full)

        dti_items = classify_family(FAMILY_DTI, dti_esrb, dti_gold_index(dti_gold), state)
        ltv_items = classify_family(FAMILY_LTV, ltv_esrb, ltv_gold_index(ltv_gold), state)

        notes: List[str] = []
        ltv_table = ltv_gold.copy() if ltv_gold is not None else pd.DataFrame()

        # First LTV gold: extract once, write CSV, treat as bootstrapped gold.
        if (ltv_gold is None or ltv_gold.empty) and ltv_esrb:
            logger.info("   -> No LTV gold CSV; extracting once to bootstrap data/ltv_gold.csv")
            try:
                boot = _extract_ltv(bbm_full, self.analyzer, self.search_config)
                if boot is not None and not boot.empty:
                    save_ltv_gold(boot, self.paths)
                    ltv_table = boot
                    ltv_gold = boot
                    ltv_items = classify_family(
                        FAMILY_LTV, ltv_esrb, ltv_gold_index(ltv_gold), {"entries": {}}
                    )
                    notes.append(
                        "LTV gold was bootstrapped from ESRB extraction. Review data/ltv_gold.csv."
                    )
                else:
                    notes.append("LTV bootstrap extraction returned no rows.")
            except Exception as exc:
                logger.warning("LTV gold bootstrap failed: %s", exc)
                notes.append(f"LTV bootstrap failed: {exc}")

        changed = [i for i in dti_items + ltv_items if i.kind == KIND_CHANGED]
        if BBM_GOLD_DELTA_AI:
            verdicts = check_gold_deltas(changed, self.analyzer)
        else:
            logger.info("   -> BBM_GOLD_DELTA_AI disabled; flagging description changes without LLM")
            verdicts = {
                i.key: {
                    "key": i.key,
                    "country": i.country,
                    "verdict": "unclear",
                    "conflicting_fields": [],
                    "evidence_excerpt": "",
                    "proposed_patch": {},
                    "reason": "BBM_GOLD_DELTA_AI disabled",
                }
                for i in changed
            }

        proposals: List[Dict[str, Any]] = []
        new_dti_countries = _unique([
            i.country for i in dti_items
            if i.kind == KIND_NEW and not should_skip_new_extract(state, i)
        ])
        new_ltv_countries = _unique([
            i.country for i in ltv_items
            if i.kind == KIND_NEW and not should_skip_new_extract(state, i)
        ])

        if len(new_dti_countries) > _MAX_NEW_EXTRACT:
            notes.append(
                f"{len(new_dti_countries)} new DTI/LTI ESRB items (cap {_MAX_NEW_EXTRACT}); "
                "listed in the delta report, not auto-extracted."
            )
            new_dti_countries = []
        if len(new_ltv_countries) > _MAX_NEW_EXTRACT:
            notes.append(
                f"{len(new_ltv_countries)} new LTV ESRB items (cap {_MAX_NEW_EXTRACT}); "
                "listed in the delta report, not auto-extracted."
            )
            new_ltv_countries = []

        if new_dti_countries:
            try:
                extracted = _extract_new_dti(
                    bbm_full, self.analyzer, self.search_config, new_dti_countries
                )
                if extracted is not None and not extracted.empty:
                    proposals.append({
                        "family": FAMILY_DTI,
                        "action": "append to data/dti_expert_table.csv after review",
                        "rows": extracted.to_dict(orient="records"),
                    })
            except Exception as exc:
                logger.warning("New DTI extract failed: %s", exc)
                notes.append(f"New DTI extract failed: {exc}")
        skipped_new = [i.key for i in dti_items + ltv_items if should_skip_new_extract(state, i)]
        if skipped_new:
            notes.append(
                "Skipped re-extract for unchanged new-country proposals: " + ", ".join(skipped_new)
            )

        if new_ltv_countries and ltv_gold is not None and not ltv_gold.empty:
            try:
                extracted = _extract_ltv(
                    bbm_full, self.analyzer, self.search_config, new_ltv_countries
                )
                if extracted is not None and not extracted.empty:
                    proposals.append({
                        "family": FAMILY_LTV,
                        "action": "append to data/ltv_gold.csv after review",
                        "rows": extracted.to_dict(orient="records"),
                    })
            except Exception as exc:
                logger.warning("New LTV extract failed: %s", exc)
                notes.append(f"New LTV extract failed: {exc}")

        state = apply_state_updates(state, dti_items + ltv_items, verdicts)
        save_state(self.paths.state_json, state)

        report = summarize_report(dti_items, ltv_items, verdicts, proposals, notes)
        save_json(self.paths.report_json, report)
        if proposals:
            save_json(self.paths.proposals_json, proposals)

        n_dti = report["dti"]
        n_ltv = report["ltv"]
        logger.info(
            "   -> DTI gold: %s unchanged, %s changed, %s new, %s withdrawn",
            n_dti["unchanged"], n_dti["changed"], n_dti["new"], n_dti["withdrawn"],
        )
        logger.info(
            "   -> LTV gold: %s unchanged, %s changed, %s new, %s withdrawn",
            n_ltv["unchanged"], n_ltv["changed"], n_ltv["new"], n_ltv["withdrawn"],
        )
        if report["review"]:
            logger.info("   -> Delta review items: %s (see %s)", len(report["review"]), self.paths.report_json)

        dti_structured = dti_gold_to_structured(dti_gold)
        dti_list_html = build_gold_list_html(dti_gold, dti_items)

        ltv_ref_date = ""
        ltv_active = bbm_full[
            (bbm_full["active_status"] == "Active")
            & (bbm_full["measure_type"].astype(str).str.contains("LTV", case=False, na=False))
        ]
        if not ltv_active.empty:
            max_date = ltv_active["date"].max()
            if pd.notna(max_date):
                ltv_ref_date = max_date.strftime("%Y-%m-%d")

        if self.supabase_writer.is_enabled() and not skip_supabase:
            logger.info("Writing gold BBM tables to Supabase...")
            results = self.supabase_writer.write_bbm_structured_data(
                dti_lti_df=dti_structured,
                ltv_df=ltv_table,
            )
            if results:
                logger.info("Supabase BBM write results: %s", results)

        return {
            "active_bbm": active_bbm,
            "bbm_decisions": bbm_decisions,
            "bbm_pivot_html": bbm_pivot_html,
            "bbm_ref_date": bbm_ref_date,
            "ltv_table": ltv_table,
            "ltv_ref_date": ltv_ref_date,
            "dti_lti_compare": dti_structured,
            "dti_expert_table": dti_gold,
            "dti_lti_eu_list_html": dti_list_html,
            "bbm_delta_report": report,
        }

    def _recent_decisions(self, bbm_full: pd.DataFrame) -> pd.DataFrame:
        bbm_decisions = bbm_full.sort_values("date", ascending=False).head(10).copy()
        cols = ["date", "iso2", "measure_type", "status", "description"]
        bbm_decisions = bbm_decisions[[c for c in cols if c in bbm_decisions.columns]]
        if bbm_decisions.empty:
            return bbm_decisions

        logger.info("   -> BBM AI cleaning (Decisions)...")
        if "date" in bbm_decisions.columns:
            bbm_decisions["date"] = pd.to_datetime(bbm_decisions["date"]).dt.strftime("%Y-%m-%d")

        if self.analyzer is not None and not self.skip_llm:
            details = self.analyzer.extract_keywords(
                bbm_decisions["description"].astype(str).tolist(),
                "targeted risk or background",
            )
            bbm_decisions["description"] = details

        bbm_decisions.columns = [c.upper() for c in bbm_decisions.columns]
        return bbm_decisions.rename(columns={
            "DATE": "DATE",
            "ISO2": "COUNTRY",
            "MEASURE_TYPE": "TYPE",
            "STATUS": "STATUS",
            "DESCRIPTION": "DETAILS",
        })
