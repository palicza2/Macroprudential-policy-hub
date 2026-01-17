import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, StateGraph

from llm_analysis import df_to_string

logger = logging.getLogger(__name__)


DEFAULT_ANALYSIS_IDS = [
    "executive_summary",
    "ccyb_section_summary",
    "syrb_section_summary",
    "bbm_section_summary",
    "ccyb_diffusion_analysis",
    "ccyb_history_analysis",
    "ccyb_level_analysis",
    "risk_analysis_text",
    "ccyb_decisions_analysis",
    "syrb_trend_analysis",
    "syrb_sectoral_analysis",
    "syrb_active_analysis",
    "syrb_decisions_analysis",
    "bbm_analysis",
    "bbm_diffusion_analysis",
    "bbm_decisions_analysis",
]


ANALYSIS_CONSTRAINTS = {
    "executive_summary": (
        "STRUCTURE: 4-5 paragraphs, each 5-6 sentences. "
        "Each paragraph must start with a <b>bold topic sentence</b>."
    ),
    "ccyb_section_summary": "STRUCTURE: 1-2 bullet points using '-' prefix. Each bullet 3-4 sentences.",
    "syrb_section_summary": "STRUCTURE: 1-2 bullet points using '-' prefix. Each bullet 3-4 sentences.",
    "bbm_section_summary": "STRUCTURE: 1-2 bullet points using '-' prefix. Each bullet 3-4 sentences.",
}


def _get_llm(config: Dict[str, Any], temperature: float = 0.2):
    return ChatGoogleGenerativeAI(
        model=config["model_name"],
        temperature=temperature,
        max_tokens=config.get("max_output_tokens", 1000),
    )


def _safe_json_loads(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        pass
    if not text:
        return None
    match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", text)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            return None
    return None


def _invoke_json(llm, prompt: str, retry_suffix: str = "", default: Optional[Any] = None) -> Any:
    res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
    parsed = _safe_json_loads(res)
    if parsed is not None:
        return parsed
    if retry_suffix:
        res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt + retry_suffix)])
        parsed = _safe_json_loads(res)
        if parsed is not None:
            return parsed
    return default


def _fallback_claims(analyses: Dict[str, str], analysis_ids: List[str]) -> List[Dict[str, Any]]:
    claims = []
    for analysis_id in analysis_ids:
        text = analyses.get(analysis_id, "") or ""
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        for sent in sentences[:3]:
            claims.append({"analysis_id": analysis_id, "claim": sent})
    return claims


def _load_allowed_domains(search_config: Dict[str, Any]) -> List[str]:
    base = list(search_config.get("allowed_domains", []))
    env_key = search_config.get("allowed_domains_env", "SEARCH_ALLOWED_DOMAINS")
    env_val = os.getenv(env_key, "")
    if env_val:
        base.extend([d.strip() for d in env_val.split(",") if d.strip()])
    return sorted(set(base))


def _google_search(query: str, search_config: Dict[str, Any]) -> List[Dict[str, str]]:
    enabled = search_config.get("enabled", True)
    enabled_env = search_config.get("search_enabled_env", "SEARCH_ENABLED")
    env_val = os.getenv(enabled_env)
    if env_val is not None:
        enabled = env_val.strip().lower() in ("1", "true", "yes", "on")
    if not enabled:
        return []
    api_key = os.getenv(search_config.get("api_key_env", "GOOGLE_API_KEY"), "")
    cse_id = os.getenv(search_config.get("cse_id_env", "GOOGLE_CSE_ID"), "")
    if not api_key or not cse_id:
        logger.warning("Google Search is not configured. Set GOOGLE_API_KEY and GOOGLE_CSE_ID.")
        return []

    allowed_domains = _load_allowed_domains(search_config)
    max_results = int(search_config.get("max_results", 5))

    domain_query = " OR ".join([f"site:{d}" for d in allowed_domains]) if allowed_domains else ""
    full_query = f"{query} {domain_query}".strip()

    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": api_key, "cx": cse_id, "q": full_query}
    try:
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code == 429:
            time.sleep(10)
            resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        items = resp.json().get("items", [])[:max_results]
    except Exception as exc:
        logger.error(f"Google Search error: {exc}")
        return []

    results = []
    for item in items:
        link = item.get("link", "")
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        if allowed_domains:
            host = urlparse(link).netloc.lower()
            if not any(host.endswith(d) for d in allowed_domains):
                continue
        results.append({"title": title, "link": link, "snippet": snippet})
    return results


def _build_data_context(data_inputs: Dict[str, Any]) -> str:
    parts = [
        "LATEST CCyB TABLE:\n" + df_to_string(data_inputs.get("latest_ccyb_df")),
        "LATEST SyRB TABLE:\n" + df_to_string(data_inputs.get("latest_syrb_df")),
        "ACTIVE SyRB TABLE:\n" + df_to_string(data_inputs.get("active_syrb_df")),
        "LATEST BBM TABLE:\n" + df_to_string(data_inputs.get("latest_bbm_df")),
    ]
    return "\n\n".join(parts)


def _summarize_trend(df, date_col: str, value_cols: List[str]) -> str:
    if df is None or df.empty:
        return "No trend data available."
    df = df.sort_values(date_col)
    start = df.iloc[0]
    end = df.iloc[-1]
    lines = [f"Start date: {start[date_col]} | End date: {end[date_col]}"]
    for col in value_cols:
        if col in df.columns:
            lines.append(
                f"{col}: start={start[col]}, end={end[col]}, max={df[col].max()}"
            )
    return "\n".join(lines)


def _build_chart_context(data: Dict[str, Any]) -> str:
    parts = []
    agg_trend = data.get("agg_trend_df")
    if agg_trend is not None and not agg_trend.empty:
        parts.append("CCyB Adoption Trend:\n" + _summarize_trend(agg_trend, "date", ["n_positive"]))

    syrb_trend = data.get("syrb_trend_df")
    if syrb_trend is not None and not syrb_trend.empty:
        parts.append(
            "SyRB Trend:\n" + _summarize_trend(syrb_trend, "date", ["General SyRB", "Sectoral SyRB"])
        )

    bbm_trend = data.get("bbm_trend_df")
    if bbm_trend is not None and not bbm_trend.empty:
        parts.append("BBM Trend:\n" + _summarize_trend(bbm_trend, "date", ["n_countries"]))

    return "\n\n".join(parts) if parts else "No chart context available."


@dataclass
class ValidatorState:
    analyses: Dict[str, str]
    analysis_ids: List[str]
    data_context: str
    chart_context: str
    claims: List[Dict[str, Any]]
    claim_checks: List[Dict[str, Any]]
    search_results: List[Dict[str, Any]]
    revised_analyses: Dict[str, str]


class GroundingValidator:
    def __init__(self, llm_config: Dict[str, Any], search_config: Dict[str, Any], clean_text_func):
        self.llm_config = llm_config
        self.search_config = search_config
        self.clean_text = clean_text_func

    def _extract_claims(self, state: ValidatorState) -> ValidatorState:
        llm = _get_llm(self.llm_config, temperature=0.1)
        analysis_payload = {k: state.analyses.get(k, "") for k in state.analysis_ids}
        prompt = (
            "TASK: Extract 3-6 factual claims from each analysis. "
            "Claims should be verifiable and include numbers, rates, directions, or country references. "
            "Return ONLY JSON array with objects: {\"analysis_id\": \"...\", \"claims\": [\"...\"]}.\n"
            f"INPUT:\n{json.dumps(analysis_payload)}"
        )
        try:
            parsed = _invoke_json(
                llm,
                prompt,
                retry_suffix="\nIMPORTANT: Return ONLY valid JSON. No prose, no markdown.",
                default=None,
            )
            if parsed is None:
                raise ValueError("Claim extraction did not return valid JSON.")
            claims = []
            for item in parsed:
                analysis_id = item.get("analysis_id")
                for claim in item.get("claims", [])[:3]:
                    if analysis_id and claim:
                        claims.append({"analysis_id": analysis_id, "claim": claim})
            state.claims = claims
        except Exception as exc:
            logger.error(f"Claim extraction failed: {exc}")
            state.claims = _fallback_claims(state.analyses, state.analysis_ids)
        return state

    def _verify_claims(self, state: ValidatorState) -> ValidatorState:
        llm = _get_llm(self.llm_config, temperature=0.1)
        checks = []
        for item in state.claims:
            claim = item["claim"]
            prompt = (
                "TASK: Verify the CLAIM using DATA CONTEXT and CHART CONTEXT. "
                "Respond in JSON with keys: verdict (supported/contradicted/unclear), "
                "correction (if contradicted), evidence (short). "
                "If unclear, suggest a short search_query.\n\n"
                f"CLAIM: {claim}\n\nDATA CONTEXT:\n{state.data_context}\n\nCHART CONTEXT:\n{state.chart_context}"
            )
            try:
                verdict_obj = _invoke_json(
                    llm,
                    prompt,
                    retry_suffix="\nIMPORTANT: Return ONLY valid JSON. No prose, no markdown.",
                    default=None,
                )
                if verdict_obj is None:
                    raise ValueError("Verification did not return valid JSON.")
            except Exception:
                verdict_obj = {"verdict": "unclear", "correction": "", "evidence": "", "search_query": claim}
            verdict_obj.update({"analysis_id": item["analysis_id"], "claim": claim})
            checks.append(verdict_obj)
        state.claim_checks = checks
        return state

    def _external_search(self, state: ValidatorState) -> ValidatorState:
        results = []
        for check in state.claim_checks:
            verdict = str(check.get("verdict", "")).lower()
            if verdict not in ("unclear", "contradicted"):
                continue
            query = check.get("search_query") or check.get("claim")
            if not query:
                continue
            hits = _google_search(query, self.search_config)
            if hits:
                results.append({"analysis_id": check["analysis_id"], "claim": check["claim"], "hits": hits})
        state.search_results = results
        return state

    def _revise_text(self, state: ValidatorState) -> ValidatorState:
        llm = _get_llm(self.llm_config, temperature=0.3)
        revised = dict(state.analyses)

        issue_map: Dict[str, List[Dict[str, Any]]] = {}
        for chk in state.claim_checks:
            verdict = str(chk.get("verdict", "")).lower()
            if verdict in ("contradicted", "unclear"):
                issue_map.setdefault(chk["analysis_id"], []).append(chk)

        sources_map: Dict[str, List[Dict[str, Any]]] = {}
        for res in state.search_results:
            sources_map.setdefault(res["analysis_id"], []).extend(res["hits"])

        for analysis_id, issues in issue_map.items():
            original = state.analyses.get(analysis_id, "")
            sources = sources_map.get(analysis_id, [])
            constraints = ANALYSIS_CONSTRAINTS.get(analysis_id, "")

            prompt = (
                "TASK: Revise the ANALYSIS text to correct any unsupported or contradicted claims. "
                "Use DATA CONTEXT and SOURCES to ground facts. "
                "If sources are provided, include at most 1-2 short citations in the form (Source: URL). "
                "Keep the tone professional and concise. "
                f"{constraints}\n\n"
                f"ANALYSIS ID: {analysis_id}\n"
                f"ORIGINAL TEXT:\n{original}\n\n"
                f"ISSUES:\n{json.dumps(issues)}\n\n"
                f"DATA CONTEXT:\n{state.data_context}\n\n"
                f"SOURCES:\n{json.dumps(sources)}\n"
            )

            try:
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
                is_global = analysis_id in {
                    "executive_summary",
                    "ccyb_section_summary",
                    "syrb_section_summary",
                    "bbm_section_summary",
                }
                revised[analysis_id] = self.clean_text(res, is_global=is_global)
            except Exception as exc:
                logger.error(f"Revision failed for {analysis_id}: {exc}")
                revised[analysis_id] = original

        state.revised_analyses = revised
        return state

    def run(self, analyses: Dict[str, str], data_inputs: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, str]:
        analysis_ids = [a for a in DEFAULT_ANALYSIS_IDS if a in analyses]
        state = ValidatorState(
            analyses=analyses,
            analysis_ids=analysis_ids,
            data_context=_build_data_context(data_inputs),
            chart_context=_build_chart_context(data),
            claims=[],
            claim_checks=[],
            search_results=[],
            revised_analyses={},
        )

        graph = StateGraph(ValidatorState)
        graph.add_node("extract_claims", self._extract_claims)
        graph.add_node("verify_claims", self._verify_claims)
        graph.add_node("external_search", self._external_search)
        graph.add_node("revise_text", self._revise_text)
        graph.set_entry_point("extract_claims")
        graph.add_edge("extract_claims", "verify_claims")
        graph.add_edge("verify_claims", "external_search")
        graph.add_edge("external_search", "revise_text")
        graph.add_edge("revise_text", END)
        compiled = graph.compile()

        final_state = compiled.invoke(state)
        report_path = self.search_config.get("report_path")
        if report_path:
            try:
                if isinstance(final_state, dict):
                    report_payload = {
                        "claims": final_state.get("claims", []),
                        "claim_checks": final_state.get("claim_checks", []),
                        "search_results": final_state.get("search_results", []),
                    }
                else:
                    report_payload = {
                        "claims": final_state.claims,
                        "claim_checks": final_state.claim_checks,
                        "search_results": final_state.search_results,
                    }
                os.makedirs(os.path.dirname(report_path), exist_ok=True)
                with open(report_path, "w", encoding="utf-8") as f:
                    json.dump(report_payload, f, ensure_ascii=False, indent=2)
            except Exception as exc:
                logger.warning(f"Failed to write validation report: {exc}")

        if isinstance(final_state, dict):
            return final_state.get("revised_analyses") or analyses
        return final_state.revised_analyses or analyses
