from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from prompts import SYSTEM_CONTEXT_LAST_12M


@dataclass(frozen=True)
class LLMTask:
    id: str
    prompt: str
    data: str = ""
    img: Optional[str] = None  # key into plot_paths
    temp: float = 0.2
    clean_global: bool = False  # whether to run _clean_text(..., is_global=True)


def build_chart_tasks(
    *,
    latest_ccyb_str: str,
    ccyb_decisions_str: str,
    active_syrb_str: str,
    syrb_decisions_str: str,
    active_bbm_str: str,
    bbm_decisions_str: str,
    ltv_table_str: str,
    news_str: str,
    capital_overall_str: str = "",
) -> List[LLMTask]:
    return [
        LLMTask(
            id="ccyb_diffusion_analysis",
            img="ccyb_diffusion",
            data=latest_ccyb_str,
            temp=0.2,
            prompt="Analyze CCyB adoption over the last 12 months. Emphasize country objectives and risks addressed (e.g., credit growth, property markets). Avoid tool descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="ccyb_history_analysis",
            img="ccyb_timeseries",
            data=latest_ccyb_str,
            temp=0.2,
            prompt="Highlight key CCyB changes in the last 12 months. Emphasize where objectives shifted and what risks authorities cite. Avoid explaining the CCyB mechanism. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="ccyb_level_analysis",
            img="cross_section_bar",
            data=latest_ccyb_str,
            temp=0.3,
            prompt="Compare current CCyB levels with emphasis on the last 12 months of changes. Focus on country goals and risks being targeted; avoid general tool descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="risk_analysis_text",
            img="risk_plot",
            data=latest_ccyb_str,
            temp=0.3,
            prompt="Interpret Credit Gap vs CCyB with a focus on the last 12 months. Emphasize risk signals and policy objectives across countries; avoid explaining mechanisms. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="ccyb_decisions_analysis",
            data=ccyb_decisions_str,
            temp=0.2,
            prompt="Summarize CCyB decisions from the last 12 months. Emphasize the risks cited and policy objectives; avoid tool explanations. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="syrb_trend_analysis",
            img="syrb_counts_trend",
            temp=0.2,
            prompt="Describe SyRB trends over the last 12 months. Emphasize objectives and risks (especially sectoral exposures) rather than tool mechanics. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="syrb_sectoral_analysis",
            img="syrb_sector",
            temp=0.2,
            prompt="Analyze SyRB sectoral composition with focus on the last 12 months. Highlight country targets and risk pockets; avoid mechanism descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="syrb_active_analysis",
            data=active_syrb_str,
            temp=0.3,
            prompt="Analyze active SyRB measures from the last 12 months. Emphasize country objectives and risks cited; avoid tool explanations. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="syrb_decisions_analysis",
            data=syrb_decisions_str,
            temp=0.2,
            prompt="Summarize SyRB decisions from the last 12 months. Emphasize risks addressed and policy objectives; avoid mechanism descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="bbm_analysis",
            data=active_bbm_str,
            temp=0.3,
            prompt="Analyze borrower-based measures with focus on the last 12 months. Emphasize country objectives and risks (e.g., housing credit risks), not tool mechanics. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="bbm_diffusion_analysis",
            img="bbm_diffusion",
            temp=0.2,
            prompt="Analyze adoption trends of borrower-based measures over the last 12 months. Emphasize what risks countries are targeting; avoid describing tool mechanics. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="bbm_decisions_analysis",
            data=bbm_decisions_str,
            temp=0.2,
            prompt="Summarize borrower-based measure decisions from the last 12 months. Emphasize objectives and risks cited; avoid mechanism descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences.",
        ),
        LLMTask(
            id="ltv_analysis",
            data=ltv_table_str,
            temp=0.2,
            prompt="Analyze LTV limits and first-time buyer exemptions with focus on the last 12 months. Emphasize objectives and risks, avoid mechanism explanations. Write ONE paragraph of 4-5 sentences.",
        ),
        LLMTask(
            id="news_summary",
            data=news_str,
            temp=0.2,
            prompt="Summarize the most important macroprudential news from the last 12 months. Focus on objectives and risks cited. Write ONE paragraph of 4-5 sentences.",
        ),
        LLMTask(
            id="capital_overall_analysis",
            img="capital_overall_buffers",
            data=capital_overall_str,
            temp=0.25,
            prompt=(
                "Analyze the overall capital buffer requirement by country based on the stacked components (CCoB, CCyB, GSII/O-SII, SyRB, sSyRB). "
                "Focus on differences across countries and what objectives/risks they reflect in the last 12 months; avoid explaining tool mechanics. "
                "Be specific and reference country patterns (high/low, concentration in specific components). "
                "Write ONE paragraph of 6-7 sentences."
            ),
        ),
    ]


def build_section_tasks(results: Dict[str, str]) -> List[LLMTask]:
    sys_ctx = SYSTEM_CONTEXT_LAST_12M
    return [
        LLMTask(
            id="ccyb_section_summary",
            temp=0.3,
            clean_global=True,
            prompt=f"""{sys_ctx}
TASK: Write a SPECIFIC high-level summary of the CCyB section focused on the last 12 months.
INPUTS (Context from charts):
- Adoption Trends: {results.get('ccyb_diffusion_analysis')}
- Current Levels: {results.get('ccyb_level_analysis')}
- Risks: {results.get('risk_analysis_text')}
- Decisions: {results.get('ccyb_decisions_analysis')}

STRUCTURE: 1-2 bullet points (HTML <li> tags).
REQUIREMENT: Be analytical. Emphasize country objectives and the risks being addressed. Avoid tool descriptions or mechanism explanations.
""",
        ),
        LLMTask(
            id="syrb_section_summary",
            temp=0.3,
            clean_global=True,
            prompt=f"""{sys_ctx}
TASK: Write a SPECIFIC high-level summary of the SyRB section focused on the last 12 months.
INPUTS (Context from charts):
- Usage Trends: {results.get('syrb_trend_analysis')}
- Sectoral Focus: {results.get('syrb_sectoral_analysis')}
- Active Measures: {results.get('syrb_active_analysis')}
- Recent Decisions: {results.get('syrb_decisions_analysis')}

STRUCTURE: 1-2 bullet points (HTML <li> tags).
REQUIREMENT: Be analytical. Emphasize objectives and targeted risks (e.g., sectoral exposures). Avoid tool descriptions or mechanism explanations.
""",
        ),
        LLMTask(
            id="bbm_section_summary",
            temp=0.3,
            clean_global=True,
            prompt=f"""{sys_ctx}
TASK: Write a SPECIFIC high-level summary of the Borrower-Based Measures (BBM) section focused on the last 12 months.
INPUTS (Context from analysis):
- Active BBM Analysis: {results.get('bbm_analysis')}

STRUCTURE: 1-2 bullet points (HTML <li> tags).
REQUIREMENT: Be analytical. Emphasize objectives and risks (housing leverage, affordability, credit quality). Avoid tool descriptions or mechanism explanations.
""",
        ),
        LLMTask(
            id="capital_overall_section_summary",
            temp=0.3,
            clean_global=True,
            prompt=f"""{sys_ctx}
TASK: Write a SPECIFIC high-level summary of the Capital Overall section focused on the last 12 months.
INPUTS:
- Overall Capital Buffer Analysis: {results.get('capital_overall_analysis')}

STRUCTURE: 1-2 bullet points (HTML <li> tags).
REQUIREMENT: Be analytical. Emphasize objectives and risks signaled by higher/lower buffer requirements. Avoid tool descriptions or mechanism explanations.
""",
        ),
    ]


def build_global_task(results: Dict[str, str]) -> LLMTask:
    sys_ctx = SYSTEM_CONTEXT_LAST_12M
    return LLMTask(
        id="executive_summary",
        temp=0.5,
        clean_global=True,
        prompt=f"""{sys_ctx}
TASK: Write a comprehensive Global Executive Summary focused on the last 12 months.
STRUCTURE: 4-5 paragraphs, each 5-6 sentences long. Each paragraph must start with a <b>bold topic sentence</b>.
CONTENT: Synthesize the findings. Emphasize country objectives and the risks being addressed, and how recent trends shifted the overall stance. Avoid explaining tool mechanics.

INPUTS:
CCyB Overview: {results.get('ccyb_section_summary')}
SyRB Overview: {results.get('syrb_section_summary')}
BBM Overview: {results.get('bbm_section_summary')}
Capital Overall Overview: {results.get('capital_overall_section_summary')}
""",
    )

