"""
DTI/LTI EU List Builder.
Builds HTML list of EU countries with DTI/LTI limits (AI verified).
"""

import pandas as pd

from .items_builder import build_dti_lti_items, EU_ISO2
from ..matrix_builder import RENAME_MAP


def build_dti_lti_eu_list_html(bbm_full: pd.DataFrame, analyzer) -> str:
    """
    Ask the AI to strictly confirm (with a direct quote) which EU countries have an explicit DTI/LTI limit
    in the ESRB BBM descriptions. Returns a small HTML snippet (safe to embed).
    Also includes explanation of differences with the pivot table.
    """
    items = build_dti_lti_items(bbm_full)
    if not items:
        return "<p class='no-data'>No DTI/LTI candidates in ESRB BBM data.</p>"

    # Get all DTI/LTI records (including inactive) for comparison
    df_all = bbm_full.copy()
    df_all["measure_short"] = df_all["measure_type"].astype(str).map(lambda x: RENAME_MAP.get(x, x))
    df_all["iso2"] = df_all.get("iso2").astype(str)
    df_all = df_all[df_all["iso2"].isin(EU_ISO2)].copy()
    df_all_dti_lti = df_all[df_all["measure_short"].isin(["DTI", "LTI"])].copy()
    pivot_countries = set(df_all_dti_lti["iso2"].dropna().unique())

    confirmations = analyzer.confirm_dti_lti_presence(items)
    confirmed = []
    confirmed_iso2s = set()
    for it, c in zip(items, confirmations):
        if not isinstance(c, dict):
            continue
        if str(c.get("confirmed", "")).strip().lower() != "yes":
            continue
        if str(c.get("confidence", "")).strip().lower() != "high":
            continue
        evidence = str(c.get("evidence_excerpt", "")).strip()
        if not evidence:
            continue
        typ = str(c.get("type", "")).strip().upper()
        if typ not in {"DTI", "LTI"}:
            continue
        iso2 = str(it.get("iso2", "")).strip()
        country = str(it.get("country", "")).strip()
        confirmed.append((iso2, country, typ, evidence))
        confirmed_iso2s.add(iso2)

    if not confirmed:
        return "<p class='no-data'>AI could not confirm any DTI/LTI limits with high confidence.</p>"

    # Deduplicate by iso2+type
    seen = set()
    lines = []
    for iso2, country, typ, evidence in confirmed:
        key = (iso2, typ)
        if key in seen:
            continue
        seen.add(key)
        safe_ev = evidence.replace("<", "&lt;").replace(">", "&gt;")
        lines.append(f"<li><strong>{iso2}</strong> ({country}) — <strong>{typ}</strong><br><span style='color:#64748b;font-size:0.85em'>\"{safe_ev}\"</span></li>")

    # Add explanation for missing countries
    missing = pivot_countries - confirmed_iso2s
    if missing:
        missing_details = []
        for iso2_m in sorted(missing):
            sub = df_all_dti_lti[df_all_dti_lti["iso2"] == iso2_m]
            if not sub.empty:
                country_m = sub["country"].iloc[0]
                statuses = sub["active_status"].unique()
                reasons = []
                if all(str(s) == "Inactive" for s in statuses):
                    reasons.append("inactive status in ESRB data")
                elif any("not active" in str(s).lower() for s in sub["status"].unique()):
                    reasons.append("marked as 'Not active' in ESRB")
                else:
                    reasons.append("AI could not confirm with high confidence")
                missing_details.append(f"<strong>{iso2_m}</strong> ({country_m}): {', '.join(reasons)}")
        
        if missing_details:
            lines.append("</ul>")
            lines.append("<div style='margin-top:12px; padding-top:12px; border-top:1px solid #e2e8f0; font-size:0.9em; color:#64748b;'>")
            lines.append("<strong>Note:</strong> The following countries appear in the pivot table but are excluded here: ")
            lines.append("<ul style='margin:6px 0 0 18px; padding-left:0;'>")
            lines.extend(f"<li>{d}</li>" for d in missing_details)
            lines.append("</ul>")
            lines.append("The AI verification only includes countries with <strong>Active</strong> status and explicit, verifiable limits.</div>")
            return f"<ul style='margin:0; padding-left: 18px;'>{''.join(lines)}"
    
    return "<ul style='margin:0; padding-left: 18px;'>" + "".join(lines) + "</ul>"
