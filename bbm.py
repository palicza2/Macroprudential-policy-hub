from __future__ import annotations

import re
from typing import Dict, Tuple

import pandas as pd


RENAME_MAP: Dict[str, str] = {
    "Loan-to-value (LTV)": "LTV",
    "Debt-service-to-income (DSTI)": "DSTI",
    "Loan-to-income (LTI)": "LTI",
    "DTI": "DTI",  # Already short in Excel
    "LTI": "LTI",  # Already short in Excel (if present)
    "Loan maturity": "Maturity",
    "Loan amortisation": "Amort.",
    "Flexibility quota": "Flex.",
    "Stress test / sensitivity test": "Stress T.",
}


def extract_ltv_details_regex(text: str) -> Tuple[str, str, str, str]:
    text = str(text or "")
    limits = re.findall(r"(\d+(?:\.\d+)?)\s*%", text)
    limits = sorted({f"{l}%" for l in limits}, key=lambda x: float(x.strip("%")))
    limits_str = ", ".join(limits) if limits else "N/A"

    ftb_markers = ["first-time buyer", "first time buyer", "ftb", "first-time buyers", "first time buyers"]
    ftb_present = any(m in text.lower() for m in ftb_markers)
    ftb_flag = "Yes" if ftb_present else "No"

    sentences = re.split(r"(?<=[.!?])\s+", text)
    ftb_details = [s.strip() for s in sentences if any(m in s.lower() for m in ftb_markers)]
    ftb_details = " ".join(ftb_details) if ftb_details else ""

    exception_markers = [
        "exception", "exempt", "exemption", "quota", "flexibility",
        "waiver", "additional", "higher limit", "region", "renovation",
        "energy", "cap", "ceiling", "special",
    ]
    other_details = [s.strip() for s in sentences if any(m in s.lower() for m in exception_markers)]
    other_details = " ".join(other_details) if other_details else ""

    return limits_str, ftb_flag, ftb_details, other_details


def build_bbm_matrix_html(bbm_full: pd.DataFrame) -> Tuple[str, str]:
    if bbm_full is None or bbm_full.empty:
        return "", ""

    bbm_ref_date = ""
    max_date = bbm_full["date"].max() if "date" in bbm_full.columns else None
    if pd.notna(max_date):
        bbm_ref_date = max_date.strftime("%Y-%m-%d")

    bbm_matrix = bbm_full.copy()
    bbm_matrix["measure_short"] = bbm_matrix["measure_type"].map(lambda x: RENAME_MAP.get(x, x))

    def status_flag(row):
        status_text = f"{row.get('active_status','')} {row.get('status','')}".lower()
        if "active" in status_text or "applicable" in status_text:
            return "active"
        if any(k in status_text for k in ["announc", "planned", "pending", "future", "not yet"]):
            return "announced"
        return ""

    bbm_matrix["status_flag"] = bbm_matrix.apply(status_flag, axis=1)

    def pick_flag(values):
        vals = [v for v in values if v]
        if "active" in vals:
            return "<span class='dot dot--active'></span>"
        if "announced" in vals:
            return "<span class='dot dot--announced'></span>"
        return ""

    pivot_df = bbm_matrix.pivot_table(
        index="iso2",
        columns="measure_short",
        values="status_flag",
        aggfunc=pick_flag,
    ).fillna("")

    pivot_df.index.name = "COUNTRY"
    pivot_df.columns.name = None

    # Exclude Amortization and Stress Test columns
    exclude_cols = ["Amort.", "Amortization", "Stress T.", "Stress Test"]
    pivot_df = pivot_df.drop(columns=[c for c in exclude_cols if c in pivot_df.columns], errors='ignore')
    
    preferred_order = [
        "LTV",
        "DSTI",
        "DTI",
        "LTI",
        "Maturity",
        "Flex.",
        "Flexibility",
    ]
    ordered_cols = [c for c in preferred_order if c in pivot_df.columns]
    ordered_cols += [c for c in pivot_df.columns if c not in ordered_cols]
    pivot_df = pivot_df[ordered_cols].sort_index(axis=0)

    return pivot_df.to_html(classes="display-table bbm-pivot", escape=False), bbm_ref_date


# EU + EEA (NO, IS, LI) + UK
EU_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT","LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE",
    "NO","IS","LI","GB"  # EEA countries + UK
}


def build_dti_lti_items(bbm_full: pd.DataFrame) -> list[dict]:
    """
    Build DTI/LTI candidate items from ESRB BBM data (EU + EEA + UK, Active only).
    
    Note: ESRB sometimes marks LTI measures as "DTI" in measure_type.
    We detect LTI from description if measure_type is "DTI" but description mentions LTI.
    """
    if bbm_full is None or bbm_full.empty:
        return []

    df = bbm_full.copy()
    if "active_status" in df.columns:
        df = df[df["active_status"].astype(str) == "Active"].copy()

    df["measure_short"] = df["measure_type"].astype(str).map(lambda x: RENAME_MAP.get(x, x))
    df["iso2"] = df.get("iso2").astype(str)
    df = df[df["iso2"].isin(EU_ISO2)].copy()
    
    # First, get explicit DTI/LTI from measure_short
    df_explicit = df[df["measure_short"].isin(["DTI", "LTI"])].copy()
    
    # Also check for DTI/LTI mentions in description, even if measure_type is not DTI/LTI
    # This handles cases where ESRB marks LTI as "DTI" in measure_type
    # Look for patterns like "8-times income", "DTI ratio", "loan-to-income", etc.
    df_desc = df[~df["measure_short"].isin(["DTI", "LTI"])].copy()
    if not df_desc.empty and "description" in df_desc.columns:
        # Look for DTI/LTI mentions in description (broader pattern)
        # Includes: "DTI", "LTI", "debt-to-income", "loan-to-income", "X-times income", "X times", etc.
        def has_dti_lti_mention(desc: str) -> bool:
            """Check if description mentions DTI/LTI (but NOT DSTI)."""
            if pd.isna(desc) or not desc:
                return False
            desc_str = str(desc).lower()
            
            # EXCLUDE DSTI (Debt-Service-to-Income) - this is different from DTI
            if "dsti" in desc_str or "debt-service-to-income" in desc_str or "debt service to income" in desc_str:
                return False
            
            # Explicit mentions (DTI or LTI, but not DSTI)
            if any(term in desc_str for term in ["dti", "lti", "debt-to-income", "loan-to-income", 
                                                  "debt to income", "loan to income"]):
                # Double-check it's not DSTI
                if "dsti" not in desc_str and "debt-service" not in desc_str:
                    return True
            
            # Pattern: "X times income" or "X-times income" (e.g., "8-times income", "6 times")
            # But exclude if it's about "debt service" or "instalment"
            if re.search(r"\d+(?:\.\d+)?[- ]times?\s+(?:yearly|annual|net|gross|disposable)?\s*income", desc_str, re.I):
                # Exclude if it's about debt service/instalment
                if "debt service" not in desc_str and "instalment" not in desc_str and "payment" not in desc_str:
                    return True
            
            # Pattern: "income" + "times" or "multiple" (e.g., "income multiple", "times income")
            # But exclude DSTI context
            if (re.search(r"income.*(?:times|multiple)", desc_str, re.I) or \
               re.search(r"(?:times|multiple).*income", desc_str, re.I)) and \
               "debt service" not in desc_str:
                return True
            
            # Pattern: "DTI ratio" or "LTI ratio" (explicit)
            if re.search(r"\b(?:dti|lti)\s+ratio\b", desc_str, re.I):
                return True
            
            # Pattern: "debt/loan" + "income" + "ratio" (but not "debt service")
            if re.search(r"(?:debt|loan).*income.*ratio", desc_str, re.I) and \
               "debt service" not in desc_str and "dsti" not in desc_str:
                return True
            
            return False
        
        df_desc["has_dti_lti"] = df_desc["description"].apply(has_dti_lti_mention)
        df_desc = df_desc[df_desc["has_dti_lti"]].copy()
        
        # Try to determine if it's DTI or LTI from description
        def detect_dti_lti_from_desc(desc: str, measure_short: str) -> str:
            desc_lower = str(desc).lower()
            # LTI indicators (loan-to-income, mortgage only, loan amount)
            lti_indicators = [
                "loan-to-income", "loan to income", "lti",
                "mortgage loan", "housing loan", "residential loan",
                "loan amount", "loan size", "loan.*income",
                "mortgage.*income"
            ]
            # DTI indicators (debt-to-income, total debt, all debt)
            dti_indicators = [
                "debt-to-income", "debt to income", "dti",
                "total debt", "all debt", "total indebtedness",
                "debt obligations", "total borrower", "debt.*income",
                "indebtedness.*income", "borrower.*indebtedness"
            ]
            
            # Check for explicit mentions
            has_lti_explicit = any(re.search(ind, desc_lower, re.I) for ind in lti_indicators)
            has_dti_explicit = any(re.search(ind, desc_lower, re.I) for ind in dti_indicators)
            
            # Also check for context clues
            has_mortgage_context = any(word in desc_lower for word in ["mortgage", "housing", "residential", "property"])
            has_total_debt_context = any(phrase in desc_lower for phrase in [
                "total debt", "all debt", "total indebtedness", 
                "including both new and existing loans",
                "debt obligations"
            ])
            
            if has_lti_explicit and not has_dti_explicit:
                return "LTI"
            elif has_dti_explicit and not has_lti_explicit:
                return "DTI"
            elif has_lti_explicit and has_dti_explicit:
                # If both, prefer based on context
                if has_mortgage_context and not has_total_debt_context:
                    return "LTI"
                elif has_total_debt_context and not has_mortgage_context:
                    return "DTI"
                else:
                    # If unclear, prefer DTI (more common)
                    return "DTI"
            elif has_mortgage_context and "income" in desc_lower:
                # Mortgage + income mention → likely LTI
                return "LTI"
            elif has_total_debt_context and "income" in desc_lower:
                # Total debt + income mention → likely DTI
                return "DTI"
            else:
                # Fallback: if measure_short is "DTI", use that; otherwise default to DTI
                if measure_short == "DTI":
                    return "DTI"
                # Default to DTI if unclear
                return "DTI"
        
        df_desc["measure_short"] = df_desc.apply(
            lambda row: detect_dti_lti_from_desc(
                row.get("description", ""),
                row.get("measure_short", "")
            ),
            axis=1
        )
        df_desc = df_desc.drop(columns=["has_dti_lti"], errors="ignore")
    
    # Combine explicit and description-detected
    df_combined = pd.concat([df_explicit, df_desc], ignore_index=True) if not df_desc.empty else df_explicit
    
    if df_combined.empty:
        return []

    items: list[dict] = []
    for _, row in df_combined.iterrows():
        items.append(
            {
                "iso2": str(row.get("iso2", "")).strip(),
                "country": str(row.get("country", "")).strip(),
                "measure_short": str(row.get("measure_short", "")).strip(),
                "description": str(row.get("description", "")).strip(),
                "status": str(row.get("status", "")).strip() if "status" in row else "",
                "related_links": str(row.get("Related links", "")).strip() if "Related links" in df_combined.columns else "",
            }
        )
    return items


def build_dti_lti_comparison_df(bbm_full: pd.DataFrame, analyzer, search_config=None) -> pd.DataFrame:
    """
    Build DTI/LTI comparison DataFrame (legacy function - uses new structured model).
    For new code, use build_dti_lti_comparison_df_structured from bbm.dti_lti_builder.
    """
    try:
        from bbm.dti_lti_builder import build_dti_lti_comparison_df_structured
        from config import SEARCH_CONFIG
        # Enable AI validation to fill missing data and improve accuracy
        return build_dti_lti_comparison_df_structured(
            bbm_full,
            analyzer,
            validate_with_ai=True,  # Enable to fill missing data
            final_validation_with_search=False,  # Keep disabled for now (can be enabled later)
            search_config=search_config or SEARCH_CONFIG
        )
    except ImportError:
        # Fallback to old implementation if new module not available
        pass
    
    # Legacy implementation (kept for backward compatibility)
    """
    Build a comparative table for countries where ESRB data indicates DTI/LTI and AI can verify details.
    Returns DataFrame with required columns:
      - COUNTRY (ISO2)
      - TYPE (DTI/LTI)
      - NUMERATOR
      - DENOMINATOR
      - LIMITS
      - BASIS (Mandatory/Guidance)
    Only includes rows with high confidence after AI self-check.
    """
    items = build_dti_lti_items(bbm_full)
    if not items:
        return pd.DataFrame(columns=["COUNTRY", "TYPE", "NUMERATOR", "DENOMINATOR", "LIMITS", "BASIS"])

    # Step 1: AI confirmation (strict, evidence-quoted)
    confirmations = analyzer.confirm_dti_lti_presence(items)

    def _is_confirmed_high(c: dict, it: dict) -> bool:
        if not isinstance(c, dict):
            return False
        if str(c.get("confirmed", "")).strip().lower() != "yes":
            return False
        if str(c.get("confidence", "")).strip().lower() != "high":
            return False
        # Require an evidence excerpt
        if not str(c.get("evidence_excerpt", "")).strip():
            return False
        # Must match the item type
        t = str(c.get("type", "")).strip().upper()
        if t not in {"DTI", "LTI"}:
            return False
        return True

    confirmed_items = []
    for it, c in zip(items, confirmations):
        if _is_confirmed_high(c, it):
            confirmed_items.append(it)

    if not confirmed_items:
        return pd.DataFrame(columns=["COUNTRY", "TYPE", "NUMERATOR", "DENOMINATOR", "LIMITS", "BASIS"])

    # Step 2: Deterministic extraction from ESRB descriptions (only what is explicitly stated)
    def _basis_from_text(desc: str) -> str:
        d = (desc or "").lower()
        if "guideline" in d or "best practice" in d or "guidelines" in d:
            return "Guidance"
        if any(k in d for k in ["shall", "must", "cannot exceed", "shall not", "loan shall not be issued", "shall be assessed"]):
            return "Mandatory"
        # Heuristic: explicit "limits" with allowance shares usually indicates binding macroprudential caps
        if "limit" in d and any(k in d for k in ["lending can take place above", "above the limits", "share of new loans", "can be granted", "cannot exceed"]):
            return "Mandatory"
        return ""

    def _extract_limits(desc: str) -> str:
        # Keep as a short, human-readable summary using the description itself.
        # We avoid aggressive parsing; just return first ~240 chars around "DTI"/"LTI" if present.
        s = " ".join(str(desc or "").split())
        if not s:
            return ""
        # Prefer sentence containing DTI/LTI
        for key in ["DTI", "LTI", "loan-to-income", "loan to gross income"]:
            idx = s.lower().find(key.lower())
            if idx != -1:
                return s[max(0, idx - 80): idx + 220].strip()
        return s[:240].strip()

    def _extract_num_denom(desc: str, typ: str) -> tuple[str, str]:
        s = " ".join(str(desc or "").split())
        if not s:
            return "", ""

        if typ == "LTI":
            # Usually loan / (gross) income
            denom = "gross income" if "gross income" in s.lower() else "income"
            return "loan amount", denom

        # DTI
        # Latvia-style explicit definitions
        if "Debt (D) is" in s and "Income (I) is" in s:
            return "Debt (D): all debt obligations", "Income (I): avg monthly income × 12"

        # Generic: use phrases from text
        if "indebtedness" in s.lower() and "income" in s.lower():
            return "total borrower's indebtedness", "yearly net disposable income" if "net disposable income" in s.lower() else "income"

        # If not explicitly stated, leave empty (strict)
        return "", ""

    rows = []
    for it in confirmed_items:
        desc = str(it.get("description", ""))
        typ = str(it.get("measure_short", "")).strip().upper()
        basis = _basis_from_text(desc)
        numerator, denominator = _extract_num_denom(desc, typ)
        limits = _extract_limits(desc)

        if not all([it.get("iso2"), typ in {"DTI", "LTI"}, basis in {"Mandatory", "Guidance"}, numerator, denominator, limits]):
            continue

        rows.append(
            {
                "COUNTRY": str(it.get("iso2")).strip(),
                "TYPE": typ,
                "NUMERATOR": numerator,
                "DENOMINATOR": denominator,
                "LIMITS": limits,
                "BASIS": basis,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["COUNTRY", "TYPE", "NUMERATOR", "DENOMINATOR", "LIMITS", "BASIS"])

    out = pd.DataFrame(rows)
    # Deduplicate (some countries may have multiple rows in ESRB)
    out = out.drop_duplicates(subset=["COUNTRY", "TYPE", "LIMITS"], keep="first")
    out = out.sort_values(["TYPE", "COUNTRY"]).reset_index(drop=True)
    return out


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
