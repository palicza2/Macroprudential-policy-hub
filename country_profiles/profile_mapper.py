"""
Canonical country-profile shape for the dashboard.

All sources (pipeline aggregators, Supabase rows, JS live fetch) must produce
this structure so Country Profiles, Institutional Setup, and BBM cards stay in sync.
"""

from typing import Any, Dict, List, Optional

from bbm.matrix_builder import RENAME_MAP, is_bbm_row_active


# Keys the dashboard (assets/app.js) reads from window.countriesData / fetchCountryProfile.
PROFILE_KEYS = (
    "country",
    "iso2",
    "institutional_setup",
    "current_status",
    "historical_evolution",
    "recent_changes",
    "active_measures",
    "comparison",
    "ai_analysis",
)

INSTITUTIONAL_TABLE_FIELDS = (
    "macroprudential_authority",
    "designated_authority",
    "institutional_model",
    "legal_basis",
    "decision_making_body",
    "relationship_to_cb",
    "key_regulations",
)

INSTITUTIONAL_AI_FIELDS = (
    "ai_description",
    "ai_confidence_score",
    "ai_grounding_notes",
    "ai_sources_cited",
    "ai_generated_at",
)


def empty_profile(country: str = "", iso2: Optional[str] = None) -> Dict[str, Any]:
    """Return a fully keyed profile with empty/None values."""
    return {
        "country": country,
        "iso2": iso2,
        "institutional_setup": None,
        "current_status": {
            "ccyb": None,
            "syrb": None,
            "osii": None,
            "bbm": [],
            "total_capital": None,
        },
        "historical_evolution": {"ccyb": [], "syrb": []},
        "recent_changes": [],
        "active_measures": {
            "ccyb": None,
            "syrb": [],
            "bbm": [],
            "osii": None,
        },
        "comparison": {
            "regional_average": None,
            "similar_countries": [],
        },
        "ai_analysis": "",
    }


def canonicalize_profile(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Fill missing keys on any profile-like dict so the dashboard always sees
    the same shape. Does not invent measure data.
    """
    base = empty_profile()
    if not raw or not isinstance(raw, dict):
        return base
    out = {**base, **{k: raw[k] for k in PROFILE_KEYS if k in raw}}
    if raw.get("country"):
        out["country"] = raw["country"]
    if raw.get("iso2"):
        out["iso2"] = raw["iso2"]
    cs = raw.get("current_status") or {}
    out["current_status"] = {**base["current_status"], **cs} if isinstance(cs, dict) else base["current_status"]
    he = raw.get("historical_evolution") or {}
    out["historical_evolution"] = {**base["historical_evolution"], **he} if isinstance(he, dict) else base["historical_evolution"]
    am = raw.get("active_measures") or {}
    out["active_measures"] = {**base["active_measures"], **am} if isinstance(am, dict) else base["active_measures"]
    cmp_ = raw.get("comparison") or {}
    out["comparison"] = {**base["comparison"], **cmp_} if isinstance(cmp_, dict) else base["comparison"]
    out["institutional_setup"] = normalize_institutional_setup(raw.get("institutional_setup"))
    if out.get("recent_changes") is None:
        out["recent_changes"] = []
    if out.get("ai_analysis") is None:
        out["ai_analysis"] = ""
    return out


def normalize_institutional_setup(inst: Any) -> Optional[Dict[str, Any]]:
    """Normalize a DB row or pipeline dict. Empty / all-null → None."""
    if not inst or not isinstance(inst, dict):
        return None
    out = dict(inst)
    for k in ("key_regulations", "ai_sources_cited"):
        if k in out and out[k] is None:
            out[k] = []
    has_table = any(out.get(k) for k in INSTITUTIONAL_TABLE_FIELDS)
    has_ai = bool(out.get("ai_description"))
    if not has_table and not has_ai:
        return None
    return out


def osii_status_from_rates(
    osii_snap: Optional[Dict[str, Any]],
    osii_rates: Optional[List[float]] = None,
) -> Optional[Dict[str, Any]]:
    """
    O-SII current_status block with min/max and rate_display (percentage scale).
    Shared by pipeline aggregators' display contract and Supabase snapshots.
    """
    rates = [r for r in (osii_rates or []) if r and r > 0]
    if osii_snap:
        total_rate = float(osii_snap.get("total_rate") or osii_snap.get("rate") or 0) or 0.0
        if 0 < total_rate < 1:
            total_rate = total_rate * 100
    else:
        total_rate = 0.0

    if rates:
        min_rate = min(rates)
        max_rate = max(rates)
        if max_rate > 0 and max_rate < 1:
            min_rate *= 100
            max_rate *= 100
        if max_rate < 0.01:
            rate_display = "0%"
        elif abs(max_rate - min_rate) < 0.01:
            rate_display = f"{int(max_rate)}%" if max_rate == int(max_rate) else f"{max_rate:.2f}%"
        elif min_rate < 0.01:
            rate_display = f"0-{int(round(max_rate))}%" if max_rate == int(max_rate) else f"0-{max_rate:.1f}%"
        else:
            min_str = f"{min_rate:.1f}" if min_rate != int(min_rate) else str(int(min_rate))
            max_str = f"{max_rate:.1f}" if max_rate != int(max_rate) else str(int(max_rate))
            rate_display = f"{min_str}-{max_str}%"
        return {
            "rate": max_rate,
            "rate_min": min_rate,
            "rate_max": max_rate,
            "rate_display": rate_display,
            "status": "Active" if max_rate > 0 else "Inactive",
        }

    if not osii_snap:
        return None
    rate_display = (
        f"{int(total_rate)}%"
        if total_rate == int(total_rate)
        else f"{total_rate:.2f}%"
        if total_rate > 0
        else "0%"
    )
    return {
        "rate": total_rate,
        "rate_min": total_rate,
        "rate_max": total_rate,
        "rate_display": rate_display,
        "status": "Active" if total_rate > 0 else "Inactive",
    }


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _bbm_short(measure_type: str) -> str:
    if not measure_type:
        return ""
    return RENAME_MAP.get(measure_type, measure_type)


def profile_from_supabase_rows(
    country_name: str,
    iso2: str,
    ccyb_snap: Optional[Dict[str, Any]] = None,
    syrb_snap: Optional[Dict[str, Any]] = None,
    osii_snap: Optional[Dict[str, Any]] = None,
    osii_rates: Optional[List[float]] = None,
    ccyb_decisions: Optional[List[Dict[str, Any]]] = None,
    syrb_measures: Optional[List[Dict[str, Any]]] = None,
    bbm_measures: Optional[List[Dict[str, Any]]] = None,
    institutional_setup: Optional[Dict[str, Any]] = None,
    ai_analysis: str = "",
) -> Dict[str, Any]:
    """
    Map Supabase table rows (snapshots, decisions, measures, institutional_setup)
    to the canonical profile dict.
    """
    ccyb_decisions = ccyb_decisions or []
    syrb_measures = syrb_measures or []
    bbm_measures = bbm_measures or []

    raw_active = [m for m in bbm_measures if is_bbm_row_active(m)]
    bbm_types: List[str] = []
    active_bbm_list: List[Dict[str, Any]] = []
    for m in raw_active:
        measure_type = m.get("measure_type") or ""
        measure_short = _bbm_short(measure_type) or measure_type
        if measure_short and measure_short not in bbm_types:
            bbm_types.append(measure_short)
        active_bbm_list.append({
            "type": measure_short or measure_type,
            "status": "Active",
            "date": m.get("effective_date") or m.get("date"),
            "description": m.get("description") or "",
        })

    ccyb_rate = _safe_float(ccyb_snap.get("rate") if ccyb_snap else None)
    current_status = {
        "ccyb": {
            "rate": ccyb_rate,
            "date": (ccyb_snap or {}).get("effective_date") or (ccyb_snap or {}).get("date") or "",
            "status": "Active" if ccyb_rate > 0 else "Inactive",
        } if ccyb_snap else None,
        "syrb": {
            "rate": _safe_float((syrb_snap or {}).get("total_rate") or (syrb_snap or {}).get("rate")),
            "date": "",
            "type": (
                "General"
                if _safe_float((syrb_snap or {}).get("general_rate")) > 0
                else (
                    "Sectoral"
                    if _safe_float((syrb_snap or {}).get("sectoral_rate")) > 0
                    else "General"
                )
            ),
            "status": "Active" if _safe_float((syrb_snap or {}).get("total_rate")) > 0 else "Inactive",
        } if syrb_snap else None,
        "osii": osii_status_from_rates(osii_snap, osii_rates),
        "bbm": bbm_types,
        "total_capital": None,
    }

    ccyb_history = [
        {
            "date": d.get("effective_date") or d.get("date") or "",
            "rate": _safe_float(d.get("rate")),
            "credit_gap": _safe_float(d.get("credit_gap"), default=None) if d.get("credit_gap") is not None else None,
        }
        for d in ccyb_decisions
    ]
    syrb_history = [
        {
            "date": m.get("effective_date") or m.get("date") or "",
            "rate_numeric": _safe_float(m.get("rate") or m.get("rate_numeric")),
        }
        for m in syrb_measures
    ]

    active_syrb = [
        m for m in syrb_measures
        if (m.get("active_status") or m.get("status") or "").lower().find("active") >= 0
        and "inactive" not in (m.get("active_status") or m.get("status") or "").lower()
    ]

    profile = empty_profile(country_name, iso2)
    profile["current_status"] = current_status
    profile["institutional_setup"] = normalize_institutional_setup(institutional_setup)
    profile["historical_evolution"] = {"ccyb": ccyb_history, "syrb": syrb_history}
    profile["active_measures"] = {
        "ccyb": current_status.get("ccyb"),
        "syrb": active_syrb,
        "bbm": active_bbm_list,
        "osii": current_status.get("osii"),
    }
    profile["ai_analysis"] = ai_analysis or ""
    return profile


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if value == "":
        return True
    if value == [] or value == {}:
        return True
    return False


def merge_profiles(
    base: Optional[Dict[str, Any]],
    overlay: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Merge two canonical profiles. `base` wins for any non-empty field;
    `overlay` fills gaps (e.g. pipeline AI into a Supabase-fetched profile).

    Typical call: merge_profiles(pipeline_profile, supabase_profile)
    so pipeline institutional AI and recent_changes are kept.
    """
    a = canonicalize_profile(base)
    b = canonicalize_profile(overlay)
    if _is_empty(a.get("country")) and b.get("country"):
        a["country"] = b["country"]
    if _is_empty(a.get("iso2")) and b.get("iso2"):
        a["iso2"] = b["iso2"]

    a_inst = a.get("institutional_setup")
    b_inst = b.get("institutional_setup")
    if not a_inst:
        a["institutional_setup"] = b_inst
    elif b_inst:
        merged_inst = dict(b_inst)
        merged_inst.update({k: v for k, v in a_inst.items() if not _is_empty(v)})
        a["institutional_setup"] = normalize_institutional_setup(merged_inst)

    for nest in ("current_status", "historical_evolution", "active_measures", "comparison"):
        a_n = a.get(nest) or {}
        b_n = b.get(nest) or {}
        combined = dict(b_n)
        for k, v in a_n.items():
            if not _is_empty(v):
                combined[k] = v
        a[nest] = combined

    if _is_empty(a.get("recent_changes")) and not _is_empty(b.get("recent_changes")):
        a["recent_changes"] = b["recent_changes"]
    if _is_empty(a.get("ai_analysis")) and not _is_empty(b.get("ai_analysis")):
        a["ai_analysis"] = b["ai_analysis"]

    return canonicalize_profile(a)
