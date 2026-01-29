from __future__ import annotations

import pandas as pd


def build_capital_overall_df(
    *,
    ccyb_df: pd.DataFrame | None,
    syrb_df: pd.DataFrame | None,
    osii_df: pd.DataFrame | None,
    ccob_rate: float = 2.5,
) -> pd.DataFrame:
    """
    Per-country overall capital buffer components:
    - CCoB: constant (default 2.5)
    - CCyB: latest ANNOUNCED rate (latest decision_date per country; independent of application date)
    - GSII/O-SII: max rate per country (from capital-based measures workbook)
    - SyRB: max GENERAL SyRB per country (includes future dates if not revoked/deactivated)
    - sSyRB: max SECTORAL SyRB per country (includes future dates if not revoked/deactivated)
    """
    out = pd.DataFrame()

    # --- CCyB (latest announced by decision_date) ---
    ccyb_series = pd.Series(dtype=float)
    if ccyb_df is not None and not ccyb_df.empty:
        df = ccyb_df.copy()
        if "decision_date" in df.columns:
            df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce")
            df = df.sort_values(["iso2", "decision_date"], ascending=[True, True])
            latest = df.dropna(subset=["iso2"]).groupby("iso2", as_index=False).tail(1)
        else:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values(["iso2", "date"], ascending=[True, True])
            latest = df.dropna(subset=["iso2"]).groupby("iso2", as_index=False).tail(1)
        ccyb_series = pd.to_numeric(latest.get("rate"), errors="coerce").fillna(0.0)
        ccyb_series.index = latest["iso2"].astype(str).values

    # --- SyRB (general vs sectoral max) ---
    syrb_general = pd.Series(dtype=float)
    syrb_sectoral = pd.Series(dtype=float)
    if syrb_df is not None and not syrb_df.empty:
        df = syrb_df.copy()
        df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
        status = df.get("status")
        status_str = status.astype(str) if status is not None else pd.Series([""] * len(df))
        mask_ok = ~status_str.str.contains("Deactivated|Revoked|No longer|Expired", case=False, na=False)
        df = df[mask_ok].copy()
        df["rate_numeric"] = pd.to_numeric(df.get("rate_numeric"), errors="coerce").fillna(0.0)
        df = df[df["rate_numeric"] > 0].copy()
        df["iso2"] = df.get("iso2").astype(str)

        # The ETL tags syrb_type as General vs Sectoral
        if "syrb_type" in df.columns:
            gen = df[df["syrb_type"].astype(str).str.contains("General", case=False, na=False)]
            sec = df[df["syrb_type"].astype(str).str.contains("Sectoral", case=False, na=False)]
        else:
            # fallback heuristic: exposure_type contains 'General'
            gen = df[df.get("exposure_type", "").astype(str).str.contains("General", case=False, na=False)]
            sec = df[~df.get("exposure_type", "").astype(str).str.contains("General", case=False, na=False)]

        syrb_general = gen.groupby("iso2")["rate_numeric"].max() if not gen.empty else pd.Series(dtype=float)
        syrb_sectoral = sec.groupby("iso2")["rate_numeric"].max() if not sec.empty else pd.Series(dtype=float)

    # --- GSII/O-SII (max per country) ---
    osii_series = pd.Series(dtype=float)
    if osii_df is not None and not osii_df.empty and "iso2" in osii_df.columns:
        df = osii_df.copy()
        df["rate_numeric"] = pd.to_numeric(df.get("rate_numeric"), errors="coerce").fillna(0.0)
        df["iso2"] = df["iso2"].astype(str)
        # Csak aktív méréseket számoljuk (status != Revoked/Deactivated)
        if "status" in df.columns:
            status_str = df["status"].astype(str)
            mask_active = ~status_str.str.contains("Revoked|Deactivated|Expired|No longer", case=False, na=False)
            df = df[mask_active].copy()
        osii_series = df.groupby("iso2")["rate_numeric"].max()

    # union of iso2s
    countries = sorted(set(ccyb_series.index) | set(syrb_general.index) | set(syrb_sectoral.index) | set(osii_series.index))
    if not countries:
        return pd.DataFrame(columns=["ISO2", "CCoB", "CCyB", "GSII/O-SII", "SyRB", "sSyRB", "TOTAL"])

    out = pd.DataFrame({"ISO2": countries})
    out["CCoB"] = float(ccob_rate)
    out["CCyB"] = out["ISO2"].map(ccyb_series).fillna(0.0)
    out["GSII/O-SII"] = out["ISO2"].map(osii_series).fillna(0.0)
    out["SyRB"] = out["ISO2"].map(syrb_general).fillna(0.0)
    out["sSyRB"] = out["ISO2"].map(syrb_sectoral).fillna(0.0)
    out["TOTAL"] = out[["CCoB", "CCyB", "GSII/O-SII", "SyRB", "sSyRB"]].sum(axis=1)
    
    # Validáció: 3% feletti értékek ellenőrzése
    import logging
    logger = logging.getLogger(__name__)
    high_total = out[out["TOTAL"] > 3.0]
    if not high_total.empty:
        for _, row in high_total.iterrows():
            logger.warning(
                f"High total capital buffer detected for {row['ISO2']}: "
                f"TOTAL={row['TOTAL']:.2f}% (CCoB={row['CCoB']:.2f}%, CCyB={row['CCyB']:.2f}%, "
                f"GSII/O-SII={row['GSII/O-SII']:.2f}%, SyRB={row['SyRB']:.2f}%, sSyRB={row['sSyRB']:.2f}%)"
            )
            # Ha SyRB > 10%, valószínűleg formátum hiba
            if row["SyRB"] > 10.0:
                logger.error(f"  -> SyRB={row['SyRB']:.2f}% seems too high, possible format error!")
            if row["sSyRB"] > 10.0:
                logger.error(f"  -> sSyRB={row['sSyRB']:.2f}% seems too high, possible format error!")
    
    out = out.sort_values(["TOTAL", "ISO2"], ascending=[False, True]).reset_index(drop=True)
    return out

