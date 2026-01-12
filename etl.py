import pandas as pd
import country_converter as coco
import logging
import re
from pathlib import Path
from utils import clean_columns, find_header_row, extract_rate, ensure_dirs, download_file_safely
from config import FILES

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, data_dir: Path, ccyb_url: str, syrb_url: str):
        self.data_dir = data_dir
        self.ccyb_url = ccyb_url
        self.syrb_url = syrb_url
        self.ccyb_file = FILES["ccyb_source"]
        self.syrb_file = FILES["syrb_source"]

    def _extract_rate_from_text(self, text):
        if pd.isna(text): return 0.0
        text = str(text).replace(',', '.')
        matches = re.findall(r'(\d+(?:\.\d+)?)\s*%', text)
        if not matches:
            matches = re.findall(r'rate\s*(?:of|is)\s*(\d+(?:\.\d+)?)', text, re.IGNORECASE)
        if matches:
            rates = [float(r) for r in matches if float(r) <= 100.0]
            return max(rates) if rates else 0.0
        return 0.0

    def _process_syrb(self):
        if not self.syrb_file.exists(): return pd.DataFrame()
        try:
            xl = pd.ExcelFile(self.syrb_file)
            sheet = next((s for s in xl.sheet_names if "SRB" in s or "Systemic" in s), None)
            if not sheet: return pd.DataFrame()

            df_raw = xl.parse(sheet, header=None, nrows=30)
            header_idx = 0
            for i, row in df_raw.iterrows():
                row_str = " ".join(row.astype(str)).lower()
                if "reference of measure" in row_str or "country" in row_str:
                    header_idx = i
                    break
            
            df = xl.parse(sheet, skiprows=header_idx)
            df = clean_columns(df)
            
            col_map = {}
            for c in df.columns:
                cl = c.lower().strip()
                if 'country' in cl: col_map['country'] = c
                elif 'measure becomes active on' in cl: col_map['date'] = c
                elif 'description of measure' in cl: col_map['description'] = c
                elif 'type of exposures applied to' in cl: col_map['exposure_type'] = c
                elif 'present status of measure' in cl: col_map['status'] = c
                elif 'rate' in cl and 'guide' not in cl: col_map['rate_col'] = c
                elif 'date of revocation' in cl: col_map['revocation_date'] = c
                elif 'reference of measure' in cl: col_map['reference'] = c

            if 'date' not in col_map:
                col_map['date'] = next((c for c in df.columns if 'date' in c.lower()), None)

            if not col_map or 'country' not in col_map: return pd.DataFrame()

            df = df.rename(columns={v: k for k, v in col_map.items()})
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['revocation_date'] = pd.to_datetime(df.get('revocation_date'), errors='coerce')
            
            df = df.dropna(subset=['country'])
            names = df['country'].tolist()
            df['iso2'] = coco.convert(names=names, to='iso2', not_found=None)

            # Rate kinyerés (alapértelmezett)
            df['rate_numeric'] = df['description'].apply(self._extract_rate_from_text)
            if 'rate_col' in df.columns:
                numeric_col_rates = pd.to_numeric(df['rate_col'], errors='coerce')
                df['rate_numeric'] = numeric_col_rates.fillna(df['rate_numeric'])

            # Kategorizálás
            def tag_exp(row):
                exp_type = str(row.get('exposure_type', '')).lower()
                desc = str(row.get('description', '')).lower()
                full_text = exp_type + " " + desc
                if 'all exposures' in exp_type: return "General"
                if 'domestic' in exp_type: return "General"
                if any(x in full_text for x in ['commercial', 'cre']):
                    if any(x in full_text for x in ['residential', 'rre', 'housing']): return "Real Estate (CRE & RRE)"
                    return "Commercial Real Estate (CRE)"
                if any(x in full_text for x in ['residential', 'rre', 'housing', 'mortgage', 'household']): return "Residential Real Estate (RRE)"
                return "Other"

            df['exposure_type'] = df.apply(tag_exp, axis=1)
            df['syrb_type'] = df['exposure_type'].apply(lambda x: 'General' if 'General' in x else 'Sectoral')

            # --- Trend-specifikus feldolgozás: Duplikáljuk a sorokat a visszavonásokhoz ---
            trend_events = []
            for (country, sy_type), group in df.sort_values('date').groupby(['country', 'syrb_type']):
                group = group.sort_values('date')
                for i, (_, row) in enumerate(group.iterrows()):
                    is_latest = (i == len(group) - 1)
                    status = str(row.get('status', '')).lower()
                    event = row.to_dict()
                    if is_latest and 'not active' in status and pd.isna(row.get('revocation_date')):
                        event['rate_numeric'] = 0.0
                    trend_events.append(event)
                    if not pd.isna(row.get('revocation_date')):
                        rev_event = row.to_dict()
                        rev_event['date'] = row['revocation_date']
                        rev_event['rate_numeric'] = 0.0
                        rev_event['status'] = 'Revoked'
                        trend_events.append(rev_event)
            
            if trend_events:
                df = pd.DataFrame(trend_events)
            
            if 'status' in df.columns:
                rev_mask = df['status'].astype(str).str.contains('Revoked', case=False, na=False)
                df.loc[rev_mask, 'rate_numeric'] = 0.0

            df['rate_text'] = df['rate_numeric'].apply(lambda x: f"{x}%" if x > 0 else "0% / Inactive")
            return df.sort_values(['country', 'date'], ascending=[True, False]).reset_index(drop=True)
        except Exception as e:
            logger.error(f"SyRB Error: {e}")
            return pd.DataFrame()

    def _process_bbm(self):
        """Processes Borrower-Based Measures (LTV, DSTI, etc.) from the BoBM sheet."""
        if not self.syrb_file.exists(): return pd.DataFrame()
        try:
            xl = pd.ExcelFile(self.syrb_file)
            sheet = next((s for s in xl.sheet_names if "BoBM" in s), None)
            if not sheet: return pd.DataFrame()

            df_raw = xl.parse(sheet, header=None, nrows=30)
            header_idx = find_header_row(df_raw)
            
            df = xl.parse(sheet, skiprows=header_idx)
            df = clean_columns(df)
            
            col_map = {}
            for c in df.columns:
                cl = c.lower().strip()
                if 'country' in cl: col_map['country'] = c
                elif 'measure becomes active on' in cl: col_map['date'] = c
                elif 'type of measure' in cl: col_map['measure_type'] = c
                elif 'present status of measure' in cl: col_map['status'] = c
                elif 'description of measure' in cl: col_map['description'] = c
                elif 'date of revocation' in cl: col_map['revocation_date'] = c

            if not col_map or 'country' not in col_map: return pd.DataFrame()

            df = df.rename(columns={v: k for k, v in col_map.items()})
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['revocation_date'] = pd.to_datetime(df.get('revocation_date'), errors='coerce')
            
            df = df.dropna(subset=['country'])
            df['iso2'] = coco.convert(names=df['country'].tolist(), to='iso2', not_found=None)
            
            # Határozzuk meg az aktív státuszt
            def check_active(row):
                status = str(row.get('status', '')).lower()
                rev_date = row.get('revocation_date')
                if 'deactivated' in status or 'revoked' in status or 'expired' in status:
                    return "Inactive"
                if pd.notna(rev_date) and rev_date <= pd.Timestamp.now():
                    return "Inactive"
                return "Active"

            df['active_status'] = df.apply(check_active, axis=1)
            
            return df.sort_values(['country', 'date'], ascending=[True, False]).reset_index(drop=True)
        except Exception as e:
            logger.error(f"BBM Error: {e}")
            return pd.DataFrame()

    def _process_ccyb(self):
        if not self.ccyb_file.exists(): return pd.DataFrame()
        try:
            xl = pd.ExcelFile(self.ccyb_file)
            df = xl.parse(0, skiprows=find_header_row(xl.parse(0, header=None, nrows=20)))
            df = clean_columns(df)
            col_map = {}
            for c in df.columns:
                cl = c.lower().strip()
                if 'country' in cl: col_map['country'] = c
                elif 'application' in cl: col_map['date'] = c
                elif 'decision on' in cl: col_map['decision_date'] = c
                elif 'ccyb rate' in cl or ('rate' in cl and 'guide' not in cl): col_map['rate'] = c
                elif 'justification' in cl and 'exceptional' not in cl: col_map['justification'] = c
                elif 'type of setting' in cl: col_map['status'] = c

            if 'date' not in col_map and 'decision_date' in col_map: col_map['date'] = col_map['decision_date']
            if not col_map or 'rate' not in col_map: return pd.DataFrame()

            df = df.rename(columns={v: k for k, v in col_map.items()})
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date', 'country'])
            df['iso2'] = coco.convert(names=df['country'].tolist(), to='iso2', not_found=None)
            df['iso3'] = coco.convert(names=df['country'].tolist(), to='iso3', not_found=None)
            from utils import extract_rate
            df['rate'] = df['rate'].apply(extract_rate)
            gap_col = next((c for c in df.columns if 'gap' in c.lower() and 'additional' not in c.lower()), None)
            df['credit_gap'] = pd.to_numeric(df[gap_col], errors='coerce').fillna(0.0) if gap_col else 0.0
            return df.sort_values(['country', 'date'], ascending=[True, False]).reset_index(drop=True)
        except Exception as e:
            logger.error(f"CCyB Error: {e}")
            return pd.DataFrame()

    def calculate_trends(self, ccyb_df, syrb_df, bbm_df=None):
        agg_trend_ccyb = pd.DataFrame()
        syrb_trend = pd.DataFrame()
        bbm_trend = pd.DataFrame()
        today = pd.Timestamp.now().normalize()

        if not ccyb_df.empty:
            pivot = ccyb_df.pivot_table(index='date', columns='country', values='rate', aggfunc='last')
            full_idx = pd.date_range(start=pivot.index.min(), end=today, freq='D')
            pivot_filled = pivot.reindex(full_idx).ffill().fillna(0)
            counts = (pivot_filled > 0.0001).sum(axis=1)
            agg_trend_ccyb = pd.DataFrame({'date': counts.index, 'n_positive': counts.values}).reset_index(drop=True)

        if not syrb_df.empty:
            df = syrb_df.dropna(subset=['date'])
            def get_country_count_series(subset):
                if subset.empty: return pd.Series(dtype=float)
                
                # Szigorúbb logika: országonként és dátumonként nézzük, van-e aktív mérés
                # Előbb aggregáljuk a napi állapotot országonként
                subset = subset.copy()
                # Ha egy nap több bejegyzés van, az utolsó (legfrissebb) döntés számít
                daily = subset.sort_values('date').groupby(['date', 'country'])['rate_numeric'].last().reset_index()
                
                # Pivot országonként
                p = daily.pivot(index='date', columns='country', values='rate_numeric')
                
                # Idővonal kiterjesztése a mai napig
                full_range = pd.date_range(start=p.index.min(), end=today, freq='D')
                p_filled = p.reindex(full_range).ffill().fillna(0)
                
                # Számoljuk az országokat, ahol a ráta > 0
                return (p_filled > 0.0001).sum(axis=1)

            gen_counts = get_country_count_series(df[df['syrb_type'] == 'General'])
            sec_counts = get_country_count_series(df[df['syrb_type'] == 'Sectoral'])
            if not gen_counts.empty or not sec_counts.empty:
                all_dates = gen_counts.index.union(sec_counts.index)
                res = pd.DataFrame(index=all_dates)
                res['General SyRB'] = gen_counts.reindex(all_dates).ffill().fillna(0)
                res['Sectoral SyRB'] = sec_counts.reindex(all_dates).ffill().fillna(0)
                res['date'] = res.index
                syrb_trend = res.reset_index(drop=True)

        if bbm_df is not None and not bbm_df.empty:
            # BBM Trend: Országok száma, ahol legalább egy aktív BBM van
            df = bbm_df.dropna(subset=['date']).copy()
            
            # Készítünk egy eseménylistát (aktiválás és deaktiválás)
            events = []
            for _, row in df.iterrows():
                # Aktiválás
                events.append({'date': row['date'], 'country': row['country'], 'change': 1})
                # Deaktiválás (ha van)
                if pd.notna(row.get('revocation_date')):
                    events.append({'date': row['revocation_date'], 'country': row['country'], 'change': -1})
                elif 'status' in row and any(s in str(row['status']).lower() for s in ['deactivated', 'revoked', 'expired']):
                    # Ha a státusz inaktív de nincs visszavonási dátum, akkor feltételezzük 
                    # (ez bizonytalanabb, de a BoBM táblában gyakran így van)
                    pass

            if events:
                ev_df = pd.DataFrame(events).sort_values('date')
                # Országonkénti aktív mérések száma az időben
                # Pivot: index=date, columns=country, values=count
                # Ezt trükkösebben kell: minden országra külön kiszámoljuk az aktív mérések számát
                all_countries = ev_df['country'].unique()
                all_dates = pd.date_range(start=ev_df['date'].min(), end=today, freq='D')
                
                country_states = pd.DataFrame(index=all_dates)
                for c in all_countries:
                    c_ev = ev_df[ev_df['country'] == c].groupby('date')['change'].sum().reindex(all_dates).fillna(0).cumsum()
                    country_states[c] = (c_ev > 0).astype(int)
                
                bbm_trend = pd.DataFrame({
                    'date': all_dates,
                    'n_countries': country_states.sum(axis=1)
                }).reset_index(drop=True)

        return agg_trend_ccyb, syrb_trend, bbm_trend

    def run_pipeline(self):
        download_file_safely(self.syrb_url, self.syrb_file)
        download_file_safely(self.ccyb_url, self.ccyb_file)
        syrb_df = self._process_syrb()
        ccyb_df = self._process_ccyb()
        bbm_df = self._process_bbm()
        agg_trend, syrb_trend, bbm_trend = self.calculate_trends(ccyb_df, syrb_df, bbm_df)
        def get_latest(df): 
            if df.empty: return df
            return df.sort_values('date').groupby('country').tail(1).reset_index(drop=True)
        latest_syrb = get_latest(syrb_df)
        latest_ccyb = get_latest(ccyb_df)
        latest_bbm = bbm_df[bbm_df['active_status'] == 'Active'].reset_index(drop=True) if not bbm_df.empty else pd.DataFrame()
        
        if not syrb_df.empty: syrb_df.to_parquet(FILES["syrb_processed"])
        if not ccyb_df.empty: ccyb_df.to_parquet(FILES["ccyb_processed"])
        if not bbm_df.empty: bbm_df.to_parquet(FILES["bbm_processed"])
        
        return {
            'ccyb_df': ccyb_df, 'syrb_df': syrb_df, 'bbm_df': bbm_df,
            'agg_trend_df': agg_trend, 'syrb_trend_df': syrb_trend, 'bbm_trend_df': bbm_trend,
            'latest_ccyb_df': latest_ccyb, 'latest_syrb_df': latest_syrb,
            'latest_bbm_df': latest_bbm
        }
