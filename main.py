import logging
import sys
import re
import os
import html
import requests
import pandas as pd
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from config import BASE_DIR, DATA_DIR, URLS, FIGURES_DIR, REPORTS_DIR, LLM_CONFIG, SEARCH_CONFIG, NEWS_CONFIG
from utils import ensure_dirs
from etl import ETLPipeline
from visualizer import Visualizer
from llm_analysis import LLMAnalyzer
from grounding_validator import GroundingValidator

logging.basicConfig(level=logging.INFO, format='%(message)s')
for noisy_lib in ['kaleido', 'urllib3', 'matplotlib', 'chromies', 'werkzeug']:
    logging.getLogger(noisy_lib).setLevel(logging.CRITICAL)

logger = logging.getLogger("MAIN")

def main():
    logger.info("STARTING...")
    run_grounding = False
    try:
        answer = input("Run grounded validation? (y/N): ").strip().lower()
        run_grounding = answer in ("y", "yes")
    except Exception:
        run_grounding = False
    partials_dir = REPORTS_DIR / "partials"
    plots_dir = REPORTS_DIR / "plots"
    downloads_dir = REPORTS_DIR / "downloads"
    ensure_dirs(DATA_DIR, FIGURES_DIR, REPORTS_DIR, partials_dir, plots_dir, downloads_dir)
    
    logger.info("1. Adatfeldolgozás...")
    etl = ETLPipeline(DATA_DIR, URLS["ccyb"], URLS["syrb"])
    data = etl.run_pipeline()
    
    # --- CCyB Tables ---
    ccyb_full = data.get('ccyb_df')
    ccyb_decisions = pd.DataFrame()
    if ccyb_full is not None and not ccyb_full.empty:
        # A legfrissebb döntések kiválasztása
        temp = ccyb_full.sort_values(['decision_date', 'date'], ascending=[False, False]).head(10).copy()
        
        # Oszlopok kiválasztása - FONTOS: kisbetűvel, mert az ETL pipeline így adja át
        req = ['iso2', 'decision_date', 'date', 'rate', 'justification']
        ccyb_decisions = temp[[c for c in req if c in temp.columns]].copy()
        
        # Dátum formázás
        for col in ['decision_date', 'date']:
            if col in ccyb_decisions.columns:
                ccyb_decisions[col] = pd.to_datetime(ccyb_decisions[col]).dt.strftime('%Y-%m-%d')

    # --- SyRB Tables (Szigorú szűrés) ---
    syrb_full = data.get('syrb_df')
    syrb_decisions = pd.DataFrame()
    active_syrb = pd.DataFrame()
    
    today = pd.Timestamp.now()
    
    if syrb_full is not None and not syrb_full.empty:
        # A) LATEST DECISIONS
        syrb_decisions = syrb_full.sort_values('date', ascending=False).head(10).copy()
        cols_needed = ['date', 'iso2', 'syrb_type', 'exposure_type', 'rate_text', 'description']
        syrb_decisions = syrb_decisions[[c for c in cols_needed if c in syrb_decisions.columns]]
        if 'date' in syrb_decisions.columns: 
            syrb_decisions['date'] = pd.to_datetime(syrb_decisions['date']).dt.strftime('%Y-%m-%d')

        # B) CURRENTLY ACTIVE MEASURES (Szigorúbb logika)
        status_str = syrb_full['status'].astype(str)
        mask_active = (
            status_str.str.contains('applicable|active', case=False, na=False) |
            (syrb_full['date'] > today)
        ) & (~status_str.str.contains('Deactivated|Revoked|No longer', case=False, na=False))
        
        df_active = syrb_full[mask_active].sort_values('date', ascending=False)
        active_syrb = df_active.groupby(['iso2', 'exposure_type']).head(1).copy()
        
        if 'rate_numeric' in active_syrb.columns:
            active_syrb = active_syrb[active_syrb['rate_numeric'] > 0].copy()

        active_syrb = active_syrb[[c for c in cols_needed if c in active_syrb.columns]]
        if 'date' in active_syrb.columns: 
            active_syrb['date'] = pd.to_datetime(active_syrb['date']).dt.strftime('%Y-%m-%d')

    # 2. Vizualizáció
    logger.info("2. Grafikonok...")
    viz = Visualizer(FIGURES_DIR)
    today_str = datetime.now().strftime("%Y-%m-%d")
    plots_inline, plot_figs, download_data, paths = viz.generate_all_plots(data, today_str)
    
    # 3. AI Elemzés
    logger.info("3. AI Elemzés...")
    analyzer = LLMAnalyzer(LLM_CONFIG)
    
    # --- CCyB Enrichment ---
    if not ccyb_decisions.empty:
        # Justification kulcsszavak generálása
        if 'justification' in ccyb_decisions.columns:
            logger.info("   -> CCyB AI keywords generation...")
            # CSAK azokat a sorokat küldjük el az AI-nak, ahol van szöveg
            raw_justs = ccyb_decisions['justification'].fillna('').astype(str).tolist()
            
            # Debugging: log a few characters of raw justifications
            for i, rj in enumerate(raw_justs[:3]):
                logger.info(f"      [Debug] Raw Justification {i}: {rj[:50]}...")

            # Ha van legalább egy nem üres szövegünk
            if any(len(j.strip()) > 5 for j in raw_justs):
                kws = analyzer.extract_keywords(raw_justs, "justification")
                ccyb_decisions['justification'] = kws
                logger.info(f"      [Debug] Generated keywords: {kws[:3]}...")
            else:
                logger.warning("   -> No substantial justification text found to process.")
        
        # Táblázat oszlopnevek véglegesítése (Nagybetűsítés és átnevezés)
        ccyb_decisions.columns = [c.upper() for c in ccyb_decisions.columns]
        ccyb_decisions = ccyb_decisions.rename(columns={
            'ISO2': 'COUNTRY', 
            'DECISION_DATE': 'ANNOUNCEMENT', 
            'DATE': 'IMPLEMENTATION', 
            'JUSTIFICATION': 'JUSTIFICATION'
        })

    # --- SyRB Enrichment ---
    def enrich_syrb(df, label):
        if df.empty: return df
        logger.info(f"   -> SyRB AI cleaning ({label})...")
        combined_text = "Rate col: " + df['rate_text'].astype(str) + " | Desc: " + df['description'].astype(str)
        
        # 1. Rate
        clean_rates = analyzer.extract_clean_rates(combined_text.tolist())
        df['rate_text'] = clean_rates
        
        # 2. Details (Targeted risks/background)
        details = analyzer.extract_keywords(df['description'].astype(str).tolist(), "targeted risk or background")
        df['description'] = details
        
        df.columns = [c.upper() for c in df.columns]
        return df.rename(columns={'DATE':'EFFECTIVE FROM', 'ISO2':'COUNTRY', 'SYRB_TYPE':'TYPE', 'RATE_TEXT':'RATE', 'DESCRIPTION':'DETAILS'})

    active_syrb = enrich_syrb(active_syrb, "Active")
    syrb_decisions = enrich_syrb(syrb_decisions, "Decisions")

    # --- BBM Processing ---
    bbm_full = data.get('bbm_df')
    active_bbm = pd.DataFrame()
    bbm_decisions = pd.DataFrame()
    bbm_pivot_html = ""
    bbm_ref_date = ""
    ltv_table = pd.DataFrame()
    ltv_ref_date = ""
    
    if bbm_full is not None and not bbm_full.empty:
        logger.info("   -> BBM processing...")
        def extract_ltv_details(text):
            text = str(text or "")
            # LTV limits as percentage values
            limits = re.findall(r"(\\d+(?:\\.\\d+)?)\\s*%", text)
            limits = sorted({f"{l}%" for l in limits}, key=lambda x: float(x.strip('%')))
            limits_str = ", ".join(limits) if limits else "N/A"

            # First-time buyer (FTB) handling
            ftb_markers = ["first-time buyer", "first time buyer", "ftb", "first-time buyers", "first time buyers"]
            ftb_present = any(m in text.lower() for m in ftb_markers)
            ftb_flag = "Yes" if ftb_present else "No"

            # Extract sentences mentioning FTB
            sentences = re.split(r"(?<=[.!?])\\s+", text)
            ftb_details = [s.strip() for s in sentences if any(m in s.lower() for m in ftb_markers)]
            ftb_details = " ".join(ftb_details) if ftb_details else ""

            # Other exceptions / specific rules
            exception_markers = [
                "exception", "exempt", "exemption", "quota", "flexibility",
                "waiver", "additional", "higher limit", "region", "renovation",
                "energy", "cap", "ceiling", "special"
            ]
            other_details = [s.strip() for s in sentences if any(m in s.lower() for m in exception_markers)]
            other_details = " ".join(other_details) if other_details else ""

            return limits_str, ftb_flag, ftb_details, other_details

        # A) Aktív eszközök (Pivot Table)
        active_bbm = bbm_full[bbm_full['active_status'] == 'Active'].copy()
        
        if not bbm_full.empty:
            max_date = bbm_full['date'].max()
            if pd.notna(max_date):
                bbm_ref_date = max_date.strftime('%Y-%m-%d')

            rename_map = {
                'Loan-to-value (LTV)': 'LTV',
                'Debt-service-to-income (DSTI)': 'DSTI',
                'Loan-to-income (LTI)': 'LTI',
                'DTI': 'DTI',
                'Loan maturity': 'Maturity',
                'Loan amortisation': 'Amort.',
                'Flexibility quota': 'Flex.',
                'Stress test / sensitivity test': 'Stress T.'
            }
            bbm_matrix = bbm_full.copy()
            bbm_matrix['measure_short'] = bbm_matrix['measure_type'].map(lambda x: rename_map.get(x, x))

            def status_flag(row):
                status_text = f"{row.get('active_status','')} {row.get('status','')}".lower()
                if "active" in status_text or "applicable" in status_text:
                    return "active"
                if any(k in status_text for k in ["announc", "planned", "pending", "future", "not yet"]):
                    return "announced"
                return ""

            bbm_matrix['status_flag'] = bbm_matrix.apply(status_flag, axis=1)

            def pick_flag(values):
                vals = [v for v in values if v]
                if "active" in vals:
                    return "<span class='dot dot--active'></span>"
                if "announced" in vals:
                    return "<span class='dot dot--announced'></span>"
                return ""

            pivot_df = bbm_matrix.pivot_table(
                index='iso2',
                columns='measure_short',
                values='status_flag',
                aggfunc=pick_flag
            ).fillna('')
            
            pivot_df.index.name = 'COUNTRY'
            pivot_df.columns.name = None
            preferred_order = [
                "LTV", "DSTI", "DTI", "LTI", "Maturity",
                "Amort.", "Amortization", "Stress T.", "Stress Test",
                "Flex.", "Flexibility"
            ]
            ordered_cols = [c for c in preferred_order if c in pivot_df.columns]
            ordered_cols += [c for c in pivot_df.columns if c not in ordered_cols]
            pivot_df = pivot_df[ordered_cols].sort_index(axis=0)
            bbm_pivot_html = pivot_df.to_html(classes='display-table bbm-pivot', escape=False)

        # A1) LTV Subsection Table
        ltv_active = bbm_full[
            (bbm_full['active_status'] == 'Active') &
            (bbm_full['measure_type'].astype(str).str.contains('LTV', case=False, na=False))
        ].copy()
        if not ltv_active.empty:
            max_date = ltv_active['date'].max()
            if pd.notna(max_date):
                ltv_ref_date = max_date.strftime('%Y-%m-%d')

            descriptions = ltv_active['description'].fillna('').astype(str).tolist()
            ltv_llm = analyzer.extract_ltv_fields(descriptions)
            ltv_llm = ltv_llm if ltv_llm else [{} for _ in descriptions]
            llm_df = pd.DataFrame(ltv_llm)
            llm_df = llm_df.reindex(range(len(ltv_active))).fillna("")

            def normalize_limits(val):
                if isinstance(val, list):
                    cleaned = [str(v).strip() for v in val if str(v).strip()]
                    return ", ".join(sorted(set(cleaned), key=lambda x: float(x.strip('%')) if x.strip('%').replace('.', '').isdigit() else x))
                if isinstance(val, str) and val.strip():
                    return val.strip()
                return ""

            limits_series = llm_df['limits'] if 'limits' in llm_df.columns else pd.Series([""] * len(ltv_active))
            ftb_flag_series = llm_df['ftb_flag'] if 'ftb_flag' in llm_df.columns else pd.Series([""] * len(ltv_active))
            ftb_details_series = llm_df['ftb_details'] if 'ftb_details' in llm_df.columns else pd.Series([""] * len(ltv_active))
            other_series = llm_df['other_exceptions'] if 'other_exceptions' in llm_df.columns else pd.Series([""] * len(ltv_active))

            ltv_active['limits'] = limits_series.apply(normalize_limits)
            ltv_active['ftb_flag'] = ftb_flag_series.replace("", "No")
            ltv_active['ftb_details'] = ftb_details_series
            ltv_active['other_details'] = other_series

            # Fallback to regex if LLM output is missing
            for idx, row in ltv_active.iterrows():
                if not row.get('limits'):
                    limits_str, ftb_flag, ftb_details, other_details = extract_ltv_details(row.get('description', ''))
                    ltv_active.at[idx, 'limits'] = limits_str
                    if row.get('ftb_flag') in ("", None):
                        ltv_active.at[idx, 'ftb_flag'] = ftb_flag
                    if not row.get('ftb_details'):
                        ltv_active.at[idx, 'ftb_details'] = ftb_details
                    if not row.get('other_details'):
                        ltv_active.at[idx, 'other_details'] = other_details

            ltv_table = (
                ltv_active.groupby('country', as_index=False)
                .agg({
                    'limits': lambda x: ", ".join(sorted(set(", ".join(x.fillna("").astype(str)).split(", ")))) if x.notna().any() else "N/A",
                    'ftb_flag': lambda x: "Yes" if (x == "Yes").any() else "No",
                    'ftb_details': lambda x: " ".join([v for v in x.fillna("").astype(str) if v]).strip(),
                    'other_details': lambda x: " ".join([v for v in x.fillna("").astype(str) if v]).strip(),
                })
            )
            ltv_table = ltv_table.rename(columns={
                'country': 'COUNTRY',
                'limits': 'LTV LIMITS',
                'ftb_flag': 'FTB DISCOUNT',
                'ftb_details': 'FTB DETAILS',
                'other_details': 'OTHER EXCEPTIONS'
            })

        # B) Legutóbbi 10 BBM döntés
        bbm_decisions = bbm_full.sort_values('date', ascending=False).head(10).copy()
        cols_bbm_dec = ['date', 'iso2', 'measure_type', 'status', 'description']
        bbm_decisions = bbm_decisions[[c for c in cols_bbm_dec if c in bbm_decisions.columns]]
        
        if not bbm_decisions.empty:
            logger.info("   -> BBM AI cleaning (Decisions)...")
            if 'date' in bbm_decisions.columns:
                bbm_decisions['date'] = pd.to_datetime(bbm_decisions['date']).dt.strftime('%Y-%m-%d')
            
            # AI Tisztítás a leírásra
            details = analyzer.extract_keywords(bbm_decisions['description'].astype(str).tolist(), "targeted risk or background")
            bbm_decisions['description'] = details
            
            bbm_decisions.columns = [c.upper() for c in bbm_decisions.columns]
            bbm_decisions = bbm_decisions.rename(columns={'DATE': 'DATE', 'ISO2': 'COUNTRY', 'MEASURE_TYPE': 'TYPE', 'STATUS': 'STATUS', 'DESCRIPTION': 'DETAILS'})

    # --- News Processing ---
    def parse_news_date(text):
        if not text:
            return ""
        text = str(text)
        iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
        if iso_match:
            return iso_match.group(1)
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7,
            "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9,
            "september": 9, "oct": 10, "october": 10, "nov": 11, "november": 11,
            "dec": 12, "december": 12
        }
        match = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", text)
        if match:
            month = month_map.get(match.group(1).lower())
            if month:
                try:
                    return datetime(int(match.group(3)), month, int(match.group(2))).strftime("%Y-%m-%d")
                except Exception:
                    return ""
        match = re.search(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", text)
        if match:
            month = month_map.get(match.group(2).lower())
            if month:
                try:
                    return datetime(int(match.group(3)), month, int(match.group(1))).strftime("%Y-%m-%d")
                except Exception:
                    return ""
        return ""
    def fetch_news():
        if not NEWS_CONFIG.get("enabled", True):
            return pd.DataFrame()
        api_key = os.getenv(NEWS_CONFIG.get("api_key_env", "CUSTOM_SEARCH_API_KEY"), "")
        cse_id = os.getenv(NEWS_CONFIG.get("cse_id_env", "GOOGLE_CSE_ID"), "")
        if not api_key or not cse_id:
            logger.warning("News search not configured (CUSTOM_SEARCH_API_KEY / GOOGLE_CSE_ID).")
            return pd.DataFrame()

        query = NEWS_CONFIG.get("query", "")
        months_back = int(NEWS_CONFIG.get("months_back", 6))
        max_results = int(NEWS_CONFIG.get("max_results", 10))

        params = {
            "key": api_key,
            "cx": cse_id,
            "q": query,
            "dateRestrict": f"m{months_back}",
            "num": max_results,
        }
        try:
            resp = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=20)
            resp.raise_for_status()
            items = resp.json().get("items", [])[:max_results]
        except Exception as exc:
            logger.warning(f"News search failed: {exc}")
            return pd.DataFrame()

        date_keys = [
            "article:published_time", "og:published_time", "date", "dc.date",
            "dc.date.issued", "citation_publication_date", "citation_date", "pubdate"
        ]
        rows = []
        for item in items:
            link = item.get("link", "")
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            source = ""
            if link:
                try:
                    source = link.split("/")[2]
                except Exception:
                    source = ""
            raw_date = ""
            for meta in item.get("pagemap", {}).get("metatags", []):
                if not isinstance(meta, dict):
                    continue
                for key in date_keys:
                    if key in meta:
                        raw_date = meta.get(key, "")
                        break
                if raw_date:
                    break
            parsed_date = parse_news_date(raw_date)
            date_source = "meta" if parsed_date else ""
            if not parsed_date:
                parsed_date = parse_news_date(snippet)
                date_source = "snippet" if parsed_date else ""
            if not parsed_date:
                parsed_date = parse_news_date(title)
                date_source = "title" if parsed_date else ""
            rows.append({
                "TITLE": title,
                "SOURCE": source,
                "SUMMARY": snippet,
                "LINK": link,
                "DATE": parsed_date,
                "DATE_SOURCE": date_source,
            })
        return pd.DataFrame(rows)

    news_df = fetch_news()
    if news_df is not None and not news_df.empty:
        try:
            news_texts = (news_df['TITLE'].fillna('') + " - " + news_df['SUMMARY'].fillna('')).tolist()
            news_tags = analyzer.classify_news_tags(news_texts)
            news_df['TAGS'] = news_tags
        except Exception as exc:
            logger.warning(f"News tag classification failed: {exc}")
        try:
            summaries = analyzer.summarize_news_items(
                (news_df['TITLE'].fillna('') + ". " + news_df['SUMMARY'].fillna('')).tolist()
            )
            news_df['SUMMARY_SHORT'] = summaries
        except Exception as exc:
            logger.warning(f"News summarization failed: {exc}")
            news_df['SUMMARY_SHORT'] = news_df['SUMMARY'].fillna('').astype(str).apply(
                lambda x: (x[:220] + '...') if len(x) > 220 else x
            )
        try:
            news_df['DATE_PARSED'] = pd.to_datetime(news_df['DATE'], errors='coerce')
            news_df = news_df.sort_values('DATE_PARSED', ascending=False, na_position='last')
        except Exception:
            pass

    analysis_inputs = {
        'latest_ccyb_df': data.get('latest_ccyb_df'),
        'ccyb_decisions_df': ccyb_decisions,
        'active_syrb_df': active_syrb,
        'syrb_decisions_df': syrb_decisions,
        'active_bbm_df': active_bbm,
        'bbm_decisions_df': bbm_decisions,
        'latest_syrb_df': data.get('latest_syrb_df'),
        'latest_bbm_df': data.get('latest_bbm_df'),
        'ltv_table_df': ltv_table,
        'news_df': news_df,
    }

    analyses = analyzer.run_analysis(analysis_inputs, paths, {})

    # 3b. Grounded validation against data, charts, and external sources
    if run_grounding:
        logger.info("3b. Grounded Validation...")
        validator = GroundingValidator(LLM_CONFIG, SEARCH_CONFIG, analyzer._clean_text)
        analyses = validator.run(analyses, analysis_inputs, data)
    
    # 4. Render
    logger.info("4. Riport...")
    def to_html(df):
        if df is None or df.empty: return "<p class='no-data'>No Data</p>"
        df_copy = df.copy()
        # Minden szöveges oszlopot kezelünk, de kiemelten a JUSTIFICATION/DETAILS-t
        for col in ['DETAILS', 'REASONS', 'JUSTIFICATION', 'FTB DETAILS', 'OTHER EXCEPTIONS', 'SUMMARY']:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(lambda x: (str(x)[:200] + '...') if len(str(x)) > 200 else x)
        return df_copy.to_html(index=False, classes='display-table', escape=False)

    def extract_news_tags(text):
        text = (text or "").lower()
        tag_defs = [
            ("CCyB", "ccyb", ["ccyb", "countercyclical capital buffer", "countercyclical buffer"]),
            ("SyRB", "syrb", ["syrb", "systemic risk buffer"]),
            ("BBM", "bbm", ["borrower-based", "bbm", "borrower based"]),
            ("LTV", "ltv", ["ltv", "loan-to-value"]),
            ("DSTI", "dsti", ["dsti", "debt-service-to-income"]),
            ("LTI", "lti", ["lti", "loan-to-income"]),
            ("DTI", "dti", ["dti", "debt-to-income"]),
            ("Real Estate", "real-estate", ["real estate", "property", "housing", "mortgage"]),
            ("Capital", "capital", ["capital buffer", "capital requirement", "capital"]),
            ("Reciprocation", "reciprocation", ["reciprocation", "reciprocity"]),
        ]
        tags = []
        for label, slug, terms in tag_defs:
            if any(term in text for term in terms):
                tags.append((label, slug))
            if len(tags) >= 4:
                break
        return tags

    def slug_to_label(slug):
        labels = {
            "ccyb": "CCyB",
            "syrb": "SyRB",
            "bbm": "BBM",
            "ltv": "LTV",
            "dsti": "DSTI",
            "lti": "LTI",
            "dti": "DTI",
            "real-estate": "Real Estate",
            "capital": "Capital",
            "reciprocation": "Reciprocation",
        }
        return labels.get(slug, slug.title())

    def build_source_initials(source):
        if not source:
            return "N"
        parts = re.split(r"[\.\-]", source)
        letters = [p[0].upper() for p in parts if p]
        initials = "".join(letters[:2]) if letters else source[:2].upper()
        return initials

    def detect_countries(text):
        text = (text or "").lower()
        countries = [
            "austria", "belgium", "bulgaria", "croatia", "cyprus", "czech republic",
            "denmark", "estonia", "finland", "france", "germany", "greece",
            "hungary", "ireland", "italy", "latvia", "lithuania", "luxembourg",
            "malta", "netherlands", "poland", "portugal", "romania", "slovakia",
            "slovenia", "spain", "sweden", "norway", "switzerland", "iceland",
            "united kingdom", "uk", "england"
        ]
        found = []
        for country in countries:
            if re.search(rf"\\b{re.escape(country)}\\b", text):
                label = "United Kingdom" if country in ("uk", "england") else country.title()
                if label not in found:
                    found.append(label)
            if len(found) >= 3:
                break
        return found

    def build_news_feed(df):
        if df is None or df.empty:
            return "<div class='empty-state'>No news available.</div>"
        cards = []
        for _, row in df.iterrows():
            title = html.escape(str(row.get("TITLE", "")).strip())
            summary_raw = str(row.get("SUMMARY_SHORT", "")).strip() or str(row.get("SUMMARY", "")).strip()
            summary = html.escape(summary_raw)
            link = html.escape(str(row.get("LINK", "")).strip())
            source_raw = str(row.get("SOURCE", "")).strip()
            source = html.escape(source_raw)
            date_text = html.escape(str(row.get("DATE", "")).strip())
            date_source = str(row.get("DATE_SOURCE", "")).strip()
            llm_tags = row.get("TAGS") if isinstance(row.get("TAGS"), list) else []
            tags = [(slug_to_label(slug), slug) for slug in llm_tags] or extract_news_tags(f"{title} {summary}")
            tag_html = "".join([
                f"<span class='news-tag news-tag--{slug}'>{label}</span>"
                for label, slug in tags
            ])
            tag_slugs = " ".join([slug for _, slug in tags])
            search_text = html.escape(f"{title} {summary} {source}").lower()
            icon_text = build_source_initials(source)
            favicon = f"https://www.google.com/s2/favicons?domain={source_raw}&sz=64" if source_raw else ""
            country_list = detect_countries(f"{title} {summary}")
            countries_html = "".join([f"<span class='news-pill'>{html.escape(c)}</span>" for c in country_list])
            published_label = "Published" if date_source == "meta" else "Reported"
            cards.append(
                f"""
                <article class="news-card" data-tags="{tag_slugs}" data-search="{search_text}">
                    <div class="news-card__header">
                        <div class="news-tags">{tag_html}</div>
                        <div class="news-meta">
                            {f"<span class='news-date'>{published_label}: {date_text}</span>" if date_text else ""}
                            <span class="news-date">Retrieved: {today_str}</span>
                        </div>
                    </div>
                    <h3 class="news-title">{title or "Untitled update"}</h3>
                    {f"<div class='news-countries'>{countries_html}</div>" if countries_html else ""}
                    <p class="news-summary">{summary or "No summary available."}</p>
                    <div class="news-divider"></div>
                    <div class="news-actions">
                        <div class="news-source">
                            <span class="news-source__icon">
                                {f'<img class="news-source__favicon" src="{favicon}" alt="">' if favicon else icon_text}
                            </span>
                            <span>{source or "Source"}</span>
                        </div>
                        <a class="news-link" href="{link}" target="_blank" rel="noopener">
                            <span class="news-link__icon" aria-hidden="true"><i data-lucide="share-2"></i></span>
                            Read original
                        </a>
                    </div>
                </article>
                """
            )
        return f"<div class='news-feed'>{''.join(cards)}</div>"

    def rel_path(path):
        try:
            return path.relative_to(BASE_DIR).as_posix()
        except Exception:
            return path.as_posix()

    def wrap_partial(body_html):
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="../../assets/embed.css">
</head>
<body>
    <div class="embed-wrapper">
        {body_html or "<p class='no-data'>No Data</p>"}
    </div>
</body>
</html>"""

    def write_partial(name, html):
        path = partials_dir / f"{name}.html"
        path.write_text(wrap_partial(html), encoding="utf-8")
        return rel_path(path)

    def write_plot_html(name, fig):
        if fig is None:
            return ""
        plot_html = fig.to_html(full_html=True, include_plotlyjs='cdn', config={"responsive": True})
        path = plots_dir / f"{name}.html"
        path.write_text(plot_html, encoding="utf-8")
        return rel_path(path)

    def write_download(name, df):
        if df is None or df.empty:
            return ""
        path = downloads_dir / f"{name}.xlsx"
        df.to_excel(path, index=False)
        return rel_path(path)

    table_files = {
        "ccyb_decisions": write_partial("ccyb_decisions", to_html(ccyb_decisions)),
        "syrb_active": write_partial("syrb_active", to_html(active_syrb)),
        "syrb_decisions": write_partial("syrb_decisions", to_html(syrb_decisions)),
        "bbm_pivot": write_partial("bbm_pivot", bbm_pivot_html or "<p class='no-data'>No Data</p>"),
        "bbm_decisions": write_partial("bbm_decisions", to_html(bbm_decisions)),
        "ltv_table": write_partial("ltv_table", to_html(ltv_table)),
    }

    plot_files = {
        "ccyb_diffusion": write_plot_html("ccyb_diffusion", plot_figs.get("ccyb_diffusion")),
        "cross_section_map": write_plot_html("cross_section_map", plot_figs.get("cross_section_map")),
        "cross_section_bar": write_plot_html("cross_section_bar", plot_figs.get("cross_section_bar")),
        "risk_plot": write_plot_html("risk_plot", plot_figs.get("risk_plot")),
        "syrb_counts_trend": write_plot_html("syrb_counts_trend", plot_figs.get("syrb_counts_trend")),
        "syrb_sector": write_plot_html("syrb_sector", plot_figs.get("syrb_sector")),
        "bbm_diffusion": write_plot_html("bbm_diffusion", plot_figs.get("bbm_diffusion")),
    }

    download_links = {
        "ccyb_diffusion": write_download("ccyb_diffusion", download_data.get("ccyb_diffusion")),
        "bbm_diffusion": write_download("bbm_diffusion", download_data.get("bbm_diffusion")),
    }

    env = Environment(loader=FileSystemLoader('.'))
    rendered_html = env.get_template('report_template.html').render(
        generation_date=today_str,
        analyses=analyses,
        plots_inline=plots_inline,
        plot_files=plot_files,
        download_links=download_links,
        table_files=table_files,
        news_feed_html=build_news_feed(news_df),
        bbm_ref_date=bbm_ref_date,
        ltv_ref_date=ltv_ref_date
    )
    
    with open("index.html", "w", encoding="utf-8") as f: f.write(rendered_html)
    logger.info("DONE: index.html")

if __name__ == "__main__":
    main()