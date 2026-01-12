import logging
import pandas as pd
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from config import DATA_DIR, URLS, FIGURES_DIR, LLM_CONFIG
from utils import ensure_dirs
from etl import ETLPipeline
from visualizer import Visualizer
from llm_analysis import LLMAnalyzer

logging.basicConfig(level=logging.INFO, format='%(message)s')
for noisy_lib in ['kaleido', 'urllib3', 'matplotlib', 'chromies', 'werkzeug']:
    logging.getLogger(noisy_lib).setLevel(logging.CRITICAL)

logger = logging.getLogger("MAIN")

def main():
    logger.info("STARTING...")
    ensure_dirs(DATA_DIR, FIGURES_DIR)
    
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
    plots, paths = viz.generate_all_plots(data, today_str)
    
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
    bbm_pivot_html = ""
    bbm_ref_date = ""
    
    if bbm_full is not None and not bbm_full.empty:
        logger.info("   -> BBM processing (Compact Pivot Table)...")
        # Aktív eszközök szűrése
        active_bbm = bbm_full[bbm_full['active_status'] == 'Active'].copy()
        
        if not active_bbm.empty:
            # Referencia dátum meghatározása (legfrissebb mérés dátuma)
            max_date = active_bbm['date'].max()
            if pd.notna(max_date):
                bbm_ref_date = max_date.strftime('%Y-%m-%d')

            # Rövidítések alkalmazása
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
            active_bbm['measure_short'] = active_bbm['measure_type'].map(lambda x: rename_map.get(x, x))

            # Pivot tábla létrehozása: Sorok = Country, Oszlopok = Measure Type
            pivot_df = active_bbm.pivot_table(
                index='iso2', 
                columns='measure_short', 
                values='active_status', 
                aggfunc=lambda x: '✅'
            ).fillna('')
            
            # Tisztítás
            pivot_df.index.name = 'COUNTRY'
            pivot_df.columns.name = None
            
            # Sorok és oszlopok rendezése
            pivot_df = pivot_df.sort_index(axis=0).sort_index(axis=1)
            
            # HTML generálás
            bbm_pivot_html = pivot_df.to_html(classes='display-table bbm-pivot', escape=False)
        else:
            bbm_pivot_html = "<p class='no-data'>No active borrower-based measures found.</p>"

    analyses = analyzer.run_analysis(
        {'latest_ccyb_df': data.get('latest_ccyb_df'), 'ccyb_decisions_df': ccyb_decisions,
         'active_syrb_df': active_syrb, 'syrb_decisions_df': syrb_decisions,
         'active_bbm_df': active_bbm}, 
        paths, {}
    )
    
    # 4. Render
    logger.info("4. Riport...")
    def to_html(df):
        if df is None or df.empty: return "<p class='no-data'>No Data</p>"
        df_copy = df.copy()
        # Minden szöveges oszlopot kezelünk, de kiemelten a JUSTIFICATION/DETAILS-t
        for col in ['DETAILS', 'REASONS', 'JUSTIFICATION']:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(lambda x: (str(x)[:200] + '...') if len(str(x)) > 200 else x)
        return df_copy.to_html(index=False, classes='display-table', escape=False)

    env = Environment(loader=FileSystemLoader('.'))
    html = env.get_template('report_template.html').render(
        generation_date=today_str, plots=plots, analyses=analyses,
        ccyb_decisions_html=to_html(ccyb_decisions),
        syrb_active_html=to_html(active_syrb),
        syrb_decisions_html=to_html(syrb_decisions),
        bbm_pivot_html=bbm_pivot_html,
        bbm_ref_date=bbm_ref_date
    )
    
    with open("index.html", "w", encoding="utf-8") as f: f.write(html)
    logger.info("DONE: index.html")

if __name__ == "__main__":
    main()