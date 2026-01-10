import pandas as pd
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timedelta
import os
import sys

# Útvonalak
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts'))

from llm_analysis import perform_llm_analysis
from etl_process import run_etl
from plot_generator import generate_interactive_plots 

def main():
    print("\n==========================================")
    print("   🇪🇺 EU MACRO POLICY HUB - AUTOMATION")
    print("==========================================\n")

    # 1. ETL
    data = run_etl()
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 2. PLOTOK
    print("--- 2. VIZUALIZÁCIÓ (Képek generálása...) ---")
    plots, plot_paths = generate_interactive_plots(data, ref_date=today_str)
    print(f"  🖼️  Kész: {len(plot_paths)} kép lementve.")

    # 3. AI ELEMZÉS
    print("--- 3. AI ELEMZÉS (Gemini) ---")
    analysis_inputs = {
        'latest_country_df': data.get('latest_country_df'),
        'latest_syrb_df': data.get('latest_syrb_df')
    }
    analyses = perform_llm_analysis(analysis_inputs, plot_paths, "2024-01-01", {}) 

    # 4. REPORT
    print("--- 4. HTML RENDERELÉS ---")
    
    ccyb_df, syrb_df = data.get('ccyb_df'), data.get('syrb_df')
    latest_syrb = data.get('latest_syrb_df')

    # CCyB Table (Last 10 - Dátumszűrő nélkül, hogy biztos legyen adat)
    policy_html = "<p>Nincs friss adat.</p>"
    if ccyb_df is not None and not ccyb_df.empty:
        recent = ccyb_df.sort_values('date', ascending=False).head(10).copy()
        recent['date'] = pd.to_datetime(recent['date']).dt.strftime('%Y-%m-%d')
        recent['rate'] = recent['rate'].astype(str) + '%'
        cols = {'country': 'Country', 'date': 'Decision Date', 'rate': 'New Rate', 'status': 'Status'}
        policy_html = recent[list(cols.keys())].rename(columns=cols).to_html(index=False, classes="custom-table", border=0)

    # SyRB Tables
    syrb_decisions_html = "<p>Nincs adat.</p>"
    if syrb_df is not None and not syrb_df.empty:
        last_10 = syrb_df.sort_values('date', ascending=False).head(10).copy()
        last_10['date'] = pd.to_datetime(last_10['date']).dt.strftime('%Y-%m-%d')
        last_10['rate'] = last_10['rate'].apply(lambda x: f"{x}%" if isinstance(x, (int, float)) else str(x))
        cols = {'country': 'Country', 'date': 'Effective Date', 'rate': 'Rate', 'exposure_type': 'Scope'}
        syrb_decisions_html = last_10[list(cols.keys())].rename(columns=cols).to_html(index=False, classes="custom-table", border=0)

    syrb_gen_html = "<p>Nincs aktív.</p>"
    syrb_sec_html = "<p>Nincs aktív.</p>"
    
    if latest_syrb is not None and not latest_syrb.empty:
        # General
        gen = latest_syrb[(pd.to_numeric(latest_syrb['rate'], errors='coerce') > 0) & (latest_syrb['syrb_type'] == 'General')].copy()
        if not gen.empty:
            gen = gen.sort_values('rate', ascending=False)
            gen['rate'] = gen['rate'].astype(str) + '%'
            cols = {'country': 'Country', 'rate': 'Rate', 'exposure_type': 'Scope'}
            syrb_gen_html = f"<h4>Active General SyRB ({today_str})</h4>" + gen[list(cols.keys())].rename(columns=cols).to_html(index=False, classes="custom-table", border=0)

        # Sectoral
        sec = latest_syrb[(pd.to_numeric(latest_syrb['rate'], errors='coerce') > 0) & (latest_syrb['syrb_type'] == 'Sectoral')].copy()
        if not sec.empty:
            sec = sec.sort_values(['country', 'rate'], ascending=[True, False])
            sec['rate'] = sec['rate'].astype(str) + '%'
            cols = {'country': 'Country', 'rate': 'Rate', 'exposure_type': 'Targeted Sector'}
            syrb_sec_html = f"<h4>Active Sectoral SyRB ({today_str})</h4>" + sec[list(cols.keys())].rename(columns=cols).to_html(index=False, classes="custom-table", border=0)

    try:
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template('report_template.html')
        
        html_out = template.render(
            generation_date=datetime.now().strftime("%Y-%m-%d %H:%M"),
            analyses=analyses, plots=plots, 
            policy_table_html=policy_html,
            syrb_decisions_html=syrb_decisions_html,
            syrb_general_html=syrb_gen_html,
            syrb_status_html=syrb_sec_html
        )
        
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(html_out)
            
        print("✅ KÉSZ! A riport elérhető: index.html")
        
    except Exception as e:
        print(f"\n❌ Hiba a renderelésnél: {e}")

if __name__ == "__main__":
    main()