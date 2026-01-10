import pandas as pd
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timedelta
import os
import sys

# Útvonalak
current_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.join(current_dir, 'scripts')
sys.path.append(scripts_dir)

from llm_analysis import perform_llm_analysis, tasks, batch_summarize_reasons
from etl_process import run_etl
from plot_generator import generate_interactive_plots 

def calculate_syrb_trend(syrb_df):
    if syrb_df is None or syrb_df.empty: return pd.DataFrame()
    
    # General Trend
    gen_df = syrb_df[syrb_df['syrb_type'] == 'General']
    trend_gen = (gen_df.pivot_table(index='date', columns='country', values='rate', aggfunc='max').ffill().fillna(0) > 0).sum(axis=1) if not gen_df.empty else pd.Series()

    # Sectoral Trend
    sec_df = syrb_df[syrb_df['syrb_type'] == 'Sectoral']
    trend_sec = (sec_df.pivot_table(index='date', columns='country', values='rate', aggfunc='max').ffill().fillna(0) > 0).sum(axis=1) if not sec_df.empty else pd.Series()
        
    # JAVÍTÁS ITT: .fillna(method='ffill') helyett .ffill()
    trend_df = pd.DataFrame({'n_general': trend_gen, 'n_sectoral': trend_sec}).ffill().fillna(0).reset_index().rename(columns={'index': 'date'})
    
    return trend_df

def main():
    print("--- 1. ETL Pipeline ---")
    try:
        etl_results = run_etl()
        df = etl_results.get('ccyb_df') 
        agg_trend_df = etl_results.get('agg_trend')
        latest_country_df = etl_results.get('latest_ccyb')
        syrb_df = etl_results.get('syrb_df')
        latest_syrb_df = etl_results.get('latest_syrb')
    except Exception as e:
        print(f"ETL Error: {e}. Exiting.")
        return

    print("   -> Calculating SyRB diffusion trends...")
    syrb_trend_df = calculate_syrb_trend(syrb_df)

    data_store = {
        'df': df, 'agg_trend_df': agg_trend_df, 'latest_country_df': latest_country_df,
        'syrb_df': syrb_df, 'latest_syrb_df': latest_syrb_df, 'syrb_trend_df': syrb_trend_df
    }
    
    print("--- 2. Generating Plots ---")
    plots = generate_interactive_plots(data_store)
    
    print("--- 3. AI Analysis ---")
    one_year_ago = datetime.now() - timedelta(days=365)
    try:
        analyses = perform_llm_analysis(data_store, plots.keys(), one_year_ago, {})
    except Exception as e:
        print(f"AI Warning: {e}")
        analyses = {}
    
    print("--- 4. Rendering Tables & Report ---")
    
    # --- A) CCyB Táblázat ---
    table_df = latest_country_df.copy()
    
    if 'date' in table_df.columns: table_df['date'] = pd.to_datetime(table_df['date']).dt.strftime('%Y-%m-%d')
    if 'announcement_date' in table_df.columns: table_df['announcement_date'] = pd.to_datetime(table_df['announcement_date']).dt.strftime('%Y-%m-%d')
    else: table_df['announcement_date'] = "N/A"
    if 'rate' in table_df.columns: table_df['rate'] = table_df['rate'].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")

    if 'date' in table_df.columns: table_df = table_df.sort_values('date', ascending=False)
    top_10_df = table_df.head(10).copy()
    
    print("  -> Generating smart summaries for CCyB...")
    
    # Fallback logika biztosítása
    if 'reasoning' not in top_10_df.columns and 'justification' in top_10_df.columns:
         top_10_df['reasoning'] = top_10_df['justification'].astype(str).str[:50] + "..."

    # AI Classificaton
    if 'justification' in top_10_df.columns:
        raw_texts = top_10_df['justification'].fillna("").tolist()
        smart_categories = batch_summarize_reasons(raw_texts)
        
        if smart_categories:
            print("     ✅ AI classification successful.")
            top_10_df['reasoning'] = smart_categories
        else:
            print("     ⚠️ AI unavailable. Keeping ETL keywords.")

    display_columns = {
        'country': 'Country', 'rate': 'Current Rate', 'announcement_date': 'Announcement Date',
        'date': 'Effective Date', 'reasoning': 'Primary Driver'
    }
    final_table_df = top_10_df[[c for c in display_columns.keys() if c in top_10_df.columns]].rename(columns=display_columns)
    
    policy_table_html = final_table_df.to_html(index=False, classes="custom-table", border=0, justify="left")

    # --- B) SyRB Táblázatok ---
    syrb_decisions_html = "<p>No recent data.</p>"
    if syrb_df is not None and not syrb_df.empty:
        last_10 = syrb_df.sort_values('date', ascending=False).head(10).copy()
        last_10['date'] = pd.to_datetime(last_10['date']).dt.strftime('%Y-%m-%d')
        last_10['rate'] = last_10['rate'].astype(str) + '%'
        last_10['description'] = last_10['description'].astype(str).str[:100] + "..."
        cols = {'country': 'Country', 'date': 'Effective Date', 'rate': 'Rate', 'syrb_type': 'Type', 'description': 'Details'}
        syrb_decisions_html = last_10[cols.keys()].rename(columns=cols).to_html(index=False, classes="custom-table", border=0, justify="left")

    syrb_status_html = "<p>No active data.</p>"
    if latest_syrb_df is not None and not latest_syrb_df.empty:
        active = latest_syrb_df[latest_syrb_df['rate'] > 0].copy()
        active['date'] = pd.to_datetime(active['date']).dt.strftime('%Y-%m-%d')
        active['rate'] = active['rate'].astype(str) + '%'
        if 'exposure_type' not in active.columns: active['exposure_type'] = "N/A"
        cols = {'country': 'Country', 'rate': 'Rate', 'syrb_type': 'Type', 'exposure_type': 'Targeted Exposures'}
        syrb_status_html = active[cols.keys()].rename(columns=cols).to_html(index=False, classes="custom-table", border=0, justify="left")

    # Template renderelés
    try:
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template('report_template.html')
        html_out = template.render(
            generation_date=datetime.now().strftime("%Y-%m-%d %H:%M"),
            analyses=analyses, plots=plots, 
            policy_table_html=policy_table_html,
            syrb_decisions_html=syrb_decisions_html,
            syrb_status_html=syrb_status_html
        )
        with open("index.html", "w", encoding="utf-8") as f: f.write(html_out)
        print("SUCCESS: index.html generated successfully!")
    except Exception as e:
        print(f"Error during HTML rendering: {e}")

if __name__ == "__main__":
    main()