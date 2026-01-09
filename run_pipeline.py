import pandas as pd
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timedelta
import os
import sys

# --- ÚTVONAL KONFIGURÁCIÓ (EZ A LÉNYEG) ---
# Hozzáadjuk a 'scripts' mappát a Python keresési útvonalához
# Így a 'run_pipeline.py' megtalálja a modulokat, hiába vannak almappában.
current_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.join(current_dir, 'scripts')
sys.path.append(scripts_dir)

# Most már simán importálhatunk, mintha a gyökérben lennének
from llm_analysis import perform_llm_analysis, tasks
from etl_process import run_etl
from plot_generator import generate_interactive_plots 

def main():
    print(f"Working Directory: {os.getcwd()}")
    
    # 1. ETL Futtatása (Adatok frissítése)
    print("--- 1. ETL Pipeline ---")
    try:
        # A run_etl visszatérési értékeit kicsomagoljuk
        df, agg_trend_df, latest_country_df = run_etl()
    except Exception as e:
        print(f"CRITICAL ERROR in ETL: {e}")
        # Ha az ETL elszáll, megpróbáljuk betölteni a meglévő fájlokat fallback-ként
        try:
            print("Attempting to load existing parquet files...")
            df = pd.read_parquet(os.path.join("data", "processed_data.parquet"))
            agg_trend_df = pd.read_parquet(os.path.join("data", "agg_trend.parquet"))
            latest_country_df = pd.read_parquet(os.path.join("data", "latest_country.parquet"))
        except:
            print("No existing data found. Exiting.")
            return

    data_store = {
        'df': df,
        'agg_trend_df': agg_trend_df,
        'latest_country_df': latest_country_df
    }
    
    # 2. Interaktív Ábrák (Plotly)
    print("--- 2. Generating Plots ---")
    plots = generate_interactive_plots(data_store)
    
    # 3. AI Elemzés (Google Search Grounding-gal)
    print("--- 3. AI Analysis ---")
    external_contexts = {} 
    
    if not latest_country_df.empty and 'date' in latest_country_df.columns:
        latest_date = str(latest_country_df['date'].max())[:10]
    else:
        latest_date = datetime.now().strftime("%Y-%m-%d")

    one_year_ago = datetime.now() - timedelta(days=365)
    
    # Futtatás
    try:
        analyses = perform_llm_analysis(data_store, plots.keys(), one_year_ago, external_contexts)
    except Exception as e:
        print(f"AI Analysis Warning: {e}")
        analyses = {} # Üres elemzés, hogy a riport legalább elkészüljön
    
    # 4. HTML Renderelés (Táblázat formázás)
    print("--- 4. Rendering Report ---")
    
    table_df = latest_country_df.copy()
    
    # Dátum és Szám formázás
    if 'date' in table_df.columns:
        table_df['date'] = pd.to_datetime(table_df['date']).dt.strftime('%Y-%m-%d')
    if 'rate' in table_df.columns:
        table_df['rate'] = table_df['rate'].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")
        
    display_columns = {
        'country': 'Country',
        'rate': 'Current Rate',
        'date': 'Effective Date',
        'reasoning': 'Primary Driver'
    }
    
    # Csak a létező oszlopok kiválasztása
    cols_to_use = [c for c in display_columns.keys() if c in table_df.columns]
    
    # Rendezés a legfrissebb döntés szerint, ha van dátum
    if 'date' in table_df.columns:
        final_table_df = table_df.sort_values('date', ascending=False)
    else:
        final_table_df = table_df
        
    final_table_df = final_table_df[cols_to_use].rename(columns=display_columns).head(10)
    
    policy_table_html = final_table_df.to_html(
        index=False,
        classes="custom-table",
        border=0,
        justify="left"
    )

    # Template betöltése és renderelés
    try:
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template('report_template.html')
        
        html_out = template.render(
            generation_date=datetime.now().strftime("%Y-%m-%d %H:%M"),
            analyses=analyses,
            plots=plots, 
            policy_table_html=policy_table_html
        )
        
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(html_out)
            
        print("SUCCESS: index.html generated successfully!")
        
    except Exception as e:
        print(f"Error during HTML rendering: {e}")

if __name__ == "__main__":
    main()