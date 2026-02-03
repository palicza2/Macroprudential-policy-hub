import logging
import sys
import re
import os
import pandas as pd
from datetime import datetime

from config import BASE_DIR, DATA_DIR, URLS, FIGURES_DIR, REPORTS_DIR, LLM_CONFIG, SEARCH_CONFIG, NEWS_CONFIG
from utils import ensure_dirs
from etl import ETLPipeline
from visualizer import Visualizer
from llm_analysis import LLMAnalyzer
from grounding_validator import GroundingValidator

from bbm import build_bbm_matrix_html, extract_ltv_details_regex
from news import fetch_news, build_news_feed_html
from render import render_report, df_to_html_table
from ccyb import prepare_ccyb_decisions
from syrb import prepare_syrb_tables
from capital_overall import build_capital_overall_df
from country_profiles import CountryProfileGenerator
import json

logging.basicConfig(level=logging.INFO, format='%(message)s')
for noisy_lib in ['kaleido', 'urllib3', 'matplotlib', 'chromies', 'werkzeug']:
    logging.getLogger(noisy_lib).setLevel(logging.CRITICAL)

logger = logging.getLogger("MAIN")


def serialize_profile(profile):
    """Convert profile data to JSON-serializable format."""
    import pandas as pd
    from datetime import datetime
    
    def convert_value(v):
        if v is None:
            return None
        if isinstance(v, (pd.Timestamp, datetime)):
            try:
                return v.isoformat() if pd.notna(v) else None
            except:
                return str(v) if v else None
        elif isinstance(v, pd.Series):
            return v.tolist()
        elif isinstance(v, dict):
            return {k: convert_value(val) for k, val in v.items()}
        elif isinstance(v, list):
            return [convert_value(item) for item in v]
        elif isinstance(v, pd.DataFrame):
            if v.empty:
                return []
            return v.to_dict('records')
        elif pd.isna(v):
            return None
        elif isinstance(v, (int, float)):
            return float(v) if pd.notna(v) else None
        else:
            return v
    
    try:
        return {k: convert_value(v) for k, v in profile.items()}
    except Exception as e:
        logger.warning(f"Error serializing profile: {e}")
        return {}


def format_profile_for_llm(profile_data):
    """Format profile data as text for LLM analysis."""
    status = profile_data.get('current_status', {})
    changes = profile_data.get('recent_changes', [])
    
    text = f"Country: {profile_data.get('country', 'Unknown')}\n\n"
    text += "Current Status:\n"
    if status.get('ccyb'):
        text += f"- CCyB: {status['ccyb'].get('rate', 0)}%\n"
    if status.get('syrb'):
        text += f"- SyRB: {status['syrb'].get('rate', 0)}%\n"
    if status.get('osii'):
        text += f"- O-SII: {status['osii'].get('rate', 0)}%\n"
    if status.get('total_capital'):
        text += f"- Total Capital: {status['total_capital'].get('total', 0)}%\n"
    
    text += "\nRecent Changes (Last 12 Months):\n"
    for change in changes[:5]:
        date_str = change.get('date', 'N/A')
        if isinstance(date_str, str) and len(date_str) > 10:
            date_str = date_str[:10]
        text += f"- {date_str}: {change.get('type', '')} {change.get('change', '')}\n"
    
    return text


def main():
    logger.info("STARTING...")
    run_grounding = False
    # Check environment variable first (for CI/CD)
    if os.getenv("RUN_GROUNDING", "").lower() in ("1", "true", "yes", "y"):
        run_grounding = True
    else:
        try:
            answer = input("Run grounded validation? (y/N): ").strip().lower()
            run_grounding = answer in ("y", "yes")
        except Exception:
            run_grounding = False
    ensure_dirs(DATA_DIR, FIGURES_DIR, REPORTS_DIR)
    
    logger.info("1. Adatfeldolgozás...")
    etl = ETLPipeline(DATA_DIR, URLS["ccyb"], URLS["syrb"], URLS.get("capital_measures"))
    data = etl.run_pipeline()
    
    ccyb_full = data.get("ccyb_df")
    syrb_full = data.get("syrb_df")

    # 2. Vizualizáció
    logger.info("2. Grafikonok...")
    # Capital overall dataframe (needs to be available for plotting)
    capital_overall_df = build_capital_overall_df(
        ccyb_df=data.get("ccyb_df"),
        syrb_df=data.get("syrb_df"),
        osii_df=data.get("osii_df"),
        ccob_rate=2.5,
    )
    data["capital_overall_df"] = capital_overall_df

    viz = Visualizer(FIGURES_DIR)
    today_str = datetime.now().strftime("%Y-%m-%d")
    plots_inline, plot_figs, download_data, paths = viz.generate_all_plots(data, today_str)
    
    # 3. AI Elemzés
    logger.info("3. AI Elemzés...")
    analyzer = LLMAnalyzer(LLM_CONFIG)

    ccyb_decisions = prepare_ccyb_decisions(ccyb_full, analyzer)
    active_syrb, syrb_decisions = prepare_syrb_tables(syrb_full, analyzer)

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
        active_bbm = bbm_full[bbm_full['active_status'] == 'Active'].copy()
        bbm_pivot_html, bbm_ref_date = build_bbm_matrix_html(bbm_full)

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
                    limits_str, ftb_flag, ftb_details, other_details = extract_ltv_details_regex(row.get('description', ''))
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
    api_key = os.getenv(NEWS_CONFIG.get("api_key_env", "CUSTOM_SEARCH_API_KEY"), "")
    cse_id = os.getenv(NEWS_CONFIG.get("cse_id_env", "GOOGLE_CSE_ID"), "")
    query = NEWS_CONFIG.get("query", "")
    months_back = int(NEWS_CONFIG.get("months_back", 12))
    max_results = int(NEWS_CONFIG.get("max_results", 10))

    try:
        news_df = fetch_news(api_key=api_key, cse_id=cse_id, query=query, months_back=months_back, max_results=max_results)
    except Exception as exc:
        logger.warning(f"News search failed: {exc}")
        news_df = pd.DataFrame()
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
        'capital_overall_df': capital_overall_df,
    }

    # 3c. Country Profiles Generation (before analysis to provide graph data)
    logger.info("3c. Country Profiles...")
    profile_gen = CountryProfileGenerator({
        'ccyb_df': data.get('ccyb_df'),
        'syrb_df': data.get('syrb_df'),
        'bbm_df': data.get('bbm_df'),
        'osii_df': data.get('osii_df'),
        'capital_overall_df': capital_overall_df,
    })
    
    logger.info(f"   -> Found {len(profile_gen.countries)} countries")
    countries_data = {}
    for country in profile_gen.countries:
        try:
            profile = profile_gen.get_country_profile(country)
            # Convert dates to strings for JSON serialization
            profile_serializable = serialize_profile(profile)
            countries_data[country] = profile_serializable
        except Exception as e:
            logger.warning(f"Failed to generate profile for {country}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    logger.info(f"   -> Generated {len(countries_data)} country profiles")
    
    # 3d. Knowledge Graph Generation
    logger.info("3d. Knowledge Graph...")
    graph_data = None
    try:
        graph_data = profile_gen.build_knowledge_graph_data()
        import json
        knowledge_graph_json = json.dumps(graph_data, default=str, ensure_ascii=False)
        logger.info(f"   -> Generated knowledge graph: {len(graph_data.get('nodes', []))} nodes, {len(graph_data.get('edges', []))} edges")
    except Exception as e:
        logger.warning(f"Failed to generate knowledge graph: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        knowledge_graph_json = '{"nodes": [], "edges": []}'
        graph_data = {"nodes": [], "edges": []}
    
    # Add knowledge graph data to analysis inputs for grounding and AI analysis
    if graph_data:
        analysis_inputs['knowledge_graph_data'] = graph_data
    
    analyses = analyzer.run_analysis(analysis_inputs, paths, {})

    # 3b. Grounded validation against data, charts, and external sources (now with graph data)
    if run_grounding:
        logger.info("3b. Grounded Validation...")
        validator = GroundingValidator(LLM_CONFIG, SEARCH_CONFIG, analyzer._clean_text)
        analyses = validator.run(analyses, analysis_inputs, data)
    
    # 3e. Knowledge Graph AI Analysis
    logger.info("3e. Knowledge Graph AI Analysis...")
    try:
        # Prepare summary data from tables
        latest_ccyb = data.get('latest_ccyb_df', pd.DataFrame())
        active_ccyb_count = 0
        if not latest_ccyb.empty and 'rate' in latest_ccyb.columns:
            active_ccyb_count = len(latest_ccyb[latest_ccyb['rate'] > 0])
        
        active_syrb_count = len(active_syrb) if active_syrb is not None and not active_syrb.empty else 0
        active_bbm_count = len(active_bbm) if active_bbm is not None and not active_bbm.empty else 0
        
        summary_data = {
            'active_ccyb': active_ccyb_count,
            'active_syrb': active_syrb_count,
            'active_bbm': active_bbm_count,
        }
        
        if graph_data and graph_data.get('nodes'):
            kg_analysis = analyzer.analyze_knowledge_graph(graph_data, summary_data)
            analyses['knowledge_graph_analysis'] = kg_analysis
            logger.info("   -> Generated knowledge graph AI analysis")
        else:
            analyses['knowledge_graph_analysis'] = "Knowledge graph data not available for analysis."
    except Exception as e:
        logger.warning(f"Failed to generate knowledge graph AI analysis: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        analyses['knowledge_graph_analysis'] = "Knowledge graph analysis unavailable."
    
    # Generate AI analysis for each country profile
    for country, profile_data in countries_data.items():
        try:
            analysis_key = f"country_profile_{country.lower().replace(' ', '_')}"
            if analysis_key not in analyses:
                # Generate AI analysis for country profile
                profile_text = format_profile_for_llm(profile_data)
                country_analysis = analyzer.summarize_text(
                    profile_text,
                    f"Provide a comprehensive 3-4 paragraph analysis of {country}'s macroprudential policy profile, focusing on current policy stance, recent trends, policy objectives, and comparison with regional context."
                )
                analyses[analysis_key] = country_analysis
                profile_data['ai_analysis'] = country_analysis
        except Exception as e:
            logger.warning(f"Failed to generate AI analysis for {country}: {e}")
            profile_data['ai_analysis'] = ''
    
    # 4. Render
    logger.info("4. Riport...")
    tables_html = {
        "ccyb_decisions": df_to_html_table(ccyb_decisions),
        "syrb_active": df_to_html_table(active_syrb),
        "syrb_decisions": df_to_html_table(syrb_decisions),
        "bbm_pivot": bbm_pivot_html or "<p class='no-data'>No Data</p>",
        "bbm_decisions": df_to_html_table(bbm_decisions),
        "ltv_table": df_to_html_table(ltv_table),
        "capital_overall": df_to_html_table(capital_overall_df),
    }

    plot_files_input = {
        "ccyb_diffusion": plot_figs.get("ccyb_diffusion"),
        "cross_section_map": plot_figs.get("cross_section_map"),
        "cross_section_bar": plot_figs.get("cross_section_bar"),
        "risk_plot": plot_figs.get("risk_plot"),
        "syrb_counts_trend": plot_figs.get("syrb_counts_trend"),
        "syrb_sector": plot_figs.get("syrb_sector"),
        "bbm_diffusion": plot_figs.get("bbm_diffusion"),
        "capital_overall_buffers": plot_figs.get("capital_overall_buffers"),
    }

    download_data_input = {
        "ccyb_diffusion": download_data.get("ccyb_diffusion"),
        "bbm_diffusion": download_data.get("bbm_diffusion"),
        "capital_overall_buffers": download_data.get("capital_overall_buffers"),
    }

    news_feed_html = build_news_feed_html(news_df, today_str=today_str)

    rendered_html = render_report(
        base_dir=BASE_DIR,
        reports_dir=REPORTS_DIR,
        template_dir=BASE_DIR,
        template_name="report_template.html",
        generation_date=today_str,
        analyses=analyses,
        plots_inline=plots_inline,
        plot_figs=plot_files_input,
        download_data=download_data_input,
        tables_html=tables_html,
        news_feed_html=news_feed_html,
        bbm_ref_date=bbm_ref_date,
        ltv_ref_date=ltv_ref_date,
        countries_data=countries_data,
        knowledge_graph_json=knowledge_graph_json,
    )
    
    with open("index.html", "w", encoding="utf-8") as f: f.write(rendered_html)
    logger.info("DONE: index.html")

if __name__ == "__main__":
    main()