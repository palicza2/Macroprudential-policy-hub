import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import io
import base64

def create_download_link(df, filename, title="Download Data"):
    """Excel letöltő link generálása."""
    if df is None or df.empty:
        return ""
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Data')
        b64 = base64.b64encode(output.getvalue()).decode()
        
        return f'''
        <div style="margin-top: 10px; text-align: right;">
            <a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" 
               download="{filename}.xlsx" 
               class="download-btn"
               style="display: inline-block; padding: 6px 12px; background-color: #27ae60; color: white; text-decoration: none; border-radius: 4px; font-size: 0.8rem; font-weight: 600;">
               📊 {title}
            </a>
        </div>
        '''
    except Exception as e:
        print(f"Error creating excel link: {e}")
        return ""

def generate_interactive_plots(data):
    """
    HTML stringeket generál a riport számára.
    Kezeli a CCyB és az új SyRB ábrákat is.
    """
    plots_html = {}

    # --- 1. CCyB Diffúzió (Meglévő) ---
    if 'agg_trend_df' in data and not data['agg_trend_df'].empty:
        fig = px.line(
            data['agg_trend_df'], x='date', y='n_positive',
            title='CCyB Diffusion: Number of Active Countries',
            labels={'n_positive': 'Countries (Rate > 0%)', 'date': 'Date'},
            template='plotly_white'
        )
        fig.update_traces(line_color='#2980b9', line_width=3)
        plots_html['diffusion_plot'] = fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False}) + \
                                       create_download_link(data['agg_trend_df'], "ccyb_diffusion")
    else:
        plots_html['diffusion_plot'] = "<p>No data available.</p>"

    # --- 2. CCyB Keresztmetszeti (Meglévő) ---
    if 'latest_country_df' in data and not data['latest_country_df'].empty:
        df = data['latest_country_df'].sort_values('rate', ascending=False)
        fig = px.bar(
            df, x='country', y='rate', color='rate',
            title='Current CCyB Rates by Country',
            color_continuous_scale='Blues'
        )
        plots_html['cross_section_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False}) + \
                                           create_download_link(df, "ccyb_latest")
    else:
        plots_html['cross_section_plot'] = "<p>No data available.</p>"

    # --- 3. CCyB Kockázati Ábra (Meglévő) ---
    if 'latest_country_df' in data and not data['latest_country_df'].empty:
        df = data['latest_country_df']
        size_arg = 'credit_to_gdp' if 'credit_to_gdp' in df.columns and df['credit_to_gdp'].notna().any() else None
        fig = px.scatter(
            df, x='credit_gap', y='rate', size=size_arg,
            color='iso2', hover_name='country',
            title='Risk Analysis: Credit Gap vs CCyB Rate',
            size_max=40
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.add_vline(x=0, line_dash="dash", line_color="gray")
        plots_html['risk_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False}) + \
                                  create_download_link(df, "ccyb_risk_analysis")
    else:
        plots_html['risk_plot'] = "<p>No data available.</p>"

    # --- 4. Historikus CCyB (Meglévő) ---
    if 'df' in data and not data['df'].empty:
        fig = px.line(
            data['df'], x='date', y='rate', color='country',
            title='Historical CCyB Evolution',
            template='plotly_white'
        )
        plots_html['history_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False}) + \
                                     create_download_link(data['df'], "ccyb_history")
    else:
        plots_html['history_plot'] = "<p>No data available.</p>"

    # --- 5. ÚJ: SyRB Diffúzió (General vs Sectoral) ---
    if 'syrb_trend_df' in data and not data['syrb_trend_df'].empty:
        # Itt egy multi-line chartot készítünk
        fig = go.Figure()
        
        # General SyRB Vonal
        fig.add_trace(go.Scatter(
            x=data['syrb_trend_df']['date'], 
            y=data['syrb_trend_df']['n_general'],
            mode='lines',
            name='General SyRB',
            line=dict(color='#e74c3c', width=3)
        ))
        
        # Sectoral SyRB Vonal
        fig.add_trace(go.Scatter(
            x=data['syrb_trend_df']['date'], 
            y=data['syrb_trend_df']['n_sectoral'],
            mode='lines',
            name='Sectoral SyRB',
            line=dict(color='#f39c12', width=3, dash='dash')
        ))
        
        fig.update_layout(
            title='SyRB Diffusion: General vs. Sectoral Approaches',
            xaxis_title='Date',
            yaxis_title='Number of Active Countries',
            template='plotly_white',
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
        )
        
        plots_html['syrb_diffusion_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False}) + \
                                            create_download_link(data['syrb_trend_df'], "syrb_diffusion")
    else:
        plots_html['syrb_diffusion_plot'] = "<p>Not enough SyRB data for trends.</p>"

    # --- 6. ÚJ: SyRB Szektorális Megoszlás ---
    if 'latest_syrb_df' in data and not data['latest_syrb_df'].empty:
        # Csak a szektorálisokat nézzük
        sectoral_df = data['latest_syrb_df'][data['latest_syrb_df']['syrb_type'] == 'Sectoral'].copy()
        
        if not sectoral_df.empty:
            # Kulcsszavas számlálás a description-ből
            def get_sector_tag(text):
                t = str(text).lower()
                if 'resident' in t or 'housing' in t or 'mortgage' in t: return 'Residential Real Estate (RRE)'
                if 'commercial' in t or 'cre' in t: return 'Commercial Real Estate (CRE)'
                if 'corporate' in t: return 'Non-Financial Corporations'
                if 'retail' in t: return 'Retail Exposures'
                return 'Other Sectoral'

            sectoral_df['sector_tag'] = sectoral_df['description'].apply(get_sector_tag)
            counts = sectoral_df['sector_tag'].value_counts().reset_index()
            counts.columns = ['Sector', 'Count']
            
            fig = px.bar(
                counts, x='Sector', y='Count', color='Sector',
                title='Focus of Sectoral SyRB Measures',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            plots_html['syrb_sector_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})
        else:
             plots_html['syrb_sector_plot'] = "<p>No active sectoral measures found.</p>"
    else:
        plots_html['syrb_sector_plot'] = "<p>No data.</p>"

    return plots_html