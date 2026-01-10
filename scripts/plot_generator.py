import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import io
import base64
import os
import sys
from contextlib import contextmanager
from datetime import datetime

# --- Csendesítő ---
@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = devnull
        sys.stderr = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

def create_download_link(df, filename, title="Download Data"):
    if df is None or df.empty: return ""
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Data')
        b64 = base64.b64encode(output.getvalue()).decode()
        return f'''<div style="margin-top: 10px; text-align: right;"><a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}.xlsx" class="download-btn" style="color:#27ae60;font-weight:600;text-decoration:none;">📊 {title}</a></div>'''
    except Exception: return ""

def save_static_plot(fig, filename, folder="figures"):
    if not os.path.exists(folder): os.makedirs(folder)
    path = os.path.join(folder, filename)
    try:
        # Itt némítjuk el a Kaleidót
        with suppress_stdout():
            fig.write_image(path, scale=2)
        return path
    except Exception: return None

def no_data_plot(msg="No data available"):
    return f"<div style='height:300px; display:flex; align-items:center; justify-content:center; background:#f8f9fa; color:#95a5a6; font-weight:bold;'>{msg}</div>"

def generate_interactive_plots(data, ref_date=None):
    if ref_date is None: ref_date = datetime.now().strftime('%Y-%m-%d')
    plots_html = {}
    saved_paths = []

    # 1. CCyB Diffusion
    if 'agg_trend_df' in data and not data['agg_trend_df'].empty:
        df = data['agg_trend_df']
        fig = px.line(df, x='date', y='n_positive', title='CCyB Diffusion (Count of Active Countries)', template='plotly_white')
        fig.update_traces(line_color='#2980b9', line_width=3)
        plots_html['diffusion_plot'] = fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False}) + create_download_link(df, "ccyb_diff")
        p = save_static_plot(fig, "ccyb_diffusion.png")
        if p: saved_paths.append(p)
    else:
        plots_html['diffusion_plot'] = no_data_plot()

    # 2. SyRB Diffusion (CUMULATIVE)
    if 'syrb_trend_df' in data and not data['syrb_trend_df'].empty:
        df = data['syrb_trend_df']
        fig = go.Figure()
        if 'General SyRB' in df.columns: fig.add_trace(go.Scatter(x=df['date'], y=df['General SyRB'], mode='lines', name='General', line=dict(color='#e67e22', width=3)))
        if 'Sectoral SyRB' in df.columns: fig.add_trace(go.Scatter(x=df['date'], y=df['Sectoral SyRB'], mode='lines', name='Sectoral', line=dict(color='#8e44ad', width=3)))
        fig.update_layout(title='SyRB Adoption Trends', xaxis_title='Date', yaxis_title='Count', template='plotly_white', legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01))
        plots_html['syrb_diffusion_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False}) + create_download_link(df, "syrb_trends")
        p = save_static_plot(fig, "syrb_diffusion.png")
        if p: saved_paths.append(p)
    else:
        plots_html['syrb_diffusion_plot'] = no_data_plot("No SyRB trend data")

    # 3. Snapshot
    if 'latest_country_df' in data and not data['latest_country_df'].empty:
        df = data['latest_country_df'].sort_values('rate', ascending=False)
        fig = px.bar(df, x='country', y='rate', color='rate', title=f'Current CCyB Rates ({ref_date})', color_continuous_scale='Blues')
        plots_html['cross_section_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})
        p = save_static_plot(fig, "cross_section.png")
        if p: saved_paths.append(p)
    else:
        plots_html['cross_section_plot'] = no_data_plot()

    # 4. Risk
    if 'latest_country_df' in data and not data['latest_country_df'].empty:
        df = data['latest_country_df']
        size = 'credit_to_gdp' if 'credit_to_gdp' in df.columns else None
        fig = px.scatter(df, x='credit_gap', y='rate', size=size, color='iso2', title='Risk Analysis', size_max=40)
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.add_vline(x=0, line_dash="dash", line_color="gray")
        plots_html['risk_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})
        p = save_static_plot(fig, "risk_plot.png")
        if p: saved_paths.append(p)
    else:
        plots_html['risk_plot'] = no_data_plot()

    # 5. Sectoral
    if 'latest_syrb_df' in data and not data['latest_syrb_df'].empty:
        sec = data['latest_syrb_df'][data['latest_syrb_df']['syrb_type'] == 'Sectoral'].copy()
        if not sec.empty:
            def get_tag(t):
                t=str(t).lower()
                if 'cre' in t and 'rre' in t: return 'Mixed (CRE & RRE)'
                if 'resident' in t or 'housing' in t: return 'Residential (RRE)'
                if 'commercial' in t: return 'Commercial (CRE)'
                return 'Other'
            sec['tag'] = sec['exposure_type'].apply(get_tag)
            counts = sec['tag'].value_counts().reset_index()
            counts.columns = ['Sector', 'Count']
            fig = px.bar(counts, x='Sector', y='Count', color='Sector', title='Sectoral Focus', color_discrete_sequence=px.colors.qualitative.Pastel)
            plots_html['syrb_sector_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})
            p = save_static_plot(fig, "syrb_sector.png")
            if p: saved_paths.append(p)
        else:
            plots_html['syrb_sector_plot'] = no_data_plot()
    else:
        plots_html['syrb_sector_plot'] = no_data_plot()

    return plots_html, saved_paths