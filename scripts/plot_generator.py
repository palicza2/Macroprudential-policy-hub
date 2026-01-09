import plotly.express as px
import pandas as pd

def generate_interactive_plots(data):
    """HTML stringeket generál a riport számára."""
    plots_html = {}

    # 1. Diffúziós ábra
    if 'agg_trend_df' in data and not data['agg_trend_df'].empty:
        fig = px.line(
            data['agg_trend_df'], x='date', y='n_positive',
            title='Diffusion: Active CCyB Rates Over Time',
            labels={'n_positive': 'Number of Countries', 'date': 'Date'},
            template='plotly_white'
        )
        fig.update_traces(line_color='#2980b9', line_width=3)
        plots_html['diffusion_plot'] = fig.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False})
    else:
        plots_html['diffusion_plot'] = "<p>No data available for Diffusion plot.</p>"

    # 2. Keresztmetszeti ábra (Legfrissebb ráták)
    if 'latest_country_df' in data and not data['latest_country_df'].empty:
        df = data['latest_country_df'].sort_values('rate', ascending=False)
        fig = px.bar(
            df, x='country', y='rate', color='rate',
            title='Current CCyB Rates',
            color_continuous_scale='Blues'
        )
        plots_html['cross_section_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})
    else:
        plots_html['cross_section_plot'] = "<p>No data available for Cross-Sectional plot.</p>"

    # 3. Kockázati (Buborék) ábra
    if 'latest_country_df' in data and not data['latest_country_df'].empty:
        df = data['latest_country_df']
        # Ha van GDP adat, az legyen a buborék mérete, ha nincs, akkor fix
        size_arg = 'credit_to_gdp' if 'credit_to_gdp' in df.columns and df['credit_to_gdp'].notna().any() else None
        
        fig = px.scatter(
            df, x='credit_gap', y='rate', size=size_arg,
            color='iso2', hover_name='country',
            title='Risk Analysis: Credit Gap vs Buffer Rate',
            size_max=40
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.add_vline(x=0, line_dash="dash", line_color="gray")
        plots_html['risk_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})
    else:
        plots_html['risk_plot'] = "<p>No data available for Risk Analysis plot.</p>"
    
    # 4. Historikus ábra (Minden ország vonala)
    if 'df' in data and not data['df'].empty:
        # Csak azokat rajzoljuk ki, ahol volt változás, hogy ne legyen túl zsúfolt
        fig = px.line(
            data['df'], x='date', y='rate', color='country',
            title='Historical Evolution by Country',
            labels={'rate': 'CCyB Rate (%)'}
        )
        plots_html['historical_plot'] = fig.to_html(full_html=False, include_plotlyjs=False, config={'displayModeBar': False})
    else:
        plots_html['historical_plot'] = "<p>No data available for Historical Evolution plot.</p>"

    return plots_html
