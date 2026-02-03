import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from utils import SuppressOutput

class Visualizer:
    def __init__(self, figures_dir: Path):
        self.figures_dir = figures_dir
        self.figures_dir.mkdir(parents=True, exist_ok=True)

    def _save(self, fig, name):
        path = self.figures_dir / name
        try:
            with SuppressOutput():
                fig.write_image(path, scale=2, engine="kaleido")
            return path
        except Exception: return None

    def generate_all_plots(self, data, ref_date):
        plots_inline = {}
        plot_figs = {}
        download_data = {}
        paths = {}

        # 1. CCyB Diffusion
        df_trend = data.get('agg_trend_df')
        if df_trend is not None and not df_trend.empty:
            fig = px.line(df_trend, x='date', y='n_positive', title='Number of Countries with Positive CCyB', template='plotly_white')
            fig.update_layout(xaxis_title="", yaxis_title="Count")
            plot_figs['ccyb_diffusion'] = fig
            download_data['ccyb_diffusion'] = df_trend
            if p := self._save(fig, "ccyb_diffusion.png"): paths['ccyb_diffusion'] = p
        else:
            plot_figs['ccyb_diffusion'] = None

        # 2. CCyB Time Series
        df_hist = data.get('ccyb_df')
        if df_hist is not None and not df_hist.empty:
            # Ensure we have the right columns and data types
            if 'country' not in df_hist.columns and 'iso2' in df_hist.columns:
                # If we only have iso2, we might need to convert it or use it as country identifier
                df_hist = df_hist.copy()
            
            # Sort by country and date to ensure proper time series
            df_hist = df_hist.sort_values(['country', 'date']).copy()
            
            # For each country, ensure we have a proper time series (not cumulative)
            # The rate should be the actual rate at that date, not cumulative
            df_plot = df_hist.copy()
            
            # Filter out invalid rates (should be between 0 and ~5% typically)
            df_plot = df_plot[
                (df_plot['rate'] >= 0) & 
                (df_plot['rate'] <= 5.0)  # Reasonable upper bound for CCyB
            ].copy()
            
            # Use step plot to show actual rate changes (not interpolated line)
            fig = go.Figure()
            
            # Group by country and create step traces
            for country in df_plot['country'].unique():
                country_data = df_plot[df_plot['country'] == country].sort_values('date')
                if not country_data.empty:
                    fig.add_trace(go.Scatter(
                        x=country_data['date'],
                        y=country_data['rate'],
                        mode='lines+markers',
                        name=country,
                        line=dict(shape='hv'),  # Step function (horizontal then vertical)
                        hovertemplate=f'<b>{country}</b><br>Date: %{{x}}<br>Rate: %{{y:.2f}}%<extra></extra>'
                    ))
            
            fig.update_layout(
                title='Historical CCyB Rates',
                template='plotly_white',
                xaxis_title="Date",
                yaxis_title="Rate (%)",
                yaxis=dict(range=[0, 3.0]),  # Set reasonable y-axis range (0-3%)
                legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
                margin=dict(b=100),
                hovermode='closest'
            )
            
            plots_inline['ccyb_timeseries'] = fig.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id='ccyb_ts_plot',
                config={"responsive": True}
            )
            if p := self._save(fig, "ccyb_timeseries.png"): paths['ccyb_timeseries'] = p
        else:
            plots_inline['ccyb_timeseries'] = "<div class='empty-state'>No Data</div>"

        # 3. Current CCyB Map & Bar
        df_latest = data.get('latest_ccyb_df')
        if df_latest is not None and not df_latest.empty:
            if 'iso3' in df_latest.columns:
                fig_map = px.choropleth(df_latest, locations="iso3", color="rate", hover_name="country",
                    color_continuous_scale="Blues", title=f"Map View ({ref_date})", scope="europe")
                fig_map.update_geos(fitbounds="locations", visible=False)
                fig_map.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
                plot_figs['cross_section_map'] = fig_map
                if p := self._save(fig_map, "cross_section_map.png"): paths['cross_section_map'] = p
            
            fig_bar = px.bar(df_latest.sort_values('rate', ascending=True), x='rate', y='country', orientation='h', 
                title=f"Comparative Levels ({ref_date})", text='rate', template='plotly_white')
            fig_bar.update_traces(textposition='outside')
            plot_figs['cross_section_bar'] = fig_bar
            if p := self._save(fig_bar, "cross_section_bar.png"): paths['cross_section_bar'] = p

            fig2 = px.scatter(df_latest, x='credit_gap', y='rate', text='iso2', title='Risk Analysis', template='plotly_white')
            fig2.update_traces(textposition='top center')
            plot_figs['risk_plot'] = fig2
            if p := self._save(fig2, "risk_plot.png"): paths['risk_plot'] = p
        else:
            plot_figs['cross_section_map'] = None
            plot_figs['cross_section_bar'] = None
            plot_figs['risk_plot'] = None

        # 4. SyRB Trend
        df_syrb_trend = data.get('syrb_trend_df')
        if df_syrb_trend is not None and not df_syrb_trend.empty:
            fig = go.Figure()
            if 'General SyRB' in df_syrb_trend.columns: 
                fig.add_trace(go.Scatter(x=df_syrb_trend['date'], y=df_syrb_trend['General SyRB'], name='General'))
            if 'Sectoral SyRB' in df_syrb_trend.columns: 
                fig.add_trace(go.Scatter(x=df_syrb_trend['date'], y=df_syrb_trend['Sectoral SyRB'], name='Sectoral'))
            fig.update_layout(title='Active SyRB Measures Count', template='plotly_white', legend=dict(orientation="h", y=-0.2))
            plot_figs['syrb_counts_trend'] = fig
            if p := self._save(fig, "syrb_counts_trend.png"): paths['syrb_counts_trend'] = p
        else:
            plot_figs['syrb_counts_trend'] = None

        # 5. SyRB Sectoral (Clustered Bar)
        df_syrb = data.get('latest_syrb_df')
        if df_syrb is not None and not df_syrb.empty:
            active = df_syrb[df_syrb['rate_numeric'] > 0].copy()
            if not active.empty:
                color_map = {"General": "#3498db", "Real Estate (CRE & RRE)": "#9b59b6", "Residential Real Estate (RRE)": "#2ecc71", "Commercial Real Estate (CRE)": "#e74c3c", "Other": "#95a5a6"}
                # Használjuk az ETL által tisztított 'exposure_type' kategóriákat
                
                fig_bar = px.bar(
                    active, x="iso2", y="rate_numeric", color="exposure_type",
                    color_discrete_map=color_map, title="SyRB Composition by Country",
                    labels={"rate_numeric": "Rate (%)", "iso2": "Country", "exposure_type": "Exposure"},
                    template="plotly_white"
                )
                # --- CLUSTERED MODE ---
                fig_bar.update_layout(barmode='group', xaxis={'categoryorder':'total descending'})
                
                plot_figs['syrb_sector'] = fig_bar
                if p := self._save(fig_bar, "syrb_sector.png"): paths['syrb_sector'] = p
            else:
                plot_figs['syrb_sector'] = None
        else:
            plot_figs['syrb_sector'] = None

        # 6. BBM Diffusion
        df_bbm_trend = data.get('bbm_trend_df')
        if df_bbm_trend is not None and not df_bbm_trend.empty:
            fig = px.line(df_bbm_trend, x='date', y='n_countries', title='Number of Countries with at least one Active BBM', template='plotly_white')
            fig.update_layout(xaxis_title="", yaxis_title="Count")
            plot_figs['bbm_diffusion'] = fig
            download_data['bbm_diffusion'] = df_bbm_trend
            if p := self._save(fig, "bbm_diffusion.png"): paths['bbm_diffusion'] = p
        else:
            plot_figs['bbm_diffusion'] = None

        # 7. Capital Overall Buffers (stacked)
        df_cap = data.get("capital_overall_df")
        if df_cap is not None and not df_cap.empty:
            df_plot = df_cap.copy()
            # plotly needs wide->stacked traces
            x = df_plot["ISO2"].astype(str).tolist()
            components = ["CCoB", "CCyB", "GSII/O-SII", "SyRB", "sSyRB"]
            fig = go.Figure()
            colors = {
                "CCoB": "#2c3e50",
                "CCyB": "#3498db",
                "GSII/O-SII": "#9b59b6",
                "SyRB": "#e67e22",
                "sSyRB": "#e74c3c",
            }
            for comp in components:
                if comp in df_plot.columns:
                    fig.add_trace(
                        go.Bar(
                            name=comp,
                            x=x,
                            y=df_plot[comp].fillna(0.0).tolist(),
                            marker_color=colors.get(comp),
                        )
                    )
            fig.update_layout(
                barmode="stack",
                title="Overall Capital Buffer Requirement by Country",
                template="plotly_white",
                xaxis_title="Country",
                yaxis_title="Buffer rate (%)",
                legend=dict(orientation="h", y=-0.25),
                margin=dict(b=110),
            )
            plot_figs["capital_overall_buffers"] = fig
            download_data["capital_overall_buffers"] = df_plot
            if p := self._save(fig, "capital_overall_buffers.png"):
                paths["capital_overall_buffers"] = p
        else:
            plot_figs["capital_overall_buffers"] = None

        return plots_inline, plot_figs, download_data, paths