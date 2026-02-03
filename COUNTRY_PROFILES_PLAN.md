# 🇪🇺 Országprofilok - Implementációs Terv

## 1. KONCEPCIÓ

### 1.1 Cél
Egy interaktív országprofil rendszer, ahol felhasználók:
- **Egy ország** macroprudential előírásait tekinthetik meg
- **Időbeli változásokat** követhetik nyomon
- **Összehasonlítást** végezhetnek más országokkal
- **AI elemzést** kapnak ország-specifikus trendekről

### 1.2 Adatforrások
- ✅ **CCyB**: Országonként, időben (processed_ccyb.parquet)
- ✅ **SyRB**: Országonként, időben (processed_syrb.parquet)
- ✅ **BBM**: Országonként, időben (processed_bbm.parquet)
- ✅ **O-SII**: Országonként (processed_osii.parquet)
- ✅ **Capital Overall**: Összesített tőkepuffer (capital_overall_df)

---

## 2. UI/UX TERV

### 2.1 Navigáció
```
Sidebar:
├── Dashboard
│   ├── Overview
│   ├── Latest News
│   ├── Capital Measures
│   └── Borrower Measures
└── Country Profiles  ← ÚJ
    ├── [Ország selector dropdown]
    └── [Dinamikus profil oldal]
```

### 2.2 Országprofil Oldal Struktúra

```
┌─────────────────────────────────────────┐
│  🇭🇺 Hungary - Macroprudential Profile  │
├─────────────────────────────────────────┤
│                                         │
│  [Ország selector: ▼ Hungary ▼]        │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │ 📊 Current Status (Snapshot)     │  │
│  │                                  │  │
│  │  CCyB:     2.5%  [Active]        │  │
│  │  SyRB:     0.0%  [None]          │  │
│  │  O-SII:    2.0%  [Active]        │  │
│  │  BBM:      Yes   [LTV, DSTI]     │  │
│  │  Total:    4.5%  [Capital Stack]│  │
│  └─────────────────────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │ 📈 Historical Evolution          │  │
│  │                                  │  │
│  │  [Interactive time-series chart] │  │
│  │  - CCyB trend (2015-2024)       │  │
│  │  - SyRB trend (ha van)          │  │
│  │  - O-SII changes                │  │
│  └─────────────────────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │ 🔄 Recent Changes (Last 12M)    │  │
│  │                                  │  │
│  │  • 2024-01-15: CCyB +0.5%       │  │
│  │  • 2023-11-20: O-SII +0.5%      │  │
│  │  • 2023-09-10: BBM LTV updated  │  │
│  └─────────────────────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │ 📋 Active Measures Details      │  │
│  │                                  │  │
│  │  CCyB:                           │  │
│  │  - Rate: 2.5%                    │  │
│  │  - Effective: 2024-01-15         │  │
│  │  - Justification: [AI summary]   │  │
│  │                                  │  │
│  │  BBM:                            │  │
│  │  - LTV: 80% (FTB: 90%)           │  │
│  │  - DSTI: 50%                     │  │
│  │  - Details: [Full table]         │  │
│  └─────────────────────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │ 🤖 AI Analysis                  │  │
│  │                                  │  │
│  │  [Ország-specifikus elemzés]    │  │
│  │  - Trend interpretation         │  │
│  │  - Risk assessment              │  │
│  │  - Policy context               │  │
│  └─────────────────────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │ 📊 Comparison with Peers        │  │
│  │                                  │  │
│  │  [Bar chart: HU vs similar]      │  │
│  │  - Regional average              │  │
│  │  - Similar countries             │  │
│  └─────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

---

## 3. BACKEND IMPLEMENTÁCIÓ

### 3.1 Country Profile Generator

```python
# country_profiles.py
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class CountryProfileGenerator:
    """
    Országprofil generálása adatokból.
    """
    
    def __init__(self, data: Dict[str, pd.DataFrame]):
        """
        Args:
            data: Dictionary a következő kulcsokkal:
                - ccyb_df: CCyB adatok
                - syrb_df: SyRB adatok
                - bbm_df: BBM adatok
                - osii_df: O-SII adatok
                - capital_overall_df: Capital overall adatok
        """
        self.data = data
        self.countries = self._get_available_countries()
    
    def _get_available_countries(self) -> List[str]:
        """Elérhető országok listája."""
        countries = set()
        
        for df_name, df in self.data.items():
            if df is not None and not df.empty:
                if 'country' in df.columns:
                    countries.update(df['country'].unique())
                elif 'iso2' in df.columns:
                    # ISO2-ből ország név konverzió
                    # ...
                    pass
        
        return sorted(list(countries))
    
    def get_country_profile(self, country: str) -> Dict[str, Any]:
        """
        Országprofil generálása.
        
        Args:
            country: Ország neve (pl. "Hungary")
        
        Returns:
            Dictionary a profil adataival
        """
        profile = {
            'country': country,
            'iso2': self._get_iso2(country),
            'current_status': self._get_current_status(country),
            'historical_evolution': self._get_historical_evolution(country),
            'recent_changes': self._get_recent_changes(country),
            'active_measures': self._get_active_measures(country),
            'comparison': self._get_comparison(country),
        }
        
        return profile
    
    def _get_current_status(self, country: str) -> Dict[str, Any]:
        """Aktuális állapot snapshot."""
        status = {
            'ccyb': None,
            'syrb': None,
            'osii': None,
            'bbm': [],
            'total_capital': None,
        }
        
        # CCyB
        if self.data.get('ccyb_df') is not None:
            ccyb = self.data['ccyb_df']
            country_ccyb = ccyb[ccyb['country'] == country]
            if not country_ccyb.empty:
                latest = country_ccyb.sort_values('date').iloc[-1]
                status['ccyb'] = {
                    'rate': latest.get('rate', 0),
                    'date': latest.get('date'),
                    'status': 'Active' if latest.get('rate', 0) > 0 else 'Inactive',
                }
        
        # SyRB
        if self.data.get('syrb_df') is not None:
            syrb = self.data['syrb_df']
            country_syrb = syrb[syrb['country'] == country]
            if not country_syrb.empty:
                # Legfrissebb aktív SyRB
                active = country_syrb[
                    (country_syrb['active_status'] == 'Active') |
                    (country_syrb['status'].str.contains('Active', case=False, na=False))
                ]
                if not active.empty:
                    latest = active.sort_values('date').iloc[-1]
                    status['syrb'] = {
                        'rate': latest.get('rate_numeric', 0),
                        'date': latest.get('date'),
                        'type': latest.get('measure_type', 'General'),
                        'status': 'Active',
                    }
        
        # O-SII
        if self.data.get('osii_df') is not None:
            osii = self.data['osii_df']
            country_osii = osii[osii['country'] == country]
            if not country_osii.empty:
                latest = country_osii.sort_values('date').iloc[-1] if 'date' in country_osii.columns else country_osii.iloc[-1]
                status['osii'] = {
                    'rate': latest.get('rate_numeric', 0),
                    'status': 'Active' if latest.get('rate_numeric', 0) > 0 else 'Inactive',
                }
        
        # BBM
        if self.data.get('bbm_df') is not None:
            bbm = self.data['bbm_df']
            country_bbm = bbm[
                (bbm['country'] == country) &
                (bbm['active_status'] == 'Active')
            ]
            if not country_bbm.empty:
                status['bbm'] = country_bbm['measure_type'].unique().tolist()
        
        # Total Capital (Capital Overall)
        if self.data.get('capital_overall_df') is not None:
            capital = self.data['capital_overall_df']
            country_capital = capital[capital['COUNTRY'] == country]
            if not country_capital.empty:
                row = country_capital.iloc[0]
                status['total_capital'] = {
                    'total': row.get('Total', 0),
                    'ccob': row.get('CCoB', 2.5),
                    'ccyb': row.get('CCyB', 0),
                    'osii': row.get('GSII/O-SII', 0),
                    'syrb': row.get('SyRB', 0),
                    'ssyrb': row.get('sSyRB', 0),
                }
        
        return status
    
    def _get_historical_evolution(self, country: str) -> Dict[str, pd.DataFrame]:
        """Időbeli változások."""
        evolution = {}
        
        # CCyB trend
        if self.data.get('ccyb_df') is not None:
            ccyb = self.data['ccyb_df']
            country_ccyb = ccyb[ccyb['country'] == country].sort_values('date')
            if not country_ccyb.empty:
                evolution['ccyb'] = country_ccyb[['date', 'rate', 'credit_gap']].copy()
        
        # SyRB trend
        if self.data.get('syrb_df') is not None:
            syrb = self.data['syrb_df']
            country_syrb = syrb[syrb['country'] == country].sort_values('date')
            if not country_syrb.empty:
                evolution['syrb'] = country_syrb[['date', 'rate_numeric', 'measure_type']].copy()
        
        return evolution
    
    def _get_recent_changes(self, country: str, months: int = 12) -> List[Dict[str, Any]]:
        """Legutóbbi változások."""
        changes = []
        cutoff_date = datetime.now() - timedelta(days=months * 30)
        
        # CCyB változások
        if self.data.get('ccyb_df') is not None:
            ccyb = self.data['ccyb_df']
            country_ccyb = ccyb[
                (ccyb['country'] == country) &
                (pd.to_datetime(ccyb['date']) >= cutoff_date)
            ].sort_values('date')
            
            if len(country_ccyb) > 1:
                for i in range(1, len(country_ccyb)):
                    prev = country_ccyb.iloc[i-1]
                    curr = country_ccyb.iloc[i]
                    
                    if prev.get('rate', 0) != curr.get('rate', 0):
                        changes.append({
                            'date': curr.get('date'),
                            'type': 'CCyB',
                            'change': f"{prev.get('rate', 0):.2f}% → {curr.get('rate', 0):.2f}%",
                            'direction': 'increase' if curr.get('rate', 0) > prev.get('rate', 0) else 'decrease',
                        })
        
        # SyRB változások
        if self.data.get('syrb_df') is not None:
            syrb = self.data['syrb_df']
            country_syrb = syrb[
                (syrb['country'] == country) &
                (pd.to_datetime(syrb['date']) >= cutoff_date)
            ].sort_values('date')
            
            # Új aktíválások
            activations = country_syrb[
                (country_syrb['active_status'] == 'Active') |
                (country_syrb['status'].str.contains('Active', case=False, na=False))
            ]
            
            for _, row in activations.iterrows():
                changes.append({
                    'date': row.get('date'),
                    'type': 'SyRB',
                    'change': f"Activated: {row.get('rate_numeric', 0):.2f}%",
                    'direction': 'activation',
                })
        
        # BBM változások
        if self.data.get('bbm_df') is not None:
            bbm = self.data['bbm_df']
            country_bbm = bbm[
                (bbm['country'] == country) &
                (pd.to_datetime(bbm['date']) >= cutoff_date)
            ].sort_values('date')
            
            for _, row in country_bbm.iterrows():
                changes.append({
                    'date': row.get('date'),
                    'type': 'BBM',
                    'change': f"{row.get('measure_type', 'BBM')} - {row.get('status', '')}",
                    'direction': 'change',
                })
        
        # Dátum szerint rendezés (legfrissebb először)
        changes.sort(key=lambda x: x['date'], reverse=True)
        
        return changes[:10]  # Legutóbbi 10 változás
    
    def _get_active_measures(self, country: str) -> Dict[str, Any]:
        """Aktív intézkedések részletei."""
        measures = {
            'ccyb': None,
            'syrb': [],
            'bbm': [],
            'osii': None,
        }
        
        # CCyB részletek
        if self.data.get('ccyb_df') is not None:
            ccyb = self.data['ccyb_df']
            country_ccyb = ccyb[ccyb['country'] == country].sort_values('date')
            if not country_ccyb.empty:
                latest = country_ccyb.iloc[-1]
                measures['ccyb'] = {
                    'rate': latest.get('rate', 0),
                    'date': latest.get('date'),
                    'justification': latest.get('justification', ''),
                    'credit_gap': latest.get('credit_gap'),
                }
        
        # SyRB részletek
        if self.data.get('syrb_df') is not None:
            syrb = self.data['syrb_df']
            country_syrb = syrb[
                (syrb['country'] == country) &
                ((syrb['active_status'] == 'Active') |
                 (syrb['status'].str.contains('Active', case=False, na=False)))
            ]
            
            for _, row in country_syrb.iterrows():
                measures['syrb'].append({
                    'rate': row.get('rate_numeric', 0),
                    'type': row.get('measure_type', 'General'),
                    'exposure': row.get('exposure_type', ''),
                    'date': row.get('date'),
                    'description': row.get('description', ''),
                })
        
        # BBM részletek
        if self.data.get('bbm_df') is not None:
            bbm = self.data['bbm_df']
            country_bbm = bbm[
                (bbm['country'] == country) &
                (bbm['active_status'] == 'Active')
            ]
            
            for _, row in country_bbm.iterrows():
                measures['bbm'].append({
                    'type': row.get('measure_type', ''),
                    'status': row.get('status', ''),
                    'date': row.get('date'),
                    'description': row.get('description', ''),
                })
        
        return measures
    
    def _get_comparison(self, country: str) -> Dict[str, Any]:
        """Összehasonlítás más országokkal."""
        comparison = {
            'regional_average': None,
            'similar_countries': [],
        }
        
        # Regional average (pl. CEE, Nordics, stb.)
        # TODO: Region mapping
        
        # Similar countries (hasonló tőkepuffer szinttel)
        if self.data.get('capital_overall_df') is not None:
            capital = self.data['capital_overall_df']
            country_total = capital[capital['COUNTRY'] == country]['Total'].iloc[0] if not capital[capital['COUNTRY'] == country].empty else 0
            
            # Hasonló országok (±0.5% tűrés)
            similar = capital[
                (capital['Total'] >= country_total - 0.5) &
                (capital['Total'] <= country_total + 0.5) &
                (capital['COUNTRY'] != country)
            ].head(5)
            
            comparison['similar_countries'] = similar[['COUNTRY', 'Total']].to_dict('records')
        
        return comparison
    
    def _get_iso2(self, country: str) -> Optional[str]:
        """ISO2 kód lekérése."""
        # ISO2 mapping
        # TODO: country_converter használata
        return None
```

### 3.2 Visualizer Extensions

```python
# visualizer.py - hozzáadandó metódusok
def generate_country_profile_plots(
    self,
    country: str,
    profile_data: Dict[str, Any],
    output_dir: Path
) -> Dict[str, Any]:
    """
    Országprofil grafikonok generálása.
    
    Returns:
        Dictionary plot HTML-ekkel
    """
    plots = {}
    
    # 1. Historical Evolution Chart
    if 'historical_evolution' in profile_data:
        plots['evolution'] = self._plot_country_evolution(
            country,
            profile_data['historical_evolution']
        )
    
    # 2. Current Status Comparison
    if 'comparison' in profile_data:
        plots['comparison'] = self._plot_country_comparison(
            country,
            profile_data['comparison']
        )
    
    # 3. Recent Changes Timeline
    if 'recent_changes' in profile_data:
        plots['timeline'] = self._plot_recent_changes(
            country,
            profile_data['recent_changes']
        )
    
    return plots

def _plot_country_evolution(
    self,
    country: str,
    evolution: Dict[str, pd.DataFrame]
) -> str:
    """Időbeli változások grafikon."""
    import plotly.graph_objects as go
    
    fig = go.Figure()
    
    # CCyB trend
    if 'ccyb' in evolution and not evolution['ccyb'].empty:
        ccyb_df = evolution['ccyb']
        fig.add_trace(go.Scatter(
            x=ccyb_df['date'],
            y=ccyb_df['rate'],
            name='CCyB',
            mode='lines+markers',
            line=dict(color='#3b82f6', width=2),
        ))
    
    # SyRB trend
    if 'syrb' in evolution and not evolution['syrb'].empty:
        syrb_df = evolution['syrb']
        fig.add_trace(go.Scatter(
            x=syrb_df['date'],
            y=syrb_df['rate_numeric'],
            name='SyRB',
            mode='lines+markers',
            line=dict(color='#10b981', width=2),
        ))
    
    fig.update_layout(
        title=f"{country} - Macroprudential Measures Evolution",
        xaxis_title="Date",
        yaxis_title="Rate (%)",
        hovermode='x unified',
        height=400,
    )
    
    return fig.to_html(include_plotlyjs='cdn', div_id=f"country-evolution-{country}")
```

### 3.3 LLM Analysis Extension

```python
# llm_tasks.py - hozzáadandó task
def build_country_profile_task(
    country: str,
    profile_data: Dict[str, Any]
) -> LLMTask:
    """
    Országprofil AI elemzés task.
    """
    # Adatok formázása
    current_status = profile_data.get('current_status', {})
    recent_changes = profile_data.get('recent_changes', [])
    historical = profile_data.get('historical_evolution', {})
    
    data_str = f"""
Country: {country}
Current Status:
- CCyB: {current_status.get('ccyb', {}).get('rate', 0)}%
- SyRB: {current_status.get('syrb', {}).get('rate', 0)}%
- O-SII: {current_status.get('osii', {}).get('rate', 0)}%
- Total Capital: {current_status.get('total_capital', {}).get('total', 0)}%

Recent Changes (Last 12 Months):
{chr(10).join([f"- {c['date']}: {c['type']} {c['change']}" for c in recent_changes[:5]])}
"""
    
    return LLMTask(
        id=f"country_profile_{country.lower().replace(' ', '_')}",
        data=data_str,
        temp=0.3,
        prompt=f"""
Analyze the macroprudential policy profile for {country}.

Focus on:
1. Current policy stance (CCyB, SyRB, BBM, O-SII)
2. Recent trends and changes
3. Policy objectives and risk management approach
4. Comparison with regional/peer context

Write a comprehensive 3-4 paragraph analysis.
"""
    )
```

---

## 4. FRONTEND IMPLEMENTÁCIÓ

### 4.1 HTML Template

```html
<!-- report_template.html - hozzáadandó szekció -->
<section id="tab-country-profiles" class="tab-content">
    <h1>Country Profiles</h1>
    
    <div class="card">
        <div class="card-title">
            <span>Select Country</span>
            <select id="country-selector" class="chart-input" style="width: 300px;">
                <option value="">-- Select Country --</option>
                {% for country in countries %}
                <option value="{{ country }}">{{ country }}</option>
                {% endfor %}
            </select>
        </div>
    </div>
    
    {% if selected_country %}
    <div id="country-profile-content">
        <!-- Current Status -->
        <div class="card">
            <div class="card-title">📊 Current Status</div>
            <div class="status-grid">
                <div class="status-item">
                    <span class="status-label">CCyB</span>
                    <span class="status-value">{{ profile.current_status.ccyb.rate }}%</span>
                    <span class="status-badge {{ 'active' if profile.current_status.ccyb.status == 'Active' else 'inactive' }}">
                        {{ profile.current_status.ccyb.status }}
                    </span>
                </div>
                <!-- További status itemek... -->
            </div>
        </div>
        
        <!-- Historical Evolution -->
        <div class="card">
            <div class="card-title">📈 Historical Evolution</div>
            <div id="country-evolution-chart"></div>
        </div>
        
        <!-- Recent Changes -->
        <div class="card">
            <div class="card-title">🔄 Recent Changes (Last 12 Months)</div>
            <div class="changes-list">
                {% for change in profile.recent_changes %}
                <div class="change-item">
                    <span class="change-date">{{ change.date }}</span>
                    <span class="change-type">{{ change.type }}</span>
                    <span class="change-detail">{{ change.change }}</span>
                </div>
                {% endfor %}
            </div>
        </div>
        
        <!-- AI Analysis -->
        <div class="card">
            <div class="card-title">🤖 AI Analysis</div>
            <div class="ai-box">
                {{ analyses['country_profile_' + selected_country.lower().replace(' ', '_')] | safe }}
            </div>
        </div>
    </div>
    {% endif %}
</section>
```

### 4.2 JavaScript

```javascript
// app.js - hozzáadandó
document.getElementById('country-selector')?.addEventListener('change', function(e) {
    const country = e.target.value;
    if (country) {
        // URL frissítése vagy AJAX hívás
        window.location.hash = `country=${encodeURIComponent(country)}`;
        loadCountryProfile(country);
    }
});

function loadCountryProfile(country) {
    // Dinamikus profil betöltése
    // (vagy előre generált HTML)
}
```

### 4.3 CSS

```css
/* styles.css - hozzáadandó */
.status-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-top: 16px;
}

.status-item {
    background: #f8fafc;
    padding: 16px;
    border-radius: 8px;
    border-left: 4px solid #3b82f6;
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.status-label {
    font-size: 0.85rem;
    color: #64748b;
    font-weight: 600;
}

.status-value {
    font-size: 1.5rem;
    font-weight: 700;
    color: #1e293b;
}

.status-badge {
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 0.75rem;
    font-weight: 600;
}

.status-badge.active {
    background: #dcfce7;
    color: #166534;
}

.status-badge.inactive {
    background: #f1f5f9;
    color: #64748b;
}

.changes-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
}

.change-item {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 12px;
    background: #f8fafc;
    border-radius: 8px;
    border-left: 3px solid #3b82f6;
}

.change-date {
    font-weight: 600;
    color: #64748b;
    min-width: 120px;
}

.change-type {
    padding: 4px 8px;
    background: #e0e7ff;
    color: #3730a3;
    border-radius: 4px;
    font-size: 0.85rem;
    font-weight: 600;
}

.change-detail {
    flex: 1;
    color: #1e293b;
}
```

---

## 5. IMPLEMENTÁCIÓS LÉPÉSEK

### Fázis 1: Alapok (1 hét)
1. ✅ `CountryProfileGenerator` osztály létrehozása
2. ✅ `get_country_profile()` metódus implementálása
3. ✅ Adatok aggregálása országonként

### Fázis 2: Visualizáció (1 hét)
1. ✅ Grafikonok generálása (evolution, comparison)
2. ✅ HTML template létrehozása
3. ✅ CSS styling

### Fázis 3: AI Elemzés (3 nap)
1. ✅ Ország-specifikus LLM task
2. ✅ Elemzés integrálása

### Fázis 4: Frontend (3 nap)
1. ✅ Ország selector
2. ✅ Dinamikus profil oldal
3. ✅ JavaScript interakciók

---

## 6. HASZNÁLATI PÉLDÁK

### Példa 1: Hungary profil
```
Country: Hungary
Current Status:
- CCyB: 2.5% (Active)
- SyRB: 0.0% (None)
- O-SII: 2.0% (Active)
- BBM: Yes (LTV, DSTI)
- Total Capital: 4.5%

Recent Changes:
- 2024-01-15: CCyB 2.0% → 2.5%
- 2023-11-20: O-SII 1.5% → 2.0%
- 2023-09-10: BBM LTV updated
```

### Példa 2: Germany profil
```
Country: Germany
Current Status:
- CCyB: 0.5% (Active, Positive Neutral)
- SyRB: 0.0% (None)
- O-SII: 0.0% (None)
- BBM: No
- Total Capital: 3.0%
```

---

## 7. BŐVÍTÉSI LEHETŐSÉGEK

### Később hozzáadható:
- **Regional comparison**: CEE, Nordics, stb.
- **Peer group analysis**: Hasonló országok automatikus csoportosítása
- **Export functionality**: PDF/Excel export
- **Historical snapshots**: "Time travel" - korábbi állapotok megtekintése
- **Alert system**: Változások email értesítése

---

## ÖSSZEFOGLALÁS

### Főbb funkciók:
1. ✅ **Ország selector** - Dropdown vagy search
2. ✅ **Current Status** - Snapshot kártyák
3. ✅ **Historical Evolution** - Időbeli trendek
4. ✅ **Recent Changes** - Legutóbbi változások
5. ✅ **Active Measures** - Részletes információk
6. ✅ **AI Analysis** - Ország-specifikus elemzés
7. ✅ **Comparison** - Összehasonlítás más országokkal

### Implementációs idő:
- **Teljes implementáció**: 2-3 hét
- **MVP (alapvető funkciók)**: 1 hét

**Készen állsz az implementációra?** 🚀
