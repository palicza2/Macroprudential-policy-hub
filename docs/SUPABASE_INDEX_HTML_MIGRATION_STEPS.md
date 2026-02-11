# Supabase-alapú index.html generálás - Lépések

## Áttekintés

Jelenleg az `index.html` statikus adatokkal van generálva a pipeline-ből (pl. `window.countriesData = {{ countries_data_json|safe }}`). A cél, hogy az `index.html` **Supabase adatok alapján** állítsa elő magát.

## Jelenlegi állapot

✅ **Kész:**
- Pipeline integráció: Supabase writer modul (`pipeline/writers/supabase_writer.py`)
- ETL stage Supabase write
- BBM stage Supabase write (DTI/LTI, LTV)
- Frontend Supabase client (`assets/supabase-client.js`)
- Supabase REST API tesztelés

❌ **Hiányzik:**
- Render stage Supabase olvasás
- Template Supabase credentials beágyazása
- Frontend dinamikus adatbetöltés Supabase-ből
- Konfiguráció (environment változók)

## Következő lépések

### 1. Render Stage módosítása - Supabase olvasás

**Cél:** A `pipeline/stages/render_stage.py` opcionálisan Supabase-ből olvassa az adatokat a pipeline adatok helyett.

**Módosítások:**

```python
# pipeline/stages/render_stage.py

from supabase_migration.config import SupabaseConfig
from supabase import create_client, Client

class RenderStage:
    def __init__(self, base_dir: Path, reports_dir: Path, news_config: Dict[str, Any], 
                 use_supabase: bool = False):
        # ...
        self.use_supabase = use_supabase
        if self.use_supabase:
            config = SupabaseConfig()
            self.supabase_client = create_client(config.url, config.anon_key)
        else:
            self.supabase_client = None
    
    def _fetch_countries_data_from_supabase(self) -> Dict[str, Any]:
        """Fetch countries data from Supabase and transform to expected format."""
        if not self.supabase_client:
            return {}
        
        # Fetch from Supabase tables
        # - countries
        # - latest_ccyb_snapshot
        # - latest_syrb_snapshot
        # - latest_osii_snapshot
        # - ccyb_decisions (historical)
        # - syrb_measures (historical)
        # - bbm_measures
        # Transform to countries_data format
        # ...
    
    def process(self, ...):
        # ...
        if self.use_supabase:
            countries_data = self._fetch_countries_data_from_supabase()
        else:
            countries_data = countries_data  # Use pipeline data
        
        # Pass supabase credentials to template
        supabase_url = os.getenv("SUPABASE_URL", "") if self.use_supabase else ""
        supabase_key = os.getenv("SUPABASE_KEY", "") if self.use_supabase else ""
        
        rendered_html = render_report(
            # ...
            countries_data=countries_data,
            supabase_url=supabase_url,
            supabase_key=supabase_key,
        )
```

**Függőségek:**
- Supabase client inicializálása
- Adatok lekérdezése Supabase-ből (JOIN-ok, aggregációk)
- Adatok transzformálása a jelenlegi `countries_data` formátumra

### 2. Template módosítása - Supabase credentials

**Cél:** A `report_template.html` beágyazza a Supabase credentials-eket és inicializálja a Supabase client-et.

**Módosítások:**

```html
<!-- report_template.html -->
<script>
    // Supabase configuration
    window.SUPABASE_URL = '{{ supabase_url|default("") }}';
    window.SUPABASE_KEY = '{{ supabase_key|default("") }}';
    
    // Initialize Supabase client if credentials are available
    if (window.SUPABASE_URL && window.SUPABASE_KEY) {
        window.useSupabase = true;
    } else {
        window.useSupabase = false;
    }
    
    // Embed countries data for country profiles (fallback)
    window.countriesData = {{ countries_data_json|safe }};
</script>
```

### 3. Frontend dinamikus adatbetöltés

**Cél:** Az `assets/app.js` opcionálisan Supabase-ből töltse be az adatokat, ha elérhető.

**Módosítások:**

```javascript
// assets/app.js

async function loadCountryProfile(countryIso2) {
    if (window.useSupabase && window.SupabaseClient && window.SupabaseClient.isEnabled()) {
        try {
            // Fetch from Supabase
            const countryData = await fetchCountryDataFromSupabase(countryIso2);
            if (countryData) {
                renderCountryProfile(countryData);
                return;
            }
        } catch (error) {
            console.error('Error loading from Supabase:', error);
        }
    }
    
    // Fallback: use static data
    const countryData = window.countriesData[countryName];
    renderCountryProfile(countryData);
}

async function fetchCountryDataFromSupabase(countryIso2) {
    const client = window.SupabaseClient;
    
    // Fetch multiple tables
    const [ccybSnapshot, syrbSnapshot, osiiSnapshot, ccybHistory, syrbHistory, bbmMeasures] = await Promise.all([
        client.fetchLatestCCyBSnapshot(countryIso2),
        client.fetchLatestSyRBSnapshot(countryIso2),
        client.fetchLatestOSIISnapshot(countryIso2),
        client.fetchCCyBDecisions(countryIso2, 100),
        client.fetchSyRBMeasures(countryIso2),
        client.fetchBBMMeasures(countryIso2),
    ]);
    
    // Transform to countries_data format
    return {
        country: countryName,
        iso2: countryIso2,
        current_status: {
            ccyb: transformCCyBSnapshot(ccybSnapshot),
            syrb: transformSyRBSnapshot(syrbSnapshot),
            osii: transformOSIISnapshot(osiiSnapshot),
            bbm: extractBBMTypes(bbmMeasures),
        },
        historical_evolution: {
            ccyb: transformCCyBHistory(ccybHistory),
            syrb: transformSyRBHistory(syrbHistory),
        },
        // ...
    };
}
```

**Függőségek:**
- Supabase client inicializálása
- Adatok lekérdezése és transzformálása
- Fallback logika statikus adatokra

### 4. Konfiguráció - Environment változók

**Cél:** Environment változók beállítása a Supabase használatához.

**Módosítások:**

```python
# pipeline/orchestrator.py vagy config.py

USE_SUPABASE_FOR_RENDER = os.getenv("USE_SUPABASE_FOR_RENDER", "false").lower() == "true"
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")  # Anon key for frontend

# Render stage initialization
self.render_stage = RenderStage(
    BASE_DIR, 
    REPORTS_DIR, 
    NEWS_CONFIG,
    use_supabase=USE_SUPABASE_FOR_RENDER
)
```

**Environment változók:**
```bash
USE_SUPABASE_FOR_RENDER=true
SUPABASE_URL=https://irrgbfvnmsqikivukxfp.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...  # Anon key
```

### 5. Adatok transzformálása - Supabase → countries_data formátum

**Cél:** A Supabase adatok transzformálása a jelenlegi `countries_data` formátumra.

**Függőségek:**
- `countries` tábla → country name, iso2
- `latest_ccyb_snapshot` → current_status.ccyb
- `latest_syrb_snapshot` → current_status.syrb
- `latest_osii_snapshot` → current_status.osii
- `ccyb_decisions` → historical_evolution.ccyb
- `syrb_measures` → historical_evolution.syrb
- `bbm_measures` → active_measures.bbm
- `dti_lti_rules` → (DTI/LTI táblázathoz)
- `ltv_rules` → (LTV táblázathoz)

**Példa transzformáció:**

```python
def transform_supabase_to_countries_data(supabase_data: Dict) -> Dict[str, Any]:
    """Transform Supabase data to countries_data format."""
    countries_data = {}
    
    for country in supabase_data['countries']:
        iso2 = country['iso2']
        country_name = country['name']
        
        # Current status
        ccyb_snapshot = supabase_data['ccyb_snapshots'].get(iso2, {})
        syrb_snapshot = supabase_data['syrb_snapshots'].get(iso2, {})
        osii_snapshot = supabase_data['osii_snapshots'].get(iso2, {})
        
        # Historical evolution
        ccyb_history = supabase_data['ccyb_decisions'].get(iso2, [])
        syrb_history = supabase_data['syrb_measures'].get(iso2, [])
        
        countries_data[country_name] = {
            'country': country_name,
            'iso2': iso2,
            'current_status': {
                'ccyb': {
                    'rate': ccyb_snapshot.get('rate', 0.0),
                    'date': ccyb_snapshot.get('snapshot_date', ''),
                    'status': 'Active' if ccyb_snapshot.get('rate', 0) > 0 else 'Inactive',
                },
                # ...
            },
            'historical_evolution': {
                'ccyb': [
                    {
                        'date': decision['effective_date'],
                        'rate': decision['rate'],
                        'credit_gap': decision.get('credit_gap', None),
                    }
                    for decision in ccyb_history
                ],
                # ...
            },
            # ...
        }
    
    return countries_data
```

## Implementációs sorrend

1. **1. lépés: Konfiguráció** (könnyű)
   - Environment változók beállítása
   - `USE_SUPABASE_FOR_RENDER` flag

2. **2. lépés: Template módosítás** (könnyű)
   - Supabase credentials beágyazása
   - Supabase client inicializálása

3. **3. lépés: Render stage Supabase olvasás** (közepes)
   - Supabase client inicializálása
   - Adatok lekérdezése
   - Transzformáció implementálása

4. **4. lépés: Frontend dinamikus betöltés** (közepes)
   - Supabase client használata
   - Adatok lekérdezése és renderelése
   - Fallback logika

5. **5. lépés: Tesztelés** (közepes)
   - Supabase adatok ellenőrzése
   - Frontend működés tesztelése
   - Fallback működés tesztelése

## Előnyök

1. **Friss adatok**: A frontend minden betöltéskor a Supabase-ben tárolt legfrissebb adatokat olvassa (nem a statikusan beágyazott adatokat)
2. **Dinamikus frissítés**: Nincs szükség teljes HTML újragenerálásra - az adatok frissülhetnek Supabase-ben, és a következő oldalbetöltéskor automatikusan friss adatokat kap
3. **Opcionális**: Ha nincs Supabase, akkor statikus adatok (fallback)
4. **Skálázható**: Supabase REST API jól skálázható

**Megjegyzés:** Ez NEM valódi "real-time" (mint WebSocket/Realtime subscriptions), hanem "on-demand data fetching" - a frontend minden betöltéskor lekéri a legfrissebb adatokat. Valódi real-time-hoz Supabase Realtime subscriptions szükségesek.

## Kockázatok és megoldások

1. **Kockázat**: Supabase elérhetetlenség
   - **Megoldás**: Fallback statikus adatokra

2. **Kockázat**: Adatok formátum eltérés
   - **Megoldás**: Robusztus transzformáció logika

3. **Kockázat**: Teljesítmény (több API hívás)
   - **Megoldás**: Batch lekérdezések, cache

## Következő lépések (opcionális)

1. **Valódi real-time subscriptions**: Supabase Realtime WebSocket kapcsolatok automatikus frissítésekhez (amikor változik az adat Supabase-ben, automatikusan frissül a frontend)
2. **Cache stratégia**: LocalStorage/IndexedDB offline működéshez
3. **Error handling**: Robusztus hibakezelés és retry logika
4. **Loading states**: Loading spinner és skeleton screens
