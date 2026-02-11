# Supabase-alapú index.html generálás - Implementáció

## Áttekintés

Az implementáció lehetővé teszi, hogy az `index.html` **opcionálisan** Supabase adatok alapján állítsa elő magát, ahelyett hogy csak statikus adatokat használna.

## Implementált funkciók

### 1. Konfiguráció ✅

**Fájl:** `config.py`

```python
SUPABASE_RENDER_CONFIG = {
    "enabled": os.getenv("USE_SUPABASE_FOR_RENDER", "false").lower() == "true",
    "url": os.getenv("SUPABASE_URL", ""),
    "anon_key": os.getenv("SUPABASE_KEY", ""),  # Anon key for frontend
}
```

**Environment változók:**
- `USE_SUPABASE_FOR_RENDER=true` - Engedélyezi a Supabase használatát
- `SUPABASE_URL` - Supabase projekt URL
- `SUPABASE_KEY` - Supabase anon key (frontend olvasáshoz)

### 2. Render Stage Supabase olvasás ✅

**Fájl:** `pipeline/stages/render_stage.py`

**Főbb változások:**
- `__init__`: Supabase client inicializálása (ha engedélyezve van)
- `_fetch_countries_data_from_supabase()`: Adatok lekérdezése Supabase-ből és transzformálása `countries_data` formátumra
- `process()`: Opcionális Supabase adatok használata pipeline adatok helyett

**Lekérdezett táblák:**
- `countries` - Ország információk
- `latest_ccyb_snapshot` - Legfrissebb CCyB snapshot
- `latest_syrb_snapshot` - Legfrissebb SyRB snapshot
- `latest_osii_snapshot` - Legfrissebb OSII snapshot
- `ccyb_decisions` - CCyB döntések (időszoros)
- `syrb_measures` - SyRB intézkedések (időszoros)
- `bbm_measures` - BBM intézkedések

**Transzformáció:**
- Supabase adatok → `countries_data` formátum
- `current_status` - Aktuális állapot (CCyB, SyRB, OSII, BBM)
- `historical_evolution` - Időszoros adatok (CCyB, SyRB)
- `active_measures` - Aktív intézkedések
- `recent_changes` - Legutóbbi változások (TODO: implementálni)

### 3. Template módosítás ✅

**Fájl:** `report_template.html`

**Változások:**
```html
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

### 4. Frontend dinamikus adatbetöltés ✅

**Fájlok:**
- `assets/supabase-client.js` - Supabase REST API client
- `assets/app.js` - Frontend logika

**Főbb funkciók:**

#### `fetchCountryProfile(countryIso2)`
Lekéri a teljes country profile-t Supabase-ből:
- Ország információk
- Latest snapshots (CCyB, SyRB, OSII)
- Időszoros adatok (CCyB decisions, SyRB measures, BBM measures)
- Transzformálás `countries_data` formátumra

#### `loadCountryProfile(country, profileData)` (módosított)
- Ha `window.useSupabase` és `window.SupabaseClient.isEnabled()` → Supabase-ből tölti be
- Ha nincs `profileData` vagy üres → Supabase-ből tölti be
- Fallback: statikus `window.countriesData` használata

**Módosítások:**
- `loadCountryProfile` → `async function`
- `selector.addEventListener('change')` → `async function`
- `checkHashForCountry()` → `async function`
- Ország elfogadása akkor is, ha nincs a statikus `countriesData`-ban (Supabase-ből töltődik)

### 5. Render függvény módosítás ✅

**Fájl:** `render.py`

**Változások:**
- `render_report()` új paraméterek: `supabase_url`, `supabase_key`
- Template-nek átadja a Supabase credentials-eket

## Használat

### 1. Environment változók beállítása

```bash
# .env fájlban vagy környezeti változóként
USE_SUPABASE_FOR_RENDER=true
SUPABASE_URL=https://irrgbfvnmsqikivukxfp.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...  # Anon key
```

### 2. Pipeline futtatása

```bash
python main.py
```

**Működés:**
- Ha `USE_SUPABASE_FOR_RENDER=true` → Render stage Supabase-ből olvassa az adatokat
- Ha `USE_SUPABASE_FOR_RENDER=false` → Render stage pipeline adatokat használ (jelenlegi működés)

### 3. Frontend működés

**Supabase engedélyezve:**
1. Oldal betöltése → `window.useSupabase = true`
2. Ország kiválasztása → `loadCountryProfile()` Supabase-ből tölti be
3. Ha Supabase nem elérhető → Fallback statikus adatokra

**Supabase nincs engedélyezve:**
1. Oldal betöltése → `window.useSupabase = false`
2. Ország kiválasztása → Statikus `window.countriesData` használata

## Előnyök

1. **Friss adatok**: A frontend minden betöltéskor a Supabase-ben tárolt legfrissebb adatokat olvassa
2. **Dinamikus frissítés**: Nincs szükség teljes HTML újragenerálásra - az adatok frissülhetnek Supabase-ben
3. **Opcionális**: Ha nincs Supabase, akkor statikus adatok (fallback)
4. **Skálázható**: Supabase REST API jól skálázható

## Kockázatok és megoldások

### 1. Supabase elérhetetlenség
**Megoldás:** Fallback statikus adatokra (`window.countriesData`)

### 2. Adatok formátum eltérés
**Megoldás:** Robusztus transzformáció logika a `_fetch_countries_data_from_supabase()` és `fetchCountryProfile()` függvényekben

### 3. Teljesítmény (több API hívás)
**Megoldás:** 
- Batch lekérdezések (`Promise.all`)
- Cache a `window.countriesData`-ban

## Tesztelés

### 1. Supabase engedélyezve

```bash
# Environment változók beállítása
export USE_SUPABASE_FOR_RENDER=true
export SUPABASE_URL=https://irrgbfvnmsqikivukxfp.supabase.co
export SUPABASE_KEY=<anon_key>

# Pipeline futtatása
python main.py
```

**Ellenőrzés:**
- Log: "Supabase client initialized for render stage"
- Log: "Fetched X countries from Supabase"
- HTML-ben: `window.SUPABASE_URL` és `window.SUPABASE_KEY` be van állítva
- Browser console: "Loading country profile from Supabase for..."

### 2. Supabase nincs engedélyezve

```bash
# Environment változók nélkül vagy false
export USE_SUPABASE_FOR_RENDER=false

# Pipeline futtatása
python main.py
```

**Ellenőrzés:**
- Log: Nincs "Supabase client initialized" üzenet
- HTML-ben: `window.SUPABASE_URL` és `window.SUPABASE_KEY` üres
- Browser console: Statikus adatok használata

### 3. Frontend tesztelés

**Browser console:**
```javascript
// Supabase engedélyezve?
console.log(window.useSupabase);

// Supabase client elérhető?
console.log(window.SupabaseClient && window.SupabaseClient.isEnabled());

// Country profile lekérdezése
window.SupabaseClient.fetchCountryProfile('HU').then(console.log);
```

## Ismert korlátok

1. **Recent changes**: Nincs implementálva (üres lista)
2. **AI analysis**: Nincs implementálva (üres string)
3. **Total capital**: Nincs implementálva (null) - szükséges `capital_overall` számítás
4. **Comparison**: Nincs implementálva (üres objektum)

## Következő lépések (opcionális)

1. **Recent changes implementálása**: Időszoros adatokból számítani az utolsó 12 hónap változásait
2. **AI analysis lekérdezése**: Külön táblából vagy cache-ből
3. **Total capital számítás**: `capital_overall` aggregáció Supabase-ből
4. **Comparison implementálása**: Regionális átlag és hasonló országok számítása
5. **Valódi real-time subscriptions**: Supabase Realtime WebSocket kapcsolatok
6. **Cache stratégia**: LocalStorage/IndexedDB offline működéshez
7. **Error handling**: Robusztus hibakezelés és retry logika
8. **Loading states**: Loading spinner és skeleton screens
