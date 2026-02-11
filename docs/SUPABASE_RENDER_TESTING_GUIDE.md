# Supabase Render Tesztelési Útmutató

## Gyors Összefoglaló

✅ **Minden alapvető teszt sikeres!**

- Konfiguráció betöltése működik
- Render Stage inicializálás működik (Supabase-pel és anélkül)
- Supabase adatok lekérdezése működik (31 ország)
- Template változók beágyazása működik

## Tesztelési Lépések

### 1. Alapvető Tesztelés (Python)

```bash
# Teszt script futtatása
python scripts/test_supabase_render.py
```

**Várt kimenet:**
- ✅ Konfiguráció tesztelése: OK
- ✅ Render Stage inicializálás: OK
- ✅ Supabase adatok lekérdezése: OK (31 ország)
- ✅ Orchestrator inicializálás: OK

### 2. Supabase Mód Tesztelése

```bash
# Environment változó beállítása
export USE_SUPABASE_FOR_RENDER=true  # Linux/Mac
# vagy
$env:USE_SUPABASE_FOR_RENDER="true"  # Windows PowerShell

# Teszt futtatása
python scripts/test_supabase_render.py
```

**Várt kimenet:**
- ✅ USE_SUPABASE_FOR_RENDER: True
- ✅ Supabase client: initialized
- ✅ 31 ország lekérdezve Supabase-ből

### 3. Fallback Mód Tesztelése

```bash
# Environment változó törlése vagy false-ra állítása
unset USE_SUPABASE_FOR_RENDER  # Linux/Mac
# vagy
Remove-Item Env:USE_SUPABASE_FOR_RENDER  # Windows PowerShell

# Teszt futtatása
python scripts/test_supabase_render.py
```

**Várt kimenet:**
- ✅ USE_SUPABASE_FOR_RENDER: False
- ✅ Supabase client: None
- ✅ Fallback mód működik

### 4. Teljes Pipeline Tesztelése

```bash
# Supabase mód
export USE_SUPABASE_FOR_RENDER=true
python main.py

# Fallback mód
unset USE_SUPABASE_FOR_RENDER
python main.py
```

**Ellenőrzés:**
1. Log üzenetek:
   - Supabase mód: "Supabase client initialized for render stage"
   - Supabase mód: "Fetched X countries from Supabase"
   - Fallback mód: Nincs Supabase üzenet

2. Generált HTML ellenőrzése:
   ```bash
   # Nyisd meg a reports/index.html fájlt
   # Keress rá: window.SUPABASE_URL
   ```

### 5. Browser Frontend Tesztelése

**Lépések:**
1. Generáld a HTML-t: `python main.py`
2. Nyisd meg: `reports/index.html` böngészőben
3. Nyisd meg a Developer Tools-t (F12)
4. Menj a Console tab-ra
5. Futtasd:

```javascript
// 1. Supabase engedélyezve?
console.log('useSupabase:', window.useSupabase);
console.log('SUPABASE_URL:', window.SUPABASE_URL);
console.log('SUPABASE_KEY:', window.SUPABASE_KEY ? 'set' : 'not set');

// 2. Supabase client elérhető?
if (window.SupabaseClient) {
    console.log('SupabaseClient.isEnabled():', window.SupabaseClient.isEnabled());
    
    // 3. Country profile lekérdezése
    window.SupabaseClient.fetchCountryProfile('HU').then(profile => {
        console.log('Hungary profile from Supabase:', profile);
    }).catch(err => {
        console.error('Error fetching profile:', err);
    });
}

// 4. Statikus adatok
console.log('Static countriesData:', Object.keys(window.countriesData || {}).length, 'countries');
```

**Várt Eredmény (Supabase engedélyezve):**
- ✅ `window.useSupabase` = `true`
- ✅ `window.SUPABASE_URL` és `window.SUPABASE_KEY` be vannak állítva
- ✅ `window.SupabaseClient.isEnabled()` = `true`
- ✅ `fetchCountryProfile('HU')` visszaad egy country profile objektumot

**Várt Eredmény (Fallback mód):**
- ✅ `window.useSupabase` = `false`
- ✅ `window.SUPABASE_URL` és `window.SUPABASE_KEY` üresek
- ✅ `window.SupabaseClient.isEnabled()` = `false`
- ✅ Statikus `window.countriesData` használata

### 6. Country Profile UI Tesztelése

**Lépések:**
1. Nyisd meg a generált `reports/index.html`-t
2. Menj a "Country Profiles" tab-ra
3. Válassz ki egy országot (pl. "Hungary")
4. Ellenőrizd a browser console-ban:
   - Supabase mód: "Loading country profile from Supabase for Hungary (HU)"
   - Fallback mód: Nincs Supabase üzenet

**Várt Eredmény:**
- ✅ Ország kiválasztása után az adatok betöltődnek
- ✅ Adatok megjelennek a UI-ban (Current Status, Historical Evolution, stb.)

## Tesztelési Scriptek

### `scripts/test_supabase_render.py`
Alapvető tesztelés:
- Konfiguráció betöltése
- Render Stage inicializálás
- Supabase adatok lekérdezése
- Orchestrator inicializálás

### `scripts/test_complete_supabase_render.py`
Teljes tesztelés:
- Fallback mód
- Supabase mód
- Template változók

## Ismert Problémák és Megoldások

### 1. Config Cache
**Probléma:** Python modul cache miatt az `importlib.reload()` nem mindig működik.

**Megoldás:** Külön shell-ben futtasd a teszteket, vagy használj külön Python process-t.

### 2. Country Name Mező
**Probléma:** A Supabase `countries` táblában a mező neve `country_name`, nem `name`.

**Megoldás:** ✅ Javítva: `country_info.get("country_name", country_info.get("name", ""))`

### 3. Empty Data Warning
**Probléma:** Néha "WARNING: Üres adatok érkeztek Supabase-ből" üzenet jelenik meg.

**Megoldás:** Ellenőrizd, hogy:
- A Supabase táblákban vannak-e adatok
- A RLS (Row Level Security) engedélyezi-e az olvasást
- A Supabase credentials helyesek-e

## Következő Lépések

1. ✅ **Alapvető Tesztelés**: Kész
2. ✅ **Supabase Mód Tesztelés**: Kész
3. ✅ **Fallback Mód Tesztelés**: Kész
4. ⏳ **Teljes Pipeline Tesztelés**: Folyamatban
5. ⏳ **Browser Frontend Tesztelés**: Folyamatban
6. ⏳ **Teljesítmény Tesztelés**: Tervezett
7. ⏳ **Error Handling Tesztelés**: Tervezett

## Összefoglaló

✅ **Sikeresen implementálva és tesztelve:**
- Konfiguráció betöltése
- Render Stage Supabase integráció
- Adatok lekérdezése Supabase-ből (31 ország)
- Template változók beágyazása
- Frontend Supabase client
- Fallback mód működése

⏳ **Folyamatban:**
- Teljes pipeline tesztelés
- Browser frontend tesztelés
- Teljesítmény optimalizálás
