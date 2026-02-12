# Supabase Frontend Integration

## Overview

A frontend integráció lehetővé teszi, hogy a dashboard **opcionálisan** Supabase-ből olvassa az adatokat, ahelyett hogy csak a statikusan beágyazott adatokat használná.

## Konfiguráció

### 1. Supabase Credentials

A Supabase URL és API key beállítása két módon lehetséges:

#### A) HTML Template-ben (Jinja2 változókkal)

```html
<script>
    window.SUPABASE_URL = '{{ supabase_url|default("") }}';
    window.SUPABASE_KEY = '{{ supabase_key|default("") }}';
</script>
```

#### B) Környezeti változók (production)

A `render.py`-ban vagy a template-ben beállítható:

```python
# render.py
supabase_url = os.getenv("SUPABASE_URL", "")
supabase_key = os.getenv("SUPABASE_KEY", "")  # Anon key
```

### 2. Supabase Client Script

A `assets/supabase-client.js` fájl tartalmazza a Supabase REST API client implementációt.

**Főbb funkciók:**
- `fetchCCyBDecisions(countryIso2, limit)` - CCyB döntések lekérdezése
- `fetchLatestCCyBSnapshot(countryIso2)` - Legfrissebb CCyB snapshot
- `fetchDTILTIRules(countryIso2)` - DTI/LTI szabályok
- `fetchLTVRules(countryIso2)` - LTV szabályok
- `fetchCCyBTrend(limit)` - CCyB trend adatok

## Használat

### Opcionális Supabase használat

A dashboard **alapértelmezetten** a statikusan beágyazott adatokat használja (jelenlegi működés). Ha a Supabase konfigurálva van, akkor **opcionálisan** használható:

```javascript
// app.js-ben
if (window.SupabaseClient && window.SupabaseClient.isEnabled()) {
    // Supabase-ből töltjük az adatokat
    const ccybData = await window.SupabaseClient.fetchCCyBDecisions('HU', 5);
    // ... adatok feldolgozása
} else {
    // Fallback: statikus adatok használata
    // ... jelenlegi logika
}
```

### Példa: DTI/LTI táblázat frissítése

```javascript
async function loadDTILTITable() {
    if (window.SupabaseClient && window.SupabaseClient.isEnabled()) {
        try {
            const rules = await window.SupabaseClient.fetchDTILTIRules();
            if (rules && rules.length > 0) {
                // Rendereljük a táblázatot a Supabase adatokkal
                renderDTILTITable(rules);
                return;
            }
        } catch (error) {
            console.error('Error loading DTI/LTI from Supabase:', error);
        }
    }
    
    // Fallback: statikus adatok
    renderDTILTITable(staticDTILTIData);
}
```

## Előnyök

1. **Real-time adatok**: A dashboard mindig a legfrissebb adatokat jeleníti meg
2. **Dinamikus frissítés**: Az adatok frissülhetnek anélkül, hogy újra kellene generálni a teljes HTML-t
3. **Flexibilis**: Opcionális - ha nincs Supabase, akkor a statikus adatokat használja
4. **Skálázható**: A Supabase REST API jól skálázható nagy adatmennyiséghez

## Biztonság

- **Anon Key használata**: Csak olvasási hozzáférés (RLS policy-k által védett)
- **RLS (Row Level Security)**: A Supabase RLS policy-k biztosítják, hogy csak a megfelelő adatok legyenek elérhetők
- **CORS**: A Supabase automatikusan kezeli a CORS-t

## Következő lépések (opcionális)

1. **Real-time subscriptions**: Supabase Realtime használata automatikus frissítésekhez
2. **Cache stratégia**: LocalStorage vagy IndexedDB használata offline működéshez
3. **Error handling**: Robusztus hibakezelés és retry logika
4. **Loading states**: Loading spinner és skeleton screens
