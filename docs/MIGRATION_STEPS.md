# Materialized Views Migration - Lépésről Lépésre Útmutató

## ⚠️ FONTOS: Olvasd el ezt az útmutatót teljes egészében, mielőtt elkezdenéd!

## Előfeltételek

1. ✅ Supabase projekt elérhető
2. ✅ Adatbázis kapcsolat működik
3. ✅ Van backup az adatbázisról (ajánlott)
4. ✅ Van hozzáférés a Supabase SQL Editor-hez vagy psql-hez

---

## Lépés 1: Ellenőrzés - Jelenlegi állapot

Először ellenőrizd, hogy a régi táblák léteznek-e:

```sql
-- Futtasd ezt a Supabase SQL Editor-ben
SELECT 
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
    'latest_ccyb_snapshot',
    'latest_syrb_snapshot',
    'latest_osii_snapshot',
    'ccyb_diffusion_trend',
    'syrb_trend',
    'bbm_diffusion_trend'
  )
ORDER BY table_name;
```

**Várt eredmény:** 6 táblát kell látnod (ha mind létezik).

---

## Lépés 2: Ellenőrzés - Alapadatok

Ellenőrizd, hogy van-e adat az alapadatokban:

```sql
-- Ellenőrizd az alapadatokat
SELECT 
    'ccyb_decisions' as table_name, COUNT(*) as row_count FROM ccyb_decisions
UNION ALL
SELECT 'syrb_measures', COUNT(*) FROM syrb_measures
UNION ALL
SELECT 'osii_banks', COUNT(*) FROM osii_banks
UNION ALL
SELECT 'bbm_measures', COUNT(*) FROM bbm_measures;
```

**Várt eredmény:** Minden táblának legyen legalább néhány sora.

---

## Lépés 3: Materialized Views létrehozása

Futtasd a **010_create_materialized_views.sql** fájlt:

### Opció A: Supabase SQL Editor-ben

1. Nyisd meg a Supabase Dashboard-ot
2. Menj a **SQL Editor** menüpontra
3. Kattints az **New Query** gombra
4. Másold be a `migrations/010_create_materialized_views.sql` fájl teljes tartalmát
5. Kattints a **Run** gombra (vagy Ctrl+Enter)

### Opció B: psql parancssorban

```bash
# Ha psql-t használsz
psql -h <your-supabase-host> -U postgres -d postgres -f migrations/010_create_materialized_views.sql
```

**Várt eredmény:** 
- ✅ "CREATE MATERIALIZED VIEW" üzenetek
- ✅ "CREATE INDEX" üzenetek
- ✅ "CREATE FUNCTION" üzenet
- ✅ "CREATE TRIGGER" üzenetek
- ✅ "REFRESH MATERIALIZED VIEW" üzenetek

**Ha hiba van:** 
- Olvasd el a hibaüzenetet
- Ellenőrizd, hogy az alapadatok léteznek-e
- Nézd meg a hiba részleteit

---

## Lépés 4: Ellenőrzés - Materialized Views működnek-e

Ellenőrizd, hogy a Materialized View-k létrejöttek-e és adatokat tartalmaznak-e:

```sql
-- Ellenőrizd a Materialized View-kat
SELECT 
    'mv_latest_ccyb_snapshot' as view_name, COUNT(*) as row_count FROM mv_latest_ccyb_snapshot
UNION ALL
SELECT 'mv_latest_syrb_snapshot', COUNT(*) FROM mv_latest_syrb_snapshot
UNION ALL
SELECT 'mv_latest_osii_snapshot', COUNT(*) FROM mv_latest_osii_snapshot
UNION ALL
SELECT 'mv_ccyb_diffusion_trend', COUNT(*) FROM mv_ccyb_diffusion_trend
UNION ALL
SELECT 'mv_syrb_trend', COUNT(*) FROM mv_syrb_trend
UNION ALL
SELECT 'mv_bbm_diffusion_trend', COUNT(*) FROM mv_bbm_diffusion_trend;
```

**Várt eredmény:** Minden Materialized View-nak legyen legalább néhány sora (vagy 0, ha nincs adat).

---

## Lépés 5: Tesztelés - Adatok ellenőrzése

Hasonlítsd össze a régi táblákkal (ha még léteznek):

```sql
-- Összehasonlítás: régi tábla vs Materialized View
SELECT 
    'CCyB Snapshot' as comparison,
    (SELECT COUNT(*) FROM latest_ccyb_snapshot) as old_table_count,
    (SELECT COUNT(*) FROM mv_latest_ccyb_snapshot) as new_view_count
UNION ALL
SELECT 
    'SyRB Snapshot',
    (SELECT COUNT(*) FROM latest_syrb_snapshot),
    (SELECT COUNT(*) FROM mv_latest_syrb_snapshot)
UNION ALL
SELECT 
    'OSII Snapshot',
    (SELECT COUNT(*) FROM latest_osii_snapshot),
    (SELECT COUNT(*) FROM mv_latest_osii_snapshot);
```

**Várt eredmény:** A számok hasonlóak legyenek (kis eltérés lehet, mert a Materialized View más logikát használ).

---

## Lépés 6: Foreign Keys hozzáadása

Futtasd a **011_add_foreign_keys_bbm.sql** fájlt:

### Supabase SQL Editor-ben

1. Új query létrehozása
2. Másold be a `migrations/011_add_foreign_keys_bbm.sql` fájl tartalmát
3. Futtasd a query-t

**Várt eredmény:**
- ✅ "ALTER TABLE" üzenetek (ha az oszlopok még nem léteztek)
- ✅ "CREATE INDEX" üzenetek
- ✅ "UPDATE" üzenetek (foreign key értékek kitöltése)

---

## Lépés 7: Ellenőrzés - Foreign Keys

Ellenőrizd, hogy a foreign key-k helyesen vannak-e kitöltve:

```sql
-- Ellenőrizd a foreign key-ket
SELECT 
    'ltv_rules' as table_name,
    COUNT(*) as total_rows,
    COUNT(bbm_measure_id) as rows_with_fk,
    COUNT(*) - COUNT(bbm_measure_id) as rows_without_fk
FROM ltv_rules
UNION ALL
SELECT 
    'dti_lti_rules',
    COUNT(*),
    COUNT(bbm_measure_id),
    COUNT(*) - COUNT(bbm_measure_id)
FROM dti_lti_rules;
```

**Várt eredmény:** A legtöbb sornak legyen `bbm_measure_id` értéke (ha van megfelelő BBM measure).

---

## Lépés 8: Trigger tesztelés

Teszteld, hogy a trigger-ek működnek-e:

```sql
-- Teszt: adj hozzá egy új CCyB decision-t (vagy módosíts egy létezőt)
-- Ez automatikusan frissítenie kellene a Materialized View-kat

-- Példa: nézd meg a jelenlegi állapotot
SELECT COUNT(*) as before_count FROM mv_latest_ccyb_snapshot;

-- Módosíts egy létező rekordot (ha van)
UPDATE ccyb_decisions 
SET updated_at = NOW() 
WHERE id = (SELECT id FROM ccyb_decisions LIMIT 1);

-- Ellenőrizd, hogy frissült-e
SELECT COUNT(*) as after_count FROM mv_latest_ccyb_snapshot;
```

**Várt eredmény:** A trigger automatikusan frissítenie kellene a Materialized View-kat.

---

## Lépés 9: Alkalmazás frissítése (ha szükséges)

Ha az alkalmazás kódja közvetlenül a régi táblákra hivatkozik, frissítsd:

### Python kódban

```python
# Régi:
# latest_ccyb = supabase.table('latest_ccyb_snapshot').select('*').execute()

# Új:
latest_ccyb = supabase.table('mv_latest_ccyb_snapshot').select('*').execute()
```

### Frontend kódban

```javascript
// Régi:
// const { data } = await supabase.from('latest_ccyb_snapshot').select('*')

// Új:
const { data } = await supabase.from('mv_latest_ccyb_snapshot').select('*')
```

---

## Lépés 10: Régi táblák törlése (OPCIONÁLIS)

⚠️ **FIGYELEM**: Csak akkor futtasd ezt, ha:
- ✅ A Materialized View-k helyesen működnek
- ✅ Az alkalmazás frissítve van
- ✅ Van backup az adatbázisról
- ✅ Tesztelted, hogy minden működik

Futtasd a **012_drop_old_snapshot_trend_tables.sql** fájlt:

### Supabase SQL Editor-ben

1. Új query létrehozása
2. Másold be a `migrations/012_drop_old_snapshot_trend_tables.sql` fájl tartalmát
3. **Olvasd el újra a fájlt**, hogy biztosan érted, mit csinál
4. Futtasd a query-t

**Várt eredmény:**
- ✅ "DROP POLICY" üzenetek
- ✅ "DROP TABLE" üzenetek
- ✅ "GRANT SELECT" üzenetek

---

## Lépés 11: Végleges ellenőrzés

Ellenőrizd, hogy minden rendben van:

```sql
-- 1. Materialized View-k léteznek
SELECT 
    schemaname,
    matviewname,
    hasindexes
FROM pg_matviews
WHERE schemaname = 'public'
  AND matviewname LIKE 'mv_%'
ORDER BY matviewname;

-- 2. Trigger-ek léteznek
SELECT 
    trigger_name,
    event_object_table,
    action_statement
FROM information_schema.triggers
WHERE trigger_schema = 'public'
  AND trigger_name LIKE 'trigger_refresh_views_%'
ORDER BY trigger_name;

-- 3. Foreign key-k léteznek
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'public'
  AND (tc.table_name = 'ltv_rules' OR tc.table_name = 'dti_lti_rules')
  AND kcu.column_name = 'bbm_measure_id';
```

**Várt eredmény:**
- ✅ 6 Materialized View
- ✅ 4 trigger
- ✅ 2 foreign key

---

## Hibaelhárítás

### Hiba: "relation already exists"

```sql
-- Ha a Materialized View már létezik, töröld először:
DROP MATERIALIZED VIEW IF EXISTS mv_latest_ccyb_snapshot CASCADE;
-- stb. minden view-hoz
```

### Hiba: "permission denied"

- Ellenőrizd, hogy service_role kulccsal vagy bejelentkezve
- Vagy futtasd service_role jogosultsággal

### Hiba: "trigger refresh is slow"

- Ez normális, a Materialized View refresh időigényes lehet
- Nagy adatmennyiség esetén érdemes lehet időzített refresh-t használni

### Materialized View üres

```sql
-- Manuálisan frissítsd:
REFRESH MATERIALIZED VIEW mv_latest_ccyb_snapshot;
```

---

## Visszaállítás (ha szükséges)

Ha valami nem működik, visszaállíthatod:

1. Töröld a Materialized View-kat (lásd Lépés 10)
2. Töröld a trigger-eket:
   ```sql
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_ccyb ON ccyb_decisions;
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_syrb ON syrb_measures;
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_osii ON osii_banks;
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_bbm ON bbm_measures;
   DROP FUNCTION IF EXISTS refresh_materialized_views();
   ```
3. Hozd vissza a régi táblákat az `001_initial_schema.sql` fájlból

---

## Kérdések?

Ha bármilyen probléma van, ellenőrizd:
- A Supabase logokat
- A SQL Editor hibaüzeneteit
- Az adatbázis kapcsolatot

---

## Összefoglalás

✅ **Lépés 1-2:** Ellenőrzés  
✅ **Lépés 3:** Materialized Views létrehozása  
✅ **Lépés 4-5:** Tesztelés  
✅ **Lépés 6-7:** Foreign Keys hozzáadása  
✅ **Lépés 8:** Trigger tesztelés  
✅ **Lépés 9:** Alkalmazás frissítése  
✅ **Lépés 10:** Régi táblák törlése (opcionális)  
✅ **Lépés 11:** Végleges ellenőrzés  

**Becsült idő:** 15-30 perc (attól függően, hogy mennyi adat van)
