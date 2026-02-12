# Materialized Views Migration - Tesztelési Checklist

## ✅ Előzetes Ellenőrzések

### 1. Kód Szintaktikai Ellenőrzés
- [x] Nincs linter hiba
- [x] Python importok helyesek
- [x] JavaScript szintaxis helyes

### 2. Migration Fájlok
- [x] `010_create_materialized_views.sql` - Materialized View-k létrehozása
- [x] `010_fix_syrb_snapshot.sql` - SyRB snapshot javítás
- [x] `011_add_foreign_keys_bbm.sql` - Foreign key-k hozzáadása
- [x] `012_drop_old_snapshot_trend_tables.sql` - Régi táblák törlése (opcionális)

### 3. Kód Frissítések
- [x] `pipeline/stages/render_stage.py` - Materialized View-k használata
- [x] `assets/supabase-client.js` - Materialized View-k használata
- [x] `pipeline/writers/supabase_writer.py` - Megjegyzések hozzáadva

---

## 🧪 Tesztelési Lépések

### Lépés 1: Supabase Adatbázis Ellenőrzés

Futtasd ezeket a query-ket a Supabase SQL Editor-ben:

```sql
-- 1. Materialized View-k léteznek
SELECT matviewname, hasindexes 
FROM pg_matviews 
WHERE schemaname = 'public' 
  AND matviewname LIKE 'mv_%'
ORDER BY matviewname;

-- Várt: 6 Materialized View (mv_latest_ccyb_snapshot, mv_latest_syrb_snapshot, mv_latest_osii_snapshot, mv_ccyb_diffusion_trend, mv_syrb_trend, mv_bbm_diffusion_trend)

-- 2. Materialized View-k adatokat tartalmaznak
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

-- Várt: Minden view-nak legyen legalább néhány sora (vagy 0, ha nincs adat)

-- 3. Trigger-ek léteznek
SELECT trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_schema = 'public'
  AND trigger_name LIKE 'trigger_refresh_views_%'
ORDER BY trigger_name;

-- Várt: 4 trigger (ccyb, syrb, osii, bbm)

-- 4. Foreign key-k léteznek
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'public'
  AND (tc.table_name = 'ltv_rules' OR tc.table_name = 'dti_lti_rules')
  AND kcu.column_name = 'bbm_measure_id';

-- Várt: 2 foreign key (ltv_rules.bbm_measure_id, dti_lti_rules.bbm_measure_id)
```

### Lépés 2: Python Kód Tesztelés

```bash
# Import ellenőrzés
python -c "from pipeline.stages.render_stage import RenderStage; print('✅ Import OK')"

# Syntax ellenőrzés
python -m py_compile pipeline/stages/render_stage.py
python -m py_compile pipeline/writers/supabase_writer.py
python -m py_compile llm/cache.py
python -m py_compile utils/json_parser.py
python -m py_compile utils/retry.py

# Várt: Nincs hiba
```

### Lépés 3: Frontend Kód Tesztelés

```bash
# JavaScript szintaxis ellenőrzés (ha van node/npm)
# node -c assets/supabase-client.js

# Vagy egyszerűen ellenőrizd, hogy nincs szintaktikai hiba a fájlban
```

### Lépés 4: Materialized View Refresh Teszt

```sql
-- Teszt: módosíts egy rekordot és nézd meg, hogy frissül-e a Materialized View
-- 1. Jelenlegi állapot
SELECT COUNT(*) as before FROM mv_latest_ccyb_snapshot;

-- 2. Módosíts egy CCyB decision-t
UPDATE ccyb_decisions 
SET updated_at = NOW() 
WHERE id = (SELECT id FROM ccyb_decisions LIMIT 1);

-- 3. Ellenőrizd, hogy frissült-e (a számnak ugyanannak kell lennie)
SELECT COUNT(*) as after FROM mv_latest_ccyb_snapshot;

-- Várt: before = after (a trigger automatikusan frissíti)
```

---

## ✅ Ha Minden Teszt Sikeres

Ha minden teszt sikeres, akkor:
1. ✅ Készíts git commit-ot
2. ✅ Push a repository-ba
3. ✅ Deploy (ha van automatikus deploy)

---

## ⚠️ Ha Van Probléma

Ha valamelyik teszt sikertelen:
1. Nézd meg a hibaüzenetet
2. Ellenőrizd a migration fájlokat
3. Ellenőrizd, hogy az alapadatok léteznek-e
4. Nézd meg a Supabase logokat
