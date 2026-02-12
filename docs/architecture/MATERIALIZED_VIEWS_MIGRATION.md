# Materialized Views Migration Guide

## Áttekintés

Ez a migration a snapshot és trend táblákat Materialized View-kra konvertálja, hogy csökkentse a redundáns adattárolást 40-70%-kal.

## Migration Fájlok

1. **010_create_materialized_views.sql** - Materialized View-k létrehozása és refresh trigger
2. **011_add_foreign_keys_bbm.sql** - Foreign key-k hozzáadása BBM measures-hez
3. **012_drop_old_snapshot_trend_tables.sql** - Régi táblák törlése (opcionális)

## Materialized View-k

### Snapshot View-k

- **mv_latest_ccyb_snapshot** - Legfrissebb CCyB adatok országonként
- **mv_latest_syrb_snapshot** - Legfrissebb SyRB adatok országonként
- **mv_latest_osii_snapshot** - Legfrissebb OSII/GSII adatok országonként

### Trend View-k

- **mv_ccyb_diffusion_trend** - CCyB diffúziós trend időszoros adatokkal
- **mv_syrb_trend** - SyRB trend időszoros adatokkal
- **mv_bbm_diffusion_trend** - BBM diffúziós trend időszoros adatokkal

## Automatikus Refresh

A Materialized View-k automatikusan frissülnek, amikor az alapadatok változnak:

- `ccyb_decisions` változás → `mv_latest_ccyb_snapshot` és `mv_ccyb_diffusion_trend` frissül
- `syrb_measures` változás → `mv_latest_syrb_snapshot` és `mv_syrb_trend` frissül
- `osii_banks` változás → `mv_latest_osii_snapshot` frissül
- `bbm_measures` változás → `mv_bbm_diffusion_trend` frissül

## Használat

### Manuális Refresh

Ha szükséges, manuálisan is frissíthető a Materialized View:

```sql
REFRESH MATERIALIZED VIEW mv_latest_ccyb_snapshot;
REFRESH MATERIALIZED VIEW mv_latest_syrb_snapshot;
REFRESH MATERIALIZED VIEW mv_latest_osii_snapshot;
REFRESH MATERIALIZED VIEW mv_ccyb_diffusion_trend;
REFRESH MATERIALIZED VIEW mv_syrb_trend;
REFRESH MATERIALIZED VIEW mv_bbm_diffusion_trend;
```

### Query Példák

```sql
-- Legfrissebb CCyB adatok lekérdezése
SELECT * FROM mv_latest_ccyb_snapshot
WHERE country_iso2 = 'HU';

-- CCyB trend adatok lekérdezése
SELECT * FROM mv_ccyb_diffusion_trend
WHERE date >= '2024-01-01'
ORDER BY date DESC;
```

## Foreign Keys

A migration hozzáadja a `bbm_measure_id` oszlopokat:

- `ltv_rules.bbm_measure_id` → `bbm_measures.id`
- `dti_lti_rules.bbm_measure_id` → `bbm_measures.id`

Ez javítja az adatintegritást és lehetővé teszi a JOIN-okat a BBM measures táblával.

## Migration Lépések

1. **Futtasd a 010_create_materialized_views.sql fájlt**
   ```sql
   \i migrations/010_create_materialized_views.sql
   ```

2. **Ellenőrizd, hogy a Materialized View-k helyesen működnek**
   ```sql
   SELECT COUNT(*) FROM mv_latest_ccyb_snapshot;
   SELECT COUNT(*) FROM mv_ccyb_diffusion_trend;
   ```

3. **Futtasd a 011_add_foreign_keys_bbm.sql fájlt**
   ```sql
   \i migrations/011_add_foreign_keys_bbm.sql
   ```

4. **Opcionálisan futtasd a 012_drop_old_snapshot_trend_tables.sql fájlt**
   - ⚠️ **FIGYELEM**: Ez törli a régi táblákat! Csak akkor futtasd, ha biztos vagy benne, hogy a Materialized View-k helyesen működnek.

## Teljesítmény Megjegyzések

- A Materialized View refresh trigger **blokkoló** művelet, ami rövid ideig zárolhatja a view-t
- Nagy adatmennyiség esetén érdemes lehet:
  - `pg_cron` extension használata időzített refresh-hez
  - Background job implementálása
  - CONCURRENTLY refresh használata (de ez nem működik trigger-ben)

## Visszaállítás

Ha vissza kell állítani a régi táblákat:

1. Töröld a Materialized View-kat:
   ```sql
   DROP MATERIALIZED VIEW IF EXISTS mv_latest_ccyb_snapshot CASCADE;
   DROP MATERIALIZED VIEW IF EXISTS mv_latest_syrb_snapshot CASCADE;
   DROP MATERIALIZED VIEW IF EXISTS mv_latest_osii_snapshot CASCADE;
   DROP MATERIALIZED VIEW IF EXISTS mv_ccyb_diffusion_trend CASCADE;
   DROP MATERIALIZED VIEW IF EXISTS mv_syrb_trend CASCADE;
   DROP MATERIALIZED VIEW IF EXISTS mv_bbm_diffusion_trend CASCADE;
   ```

2. Töröld a trigger-eket:
   ```sql
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_ccyb ON ccyb_decisions;
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_syrb ON syrb_measures;
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_osii ON osii_banks;
   DROP TRIGGER IF EXISTS trigger_refresh_views_on_bbm ON bbm_measures;
   DROP FUNCTION IF EXISTS refresh_materialized_views();
   ```

3. Hozd vissza a régi táblákat az `001_initial_schema.sql` fájlból.

## Üzleti Érték

- ✅ **-40-70% redundáns adattárolás**: A snapshot és trend adatok nem tárolódnak duplikálva
- ✅ **Jobb adatintegritás**: Foreign key-k garantálják a relációkat
- ✅ **Automatikus frissítés**: Nincs szükség manuális frissítésre
- ✅ **Jobb teljesítmény**: Materialized View-k indexekkel gyorsabbak, mint a dinamikus query-k
