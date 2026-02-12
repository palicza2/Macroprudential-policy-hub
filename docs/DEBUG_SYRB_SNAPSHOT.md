# SyRB Snapshot Debug - Lépésről Lépésre

## Probléma
A `mv_latest_syrb_snapshot` Materialized View üres (0 sor), de a régi `latest_syrb_snapshot` táblában 25 sor van.

## Lépés 1: Ellenőrizd a syrb_measures táblát

Futtasd ezt a query-t, hogy lássd, milyen adatok vannak:

```sql
-- Nézd meg, hány sor van összesen
SELECT COUNT(*) as total_rows FROM syrb_measures;

-- Nézd meg, milyen status értékek vannak
SELECT status, COUNT(*) as count
FROM syrb_measures
GROUP BY status
ORDER BY count DESC;

-- Nézd meg, hány aktív rekord van (különböző status értékekkel)
SELECT 
    COUNT(*) FILTER (WHERE status = 'Active') as status_active,
    COUNT(*) FILTER (WHERE status ILIKE '%active%') as status_contains_active,
    COUNT(*) FILTER (WHERE status IS NOT NULL) as status_not_null,
    COUNT(*) as total
FROM syrb_measures;
```

## Lépés 2: Nézd meg a régi tábla struktúráját

```sql
-- Nézd meg, hogyan néz ki a régi snapshot tábla
SELECT * FROM latest_syrb_snapshot LIMIT 5;

-- Nézd meg, milyen country_iso2 értékek vannak
SELECT country_iso2, total_rate, general_rate, sectoral_rate
FROM latest_syrb_snapshot
ORDER BY country_iso2;
```

## Lépés 3: Teszteld a Materialized View query-t

Futtasd ezt a query-t, hogy lássd, mi jönne ki a Materialized View-ból:

```sql
-- Ez a query ugyanaz, mint amit a Materialized View használ
SELECT 
    country_iso2,
    COALESCE(SUM(CASE WHEN status = 'Active' THEN rate ELSE 0 END), 0) as total_rate,
    COALESCE(SUM(CASE WHEN status = 'Active' AND measure_type = 'General' THEN rate ELSE 0 END), 0) as general_rate,
    COALESCE(SUM(CASE WHEN status = 'Active' AND measure_type = 'Sectoral' THEN rate ELSE 0 END), 0) as sectoral_rate,
    NOW() as updated_at
FROM syrb_measures
WHERE status = 'Active'
GROUP BY country_iso2;
```

**Ha ez a query is 0 sort ad vissza**, akkor a probléma az, hogy a `status = 'Active'` feltétel nem talál egyezést.

## Lépés 4: Próbáld ki más status értékekkel

```sql
-- Próbáld ki, hogy milyen status értékekkel van adat
SELECT 
    status,
    COUNT(*) as count,
    COUNT(DISTINCT country_iso2) as countries
FROM syrb_measures
GROUP BY status
ORDER BY count DESC;

-- Próbáld ki ILIKE-tal (case-insensitive)
SELECT 
    country_iso2,
    COALESCE(SUM(CASE WHEN status ILIKE '%active%' THEN rate ELSE 0 END), 0) as total_rate
FROM syrb_measures
WHERE status ILIKE '%active%'
GROUP BY country_iso2;
```

## Lépés 5: Javított Materialized View

Ha a status értékek nem pontosan 'Active'-ek, akkor frissítsd a Materialized View definícióját:

```sql
-- Töröld a régi Materialized View-t
DROP MATERIALIZED VIEW IF EXISTS mv_latest_syrb_snapshot CASCADE;

-- Hozd létre újra a javított verzióval
CREATE MATERIALIZED VIEW mv_latest_syrb_snapshot AS
SELECT 
    country_iso2,
    COALESCE(SUM(CASE 
        WHEN status ILIKE '%active%' 
         AND status NOT ILIKE '%inactive%'
         AND status NOT ILIKE '%revoked%'
         AND status NOT ILIKE '%deactivated%'
        THEN rate ELSE 0 
    END), 0) as total_rate,
    COALESCE(SUM(CASE 
        WHEN status ILIKE '%active%' 
         AND status NOT ILIKE '%inactive%'
         AND status NOT ILIKE '%revoked%'
         AND status NOT ILIKE '%deactivated%'
         AND measure_type = 'General' 
        THEN rate ELSE 0 
    END), 0) as general_rate,
    COALESCE(SUM(CASE 
        WHEN status ILIKE '%active%' 
         AND status NOT ILIKE '%inactive%'
         AND status NOT ILIKE '%revoked%'
         AND status NOT ILIKE '%deactivated%'
         AND measure_type = 'Sectoral' 
        THEN rate ELSE 0 
    END), 0) as sectoral_rate,
    NOW() as updated_at
FROM syrb_measures
WHERE status ILIKE '%active%'
  AND status NOT ILIKE '%inactive%'
  AND status NOT ILIKE '%revoked%'
  AND status NOT ILIKE '%deactivated%'
GROUP BY country_iso2;

-- Hozd létre az indexet
CREATE UNIQUE INDEX idx_mv_latest_syrb_country 
    ON mv_latest_syrb_snapshot(country_iso2);

-- Frissítsd
REFRESH MATERIALIZED VIEW mv_latest_syrb_snapshot;
```

## Alternatív megoldás: Nézd meg, hogyan van feltöltve a régi tábla

Ha a régi táblát valamilyen Python kód tölti fel, nézd meg, hogy milyen logikát használ:

```sql
-- Nézd meg, hogy a régi tábla hogyan van feltöltve
-- (Ez segít megérteni, milyen logikát kell használni)
```

A Python kódban (syrb.py) látom, hogy:
- `status_str.str.contains("applicable|active", case=False)`
- És kizárja: `~status_str.str.contains("Deactivated|Revoked|No longer", case=False)`

Ez azt jelenti, hogy a Materialized View-nek is hasonló logikát kell használnia!
