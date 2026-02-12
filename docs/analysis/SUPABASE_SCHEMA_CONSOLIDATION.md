# 🔄 Supabase Adatséma Konszolidáció Javaslat

**Dátum:** 2024 Q4  
**Cél:** Átfedő táblázatok azonosítása és konszolidált adatmodell javaslata

---

## 📊 Jelenlegi Séma Áttekintés

### Főbb Táblázatok (13 tábla)

1. **Countries** (lookup)
   - `countries` - ország lookup tábla

2. **Időszoros Adatok** (3 tábla)
   - `ccyb_decisions` - CCyB időszoros adatok
   - `syrb_measures` - SyRB időszoros adatok
   - `osii_banks` - OSII/GSII bank adatok

3. **BBM Adatok** (3 tábla) ⚠️ **ÁTFEDÉS**
   - `bbm_measures` - Raw ESRB BBM adatok (LTV, DTI, LTI, DSTI)
   - `ltv_rules` - Strukturált LTV szabályok
   - `dti_lti_rules` - Strukturált DTI/LTI szabályok

4. **Snapshot Táblázatok** (3 tábla) ⚠️ **REDUNDÁNS**
   - `latest_ccyb_snapshot` - Legfrissebb CCyB snapshot
   - `latest_syrb_snapshot` - Legfrissebb SyRB snapshot
   - `latest_osii_snapshot` - Legfrissebb OSII snapshot

5. **Trend Táblázatok** (3 tábla) ⚠️ **REDUNDÁNS**
   - `ccyb_diffusion_trend` - CCyB trend aggregáció
   - `syrb_trend` - SyRB trend aggregáció
   - `bbm_diffusion_trend` - BBM trend aggregáció

---

## 🔍 Azonosított Átfedések

### 1. ⚠️ **BBM Measures vs. Strukturált Rules** (KRITIKUS)

**Probléma:**
- `bbm_measures` tartalmazza a **raw ESRB adatokat** (LTV, DTI, LTI, DSTI)
- `ltv_rules` és `dti_lti_rules` tartalmazza a **strukturált, AI-validált verziókat**
- **Ugyanazok az országok, ugyanazok a mértékek, csak más formátumban**

**Példa átfedés:**
```
bbm_measures: {country: "IE", measure_short: "LTV", description: "80% LTV limit..."}
ltv_rules: {country_iso2: "IE", limit_standard: "80.0%", ...}
```
→ **Ugyanaz az adat, csak strukturált formában!**

**Megoldás:** 
- `bbm_measures` megtartása **audit trail** céljából (raw ESRB adatok)
- `ltv_rules` és `dti_lti_rules` **foreign key** hozzáadása `bbm_measures.id`-hez
- Vagy: `bbm_measures` csak **nem-strukturált mértékekhez** (pl. DSTI, Maturity)

---

### 2. ⚠️ **Snapshot Táblázatok** (REDUNDÁNS)

**Probléma:**
- `latest_ccyb_snapshot`, `latest_syrb_snapshot`, `latest_osii_snapshot`
- **Ezek VIEW-ként vagy materialized view-ként is működhetnének**
- **Redundáns adattárolás** - ugyanaz az adat két helyen

**Példa:**
```sql
-- Jelenlegi:
latest_ccyb_snapshot: {country_iso2: "IE", rate: 1.0, effective_date: "2024-01-01"}
ccyb_decisions: {country_iso2: "IE", rate: 1.0, effective_date: "2024-01-01", ...}
```
→ **Ugyanaz az adat!**

**Megoldás:**
- **Materialized Views** vagy **Computed Columns**
- Vagy: **Snapshot táblázatok törlése**, helyette SQL query-kel számolni

---

### 3. ⚠️ **Trend Táblázatok** (REDUNDÁNS)

**Probléma:**
- `ccyb_diffusion_trend`, `syrb_trend`, `bbm_diffusion_trend`
- **Ezek aggregációk** - számíthatók az időszoros adatokból

**Példa:**
```sql
-- Jelenlegi:
ccyb_diffusion_trend: {date: "2024-01-01", countries_with_buffer: 15, avg_rate: 1.2}
-- Számítható:
SELECT date, COUNT(*) as countries_with_buffer, AVG(rate) as avg_rate
FROM ccyb_decisions
GROUP BY date
```

**Megoldás:**
- **Materialized Views** vagy **Computed Columns**
- Vagy: **Trend táblázatok törlése**, helyette SQL query-kel számolni

---

## 🎯 Konszolidált Adatmodell Javaslat

### Opció 1: **Materialized Views** (AJÁNLOTT)

**Előnyök:**
- ✅ Nincs redundáns adattárolás
- ✅ Automatikus frissítés (refresh)
- ✅ Jobb adatintegritás
- ✅ Könnyebb karbantartás

**Implementáció:**
```sql
-- 1. Snapshot táblázatok → Materialized Views
CREATE MATERIALIZED VIEW latest_ccyb_snapshot AS
SELECT DISTINCT ON (country_iso2)
    country_iso2,
    rate,
    effective_date,
    credit_gap,
    credit_to_gdp,
    updated_at
FROM ccyb_decisions
ORDER BY country_iso2, effective_date DESC;

CREATE UNIQUE INDEX ON latest_ccyb_snapshot(country_iso2);

-- 2. Trend táblázatok → Materialized Views
CREATE MATERIALIZED VIEW ccyb_diffusion_trend AS
SELECT 
    effective_date as date,
    COUNT(DISTINCT country_iso2) as countries_with_buffer,
    AVG(rate) as avg_rate,
    MAX(rate) as max_rate,
    MIN(rate) as min_rate,
    NOW() as updated_at
FROM ccyb_decisions
WHERE rate > 0
GROUP BY effective_date;

CREATE UNIQUE INDEX ON ccyb_diffusion_trend(date);

-- 3. Refresh function
CREATE OR REPLACE FUNCTION refresh_snapshots_and_trends()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_ccyb_snapshot;
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_syrb_snapshot;
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_osii_snapshot;
    REFRESH MATERIALIZED VIEW CONCURRENTLY ccyb_diffusion_trend;
    REFRESH MATERIALIZED VIEW CONCURRENTLY syrb_trend;
    REFRESH MATERIALIZED VIEW CONCURRENTLY bbm_diffusion_trend;
END;
$$ LANGUAGE plpgsql;
```

---

### Opció 2: **Unified Measures Table** (RADIKÁLIS)

**Előnyök:**
- ✅ Teljes konszolidáció
- ✅ Egységes adatmodell
- ✅ Könnyebb query-k

**Implementáció:**
```sql
-- Unified measures table
CREATE TABLE macroprudential_measures (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2),
    
    -- Measure classification
    measure_category VARCHAR(20) CHECK (measure_category IN ('CCyB', 'SyRB', 'BBM', 'OSII')),
    measure_type VARCHAR(50), -- "General", "Sectoral", "LTV", "DTI", "LTI", "DSTI"
    measure_subtype VARCHAR(50), -- "Residential RE", "Commercial RE", NULL
    
    -- Time-series data
    effective_date DATE,
    decision_date DATE,
    announcement_date DATE,
    revocation_date DATE,
    
    -- Status
    status VARCHAR(50), -- "Active", "Withdrawn", "Announced", "Revoked"
    implementation_status VARCHAR(20), -- "Active", "Inactive", "Announced"
    legal_form VARCHAR(20), -- "Binding", "Recommendation"
    
    -- Rates/Limits (flexible)
    rate DECIMAL(5,2), -- For CCyB, SyRB, OSII
    limit_standard TEXT, -- For BBM (can be single value or list)
    limit_ftb DECIMAL(5,2), -- First-time buyer limit
    limit_btl DECIMAL(5,2), -- Buy-to-let limit
    limit_green DECIMAL(4,2), -- Green mortgage limit
    
    -- Additional fields (JSONB for flexibility)
    metadata JSONB, -- For measure-specific fields (credit_gap, income_basis, etc.)
    
    -- Descriptions
    description TEXT,
    notes TEXT,
    
    -- Links
    regulation_url TEXT,
    related_links TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_measures_country_category ON macroprudential_measures(country_iso2, measure_category);
CREATE INDEX idx_measures_type ON macroprudential_measures(measure_type);
CREATE INDEX idx_measures_status ON macroprudential_measures(status) WHERE status = 'Active';
CREATE INDEX idx_measures_date ON macroprudential_measures(effective_date);

-- Views for backward compatibility
CREATE VIEW ccyb_decisions AS
SELECT * FROM macroprudential_measures WHERE measure_category = 'CCyB';

CREATE VIEW syrb_measures AS
SELECT * FROM macroprudential_measures WHERE measure_category = 'SyRB';

CREATE VIEW bbm_measures AS
SELECT * FROM macroprudential_measures WHERE measure_category = 'BBM';

CREATE VIEW ltv_rules AS
SELECT * FROM macroprudential_measures 
WHERE measure_category = 'BBM' AND measure_type = 'LTV';

CREATE VIEW dti_lti_rules AS
SELECT * FROM macroprudential_measures 
WHERE measure_category = 'BBM' AND measure_type IN ('DTI', 'LTI');
```

**Hátrányok:**
- ⚠️ Nagyobb migrációs munka
- ⚠️ Komplexebb query-k (JSONB használat)
- ⚠️ Backward compatibility problémák

---

### Opció 3: **Foreign Key Relations** (KONZERVATÍV)

**Előnyök:**
- ✅ Minimális változás
- ✅ Backward compatible
- ✅ Könnyebb migráció

**Implementáció:**
```sql
-- 1. BBM Measures → Rules foreign key
ALTER TABLE ltv_rules ADD COLUMN bbm_measure_id BIGINT REFERENCES bbm_measures(id);
ALTER TABLE dti_lti_rules ADD COLUMN bbm_measure_id BIGINT REFERENCES bbm_measures(id);

-- 2. Snapshot táblázatok → Foreign keys
ALTER TABLE latest_ccyb_snapshot ADD COLUMN latest_decision_id BIGINT REFERENCES ccyb_decisions(id);
ALTER TABLE latest_syrb_snapshot ADD COLUMN latest_measure_id BIGINT REFERENCES syrb_measures(id);
ALTER TABLE latest_osii_snapshot ADD COLUMN latest_bank_id BIGINT REFERENCES osii_banks(id);

-- 3. Trend táblázatok → Computed columns (trigger-based)
-- Vagy materialized views (lásd Opció 1)
```

---

## 📋 Ajánlott Megoldás: **Hibrid Megközelítés**

### Fázis 1: Materialized Views (Snapshot & Trends)
- ✅ Snapshot táblázatok → Materialized Views
- ✅ Trend táblázatok → Materialized Views
- ✅ Automatikus refresh trigger

### Fázis 2: Foreign Key Relations (BBM)
- ✅ `ltv_rules.bbm_measure_id` → `bbm_measures.id`
- ✅ `dti_lti_rules.bbm_measure_id` → `bbm_measures.id`
- ✅ Explicit kapcsolat a raw és strukturált adatok között

### Fázis 3: Opcionális Unified Table (Hosszú táv)
- ⚠️ Csak ha szükséges (pl. komplex cross-measure query-k)
- ⚠️ Nagyobb refaktorálás

---

## 🔄 Migrációs Terv

### 1. Materialized Views Létrehozása
```sql
-- migrations/010_create_materialized_views.sql
CREATE MATERIALIZED VIEW latest_ccyb_snapshot AS ...
CREATE MATERIALIZED VIEW latest_syrb_snapshot AS ...
CREATE MATERIALIZED VIEW latest_osii_snapshot AS ...
CREATE MATERIALIZED VIEW ccyb_diffusion_trend AS ...
CREATE MATERIALIZED VIEW syrb_trend AS ...
CREATE MATERIALIZED VIEW bbm_diffusion_trend AS ...
```

### 2. Régi Táblázatok Törlése
```sql
-- migrations/011_drop_redundant_tables.sql
DROP TABLE IF EXISTS latest_ccyb_snapshot CASCADE;
DROP TABLE IF EXISTS latest_syrb_snapshot CASCADE;
DROP TABLE IF EXISTS latest_osii_snapshot CASCADE;
DROP TABLE IF EXISTS ccyb_diffusion_trend CASCADE;
DROP TABLE IF EXISTS syrb_trend CASCADE;
DROP TABLE IF EXISTS bbm_diffusion_trend CASCADE;
```

### 3. Foreign Keys Hozzáadása (BBM)
```sql
-- migrations/012_add_bbm_foreign_keys.sql
ALTER TABLE ltv_rules ADD COLUMN bbm_measure_id BIGINT REFERENCES bbm_measures(id);
ALTER TABLE dti_lti_rules ADD COLUMN bbm_measure_id BIGINT REFERENCES bbm_measures(id);
```

### 4. Refresh Trigger
```sql
-- migrations/013_add_refresh_trigger.sql
CREATE OR REPLACE FUNCTION refresh_snapshots_and_trends()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_ccyb_snapshot;
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_syrb_snapshot;
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_osii_snapshot;
    REFRESH MATERIALIZED VIEW CONCURRENTLY ccyb_diffusion_trend;
    REFRESH MATERIALIZED VIEW CONCURRENTLY syrb_trend;
    REFRESH MATERIALIZED VIEW CONCURRENTLY bbm_diffusion_trend;
END;
$$ LANGUAGE plpgsql;

-- Auto-refresh after data updates
CREATE TRIGGER refresh_after_ccyb_update
AFTER INSERT OR UPDATE OR DELETE ON ccyb_decisions
FOR EACH STATEMENT EXECUTE FUNCTION refresh_snapshots_and_trends();
```

---

## 📊 Várható Hatások

### Adattárolás
- **Snapshot táblázatok törlése:** -30-50% adattárolás
- **Trend táblázatok törlése:** -10-20% adattárolás
- **Összesen:** -40-70% redundáns adattárolás

### Teljesítmény
- **Materialized Views:** +20-30% query teljesítmény (indexekkel)
- **Foreign Keys:** Jobb adatintegritás, könnyebb join-ok

### Karbantarthatóság
- **Egyetlen forrás:** Nincs adat szinkronizálási probléma
- **Automatikus refresh:** Nincs manuális frissítés szükség

---

## ⚠️ Kockázatok és Mitigáció

### Kockázatok
1. **Backward Compatibility:**
   - **Mitigáció:** VIEW-k létrehozása régi táblázatok neveivel
   - **Mitigáció:** Fokozatos migráció (egy táblázat egyszerre)

2. **Materialized View Refresh Teljesítmény:**
   - **Mitigáció:** `CONCURRENTLY` használata (non-blocking)
   - **Mitigáció:** Indexek létrehozása refresh előtt

3. **Foreign Key Constraints:**
   - **Mitigáció:** `NULL` értékek engedélyezése (opcionális kapcsolat)
   - **Mitigáció:** Migrációs script azonosítja a kapcsolatokat

---

## ✅ Következő Lépések

1. **Azonnal (1-2 nap):**
   - Materialized Views létrehozása (snapshot & trends)
   - Régi táblázatok törlése
   - Refresh trigger implementálása

2. **Rövid táv (1 hét):**
   - Foreign keys hozzáadása (BBM)
   - Migrációs script azonosítja a kapcsolatokat
   - Backward compatibility VIEW-k

3. **Hosszú táv (opcionális):**
   - Unified measures table (ha szükséges)
   - Teljes sémarefaktorálás

---

## 📝 Összefoglalás

### Főbb Átfedések:
1. ⚠️ **BBM Measures vs. Rules** - Foreign key kapcsolat
2. ⚠️ **Snapshot táblázatok** - Materialized Views
3. ⚠️ **Trend táblázatok** - Materialized Views

### Ajánlott Megoldás:
- ✅ **Materialized Views** (snapshot & trends)
- ✅ **Foreign Keys** (BBM measures → rules)
- ✅ **Automatikus refresh** (trigger-based)

### Várható Eredmények:
- ✅ **-40-70% redundáns adattárolás**
- ✅ **+20-30% query teljesítmény**
- ✅ **Jobb adatintegritás**
- ✅ **Könnyebb karbantartás**

---

**Megjegyzés:** A konszolidáció fokozatosan történjen, egy táblázat/típus egyszerre, hogy ne törjön el a működő rendszer.
