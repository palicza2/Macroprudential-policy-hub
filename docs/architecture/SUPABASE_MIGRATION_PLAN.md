# Supabase Migrációs Terv és Implementációs Lépések

## 📋 Áttekintés

Ez a dokumentum részletes lépéseket tartalmaz a jelenlegi Parquet-alapú adattárolás Supabase PostgreSQL adatbázisba való migrálásához.

## 🎯 Célok

1. **Adatintegritás**: Foreign key constraints, check constraints
2. **Normalizálás**: Országok, dátumok, bankok közötti relációk
3. **REST API**: Automatikus API generálás Supabase-ből
4. **Skálázhatóság**: Később könnyen bővíthető
5. **Több limit támogatás**: LTV és DTI/LTI táblákban lista/ranges támogatás

---

## 📊 Jelenlegi Adatstruktúra Elemzése

### Parquet Fájlok
- `processed_ccyb.parquet` - CCyB időszoros adatok (690 sor, 19 oszlop)
- `processed_syrb.parquet` - SyRB időszoros adatok
- `processed_bbm.parquet` - BBM adatok (195 sor, 21 oszlop)
- `processed_osii.parquet` - OSII/GSII bank adatok
- `latest_ccyb.parquet` - Legfrissebb CCyB snapshot
- `latest_syrb.parquet` - Legfrissebb SyRB snapshot
- `latest_osii.parquet` - Legfrissebb OSII snapshot
- `agg_trend.parquet` - Agregált trend adatok

### CSV Fájlok
- `dti_lti_rules.csv` - Strukturált DTI/LTI szabályok (6 sor, 12 oszlop)
  - **Fontos**: `Limit_Standard` lehet lista (pl. "3.0x, 8.0x")
  - **Mezők**: Country, Measure_Code, Implementation_Status, Legal_Form, Limit_Standard (TEXT), Limit_FTB, Limit_BTL, Limit_Green, Income_Basis, Allowance_Share, Regulation_URL, Notes

### LTV Adatok
- Jelenleg a `processed_bbm.parquet`-ből generálódik strukturált formában
- **Mezők**: Country, Implementation_Status, Legal_Form, Limit_Standard (TEXT - lehet lista), Limit_FTB, Limit_BTL, Exception_Quota, Notes

---

## 🗄️ Frissített Supabase Séma

### 1. Countries Lookup Table

```sql
CREATE TABLE countries (
    iso2 CHAR(2) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    iso3 CHAR(3),
    region VARCHAR(50),
    eea_member BOOLEAN DEFAULT FALSE,
    eu_member BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_countries_region ON countries(region);
```

### 2. CCyB Decisions (Időszoros)

```sql
CREATE TABLE ccyb_decisions (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    effective_date DATE NOT NULL,
    decision_date DATE,
    announcement_date DATE,
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 20),
    status VARCHAR(50), -- "Confirmation", "Increase", "Decrease", etc.
    credit_gap DECIMAL(5,2),
    credit_to_gdp DECIMAL(8,2),
    buffer_guide DECIMAL(5,2),
    justification TEXT,
    justification_exceptional TEXT,
    link TEXT,
    reference_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2, effective_date)
);

CREATE INDEX idx_ccyb_country_date ON ccyb_decisions(country_iso2, effective_date);
CREATE INDEX idx_ccyb_date ON ccyb_decisions(effective_date);
CREATE INDEX idx_ccyb_status ON ccyb_decisions(status);
```

### 3. SyRB Measures (Időszoros)

```sql
CREATE TABLE syrb_measures (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_type VARCHAR(50), -- "General", "Sectoral"
    sector VARCHAR(50), -- "Residential RE", "Commercial RE", NULL
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 20),
    effective_date DATE,
    decision_date DATE,
    status VARCHAR(20), -- "Active", "Withdrawn", "Announced", "Revoked"
    description TEXT,
    basis_in_union_law TEXT,
    related_links TEXT,
    revocation_date DATE,
    revocation_note TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_syrb_country ON syrb_measures(country_iso2);
CREATE INDEX idx_syrb_status ON syrb_measures(status) WHERE status = 'Active';
CREATE INDEX idx_syrb_type ON syrb_measures(measure_type);
```

### 4. BBM Measures (Raw ESRB Data)

```sql
CREATE TABLE bbm_measures (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_type VARCHAR(50), -- "Loan-to-value (LTV)", "Debt-to-income (DTI)", etc.
    measure_short VARCHAR(10), -- "LTV", "DTI", "LTI", "DSTI"
    status VARCHAR(20), -- "Active", "Withdrawn", "Announced"
    active_status VARCHAR(20), -- "Active", "Inactive"
    description TEXT,
    intermediate_objective TEXT,
    basis_in_union_law TEXT,
    effective_date DATE,
    decision_date DATE,
    authority VARCHAR(200),
    year_initiative INTEGER,
    parent_measure VARCHAR(200),
    has_been_revoked BOOLEAN DEFAULT FALSE,
    revocation_date DATE,
    revocation_note TEXT,
    related_links TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_bbm_country_type ON bbm_measures(country_iso2, measure_short);
CREATE INDEX idx_bbm_status ON bbm_measures(active_status) WHERE active_status = 'Active';
CREATE INDEX idx_bbm_measure_short ON bbm_measures(measure_short);
```

### 5. LTV Rules (Strukturált) ⭐ ÚJ

```sql
CREATE TABLE ltv_rules (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    implementation_status VARCHAR(20) CHECK (implementation_status IN ('Active', 'Inactive', 'Announced')),
    legal_form VARCHAR(20) CHECK (legal_form IN ('Binding', 'Recommendation')),
    limit_standard TEXT, -- Can be single value (e.g., "80.0%") or list (e.g., "80.0%, 90.0%")
    limit_ftb DECIMAL(5,2) CHECK (limit_ftb >= 0 AND limit_ftb <= 100),
    limit_btl DECIMAL(5,2) CHECK (limit_btl >= 0 AND limit_btl <= 100),
    exception_quota VARCHAR(100), -- e.g., "15% of volume"
    notes TEXT, -- Explains what list values mean (e.g., "80% for owner-occupied, 70% for investment")
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2)
);

CREATE INDEX idx_ltv_country ON ltv_rules(country_iso2);
CREATE INDEX idx_ltv_status ON ltv_rules(implementation_status) WHERE implementation_status = 'Active';
```

### 6. DTI/LTI Rules (Strukturált) - FRISSÍTVE

```sql
CREATE TABLE dti_lti_rules (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_code VARCHAR(3) CHECK (measure_code IN ('DTI', 'LTI')),
    implementation_status VARCHAR(20) CHECK (implementation_status IN ('Active', 'Withdrawn', 'Announced')),
    legal_form VARCHAR(20) CHECK (legal_form IN ('Binding', 'Recommendation')),
    limit_standard TEXT, -- Can be single value (e.g., "4.5x") or list (e.g., "3.0x, 8.0x")
    limit_ftb DECIMAL(4,2),
    limit_btl DECIMAL(4,2),
    limit_green DECIMAL(4,2), -- Green/sustainable mortgage limit (e.g., for LV)
    income_basis VARCHAR(10) CHECK (income_basis IN ('Gross', 'Net', 'Unknown')),
    allowance_share VARCHAR(10), -- "15%"
    regulation_url TEXT,
    notes TEXT, -- Explains what list values mean (e.g., "Decreasing by age" for SK's 3-8x range)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2, measure_code)
);

CREATE INDEX idx_dti_lti_country ON dti_lti_rules(country_iso2);
CREATE INDEX idx_dti_lti_status ON dti_lti_rules(implementation_status) WHERE implementation_status = 'Active';
CREATE INDEX idx_dti_lti_measure ON dti_lti_rules(measure_code);
```

### 7. OSII/GSII Banks

```sql
CREATE TABLE osii_banks (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    bank_name VARCHAR(200) NOT NULL,
    lei_code VARCHAR(20),
    buffer_type VARCHAR(20), -- "OSII", "GSII"
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 5),
    effective_date DATE,
    status VARCHAR(20), -- "Active", "Inactive"
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_osii_country ON osii_banks(country_iso2);
CREATE INDEX idx_osii_status ON osii_banks(status) WHERE status = 'Active';
CREATE INDEX idx_osii_type ON osii_banks(buffer_type);
```

### 8. Latest Snapshots (Materialized Views)

```sql
CREATE TABLE latest_ccyb_snapshot (
    country_iso2 CHAR(2) PRIMARY KEY REFERENCES countries(iso2),
    rate DECIMAL(5,2),
    effective_date DATE,
    credit_gap DECIMAL(5,2),
    credit_to_gdp DECIMAL(8,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE latest_syrb_snapshot (
    country_iso2 CHAR(2) PRIMARY KEY REFERENCES countries(iso2),
    total_rate DECIMAL(5,2), -- Sum of all active SyRB rates
    general_rate DECIMAL(5,2),
    sectoral_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE latest_osii_snapshot (
    country_iso2 CHAR(2) PRIMARY KEY REFERENCES countries(iso2),
    total_rate DECIMAL(5,2), -- Sum of all active OSII/GSII rates
    osii_count INTEGER,
    gsii_count INTEGER,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### 9. Aggregated Trends

```sql
CREATE TABLE ccyb_diffusion_trend (
    date DATE PRIMARY KEY,
    countries_with_buffer INTEGER,
    avg_rate DECIMAL(5,2),
    max_rate DECIMAL(5,2),
    min_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE syrb_trend (
    date DATE PRIMARY KEY,
    countries_with_general INTEGER,
    countries_with_sectoral INTEGER,
    avg_general_rate DECIMAL(5,2),
    avg_sectoral_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE bbm_diffusion_trend (
    date DATE PRIMARY KEY,
    countries_with_bbm INTEGER,
    ltv_count INTEGER,
    dti_lti_count INTEGER,
    dsti_count INTEGER,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

---

## 🚀 Implementációs Lépések

### Fázis 1: Supabase Projekt Setup (1-2 nap)

#### 1.1 Supabase Projekt Létrehozás
1. Látogasd meg: https://supabase.com
2. Regisztráció/Bejelentkezés
3. "New Project" létrehozás
4. Projekt neve: `macroprudential-hub` (vagy hasonló)
5. Database password generálás és mentés
6. Region választás (EU - közelebb van)

#### 1.2 Környezeti Változók
Hozzáadás `.env` fájlhoz:
```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key  # Public anon key (read/write)
SUPABASE_SERVICE_KEY=your-service-key  # Service role key (admin operations)
SUPABASE_DB_PASSWORD=your-database-password
```

#### 1.3 SQL Séma Létrehozás
- Fájl: `migrations/001_initial_schema.sql`
- Supabase Dashboard → SQL Editor → Futtatás

### Fázis 2: Migrációs Script Készítése (2-3 nap)

#### 2.1 Python Dependencies
```bash
pip install supabase pandas pyarrow
```

#### 2.2 Migrációs Modul Struktúra
```
supabase_migration/
├── __init__.py
├── config.py              # Supabase connection config
├── schema.py              # SQL schema definitions
├── transformers.py        # Data transformation (parquet → Supabase format)
├── migrator.py            # Main migration script
└── validators.py          # Data validation before migration
```

#### 2.3 Főbb Funkciók

**transformers.py**:
- `transform_ccyb_data()` - CCyB parquet → Supabase format
- `transform_syrb_data()` - SyRB parquet → Supabase format
- `transform_bbm_data()` - BBM parquet → Supabase format
- `transform_osii_data()` - OSII parquet → Supabase format
- `transform_dti_lti_data()` - CSV → Supabase format (limit_standard TEXT)
- `transform_ltv_data()` - LTV DataFrame → Supabase format (limit_standard TEXT)
- `transform_countries()` - Countries lookup table
- `transform_snapshots()` - Latest snapshots
- `transform_trends()` - Aggregated trends

**migrator.py**:
- `migrate_all()` - Teljes migráció
- `migrate_incremental()` - Csak új/updated rekordok
- `validate_data()` - Adatvalidáció migráció előtt
- `rollback()` - Rollback funkció (ha kell)

### Fázis 3: Integráció a Pipeline-be (1-2 nap)

#### 3.1 ETL Stage Frissítése
- Opcionális Supabase write a parquet mentés mellett
- Config flag: `ENABLE_SUPABASE` (default: False)

#### 3.2 BBM Stage Frissítése
- LTV és DTI/LTI táblák automatikus feltöltése Supabase-be
- Upsert logika (update if exists, insert if new)

### Fázis 4: Tesztelés és Validáció (1-2 nap)

#### 4.1 Adatvalidáció
- Sorok száma: Parquet vs Supabase
- Foreign key constraints ellenőrzése
- Check constraints ellenőrzése
- Unique constraints ellenőrzése

#### 4.2 REST API Tesztelés
- Supabase Dashboard → API Docs
- Python client tesztelés
- JavaScript client tesztelés (ha kell)

---

## 📝 Konkrét Implementációs Lépések

### Lépés 1: Supabase Projekt Létrehozás ✅
- [ ] Supabase account létrehozás
- [ ] Projekt létrehozás
- [ ] Database password mentés
- [ ] URL és API keys mentése

### Lépés 2: SQL Séma Létrehozás
- [ ] `migrations/001_initial_schema.sql` fájl létrehozása
- [ ] Séma futtatása Supabase SQL Editor-ben
- [ ] Indexek létrehozása
- [ ] Foreign key constraints ellenőrzése

### Lépés 3: Python Migrációs Script
- [ ] `supabase_migration/` könyvtár létrehozása
- [ ] `config.py` - Supabase connection
- [ ] `transformers.py` - Data transformation functions
- [ ] `migrator.py` - Main migration script
- [ ] `validators.py` - Data validation

### Lépés 4: Első Migráció
- [ ] Countries tábla feltöltése
- [ ] CCyB decisions migrálása
- [ ] SyRB measures migrálása
- [ ] BBM measures migrálása
- [ ] DTI/LTI rules migrálása (limit_standard TEXT)
- [ ] LTV rules migrálása (limit_standard TEXT)
- [ ] OSII banks migrálása
- [ ] Snapshots és trends migrálása

### Lépés 5: Pipeline Integráció
- [ ] Config flag hozzáadása (`ENABLE_SUPABASE`)
- [ ] ETL stage frissítése (opcionális Supabase write)
- [ ] BBM stage frissítése (LTV/DTI-LTI auto-upload)
- [ ] Error handling és logging

### Lépés 6: Dokumentáció és Tesztelés
- [ ] README frissítése Supabase részletekkel
- [ ] API dokumentáció
- [ ] Tesztelés: teljes migráció
- [ ] Tesztelés: inkrementális update

---

## 🔧 Technikai Részletek

### Limit_Standard TEXT Mező
Mivel a `limit_standard` lehet lista (pl. "3.0x, 8.0x" vagy "80.0%, 90.0%"), TEXT típust használunk:
- **DTI/LTI**: "4.5x" vagy "3.0x, 8.0x"
- **LTV**: "80.0%" vagy "80.0%, 90.0%"
- **Notes oszlop**: Magyarázza, mit jelent a lista (pl. "Decreasing by age")

### Upsert Stratégia
- **Unique constraint**: `(country_iso2, measure_code)` DTI/LTI-nál
- **Unique constraint**: `(country_iso2)` LTV-nál
- **Upsert**: `INSERT ... ON CONFLICT ... DO UPDATE`

### Időbélyegek
- `created_at`: Első beszúrás ideje
- `updated_at`: Utolsó frissítés ideje (automatikus trigger)

---

## 📊 Adatmennyiség Becslés

**Jelenlegi:**
- Parquet fájlok: ~10-50 MB
- PostgreSQL-ben: ~50-150 MB (indexekkel)

**Supabase Ingyenes Tier:**
- Database: 500 MB → ✅ Bőven elég
- Bandwidth: 2 GB/hó → ✅ Elég
- API requests: Korlátlan (500 req/s) → ✅ Elég

---

## 🎯 Következő Lépések

1. **Supabase projekt létrehozás** (5 perc)
2. **SQL séma generálás és futtatás** (30 perc)
3. **Migrációs script fejlesztése** (2-3 nap)
4. **Első migráció tesztelése** (1 nap)
5. **Pipeline integráció** (1-2 nap)
6. **Dokumentáció frissítése** (1 nap)

**Összesen: ~1 hét**

---

## ❓ Kérdések

- Szeretnéd, hogy készítsek egy teljes migrációs scriptet?
- Inkrementális update-t szeretnél, vagy csak teljes migrációt?
- Szeretnéd, hogy a pipeline automatikusan írjon Supabase-be, vagy csak manuális migrációt?
