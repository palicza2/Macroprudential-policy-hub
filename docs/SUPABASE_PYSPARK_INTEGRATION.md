# Supabase + PySpark Integrációs Terv

## 1. Supabase Első Körben ✅

### 1.1 Miért Supabase?

**Előnyök:**
- ✅ **PostgreSQL-alapú**: Teljes SQL támogatás, standard toolokkal kompatibilis
- ✅ **Ingyenes tier**: 500 MB adat, 2 GB bandwidth/hó (jelenlegi adatmennyiséghez elég)
- ✅ **Automatikus REST API**: API generálás anélkül, hogy kódot kellene írni
- ✅ **Realtime subscriptions**: Ha később real-time dashboard kell
- ✅ **Dashboard/Admin UI**: Adatbázis kezelés böngészőből
- ✅ **Row Level Security (RLS)**: Ha később multi-user kell
- ✅ **Storage**: Ha később fájlokat is kell tárolni (pl. Excel exportok)

**Jelenlegi adatmennyiség becslés:**
- Parquet fájlok: ~10-50 MB összesen
- PostgreSQL-ben: ~50-150 MB (indexekkel, overhead-tel)
- **Supabase ingyenes tier (500 MB) → bőven elég!** ✅

### 1.2 Supabase Séma Tervezés

```sql
-- Countries lookup table
CREATE TABLE countries (
    iso2 CHAR(2) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    eea_member BOOLEAN DEFAULT FALSE,
    eu_member BOOLEAN DEFAULT FALSE
);

-- CCyB Decisions (időszoros)
CREATE TABLE ccyb_decisions (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    effective_date DATE NOT NULL,
    decision_date DATE,
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 20),
    credit_gap DECIMAL(5,2),
    credit_to_gdp DECIMAL(8,2),
    justification TEXT,
    reasoning_keywords TEXT, -- "Systemic Resilience | Credit Growth"
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2, effective_date)
);

CREATE INDEX idx_ccyb_country_date ON ccyb_decisions(country_iso2, effective_date);
CREATE INDEX idx_ccyb_date ON ccyb_decisions(effective_date);

-- SyRB Measures (időszoros)
CREATE TABLE syrb_measures (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_type VARCHAR(50), -- "General", "Sectoral"
    sector VARCHAR(50), -- "Residential RE", "Commercial RE", NULL
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 20),
    effective_date DATE,
    decision_date DATE,
    status VARCHAR(20), -- "Active", "Withdrawn", "Announced"
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_syrb_country ON syrb_measures(country_iso2);
CREATE INDEX idx_syrb_status ON syrb_measures(status) WHERE status = 'Active';

-- BBM Measures
CREATE TABLE bbm_measures (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_type VARCHAR(50), -- "LTV", "DTI", "LTI", "DSTI"
    measure_short VARCHAR(10), -- "LTV", "DTI", etc.
    status VARCHAR(20), -- "Active", "Withdrawn", "Announced"
    description TEXT,
    effective_date DATE,
    decision_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_bbm_country_type ON bbm_measures(country_iso2, measure_short);
CREATE INDEX idx_bbm_status ON bbm_measures(status) WHERE status = 'Active';

-- DTI/LTI Rules (strukturált)
CREATE TABLE dti_lti_rules (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_code VARCHAR(3) CHECK (measure_code IN ('DTI', 'LTI')),
    implementation_status VARCHAR(20) CHECK (implementation_status IN ('Active', 'Withdrawn', 'Announced')),
    legal_form VARCHAR(20) CHECK (legal_form IN ('Binding', 'Recommendation')),
    limit_standard DECIMAL(4,2),
    limit_ftb DECIMAL(4,2),
    limit_btl DECIMAL(4,2),
    income_basis VARCHAR(10) CHECK (income_basis IN ('Gross', 'Net', 'Unknown')),
    allowance_share VARCHAR(10), -- "15%"
    regulation_url TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2, measure_code, limit_standard)
);

CREATE INDEX idx_dti_lti_country ON dti_lti_rules(country_iso2);
CREATE INDEX idx_dti_lti_status ON dti_lti_rules(implementation_status) WHERE implementation_status = 'Active';

-- OSII/GSII Banks
CREATE TABLE osii_banks (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    bank_name VARCHAR(200) NOT NULL,
    lei_code VARCHAR(20),
    buffer_type VARCHAR(20), -- "OSII", "GSII"
    rate DECIMAL(5,2),
    effective_date DATE,
    status VARCHAR(20), -- "Active", "Inactive"
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_osii_country ON osii_banks(country_iso2);
CREATE INDEX idx_osii_status ON osii_banks(status) WHERE status = 'Active';

-- Latest Snapshots (materialized views vagy táblák)
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

-- Aggregated Trends
CREATE TABLE ccyb_diffusion_trend (
    date DATE PRIMARY KEY,
    countries_with_buffer INTEGER,
    avg_rate DECIMAL(5,2),
    max_rate DECIMAL(5,2),
    min_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### 1.3 Supabase Integráció Python-ból

```python
# supabase_integration.py
from supabase import create_client, Client
import pandas as pd
from pathlib import Path

# Supabase connection
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-anon-key"  # Public anon key (read/write)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def migrate_parquet_to_supabase():
    """Migrate all parquet files to Supabase."""
    
    # 1. Countries
    countries_df = pd.DataFrame({
        'iso2': ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 
                 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 
                 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE', 'NO', 'IS', 'LI', 'GB'],
        'country_name': ['Austria', 'Belgium', 'Bulgaria', ...],
        'eea_member': [True, True, ...],
        'eu_member': [True, True, ...]
    })
    supabase.table('countries').upsert(countries_df.to_dict('records')).execute()
    
    # 2. CCyB Decisions
    ccyb_df = pd.read_parquet('data/processed_ccyb.parquet')
    # Transform to match schema
    ccyb_records = ccyb_df.to_dict('records')
    supabase.table('ccyb_decisions').upsert(ccyb_records).execute()
    
    # 3. SyRB Measures
    syrb_df = pd.read_parquet('data/processed_syrb.parquet')
    syrb_records = syrb_df.to_dict('records')
    supabase.table('syrb_measures').upsert(syrb_records).execute()
    
    # 4. BBM Measures
    bbm_df = pd.read_parquet('data/processed_bbm.parquet')
    bbm_records = bbm_df.to_dict('records')
    supabase.table('bbm_measures').upsert(bbm_records).execute()
    
    # 5. DTI/LTI Rules
    dti_lti_df = pd.read_csv('data/dti_lti_rules.csv')
    dti_lti_records = dti_lti_df.to_dict('records')
    supabase.table('dti_lti_rules').upsert(dti_lti_records).execute()
    
    # 6. OSII Banks
    osii_df = pd.read_parquet('data/processed_osii.parquet')
    osii_records = osii_df.to_dict('records')
    supabase.table('osii_banks').upsert(osii_records).execute()

def query_from_supabase():
    """Query data from Supabase."""
    
    # Complex query: Countries with multiple active measures
    response = supabase.rpc('get_countries_with_multiple_measures').execute()
    # Or using SQL:
    response = supabase.table('ccyb_decisions') \
        .select('*, countries(country_name)') \
        .eq('rate', 0, invert=True) \
        .execute()
    
    return pd.DataFrame(response.data)

def update_latest_snapshots():
    """Update latest snapshot tables (can be scheduled)."""
    # This can be a PostgreSQL function or Python script
    pass
```

### 1.4 Supabase Automatikus REST API

Supabase automatikusan generál REST API-t:

```javascript
// Frontend-ből közvetlenül elérhető
const { data, error } = await supabase
  .from('ccyb_decisions')
  .select('*, countries(country_name)')
  .eq('country_iso2', 'HU')
  .order('effective_date', { ascending: false })

// Realtime subscription
const subscription = supabase
  .channel('ccyb_changes')
  .on('postgres_changes', 
    { event: 'INSERT', schema: 'public', table: 'ccyb_decisions' },
    (payload) => {
      console.log('New CCyB decision:', payload.new)
    }
  )
  .subscribe()
```

### 1.5 Supabase Ingyenes Tier Korlátok

- **Database size**: 500 MB (jelenlegi adatmennyiséghez elég)
- **Bandwidth**: 2 GB/hó (API hívások)
- **API requests**: Korlátlan (rate limit: 500 req/s)
- **File storage**: 1 GB (ha később Excel exportokat is tárolunk)

**Jelenlegi használat becslés:**
- Adatbázis: ~50-150 MB → ✅ Bőven elég
- Bandwidth: ~100-500 MB/hó (dashboard használat) → ✅ Elég
- API requests: ~1000-5000/hó → ✅ Korlátlan

---

## 2. PySpark Ingyenes Megoldások Később

### 2.1 Opciók

#### A) Local Mode (Legkönnyebb)
```python
# Nincs cluster, csak local Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MacroPolicyHub") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .getOrCreate()

# Supabase-ből olvasás (PostgreSQL JDBC)
ccyb_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db.xxx.supabase.co:5432/postgres") \
    .option("dbtable", "ccyb_decisions") \
    .option("user", "postgres") \
    .option("password", "your-password") \
    .load()

# Vagy parquet fájlokból (ha még mindig használjuk)
ccyb_df = spark.read.parquet("data/processed_ccyb.parquet")
```

**Előnyök:**
- ✅ Ingyenes (nincs cluster)
- ✅ Könnyű setup
- ✅ Parquet fájlok közvetlenül olvashatóak
- ✅ Supabase-ből JDBC-vel olvasható

**Hátrányok:**
- ❌ Nincs distributed processing (csak local)
- ❌ Memória korlátozott (laptop RAM)

#### B) Google Colab (Ingyenes, Cloud-based)
```python
# Colab notebook-ban
!pip install pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MacroPolicyHub") \
    .master("local[*]") \
    .getOrCreate()

# Supabase-ből olvasás
ccyb_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db.xxx.supabase.co:5432/postgres") \
    .option("dbtable", "ccyb_decisions") \
    .load()
```

**Előnyök:**
- ✅ Ingyenes (Google account)
- ✅ Cloud-based (nincs local setup)
- ✅ Jupyter notebook környezet
- ✅ Shareable (notebook megosztás)

**Hátrányok:**
- ❌ Session timeout (12 óra)
- ❌ RAM korlátozott (~12 GB)
- ❌ Nincs persistent storage (session végén törlődik)

#### C) Databricks Community Edition (Ingyenes, Korlátozott)
- **Ingyenes tier**: Single cluster, 15 GB RAM, 2 worker nodes
- **Korlátok**: Idle timeout (2 óra), storage korlátozott
- **Előnyök**: Teljes Databricks platform, notebook sharing
- **Hátrányok**: Korlátozott erőforrások, idle timeout

#### D) Supabase + PySpark Local (Ajánlott Kombináció)

**Workflow:**
1. **Supabase**: Production adatbázis (adatintegritás, REST API)
2. **PySpark Local**: Analytics layer (komplex aggregációk, ML)

```python
# 1. Supabase-ből olvasás PySpark-ból
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MacroPolicyAnalytics") \
    .master("local[*]") \
    .config("spark.jars", "postgresql-42.5.0.jar") \
    .getOrCreate()

# Supabase PostgreSQL-ből olvasás
ccyb_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db.xxx.supabase.co:5432/postgres") \
    .option("dbtable", "ccyb_decisions") \
    .option("user", "postgres") \
    .option("password", "your-password") \
    .load()

# Komplex analízis
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, avg

window = Window.partitionBy("country_iso2").orderBy("effective_date")

trend_analysis = ccyb_df \
    .withColumn("rate_change", col("rate") - lag("rate", 1).over(window)) \
    .groupBy("country_iso2") \
    .agg(avg("rate").alias("avg_rate"), 
         avg("rate_change").alias("avg_change"))

# Eredmények visszaírása Supabase-be (opcionális)
trend_analysis.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db.xxx.supabase.co:5432/postgres") \
    .option("dbtable", "ccyb_trend_analysis") \
    .option("user", "postgres") \
    .option("password", "your-password") \
    .mode("overwrite") \
    .save()
```

---

## 3. Implementációs Roadmap

### Fázis 1: Supabase Setup (1-2 hét)
1. ✅ Supabase projekt létrehozás
2. ✅ Séma létrehozás (SQL script)
3. ✅ Parquet → Supabase migráció script
4. ✅ REST API tesztelés
5. ✅ Dashboard integráció (Supabase client)

### Fázis 2: PySpark Local Demonstráció (opcionális, 2-3 hét)
1. ✅ Local Spark session setup
2. ✅ Supabase JDBC integráció
3. ✅ Komplex aggregációk demonstrálása
4. ✅ ML pipeline (opcionális)

### Fázis 3: Production (ha szükséges)
1. ✅ Supabase paid tier (ha megnő az adatmennyiség)
2. ✅ Databricks Community Edition (ha cluster kell)
3. ✅ Google Colab notebooks (shareable analytics)

---

## 4. Konkrét Használati Esetek

### 4.1 Supabase-ből Dashboard

```python
# Python dashboard (Streamlit/Plotly Dash)
from supabase import create_client
import plotly.express as px

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Országprofil lekérdezés
response = supabase.table('ccyb_decisions') \
    .select('*, countries(country_name)') \
    .eq('country_iso2', 'HU') \
    .order('effective_date', ascending=False) \
    .execute()

df = pd.DataFrame(response.data)
fig = px.line(df, x='effective_date', y='rate', title='Hungary CCyB Trend')
fig.show()
```

### 4.2 PySpark Analytics Supabase-ből

```python
# Komplex trend analízis
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Analytics").getOrCreate()

# Supabase-ből olvasás
ccyb = spark.read.format("jdbc").option(...).load()
syrb = spark.read.format("jdbc").option(...).load()

# Cross-measure analízis
total_buffers = ccyb.join(syrb, ["country_iso2", "effective_date"], "outer") \
    .withColumn("total_buffer", 
                coalesce(col("ccyb_rate"), lit(0)) + 
                coalesce(col("syrb_rate"), lit(0))) \
    .groupBy("country_iso2") \
    .agg(avg("total_buffer").alias("avg_total_buffer"),
         max("total_buffer").alias("max_total_buffer"))

# Eredmények Supabase-be (opcionális)
total_buffers.write.format("jdbc").option(...).save()
```

---

## 5. Összefoglalás

### Supabase: **ERŐSEN AJÁNLOTT** ✅
- ✅ Ingyenes tier elég jelenlegi adatmennyiséghez
- ✅ PostgreSQL-alapú (standard SQL)
- ✅ Automatikus REST API
- ✅ Dashboard/Admin UI
- ✅ Realtime subscriptions (ha kell)

### PySpark: **DEMONSTRÁCIÓS CÉLLAL ÉRDEKES** ⚠️
- ✅ **Local mode**: Ingyenes, könnyű setup
- ✅ **Google Colab**: Ingyenes, cloud-based
- ✅ **Supabase integráció**: JDBC-vel olvasható
- ⚠️ Jelenlegi adatmennyiséghez "overkill", de demonstrációs érték

### Ajánlás: **Supabase → PySpark Local**
1. **Supabase**: Production adatbázis (adatintegritás, REST API)
2. **PySpark Local**: Analytics layer (komplex aggregációk, ML)
3. **Google Colab**: Shareable analytics notebooks

---

## 6. Következő Lépések (ha implementáljuk)

1. Supabase projekt létrehozás
2. Séma SQL script generálás
3. Migrációs script (parquet → Supabase)
4. Dashboard integráció (Supabase client)
5. PySpark demonstráció (opcionális)

**Kérdés**: Szeretnéd, hogy készítsek egy migrációs scriptet Supabase-hez?
