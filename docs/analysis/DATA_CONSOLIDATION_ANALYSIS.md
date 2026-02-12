# Adatkonszolidáció Elemzés: Relációs Adatbázis vs. PySpark

## Jelenlegi Adatstruktúra Áttekintése

### Parquet Fájlok
1. **processed_ccyb.parquet** - CCyB teljes időszoros adatok
2. **processed_syrb.parquet** - SyRB teljes időszoros adatok  
3. **processed_bbm.parquet** - BBM (Borrower-Based Measures) teljes adatok
4. **processed_osii.parquet** - OSII/GSII bank-specifikus adatok
5. **latest_ccyb.parquet** - Legfrissebb CCyB snapshot
6. **latest_syrb.parquet** - Legfrissebb SyRB snapshot
7. **latest_osii.parquet** - Legfrissebb OSII snapshot
8. **agg_trend.parquet** - Agregált trend adatok
9. **latest_country.parquet** - Ország-szintű snapshot

### További Strukturált Adatok
- **dti_lti_rules.csv** - DTI/LTI szabályok strukturált formátumban
- **validation_report.json** - AI validációs jelentések
- Excel export fájlok (diffusion, decisions, snapshot, stb.)

### Főbb Relációk
- **Kulcs mezők**: `country`, `iso2`, `date`
- **Időszoros adatok**: CCyB, SyRB (több dátum/verzió)
- **Snapshot adatok**: Latest verziók országonként
- **Bank-specifikus**: OSII/GSII (bank neve, LEI kód)
- **Mértékek**: BBM (LTV, DTI, LTI, DSTI típusok)

---

## 1. Relációs Adatbázis Opció (PostgreSQL/SQLite)

### ✅ Előnyök

#### 1.1 Adatintegritás és Normalizálás
- **Foreign Key Constraints**: Országok, dátumok, bankok közötti relációk garantálása
- **Normalizált séma**: Duplikációk elkerülése (pl. ország nevek, ISO kódok)
- **ACID tranzakciók**: Konzisztens adatállapot garantálása
- **Adatvalidáció**: Check constraints (pl. rate 0-20% között, dátumok logikus sorrendben)

**Példa séma:**
```sql
CREATE TABLE countries (
    iso2 CHAR(2) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50)
);

CREATE TABLE ccyb_decisions (
    id SERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2),
    effective_date DATE NOT NULL,
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 20),
    credit_gap DECIMAL(5,2),
    justification TEXT,
    decision_date DATE,
    UNIQUE(country_iso2, effective_date)
);

CREATE TABLE syrb_measures (
    id SERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2),
    measure_type VARCHAR(50),
    sector VARCHAR(50),
    rate DECIMAL(5,2),
    effective_date DATE,
    status VARCHAR(20)
);
```

#### 1.2 Komplex Lekérdezések
- **JOIN műveletek**: Országok, dátumok, mértékek közötti összekapcsolások
- **Aggregációk**: Window functions, GROUP BY, HAVING
- **Időszoros analízis**: LAG/LEAD functions, időszoros trendek
- **Subquery-k**: Komplex logika SQL-ben

**Példa lekérdezés:**
```sql
-- Országok, ahol CCyB és SyRB is aktív
SELECT c.country_name, 
       ccyb.rate as ccyb_rate,
       syrb.rate as syrb_rate,
       (ccyb.rate + syrb.rate) as total_buffer
FROM countries c
JOIN latest_ccyb ccyb ON c.iso2 = ccyb.country_iso2
JOIN latest_syrb syrb ON c.iso2 = syrb.country_iso2
WHERE ccyb.rate > 0 AND syrb.rate > 0;
```

#### 1.3 Adatbázis-szintű Indexelés
- **Performance**: Gyors keresés ország, dátum, bank alapján
- **Composite indexes**: (country, date), (country, measure_type)
- **Full-text search**: Justification mezőkben keresés

#### 1.4 Standardizált API
- **SQL**: Univerzális nyelv, könnyen tanulható
- **ODBC/JDBC**: Bármilyen tool-ból elérhető (Excel, Power BI, Tableau)
- **REST API**: Flask/FastAPI wrapper könnyen implementálható

#### 1.5 Backup és Recovery
- **Point-in-time recovery**: Adatvesztés esetén visszaállítás
- **Replication**: Master-slave setup production környezetben
- **Versioning**: Adatverziók követése (pl. temporal tables)

### ❌ Hátrányok

#### 1.1 Skálázhatóság
- **Vertikális skálázás**: Nagy adatmennyiség esetén drága hardware szükséges
- **Horizontális skálázás**: Nehezebb (sharding komplexitás)
- **Concurrent writes**: Több felhasználó írása bottleneck lehet

#### 1.2 Adatmennyiség
- **Jelenlegi méret**: ~10-50 MB parquet fájlok → kis adatbázis
- **Jövőbeli növekedés**: Ha több év adat, több ország → még mindig kicsi (<1 GB)
- **Banki kontextus**: Nagy bankoknál ez még mindig "small data"

#### 1.3 Komplexitás
- **Setup overhead**: Adatbázis szerver telepítés, karbantartás
- **Migration**: Parquet → SQL migráció scriptek
- **Schema changes**: ALTER TABLE műveletek production-ben

---

## 2. PySpark Opció (Apache Spark)

### ✅ Előnyök

#### 2.1 Banki Kontextusban "Hot Topic"
- **Big Data platform**: Bankoknál standard tool (regulációs reporting, risk analytics)
- **Demonstrációs érték**: Modern, skálázható technológia mutatása
- **Karrier relevancia**: Spark skills keresett a pénzügyi szektorban
- **Ecosystem**: Integráció más big data toolokkal (Hadoop, Delta Lake, Databricks)

#### 2.2 Skálázhatóság
- **Horizontális skálázás**: Automatikus partíciók kezelése
- **Distributed processing**: Több node-on párhuzamos feldolgozás
- **Memory management**: Optimalizált memória használat
- **Future-proof**: Ha később nagyobb adatmennyiség → már készen áll

#### 2.3 Parquet Integráció
- **Native support**: Parquet fájlok közvetlenül olvashatóak
- **Schema evolution**: Parquet schema változások kezelése
- **Columnar storage**: Optimalizált olvasás (csak szükséges oszlopok)

**Példa kód:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

spark = SparkSession.builder.appName("MacroPolicyHub").getOrCreate()

# Parquet fájlok közvetlenül olvashatóak
ccyb_df = spark.read.parquet("data/processed_ccyb.parquet")
syrb_df = spark.read.parquet("data/processed_syrb.parquet")
bbm_df = spark.read.parquet("data/processed_bbm.parquet")

# Komplex aggregációk
total_buffers = ccyb_df.join(syrb_df, ["country_iso2", "date"], "outer") \
    .withColumn("total_buffer", 
                when(col("ccyb_rate").isNotNull(), col("ccyb_rate")).otherwise(0) +
                when(col("syrb_rate").isNotNull(), col("syrb_rate")).otherwise(0)) \
    .groupBy("country_iso2") \
    .agg(spark_sum("total_buffer").alias("cumulative_buffer"))
```

#### 2.4 Advanced Analytics
- **Window functions**: Időszoros trendek, moving averages
- **Machine Learning**: MLlib integráció (pl. rate prediction models)
- **Streaming**: Ha később real-time adatok → Spark Streaming
- **Graph processing**: GraphX a knowledge graph analízishez

#### 2.5 Data Lake Pattern
- **Delta Lake**: ACID tranzakciók + time travel (adathistória)
- **Data versioning**: Adatverziók követése
- **Schema enforcement**: Adatintegritás garantálása

### ❌ Hátrányok

#### 2.1 Overhead Kis Adatmennyiséghez
- **Jelenlegi méret**: ~10-50 MB → Spark "overkill"
- **Startup time**: Spark session indítás lassabb, mint pandas
- **Memory footprint**: Több memória, mint pandas (distributed overhead)
- **Complexity**: Learning curve, több konfiguráció

#### 2.2 Infrastruktúra
- **Local mode**: Jelenleg elég, de korlátozott
- **Cluster setup**: Production-ben cluster szükséges (költség, komplexitás)
- **Resource management**: YARN, Mesos, Kubernetes integráció

#### 2.3 Development Experience
- **Debugging**: Nehezebb, mint pandas (distributed execution)
- **Testing**: Local vs. cluster különbségek
- **IDE support**: Kevesebb tool, mint SQL adatbázisokhoz

---

## 3. Összehasonlító Táblázat

| Kritérium | Relációs DB (PostgreSQL) | PySpark | Jelenlegi (Parquet) |
|-----------|-------------------------|---------|---------------------|
| **Adatintegritás** | ⭐⭐⭐⭐⭐ (FK, constraints) | ⭐⭐⭐ (schema enforcement) | ⭐⭐ (nincs validáció) |
| **Komplex lekérdezések** | ⭐⭐⭐⭐⭐ (SQL) | ⭐⭐⭐⭐ (DataFrame API) | ⭐⭐ (pandas merge) |
| **Skálázhatóság** | ⭐⭐⭐ (vertikális) | ⭐⭐⭐⭐⭐ (horizontális) | ⭐⭐⭐ (fájlok) |
| **Setup komplexitás** | ⭐⭐⭐ (szerver kell) | ⭐⭐ (local mode egyszerű) | ⭐⭐⭐⭐⭐ (nincs) |
| **Banki relevancia** | ⭐⭐⭐⭐ (standard) | ⭐⭐⭐⭐⭐ (hot topic) | ⭐⭐ (basic) |
| **Performance (kis adat)** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Learning curve** | ⭐⭐⭐⭐ (SQL ismert) | ⭐⭐ (új framework) | ⭐⭐⭐⭐⭐ (pandas) |
| **Ecosystem** | ⭐⭐⭐⭐⭐ (ODBC, BI tools) | ⭐⭐⭐⭐ (ML, streaming) | ⭐⭐⭐ (Python) |

---

## 4. Ajánlások

### 4.1 Rövid távú (1-3 hónap)
**Javaslat: Relációs Adatbázis (SQLite → PostgreSQL)**

**Indokok:**
- ✅ **Kis overhead**: SQLite file-based, nincs szerver szükséges
- ✅ **Gyors implementáció**: Parquet → SQL migráció egyszerű
- ✅ **Adatintegritás**: FK constraints, check constraints
- ✅ **Komplex lekérdezések**: SQL-ben könnyebb, mint pandas merge-ek
- ✅ **Standard toolok**: Excel, Power BI, Tableau integráció
- ✅ **Demonstrációs érték**: "Production-ready" adatstruktúra

**Implementáció:**
```python
# SQLite starter (később PostgreSQL-re migrálható)
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/macro_policy.db')

# Parquet → SQL migráció
ccyb_df = pd.read_parquet('data/processed_ccyb.parquet')
ccyb_df.to_sql('ccyb_decisions', conn, if_exists='replace', index=False)

# Foreign key constraints, indexes
conn.execute('''
    CREATE INDEX idx_ccyb_country_date ON ccyb_decisions(country_iso2, effective_date);
''')
```

### 4.2 Közép távú (3-6 hónap)
**Javaslat: PySpark Demonstráció**

**Indokok:**
- ✅ **Banki relevancia**: Modern, skálázható technológia mutatása
- ✅ **Future-proof**: Ha később nagyobb adat → már készen áll
- ✅ **Parquet kompatibilitás**: Jelenlegi fájlok közvetlenül használhatóak
- ✅ **Advanced analytics**: ML, streaming lehetőségek

**Implementáció:**
```python
# PySpark local mode (később cluster-re skálázható)
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MacroPolicyHub") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .getOrCreate()

# Parquet fájlok közvetlenül olvashatóak
ccyb_df = spark.read.parquet("data/processed_ccyb.parquet")
syrb_df = spark.read.parquet("data/processed_syrb.parquet")

# Komplex analízis
total_buffers = ccyb_df.join(syrb_df, ["country_iso2", "date"], "outer") \
    .groupBy("country_iso2") \
    .agg({"ccyb_rate": "sum", "syrb_rate": "sum"})
```

### 4.3 Hosszú távú (6+ hónap)
**Javaslat: Hibrid Megoldás**

- **PostgreSQL**: Production adatbázis (adatintegritás, komplex lekérdezések)
- **PySpark**: Analytics layer (ML, advanced aggregációk, streaming)
- **Delta Lake**: Adatverziók, time travel (ha szükséges)

---

## 5. Konkrét Használati Esetek

### 5.1 Relációs Adatbázis Használati Esetek

1. **Országprofilok generálása**
   ```sql
   -- Összes aktív intézkedés egy országban
   SELECT 
       c.country_name,
       ccyb.rate as ccyb_rate,
       syrb.rate as syrb_rate,
       bbm.measure_type,
       osii.bank_name
   FROM countries c
   LEFT JOIN latest_ccyb ccyb ON c.iso2 = ccyb.country_iso2
   LEFT JOIN latest_syrb syrb ON c.iso2 = syrb.country_iso2
   LEFT JOIN active_bbm bbm ON c.iso2 = bbm.country_iso2
   LEFT JOIN latest_osii osii ON c.iso2 = osii.country_iso2
   WHERE c.iso2 = 'HU';
   ```

2. **Trend analízis**
   ```sql
   -- CCyB diffúzió időben
   SELECT 
       date,
       COUNT(*) FILTER (WHERE rate > 0) as countries_with_buffer,
       AVG(rate) as avg_rate
   FROM ccyb_decisions
   GROUP BY date
   ORDER BY date;
   ```

3. **Cross-measure analízis**
   ```sql
   -- Országok, ahol több intézkedés is aktív
   SELECT 
       country_iso2,
       COUNT(DISTINCT 'ccyb') as has_ccyb,
       COUNT(DISTINCT 'syrb') as has_syrb,
       COUNT(DISTINCT 'bbm') as has_bbm
   FROM (
       SELECT country_iso2, 'ccyb' as measure FROM latest_ccyb WHERE rate > 0
       UNION ALL
       SELECT country_iso2, 'syrb' FROM latest_syrb WHERE rate > 0
       UNION ALL
       SELECT country_iso2, 'bbm' FROM active_bbm
   ) measures
   GROUP BY country_iso2
   HAVING COUNT(DISTINCT measure) >= 2;
   ```

### 5.2 PySpark Használati Esetek

1. **Window functions időszoros trendekhez**
   ```python
   from pyspark.sql.window import Window
   from pyspark.sql.functions import lag, col

   window = Window.partitionBy("country_iso2").orderBy("date")
   
   ccyb_with_change = ccyb_df.withColumn(
       "rate_change",
       col("rate") - lag("rate", 1).over(window)
   )
   ```

2. **Machine Learning (rate prediction)**
   ```python
   from pyspark.ml.regression import LinearRegression
   from pyspark.ml.feature import VectorAssembler

   # Feature engineering
   assembler = VectorAssembler(
       inputCols=["credit_gap", "credit_to_gdp", "lag_rate"],
       outputCol="features"
   )
   
   # Model training
   lr = LinearRegression(featuresCol="features", labelCol="rate")
   model = lr.fit(training_data)
   ```

3. **Streaming (ha később real-time adatok)**
   ```python
   # Ha később API-ból jönnek az adatok
   streaming_df = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "localhost:9092") \
       .load()
   ```

---

## 6. Migrációs Stratégia

### Fázis 1: SQLite Prototípus (1-2 hét)
1. Parquet fájlok → SQLite táblák
2. Foreign key constraints, indexes
3. Alap SQL lekérdezések tesztelése
4. Dashboard integráció (SQLite → pandas)

### Fázis 2: PostgreSQL Production (2-4 hét)
1. SQLite → PostgreSQL migráció
2. User management, permissions
3. Backup stratégia
4. Performance tuning (indexes, query optimization)

### Fázis 3: PySpark Demonstráció (opcionális, 4-6 hét)
1. Local Spark session setup
2. Parquet fájlok olvasása Spark-ból
3. Komplex aggregációk demonstrálása
4. ML pipeline (opcionális)

---

## 7. Következtetés

### Relációs Adatbázis: **ERŐSEN AJÁNLOTT** ✅
- **Kis overhead, nagy haszon**: SQLite → PostgreSQL path
- **Adatintegritás**: FK constraints, validáció
- **Komplex lekérdezések**: SQL-ben könnyebb, mint pandas
- **Standard toolok**: Excel, Power BI, Tableau integráció
- **Production-ready**: Demonstrációs érték

### PySpark: **DEMONSTRÁCIÓS CÉLLAL ÉRDEKES** ⚠️
- **Banki relevancia**: Hot topic, modern technológia
- **Overhead kis adathoz**: Jelenlegi méret esetén "overkill"
- **Future-proof**: Ha később nagyobb adat → már készen áll
- **Parquet kompatibilitás**: Jelenlegi fájlok közvetlenül használhatóak

### Ajánlás: **SQLite → PostgreSQL path**
1. **Kezdés SQLite-tel** (file-based, nincs szerver)
2. **Production-ben PostgreSQL** (ha szükséges)
3. **PySpark demonstráció** (opcionális, ha van idő/igény)

---

## 8. Implementációs Lépések (SQLite Starter)

```python
# 1. Migrációs script
import sqlite3
import pandas as pd
from pathlib import Path

def migrate_to_sqlite():
    conn = sqlite3.connect('data/macro_policy.db')
    
    # Countries lookup table
    countries_df = pd.DataFrame({
        'iso2': ['AT', 'BE', 'BG', ...],
        'country_name': ['Austria', 'Belgium', 'Bulgaria', ...]
    })
    countries_df.to_sql('countries', conn, if_exists='replace', index=False)
    
    # CCyB decisions
    ccyb_df = pd.read_parquet('data/processed_ccyb.parquet')
    ccyb_df.to_sql('ccyb_decisions', conn, if_exists='replace', index=False)
    
    # SyRB measures
    syrb_df = pd.read_parquet('data/processed_syrb.parquet')
    syrb_df.to_sql('syrb_measures', conn, if_exists='replace', index=False)
    
    # BBM measures
    bbm_df = pd.read_parquet('data/processed_bbm.parquet')
    bbm_df.to_sql('bbm_measures', conn, if_exists='replace', index=False)
    
    # OSII banks
    osii_df = pd.read_parquet('data/processed_osii.parquet')
    osii_df.to_sql('osii_banks', conn, if_exists='replace', index=False)
    
    # Indexes
    conn.execute('CREATE INDEX idx_ccyb_country_date ON ccyb_decisions(country_iso2, effective_date);')
    conn.execute('CREATE INDEX idx_syrb_country ON syrb_measures(country_iso2);')
    conn.execute('CREATE INDEX idx_bbm_country ON bbm_measures(country_iso2);')
    
    conn.close()
```

---

**Összefoglalás**: Relációs adatbázis (SQLite → PostgreSQL) **erősen ajánlott** a jelenlegi adatstruktúrához. PySpark **demonstrációs célból érdekes**, de jelenlegi adatmennyiséghez "overkill". SQLite-tel kezdés, majd PostgreSQL-re migrálás production-ben optimális út.
