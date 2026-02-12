# 🔧 Projekt Refaktorálási Terv

## 📊 Jelenlegi Állapot Elemzése

### Főbb Problémák

1. **Duplikáció és Elavult Fájlok**
2. **Dokumentáció Túlsúly**
3. **Kód Duplikáció**
4. **Nem Használt Scriptek**

---

## 🗑️ 1. Törölhető/Elavult Fájlok

### 1.1. Elavult Mappák

- ✅ **`supabase_migration/`** - Elavult, mert:
  - Most már `migrations/` SQL fájlok vannak
  - `scripts/run_migrations.py` és `scripts/run_migrations_cli.py` használatos
  - **Művelet:** Törlés vagy `archive/` mappába mozgatás

- ✅ **`output/`** - Lehet elavult:
  - 4 PNG fájl van benne
  - Ellenőrizni kell, hogy használják-e
  - **Művelet:** Ellenőrzés után törlés vagy `archive/` mappába mozgatás

### 1.2. Elavult Scriptek

- ✅ **`run_all.R`** - R script, elavult:
  - Most már Python pipeline van (`main.py` → `pipeline/orchestrator.py`)
  - **Művelet:** Törlés vagy `archive/` mappába mozgatás

- ✅ **`scripts/run_supabase_migration.py`** - Lehet elavult:
  - Most már `scripts/run_migrations.py` és `scripts/run_migrations_cli.py` van
  - **Művelet:** Ellenőrzés után törlés

### 1.3. Test Scriptek Csoportosítása

A `scripts/` mappában sok test script van. Javaslat: **`scripts/tests/`** mappába mozgatás:

```
scripts/
├── tests/                    # Test scriptek
│   ├── test_bbm_dti_lti.py
│   ├── test_complete_supabase_render.py
│   ├── test_dti_lti_extraction.py
│   ├── test_full_pipeline_dti_lti.py
│   ├── test_supabase_api.py
│   ├── test_supabase_render.py
│   ├── test_ai_validation_dk.py
│   └── ...
├── run_migrations.py        # Production scriptek
├── run_migrations_cli.py
└── etl_process.py
```

---

## 📁 2. Kód Duplikáció és Refaktorálás

### 2.1. `bbm.py` vs `bbm/` Mappa - ⚠️ KRITIKUS

**Probléma:**
- `bbm.py` (497 sor) tartalmaz függvényeket, amiket a `bbm/` mappában lévő modulok használnak
- Circular import problémák (`bbm_stage.py` különleges import logikát használ)
- Duplikáció: `extract_ltv_details_regex` mindkét helyen van

**Javaslat:**
1. **Mozgasd át a `bbm.py` függvényeket a `bbm/` mappába:**
   - `extract_ltv_details_regex` → `bbm/extractors/ltv_extractor.py` (már van!)
   - `build_bbm_matrix_html` → `bbm/matrix_builder.py` (ÚJ)
   - `build_dti_lti_items` → `bbm/dti_lti/items_builder.py` (ÚJ)
   - `build_dti_lti_comparison_df` → `bbm/dti_lti/comparison_builder.py` (már van `dti_lti_builder.py`!)
   - `build_dti_lti_eu_list_html` → `bbm/dti_lti/list_builder.py` (ÚJ)

2. **Frissítsd az importokat:**
   - `pipeline/stages/bbm_stage.py` - használja a `bbm/` modulokat
   - `scripts/test_*.py` - használja a `bbm/` modulokat

3. **Töröld a `bbm.py` fájlt** (vagy tartsd meg backward compatibility-ért, de deprecated-ként)

### 2.2. `utils.py` vs `utils/` Mappa

**Jelenlegi állapot:**
- `utils.py` - 92 sor, helper függvények
- `utils/` - 3 fájl: `json_parser.py`, `retry.py`, `__init__.py`

**Javaslat:**
1. **Mozgasd át a `utils.py` függvényeket a `utils/` mappába:**
   - `SuppressOutput` → `utils/output.py` (ÚJ)
   - `ensure_dirs` → `utils/paths.py` (ÚJ)
   - `download_file_safely` → `utils/download.py` (ÚJ)
   - `clean_columns`, `find_header_row`, `extract_rate` → `utils/dataframe.py` (ÚJ)
   - `create_download_link` → `utils/html.py` (ÚJ)

2. **Frissítsd az importokat:**
   - `from utils import ensure_dirs` → `from utils.paths import ensure_dirs`

3. **Töröld a `utils.py` fájlt** (vagy deprecated-ként)

### 2.3. Dokumentáció Konszolidálás

**43 markdown fájl a `docs/` mappában!**

**Javaslat: Csoportosítás:**

```
docs/
├── README.md                          # Fő dokumentáció
├── guides/                            # Útmutatók
│   ├── SUPABASE_CLI_QUICK_START.md
│   ├── SUPABASE_CLI_SETUP.md
│   ├── MIGRATION_RUNNER.md
│   ├── MIGRATION_STEPS.md
│   └── ...
├── deployment/                        # Deploy dokumentáció
│   ├── DEPLOY_STATUS.md
│   ├── DEPLOY_SUMMARY.md
│   ├── DEPLOYMENT_STEPS.md
│   ├── TESTING_CHECKLIST.md
│   └── ...
├── architecture/                     # Architektúra dokumentáció
│   ├── MATERIALIZED_VIEWS_MIGRATION.md
│   ├── SUPABASE_RENDER_IMPLEMENTATION.md
│   ├── SUPABASE_FRONTEND_INTEGRATION.md
│   └── ...
├── analysis/                          # Elemzések (régi)
│   ├── SUPABASE_RAG_ANALYSIS.md
│   ├── DATA_CONSOLIDATION_ANALYSIS.md
│   ├── GEMINI_COST_ANALYSIS.md
│   └── ...
└── archive/                           # Elavult dokumentáció
    ├── CONSOLIDATED_ROADMAP_2024.md
    ├── CONSOLIDATED_ROADMAP_2026.md
    ├── DEVELOPMENT_ROADMAP.md
    ├── DEVELOPMENT_ROADMAP_V2.md
    └── ...
```

**Vagy törlés:**
- Régi roadmap fájlok (2024, 2026) - ha már nem relevánsak
- Duplikált analysis fájlok

---

## 🔄 3. Refaktorálási Prioritások

### ⭐⭐⭐ KRITIKUS (Azonnal)

1. **`bbm.py` refaktorálás**
   - Circular import problémák megoldása
   - Kód duplikáció eltávolítása
   - **Becsült idő:** 2-3 óra

2. **`utils.py` refaktorálás**
   - Modulokra bontás
   - Import frissítések
   - **Becsült idő:** 1-2 óra

### ⭐⭐ FONTOS (Rövid távon)

3. **Dokumentáció konszolidálás**
   - Csoportosítás
   - Elavult fájlok archiválása
   - **Becsült idő:** 1-2 óra

4. **Test scriptek csoportosítása**
   - `scripts/tests/` mappába mozgatás
   - **Becsült idő:** 30 perc

### ⭐ KÖZEPES (Hosszú távon)

5. **Elavult fájlok törlése**
   - `supabase_migration/` mappa
   - `run_all.R`
   - `output/` mappa (ha elavult)
   - **Becsült idő:** 30 perc

---

## 📋 4. Implementációs Lépések

### Fázis 1: Elavult Fájlok Archiválása

1. Hozz létre egy `archive/` mappát
2. Mozgasd ide:
   - `supabase_migration/`
   - `run_all.R`
   - `output/` (ha elavult)
3. Commit: `chore: Archive deprecated files`

### Fázis 2: `bbm.py` Refaktorálás

1. Hozz létre hiányzó modulokat a `bbm/` mappában
2. Mozgasd át a függvényeket
3. Frissítsd az importokat
4. Teszteld
5. Töröld a `bbm.py` fájlt (vagy deprecated-ként)
6. Commit: `refactor: Move bbm.py functions to bbm/ package`

### Fázis 3: `utils.py` Refaktorálás

1. Hozz létre új modulokat a `utils/` mappában
2. Mozgasd át a függvényeket
3. Frissítsd az importokat
4. Teszteld
5. Töröld a `utils.py` fájlt (vagy deprecated-ként)
6. Commit: `refactor: Move utils.py functions to utils/ package`

### Fázis 4: Dokumentáció Konszolidálás

1. Hozz létre a `docs/guides/`, `docs/deployment/`, stb. mappákat
2. Mozgasd át a fájlokat
3. Frissítsd a linkeket (ha vannak)
4. Commit: `docs: Reorganize documentation structure`

### Fázis 5: Test Scriptek Csoportosítása

1. Hozz létre `scripts/tests/` mappát
2. Mozgasd át a test scripteket
3. Commit: `chore: Organize test scripts`

---

## ✅ Várható Eredmények

- ✅ **-20-30% fájl duplikáció**
- ✅ **Tisztább projekt struktúra**
- ✅ **Könnyebb karbantarthatóság**
- ✅ **Jobb import struktúra**
- ✅ **Kisebb dokumentáció túlsúly**

---

## ⚠️ Figyelmeztetések

1. **Backward Compatibility:**
   - Ha van külső függőség a `bbm.py`-ra vagy `utils.py`-ra, tartsd meg deprecated-ként
   - Vagy hozz létre `__init__.py` fájlokat, amik importálják a régi helyekről

2. **Tesztelés:**
   - Minden refaktorálás után futtasd a teljes pipeline-t
   - Ellenőrizd, hogy nincs broken import

3. **Git History:**
   - Használj `git mv` parancsokat, hogy megtartsd a fájl history-t
   - Commit-ok legyenek logikusak és kicsik
