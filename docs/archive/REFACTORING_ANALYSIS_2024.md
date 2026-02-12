# 🔧 Refaktorálási Elemzés és Egyszerűsítési Javaslatok

**Dátum:** 2024 Q4  
**Cél:** Projekt áttekintése, refaktorálási lehetőségek azonosítása

---

## 📊 Projekt Áttekintés

### Fájl Struktúra
- **Python fájlok:** ~68 fájl
- **Legnagyobb fájlok:**
  - `etl.py`: ~594 sor
  - `llm_analysis.py`: ~1135 sor
  - `bbm.py`: ~497 sor
  - `grounding_validator.py`: ~368 sor
  - `pipeline/orchestrator.py`: ~167 sor

### Jelenlegi Architektúra
- ✅ **Stage-based pipeline** (orchestrator.py) - **JÓ**
- ✅ **BBM modulok szétbontva** (dti_lti, ltv) - **JÓ**
- ✅ **Supabase integráció** - **JÓ**
- ⚠️ **ETL pipeline monolitikus** - **JAVÍTANDÓ**
- ⚠️ **LLM analyzer túl nagy** - **JAVÍTANDÓ**
- ⚠️ **Kód duplikáció** - **JAVÍTANDÓ**

---

## 🎯 Prioritásos Refaktorálási Javaslatok

### 1. ⭐⭐⭐ **ETL Pipeline Modularizálás** (KRITIKUS)

**Jelenlegi probléma:**
- `etl.py` (594 sor) tartalmazza minden extractor logikát
- Rate extraction duplikálva (`_extract_rate_from_text`, `_extract_max_rate_from_text`)
- Nincs konzisztens error handling
- O-SII parsing logika komplex és nehezen karbantartható

**Javasolt struktúra:**
```
etl/
├── __init__.py
├── pipeline.py              # Fő ETLPipeline orchestrator
├── extractors/
│   ├── base_extractor.py    # Abstract base class
│   ├── ccyb_extractor.py    # CCyB specifikus logika
│   ├── syrb_extractor.py    # SyRB specifikus logika
│   ├── bbm_extractor.py     # BBM specifikus logika
│   └── osii_extractor.py    # O-SII specifikus logika
├── parsers/
│   ├── rate_parser.py       # Unifikált rate extraction
│   ├── date_parser.py       # Date parsing logika
│   └── country_parser.py    # Country/ISO2 conversion
└── utils/
    ├── excel_utils.py        # Excel file handling
    └── validation.py        # Data validation
```

**Előnyök:**
- ✅ Kisebb, fókuszált modulok
- ✅ Könnyebb tesztelhetőség
- ✅ Rate parser unifikálás (nincs duplikáció)
- ✅ Jobb error handling

**Implementáció lépések:**
1. `etl/parsers/rate_parser.py` létrehozása (unifikált rate extraction)
2. `etl/extractors/ccyb_extractor.py` kiszervezése
3. `etl/extractors/syrb_extractor.py` kiszervezése
4. `etl/extractors/osii_extractor.py` kiszervezése
5. `etl/pipeline.py` refaktorálása (orchestrator pattern)

**Becsült idő:** 2-3 nap

---

### 2. ⭐⭐⭐ **LLM Analyzer Modularizálás** (KRITIKUS)

**Jelenlegi probléma:**
- `llm_analysis.py` (1135 sor) tartalmazza minden extractor logikát
- Nincs caching az LLM hívásokhoz
- Temperature és retry logika szétszórt
- `_clean_text` logika komplex és duplikált

**Javasolt struktúra:**
```
llm/
├── __init__.py
├── analyzer.py              # Fő LLMAnalyzer osztály
├── cache.py                 # LLM response caching (ÚJ)
├── client.py                # LLM client factory
├── extractors/
│   ├── keyword_extractor.py
│   ├── rate_extractor.py
│   ├── tag_classifier.py
│   ├── ltv_extractor.py
│   └── dti_lti_verifier.py  # confirm_dti_lti_presence
├── formatters/
│   └── text_cleaner.py      # _clean_text logika
└── summarizers/
    └── text_summarizer.py   # summarize_text (HIÁNYZÓ!)
```

**Előnyök:**
- ✅ Kisebb, fókuszált modulok
- ✅ LLM cache (költségcsökkentés)
- ✅ Könnyebb tesztelhetőség
- ✅ Retry logika centralizálva

**Implementáció lépések:**
1. `llm/cache.py` létrehozása (file-based cache)
2. `llm/formatters/text_cleaner.py` kiszervezése
3. `llm/extractors/` modulok kiszervezése
4. `llm/analyzer.py` refaktorálása (orchestrator pattern)

**Becsült idő:** 2-3 nap

---

### 3. ⭐⭐ **BBM Extractor Duplikáció Eltávolítása** (FONTOS)

**Jelenlegi probléma:**
- `bbm/dti_lti_extractor.py` és `bbm/ltv_extractor.py` hasonló regex logikát használnak
- Rate/limit extraction duplikálva
- Nincs közös base class

**Javasolt struktúra:**
```
bbm/
├── __init__.py
├── extractors/
│   ├── base_extractor.py    # Abstract base class
│   ├── dti_lti_extractor.py
│   └── ltv_extractor.py
├── parsers/
│   ├── limit_parser.py      # Unifikált limit extraction (regex)
│   └── regulation_parser.py  # URL extraction
└── ... (existing files)
```

**Előnyök:**
- ✅ Nincs duplikáció
- ✅ Könnyebb karbantartás
- ✅ Konzisztens regex patterns

**Becsült idő:** 1 nap

---

### 4. ⭐⭐ **Error Handling Unifikálás** (FONTOS)

**Jelenlegi probléma:**
- Retry logika szétszórt (`llm_runner.py`, `grounding_validator.py`)
- Nincs exponential backoff
- Nincs rate limiting kezelés
- Nincs strukturált error handling

**Javasolt struktúra:**
```
utils/
├── retry.py                 # Retry decorator (exponential backoff)
├── rate_limiter.py          # Rate limiting (API calls)
└── error_handler.py         # Centralized error handling
```

**Implementáció:**
```python
# utils/retry.py
from functools import wraps
import time
import logging

def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: tuple = (Exception,)
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1}/{max_retries} for {func.__name__}: {e}")
                    time.sleep(min(delay, max_delay))
                    delay *= exponential_base
            return None
        return wrapper
    return decorator
```

**Becsült idő:** 1 nap

---

### 5. ⭐ **LLM Cache Implementáció** (KÖZEPES)

**Jelenlegi probléma:**
- Minden pipeline run újra hívja az LLM-et
- Ugyanazokra a promptokra újra válaszol
- Felesleges API költség

**Javasolt megoldás:**
```python
# llm/cache.py
import hashlib
import json
from pathlib import Path
from typing import Optional

class LLMCache:
    def __init__(self, cache_dir: Path = Path("cache/llm")):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_cache_key(self, prompt: str, model: str, temperature: float) -> str:
        """Generate cache key from prompt, model, and temperature."""
        content = f"{prompt}|{model}|{temperature}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, prompt: str, model: str, temperature: float) -> Optional[str]:
        """Get cached response if exists."""
        cache_key = self._get_cache_key(prompt, model, temperature)
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                return json.load(f).get('response')
        return None
    
    def set(self, prompt: str, model: str, temperature: float, response: str):
        """Cache response."""
        cache_key = self._get_cache_key(prompt, model, temperature)
        cache_file = self.cache_dir / f"{cache_key}.json"
        with open(cache_file, 'w') as f:
            json.dump({'response': response, 'prompt': prompt}, f)
```

**Előnyök:**
- ✅ Költségcsökkentés (50-70% API hívások csökkenése)
- ✅ Gyorsabb pipeline futás
- ✅ Offline tesztelés lehetősége

**Becsült idő:** 1 nap

---

### 6. ⭐ **Grounding Validator Optimalizáció** (KÖZEPES)

**Jelenlegi probléma:**
- Minden claim külön LLM hívást csinál
- Nincs batch verification
- Search result caching hiányzik
- Nincs claim prioritás

**Javasolt megoldás:**
```python
# grounding/claim_processors/verifier.py
class BatchClaimVerifier:
    def verify_batch(self, claims: List[Dict], context: str) -> List[Dict]:
        """Verify multiple claims in a single LLM call."""
        prompt = f"""Verify the following claims using the context.
        Return JSON array with verdict for each claim.
        
        CLAIMS:
        {json.dumps(claims, indent=2)}
        
        CONTEXT:
        {context}
        """
        # Single LLM call for batch
        pass

# grounding/search/cache.py
class SearchCache:
    """Cache Google Search results to avoid redundant API calls."""
    def __init__(self, cache_dir: Path = Path("cache/search")):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, query: str) -> Optional[List[Dict]]:
        # Check cache
        pass
    
    def set(self, query: str, results: List[Dict]):
        # Save to cache
        pass
```

**Előnyök:**
- ✅ Kevesebb LLM hívás (batch processing)
- ✅ Kevesebb search API hívás (caching)
- ✅ Gyorsabb validation

**Becsült idő:** 1-2 nap

---

### 7. ⭐ **Konfiguráció Centralizálás** (KÖZEPES)

**Jelenlegi probléma:**
- Konfiguráció szétszórt (`config.py`, environment variables)
- Nincs validáció
- Nincs default értékek dokumentációja

**Javasolt megoldás:**
```python
# config/settings.py
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass
class Settings:
    # Data paths
    data_dir: Path = Path("data")
    figures_dir: Path = Path("figures")
    reports_dir: Path = Path("reports")
    
    # LLM config
    model_name: str = "gemini-2.5-flash-lite"
    max_output_tokens: int = 2000
    temperature: float = 0.2
    
    # Supabase config
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_key: str = os.getenv("SUPABASE_KEY", "")
    
    def validate(self):
        """Validate settings."""
        if not self.data_dir.exists():
            raise ValueError(f"Data directory not found: {self.data_dir}")
        # ... more validation

# Singleton pattern
_settings = None

def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.validate()
    return _settings
```

**Előnyök:**
- ✅ Centralizált konfiguráció
- ✅ Type safety (dataclass)
- ✅ Validáció

**Becsült idő:** 0.5 nap

---

### 8. ⭐ **Test Scripts Tisztítás** (ALACSONY)

**Jelenlegi probléma:**
- Sok debug/test script a `scripts/` mappában
- Nincs kategorizálás
- Nincs dokumentáció

**Javasolt struktúra:**
```
scripts/
├── tests/                   # Unit/integration tests
│   ├── test_etl.py
│   ├── test_llm.py
│   └── test_bbm.py
├── debug/                   # Debug scripts
│   ├── debug_dti_lti.py
│   └── debug_extraction.py
├── migration/               # Migration scripts
│   └── run_supabase_migration.py
└── utils/                   # Utility scripts
    └── check_dti_lti_csv.py
```

**Becsült idő:** 0.5 nap

---

## 📈 Várható Hatások

### Teljesítmény
- **Pipeline futási idő:** -20-30% (caching miatt)
- **API költség:** -50-70% (LLM cache + batch processing)
- **Karbantarthatóság:** +40% (modularizálás)

### Kód Minőség
- **Cyclomatic complexity:** -30% (kisebb függvények)
- **Code duplication:** -50% (unifikált parsers)
- **Test coverage:** +20% (könnyebb tesztelhetőség)

---

## 🗓️ Implementációs Terv

### Fázis 1: Alapvető Refaktorálás (1 hét)
1. ✅ ETL extractorok kiszervezése
2. ✅ Rate parser unifikálás
3. ✅ Error handling unifikálás

### Fázis 2: LLM Optimalizáció (1 hét)
4. ✅ LLM cache implementáció
5. ✅ LLM analyzer modularizálás
6. ✅ Grounding validator optimalizáció

### Fázis 3: Finomhangolás (3-5 nap)
7. ✅ Konfiguráció centralizálás
8. ✅ Test scripts tisztítás
9. ✅ Dokumentáció frissítés

---

## ⚠️ Kockázatok és Mitigáció

### Kockázatok
1. **Refaktorálás során regressziók:**
   - **Mitigáció:** Unit tesztek írása kritikus részekhez
   - **Mitigáció:** Fokozatos refaktorálás (egy modul egyszerre)

2. **Cache invalidation problémák:**
   - **Mitigáció:** Cache versioning, manual cache clear opció

3. **Batch processing során timeout-ok:**
   - **Mitigáció:** Batch size limit, timeout handling

---

## ✅ Következő Lépések

1. **Azonnal (1-2 nap):**
   - ETL extractorok kiszervezése
   - Rate parser unifikálás

2. **Rövid táv (1 hét):**
   - LLM cache implementáció
   - Error handling unifikálás

3. **Középtáv (2-3 hét):**
   - LLM analyzer modularizálás
   - Grounding validator optimalizáció

4. **Hosszú táv (1 hónap):**
   - Teljes refaktorálás
   - Test coverage növelése
   - Dokumentáció frissítés

---

## 📝 Összefoglalás

### Főbb Refaktorálási Területek:
1. ⭐⭐⭐ **ETL Pipeline Modularizálás** (2-3 nap)
2. ⭐⭐⭐ **LLM Analyzer Modularizálás** (2-3 nap)
3. ⭐⭐ **BBM Extractor Duplikáció** (1 nap)
4. ⭐⭐ **Error Handling Unifikálás** (1 nap)
5. ⭐ **LLM Cache** (1 nap)
6. ⭐ **Grounding Validator Optimalizáció** (1-2 nap)

### Várható Eredmények:
- ✅ **-50-70% API költség** (caching)
- ✅ **-20-30% pipeline futási idő** (optimalizáció)
- ✅ **+40% karbantarthatóság** (modularizálás)
- ✅ **-50% code duplication** (unifikálás)

### Összes Becsült Idő: **8-12 nap**

---

**Megjegyzés:** A refaktorálás fokozatosan történjen, egy modul egyszerre, hogy ne törjön el a működő rendszer.
