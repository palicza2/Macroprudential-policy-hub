# 🔍 Projekt Áttekintés és Javaslatok

## 1. Refaktorálási Javaslatok

### 1.1. **main.py** (462 sor) - ⭐⭐⭐ KRITIKUS

**Problémák:**
- Egyetlen `main()` függvény orchestrál mindent (462 sor)
- Helper függvények a fájlban (`serialize_profile`, `format_profile_for_llm`)
- Hosszú, nehezen követhető flow
- Nincs stage-based error recovery
- BBM processing logika közvetlenül a main-ben (150+ sor)
- Country profile AI analysis hiba: `analyzer.summarize_text` nem létezik

**Javaslat:**
```python
pipeline/
├── __init__.py
├── orchestrator.py          # Fő PipelineOrchestrator osztály
├── stages/
│   ├── data_stage.py        # ETL stage (wraps ETLPipeline)
│   ├── visualization_stage.py # Visualization stage
│   ├── ai_stage.py          # AI analysis stage
│   ├── profile_stage.py     # Country profiles stage
│   ├── bbm_stage.py         # BBM processing stage (kiszervezve)
│   └── render_stage.py      # Rendering stage
└── serializers/
    ├── profile_serializer.py # serialize_profile
    └── llm_formatter.py     # format_profile_for_llm
```

**Előnyök:**
- Tiszta stage-based architecture
- Könnyebb debugging és error recovery
- Parallel stage execution lehetőség
- Jobb tesztelhetőség

---

### 1.2. **etl.py** (594 sor) - ⭐⭐ FONTOS

**Problémák:**
- `ETLPipeline` osztály tartalmazza minden extractor logikát
- Rate extraction logika duplikálva (`_extract_rate_from_text`, `_extract_max_rate_from_text`)
- Nincs konzisztens error handling
- O-SII parsing logika komplex és nehezen karbantartható

**Javaslat:**
```python
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
│   ├── rate_parser.py       # Unified rate extraction
│   ├── date_parser.py       # Date parsing utilities
│   └── text_cleaner.py      # Text cleaning utilities
└── validators/
    └── data_validator.py    # Data validation logika
```

**Előnyök:**
- Moduláris extractorok (könnyű új adatforrás hozzáadása)
- Újrafelhasználható parserek
- Jobb error handling lehetőség
- Rate extraction logika egy helyen

---

### 1.3. **country_profiles/** (681 sor) - ⭐⭐⭐ KRITIKUS

**Probléma:**
- Egyetlen nagy fájl (`country_profiles.py`)
- Knowledge graph építés keverve a profil generálással
- Region mapper logika beágyazva

**Javaslat:**
```
country_profiles/
├── __init__.py
├── profile_generator.py      # CountryProfileGenerator osztály
├── data_aggregators.py       # _get_current_status, _get_historical_evolution
├── region_mapper.py          # get_iso2, get_region helper függvények
└── knowledge_graph/           # (ha újra aktiváljuk)
    ├── builder.py
    └── rag_retriever.py
```

---

### 1.4. **grounding_validator.py** (419 sor) - ⭐⭐ FONTOS

**Problémák:**
- Context building logika keverve a validation logikával
- Google Search integration közvetlenül a fájlban
- Nincs caching a search results-hoz
- Knowledge graph context building van, de a graph kikapcsolva

**Javaslat:**
```python
grounding/
├── __init__.py
├── validator.py             # Fő GroundingValidator
├── state.py                 # ValidatorState dataclass
├── context_builders/
│   ├── data_context.py     # _build_data_context
│   ├── chart_context.py    # _build_chart_context
│   └── graph_context.py    # Knowledge graph context (jelenleg nem használt)
├── search/
│   ├── google_search.py    # _google_search logika
│   └── cache.py            # Search result caching (ÚJ)
└── claim_processors/
    ├── extractor.py        # Claim extraction
    ├── verifier.py         # Claim verification
    └── reviser.py          # Text revision
```

---

### 1.5. **llm_analysis.py** (640 sor) - ⭐ KÖZEPES

**Problémák:**
- `LLMAnalyzer` osztály tartalmazza minden extractor logikát
- `summarize_text` metódus hiányzik (hiba a main.py-ban)
- Nincs caching az LLM hívásokhoz
- Temperature és retry logika szétszórt

**Javaslat:**
```python
llm/
├── __init__.py
├── analyzer.py              # Fő LLMAnalyzer osztály
├── cache.py                 # LLM response caching (ÚJ)
├── extractors/
│   ├── keyword_extractor.py
│   ├── rate_extractor.py
│   ├── tag_classifier.py
│   ├── ltv_extractor.py
│   └── dti_lti_verifier.py  # confirm_dti_lti_presence
├── formatters/
│   └── text_cleaner.py     # _clean_text logika
└── summarizers/
    └── text_summarizer.py  # summarize_text (HIÁNYZÓ!)
```

---

### 1.6. **bbm.py** (322 sor) - ⭐ KÖZEPES

**Javaslat:**
```python
bbm/
├── __init__.py
├── matrix_builder.py        # build_bbm_matrix_html
├── ltv_processor.py         # LTV extraction logika
├── dti_lti/
│   ├── verifier.py          # build_dti_lti_items, confirm_dti_lti_presence
│   ├── comparison_builder.py # build_dti_lti_comparison_df
│   └── list_builder.py      # build_dti_lti_eu_list_html
└── extractors/
    └── ltv_extractor.py     # extract_ltv_details_regex
```

---

### 1.7. **Tisztítási Javaslatok**

**Törölhető fájlok:**
- `debug_syrb.py`
- `debug_syrb_v2.py`
- `debug_syrb_v3.py`
- `debug_syrb_v4.py`
- `debug_syrb_v5.py`

**Dokumentáció konszolidálás:**
- Összes `.md` fájl `docs/` mappába
- `docs/README.md` index fájl

---

## 2. AI Alkalmazási és Grounding Logika Javaslatok

### 2.1. **LLM Response Caching** - ⭐⭐⭐ KRITIKUS

**Probléma:**
- Minden futtatáskor újra hívódik az LLM ugyanazokra a promptokra
- Lassú és költséges

**Javaslat:**
```python
# llm/cache.py
import hashlib
import json
from pathlib import Path

class LLMCache:
    def __init__(self, cache_dir: Path = Path("cache/llm")):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _hash_prompt(self, prompt: str, data: str = "", img_key: str = "") -> str:
        content = f"{prompt}|{data}|{img_key}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def get(self, prompt: str, data: str = "", img_key: str = "") -> str | None:
        key = self._hash_prompt(prompt, data, img_key)
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f).get("response")
        return None
    
    def set(self, prompt: str, data: str, img_key: str, response: str):
        key = self._hash_prompt(prompt, data, img_key)
        cache_file = self.cache_dir / f"{key}.json"
        with open(cache_file, "w") as f:
            json.dump({"prompt": prompt, "response": response}, f, indent=2)
```

**Használat:**
```python
# llm_runner.py-ben
cache = LLMCache()
cached = cache.get(t.prompt, t.data, t.img)
if cached:
    results[t.id] = analyzer._clean_text(cached, is_global=t.clean_global)
    continue
# ... LLM hívás ...
cache.set(t.prompt, t.data, t.img, res)
```

---

### 2.2. **Batch Processing LLM Hívásokhoz** - ⭐⭐ FONTOS

**Probléma:**
- Minden task külön LLM hívást csinál
- Lassú, nincs párhuzamosítás

**Javaslat:**
```python
# llm_runner.py
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_tasks_parallel(analyzer, tasks, plot_paths, max_workers=3):
    """Run LLM tasks in parallel batches."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_single_task, analyzer, t, plot_paths): t.id
            for t in tasks
        }
        for future in as_completed(futures):
            task_id = futures[future]
            try:
                results[task_id] = future.result()
            except Exception as exc:
                logger.error(f"Task {task_id} failed: {exc}")
                results[task_id] = "N/A"
    return results
```

---

### 2.3. **Grounding Validator Javítások** - ⭐⭐ FONTOS

**Problémák:**
1. **Claim extraction nem optimalizált:**
   - Minden analysis-ból 3-6 claim-et próbál kinyerni
   - Nincs prioritás (melyik claim fontosabb)
   - Nincs claim deduplication

2. **Verification nem hatékony:**
   - Minden claim külön LLM hívást csinál
   - Nincs batch verification

3. **Search result caching hiányzik:**
   - Ugyanazokra a query-kre újra keres

4. **Knowledge graph context nem használatos:**
   - Graph kikapcsolva, de a context builder még mindig próbálja használni

**Javaslatok:**

```python
# grounding/claim_processors/extractor.py
class ClaimExtractor:
    def extract_prioritized_claims(self, analyses: Dict[str, str]) -> List[Dict]:
        """
        Extract claims with priority scoring:
        - High: Contains numbers, rates, country names
        - Medium: Contains policy terms
        - Low: Generic statements
        """
        # Implement priority-based extraction
        pass

# grounding/search/cache.py
class SearchCache:
    """Cache Google Search results to avoid redundant API calls."""
    def __init__(self, cache_dir: Path = Path("cache/search")):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, query: str) -> List[Dict] | None:
        # Check cache
        pass
    
    def set(self, query: str, results: List[Dict]):
        # Save to cache
        pass

# grounding/claim_processors/verifier.py
class BatchClaimVerifier:
    def verify_batch(self, claims: List[Dict], context: str) -> List[Dict]:
        """
        Verify multiple claims in a single LLM call.
        More efficient than individual calls.
        """
        prompt = f"""Verify the following claims using the context.
        Return JSON array with verdict for each claim.
        
        CLAIMS:
        {json.dumps(claims, indent=2)}
        
        CONTEXT:
        {context}
        """
        # Single LLM call for batch
        pass
```

---

### 2.4. **Error Handling és Retry Logika** - ⭐⭐ FONTOS

**Problémák:**
- Retry logika szétszórt (`llm_runner.py`, `grounding_validator.py`)
- Nincs exponential backoff
- Nincs rate limiting kezelés

**Javaslat:**
```python
# utils/retry.py
import time
from functools import wraps
from typing import Callable, TypeVar

T = TypeVar('T')

def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: tuple = (Exception,)
):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(delay)
                    delay = min(delay * exponential_base, max_delay)
            raise RuntimeError(f"Function {func.__name__} failed after {max_retries} attempts")
        return wrapper
    return decorator

# Használat:
@retry_with_backoff(max_retries=3, exceptions=(RateLimitError,))
def call_llm(prompt):
    # LLM hívás
    pass
```

---

### 2.5. **Prompt Engineering Javítások** - ⭐ FONTOS

**Problémák:**
1. **Prompt consistency:**
   - Ugyanazok a promptok ismétlődnek
   - Nincs centralizált prompt management

2. **Few-shot examples hiányzik:**
   - Az LLM-nek nincs példa a kívánt output formátumra

3. **Temperature tuning nincs optimalizálva:**
   - Minden task ugyanazt a temperature-t használja (0.2-0.3)

**Javaslat:**
```python
# prompts/templates.py
class PromptTemplates:
    CHART_ANALYSIS = """Analyze the chart focusing on the last 12 months.
    Emphasize country objectives and risks addressed.
    Avoid tool descriptions.
    Start with a strong topic sentence.
    Write ONE paragraph of 6-7 sentences.
    
    EXAMPLE OUTPUT:
    [Few-shot example here]
    """
    
    SECTION_SUMMARY = """Write a SPECIFIC high-level summary.
    STRUCTURE: 1-2 bullet points (HTML <li> tags).
    REQUIREMENT: Be analytical. Emphasize objectives and risks.
    
    EXAMPLE OUTPUT:
    <ul>
    <li>First bullet point with key insights...</li>
    <li>Second bullet point if needed...</li>
    </ul>
    """

# prompts/temperature_config.py
TASK_TEMPERATURES = {
    "extraction": 0.0,      # Deterministic extraction
    "analysis": 0.2,        # Balanced analysis
    "summary": 0.3,         # Slightly creative summaries
    "revision": 0.3,        # Text revision
    "creative": 0.5,        # Executive summary
}
```

---

### 2.6. **Grounding Validator Optimalizáció** - ⭐⭐ FONTOS

**Javaslatok:**

1. **Selective Validation:**
   - Ne validáljunk minden analysis-t, csak a kritikusakat
   - Executive summary, section summaries prioritás

2. **Incremental Validation:**
   - Ha egy claim supported, ne keressünk rá
   - Csak unclear/contradicted claim-ekre search

3. **Context Compression:**
   - A data context túl hosszú lehet
   - Kompresszáljuk a releváns részeket

```python
# grounding/context_builders/data_context.py
class DataContextBuilder:
    def build_compressed_context(
        self, 
        data_inputs: Dict[str, Any],
        relevant_countries: List[str] = None
    ) -> str:
        """
        Build compressed context focusing on relevant countries.
        """
        # Only include relevant countries in context
        # Summarize large tables
        pass
```

---

### 2.7. **Hiányzó Metódus Javítás** - ⭐⭐⭐ KRITIKUS

**Probléma:**
- `main.py:389` - `analyzer.summarize_text()` nem létezik
- Country profile AI analysis nem működik

**Javaslat:**
```python
# llm_analysis.py-ben hozzáadni:
def summarize_text(self, text: str, instruction: str = "") -> str:
    """
    Summarize text using LLM.
    """
    if not text:
        return ""
    
    prompt = f"""TASK: {instruction}
    
    TEXT:
    {text[:2000]}
    
    Return a concise summary."""
    
    try:
        llm = self._get_llm(temperature=0.3)
        res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
        return self._clean_text(res, is_global=False)
    except Exception as e:
        logger.error(f"Text summarization failed: {e}")
        return ""
```

---

## 3. Prioritási Rangsor

### Phase 1 (Kritikus - azonnal):
1. ✅ **Hiányzó `summarize_text` metódus hozzáadása** - Country profile AI analysis nem működik
2. ✅ **LLM Response Caching** - Jelentős sebességjavulás és költségcsökkentés
3. ✅ **main.py refaktorálás** - Stage-based architecture

### Phase 2 (Fontos - rövid távon):
4. ✅ **Grounding Validator optimalizáció** - Batch verification, search caching
5. ✅ **ETL pipeline modularizálás** - Jobb karbantarthatóság
6. ✅ **Error handling és retry logika** - Robusztusabb rendszer

### Phase 3 (Közepes - középtávon):
7. ✅ **Country profiles refaktorálás** - Moduláris struktúra
8. ✅ **Prompt engineering javítások** - Few-shot examples, centralizált templates
9. ✅ **Batch processing LLM hívásokhoz** - Párhuzamosítás

### Phase 4 (Alacsony - hosszú távon):
10. ✅ **Frontend modularizálás** - JavaScript modulok
11. ✅ **Dokumentáció konszolidálás** - `docs/` mappa
12. ✅ **Debug fájlok törlése** - Tisztítás

---

## 4. Konkrét Javítási Lépések

### 4.1. Azonnali Javítások (1-2 óra)

1. **`summarize_text` metódus hozzáadása:**
   ```python
   # llm_analysis.py-ben, az LLMAnalyzer osztályba
   def summarize_text(self, text: str, instruction: str = "") -> str:
       # Implementáció fent
   ```

2. **LLM Cache alap implementáció:**
   ```python
   # llm/cache.py létrehozása
   # llm_runner.py módosítása cache használatára
   ```

### 4.2. Rövid Távú Javítások (1-2 nap)

3. **main.py stage-based refaktorálás:**
   - BBM processing kiszervezése `bbm_stage.py`-ba
   - Helper függvények kiszervezése

4. **Grounding validator optimalizáció:**
   - Search caching
   - Batch claim verification

### 4.3. Középtávú Javítások (1 hét)

5. **ETL pipeline modularizálás:**
   - Extractorok kiszervezése
   - Rate parser unifikálás

6. **Error handling unifikálás:**
   - Retry decorator
   - Rate limiting kezelés

---

## 5. Mérési Mutatók

**Refaktorálás előtt/után:**
- Fájl méretek (sorok száma)
- Cyclomatic complexity
- Test coverage (ha van)
- Build time
- Runtime performance

**AI/Grounding javítások:**
- LLM API hívások száma (csökkentés caching-gel)
- Grounding validation idő (optimalizációval)
- Search API hívások száma (caching-gel)
- Cost per run (API hívások csökkentése)

---

## 6. Kockázatok és Mitigáció

**Kockázatok:**
1. **Refaktorálás során regressziók:**
   - Mitigáció: Unit tesztek írása kritikus részekhez
   - Fokozatos refaktorálás (egy modul egyszerre)

2. **Cache invalidation problémák:**
   - Mitigáció: Cache versioning, manual cache clear opció

3. **Batch processing során timeout-ok:**
   - Mitigáció: Batch size limit, timeout handling

---

## 7. Következő Lépések

1. **Azonnal:**
   - `summarize_text` metódus hozzáadása
   - LLM cache alap implementáció

2. **Ezen a héten:**
   - main.py BBM processing kiszervezése
   - Grounding validator search caching

3. **Következő hét:**
   - ETL extractorok kiszervezése
   - Error handling unifikálás

4. **Hosszú táv:**
   - Teljes stage-based architecture
   - Frontend modularizálás
