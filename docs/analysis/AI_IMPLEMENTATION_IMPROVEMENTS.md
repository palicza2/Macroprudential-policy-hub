# 🤖 AI Implementáció Fejlesztési Javaslatok

**Dátum:** 2024 Q4  
**Cél:** AI implementáció áttekintése és optimalizálási lehetőségek azonosítása

---

## 📊 Jelenlegi AI Implementáció Áttekintés

### Főbb Komponensek

1. **LLMAnalyzer** (`llm_analysis.py` - 1135 sor)
   - LLM client factory (`_get_llm`)
   - Text cleaning (`_clean_text`)
   - Field extraction (rates, keywords, LTV, DTI/LTI)
   - Validation (DTI/LTI, LTV)
   - Confirmation (DTI/LTI presence)

2. **LLM Runner** (`llm_runner.py` - 70 sor)
   - Task execution (`run_tasks`, `run_task`)
   - Basic retry logic (fixed sleep)
   - Image handling (base64 encoding)

3. **Grounding Validator** (`grounding_validator.py` - 368 sor)
   - Claim extraction
   - Claim verification
   - Google Search integration
   - JSON parsing (`_safe_json_loads`)

4. **Validators** (`bbm/dti_lti_validator.py`, `bbm/ltv_validator.py`)
   - Rule validation
   - AI-based data filling
   - External search integration

---

## 🔍 Azonosított Problémák

### 1. ⚠️ **Nincs LLM Cache** (KRITIKUS)

**Probléma:**
- Minden pipeline run újra hívja az LLM-et
- Ugyanazokra a promptokra újra válaszol
- Felesleges API költség (50-70% megtakarítható)

**Példa:**
```python
# Jelenlegi: Minden run újra hívja
def extract_keywords(self, text_list, context="justification"):
    prompt = f"TASK: Extract keywords... {input_text}"
    llm = self._get_llm(temperature=0.0)
    res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
    # Nincs cache check!
```

**Megoldás:** File-based cache (MD5 hash prompt + model + temperature)

---

### 2. ⚠️ **Egyszerű Retry Logika** (FONTOS)

**Probléma:**
- Fix sleep idő (`time.sleep(sleep_s)`)
- Nincs exponential backoff
- Nincs rate limiting kezelés
- Nincs különbség API error típusok között

**Példa:**
```python
# Jelenlegi: Egyszerű retry
except Exception as exc:
    attempt += 1
    if attempt > default_retries:
        return "N/A"
    time.sleep(sleep_s)  # Fix sleep, nincs backoff
```

**Megoldás:** Exponential backoff + rate limiting

---

### 3. ⚠️ **Nincs Batch Processing** (FONTOS)

**Probléma:**
- Minden item külön LLM hívás
- Lassú feldolgozás
- Magas API költség

**Példa:**
```python
# Jelenlegi: Soros feldolgozás
for rule in rules:
    validated = analyzer.validate_dti_lti_rules([rule], ...)  # 1 hívás/rule
```

**Megoldás:** Batch processing (pl. 5-10 item/batch)

---

### 4. ⚠️ **Duplikált JSON Parsing** (KÖZEPES)

**Probléma:**
- `_safe_json_loads` duplikálva (`llm_analysis.py`, `grounding_validator.py`)
- Inkonzisztens error handling

**Megoldás:** Centralizált JSON parser utility

---

### 5. ⚠️ **Nincs Parallel Processing** (KÖZEPES)

**Probléma:**
- Soros feldolgozás (`for t in tasks`)
- Lassú pipeline futás
- Nincs async/threading

**Megoldás:** ThreadPoolExecutor vagy async/await

---

### 6. ⚠️ **Nincs Token Usage Tracking** (KÖZEPES)

**Probléma:**
- Nem tudjuk mennyi token megy el
- Nincs költség monitoring
- Nehéz optimalizálni

**Megoldás:** Token usage tracking + logging

---

### 7. ⚠️ **Temperature Hardcoded** (ALACSONY)

**Probléma:**
- Temperature értékek szétszórva a kódban
- Nincs centralizált konfiguráció
- Nehéz finomhangolni

**Megoldás:** Centralizált temperature config

---

### 8. ⚠️ **Nincs Prompt Versioning** (ALACSONY)

**Probléma:**
- Prompt változások nincsenek dokumentálva
- Nehéz A/B tesztelni
- Nincs prompt cache

**Megoldás:** Prompt versioning + cache

---

## 🎯 Fejlesztési Javaslatok

### 1. ⭐⭐⭐ **LLM Cache Implementáció** (KRITIKUS)

**Előnyök:**
- ✅ 50-70% API költség csökkentés
- ✅ Gyorsabb pipeline futás
- ✅ Offline tesztelés lehetősége

**Implementáció:**
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
            json.dump({
                'response': response,
                'prompt': prompt,
                'model': model,
                'temperature': temperature,
                'cached_at': str(Path(cache_file).stat().st_mtime)
            }, f, indent=2)
    
    def clear(self):
        """Clear all cached responses."""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()
```

**Használat:**
```python
# llm_analysis.py
def extract_keywords(self, text_list, context="justification"):
    # ... prompt generation ...
    
    # Check cache first
    cache = LLMCache()
    cached = cache.get(prompt, self.config["model_name"], 0.0)
    if cached:
        logger.debug(f"Cache hit for extract_keywords")
        return cached
    
    # LLM call
    llm = self._get_llm(temperature=0.0)
    res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
    
    # Cache result
    cache.set(prompt, self.config["model_name"], 0.0, res)
    return res
```

**Becsült idő:** 1 nap

---

### 2. ⭐⭐ **Exponential Backoff Retry** (FONTOS)

**Előnyök:**
- ✅ Jobb error recovery
- ✅ Rate limiting kezelés
- ✅ API limit problémák kezelése

**Implementáció:**
```python
# utils/retry.py
import time
import logging
from functools import wraps
from typing import Callable, TypeVar, Tuple

logger = logging.getLogger(__name__)
T = TypeVar('T')

def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[type, ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts: {e}")
                        raise
                    
                    if on_retry:
                        on_retry(attempt + 1, max_retries, e)
                    
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(min(delay, max_delay))
                    delay *= exponential_base
            
            if last_exception:
                raise last_exception
            return None
        return wrapper
    return decorator
```

**Használat:**
```python
# llm_runner.py
from utils.retry import retry_with_backoff

@retry_with_backoff(
    max_retries=3,
    initial_delay=1.0,
    max_delay=60.0,
    exponential_base=2.0,
    exceptions=(Exception,)
)
def run_single_task(analyzer, task, plot_paths):
    # ... task execution ...
```

**Becsült idő:** 0.5 nap

---

### 3. ⭐⭐ **Batch Processing** (FONTOS)

**Előnyök:**
- ✅ Kevesebb API hívás
- ✅ Gyorsabb feldolgozás
- ✅ Költségcsökkentés

**Implementáció:**
```python
# llm_analysis.py
def validate_dti_lti_rules_batch(
    self,
    rules: List[Dict],
    descriptions: Dict[str, str],
    batch_size: int = 5,
    use_external_search: bool = False,
    search_config: Optional[Dict] = None
) -> List[Dict]:
    """
    Validate DTI/LTI rules in batches.
    
    Args:
        rules: List of rule dictionaries
        descriptions: Dictionary mapping country+measure to description
        batch_size: Number of rules per batch
        use_external_search: Whether to use external search
        search_config: Optional search configuration
        
    Returns:
        List of validated rule dictionaries
    """
    validated_rules = []
    
    # Process in batches
    for i in range(0, len(rules), batch_size):
        batch = rules[i:i + batch_size]
        
        # Build batch prompt
        batch_prompt = self._build_batch_validation_prompt(batch, descriptions)
        
        # LLM call for batch
        llm = self._get_llm(temperature=0.0)
        res = (llm | StrOutputParser()).invoke([HumanMessage(content=batch_prompt)])
        
        # Parse batch results
        batch_results = self._parse_batch_validation_results(res, len(batch))
        validated_rules.extend(batch_results)
    
    return validated_rules
```

**Becsült idő:** 1-2 nap

---

### 4. ⭐ **Parallel Processing** (KÖZEPES)

**Előnyök:**
- ✅ Gyorsabb pipeline futás
- ✅ Jobb erőforrás kihasználás

**Implementáció:**
```python
# llm_runner.py
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

def run_tasks_parallel(
    *,
    analyzer,
    tasks: List[LLMTask],
    plot_paths: Dict[str, object],
    max_workers: int = 3,
    default_retries: int = 2,
    sleep_s: float = 1.0,
) -> Dict[str, str]:
    """
    Run LLM tasks in parallel batches.
    
    Args:
        analyzer: LLMAnalyzer instance
        tasks: List of LLMTask objects
        plot_paths: Dictionary mapping task IDs to plot paths
        max_workers: Maximum number of parallel workers
        default_retries: Number of retries per task
        sleep_s: Sleep time between retries
        
    Returns:
        Dictionary mapping task IDs to results
    """
    results: Dict[str, str] = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                run_single_task,
                analyzer=analyzer,
                task=t,
                plot_paths=plot_paths,
                default_retries=default_retries,
                sleep_s=sleep_s
            ): t.id
            for t in tasks
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            task_id = futures[future]
            try:
                results[task_id] = future.result()
            except Exception as exc:
                logger.error(f"Task {task_id} failed: {exc}")
                results[task_id] = "N/A"
    
    return results
```

**Becsült idő:** 1 nap

---

### 5. ⭐ **Token Usage Tracking** (KÖZEPES)

**Előnyök:**
- ✅ Költség monitoring
- ✅ Optimalizálási lehetőségek
- ✅ Usage analytics

**Implementáció:**
```python
# llm/token_tracker.py
import logging
from typing import Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class TokenUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

class TokenTracker:
    def __init__(self):
        self.usage_history: List[TokenUsage] = []
        # Gemini 2.5 Flash Lite pricing (per 1M tokens)
        self.input_price_per_1m = 0.075
        self.output_price_per_1m = 0.30
    
    def track_usage(
        self,
        prompt_tokens: int,
        completion_tokens: int,
        model: str = "gemini-2.5-flash-lite"
    ):
        """Track token usage for a single API call."""
        total_tokens = prompt_tokens + completion_tokens
        cost = (
            (prompt_tokens / 1_000_000) * self.input_price_per_1m +
            (completion_tokens / 1_000_000) * self.output_price_per_1m
        )
        
        usage = TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            cost_usd=cost
        )
        
        self.usage_history.append(usage)
        logger.debug(
            f"Token usage: {prompt_tokens} input + {completion_tokens} output = "
            f"{total_tokens} total (${cost:.4f})"
        )
    
    def get_total_usage(self) -> Dict[str, Any]:
        """Get total token usage statistics."""
        if not self.usage_history:
            return {
                "total_prompt_tokens": 0,
                "total_completion_tokens": 0,
                "total_tokens": 0,
                "total_cost_usd": 0.0,
                "call_count": 0
            }
        
        total_prompt = sum(u.prompt_tokens for u in self.usage_history)
        total_completion = sum(u.completion_tokens for u in self.usage_history)
        total_tokens = sum(u.total_tokens for u in self.usage_history)
        total_cost = sum(u.cost_usd for u in self.usage_history)
        
        return {
            "total_prompt_tokens": total_prompt,
            "total_completion_tokens": total_completion,
            "total_tokens": total_tokens,
            "total_cost_usd": total_cost,
            "call_count": len(self.usage_history)
        }
    
    def log_summary(self):
        """Log token usage summary."""
        stats = self.get_total_usage()
        logger.info(
            f"Token usage summary: {stats['total_tokens']:,} tokens "
            f"({stats['total_prompt_tokens']:,} input + {stats['total_completion_tokens']:,} output) "
            f"across {stats['call_count']} calls = ${stats['total_cost_usd']:.4f}"
        )
```

**Használat:**
```python
# llm_analysis.py
class LLMAnalyzer:
    def __init__(self, config, rag_retriever=None):
        self.config = config
        self.rag_retriever = rag_retriever
        self.token_tracker = TokenTracker()  # Add tracker
    
    def _get_llm(self, temperature):
        # ... existing code ...
        # Note: LangChain doesn't expose token usage directly
        # Would need to wrap the LLM call or use response metadata
        return ChatGoogleGenerativeAI(...)
```

**Becsült idő:** 1 nap (ha LangChain támogatja)

---

### 6. ⭐ **Centralizált JSON Parser** (KÖZEPES)

**Előnyök:**
- ✅ Nincs duplikáció
- ✅ Konzisztens error handling
- ✅ Könnyebb karbantartás

**Implementáció:**
```python
# utils/json_parser.py
import json
import re
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

def safe_json_loads(text: str, fallback: Optional[Any] = None) -> Optional[Any]:
    """
    Safely parse JSON from text, with fallback regex extraction.
    
    Args:
        text: Text to parse
        fallback: Fallback value if parsing fails
        
    Returns:
        Parsed JSON object or fallback
    """
    if not text:
        return fallback
    
    # Try direct JSON parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Try to extract JSON from text using regex
    match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            logger.debug(f"Failed to parse JSON from extracted match: {match.group(1)[:100]}")
    
    logger.warning(f"Could not parse JSON from text: {text[:200]}...")
    return fallback
```

**Becsült idő:** 0.5 nap

---

### 7. ⭐ **Centralizált Temperature Config** (ALACSONY)

**Előnyök:**
- ✅ Könnyebb finomhangolás
- ✅ Konzisztens temperature értékek

**Implementáció:**
```python
# config.py
LLM_TEMPERATURE_CONFIG = {
    "extraction": 0.0,  # Deterministic extraction
    "validation": 0.0,  # Deterministic validation
    "analysis": 0.2,    # Slightly creative analysis
    "summarization": 0.3,  # More creative summarization
    "grounding": 0.2,   # Balanced grounding
}
```

**Becsült idő:** 0.5 nap

---

## 📊 Prioritásos Implementációs Terv

### Fázis 1: Alapvető Optimalizációk (2-3 nap)
1. ✅ **LLM Cache** (1 nap)
2. ✅ **Exponential Backoff Retry** (0.5 nap)
3. ✅ **Centralizált JSON Parser** (0.5 nap)

### Fázis 2: Teljesítmény Javítások (2-3 nap)
4. ✅ **Batch Processing** (1-2 nap)
5. ✅ **Parallel Processing** (1 nap)

### Fázis 3: Monitoring és Analytics (1-2 nap)
6. ✅ **Token Usage Tracking** (1 nap)
7. ✅ **Centralizált Temperature Config** (0.5 nap)

---

## 📈 Várható Hatások

### Költség
- **LLM Cache:** -50-70% API költség
- **Batch Processing:** -20-30% API hívások száma
- **Összesen:** -60-80% API költség

### Teljesítmény
- **Parallel Processing:** -30-50% pipeline futási idő
- **Batch Processing:** -20-30% feldolgozási idő
- **Összesen:** -40-60% pipeline futási idő

### Karbantarthatóság
- **Centralizált utilities:** +30% kód minőség
- **Token tracking:** Jobb monitoring
- **Error handling:** Jobb reliability

---

## ⚠️ Kockázatok és Mitigáció

### Kockázatok
1. **Cache invalidation:**
   - **Mitigáció:** Cache versioning, manual clear opció
   - **Mitigáció:** Cache TTL (time-to-live)

2. **Batch processing során timeout-ok:**
   - **Mitigáció:** Batch size limit, timeout handling
   - **Mitigáció:** Fallback to individual calls

3. **Parallel processing során race conditions:**
   - **Mitigáció:** Thread-safe cache, proper locking
   - **Mitigáció:** Async/await használata

---

## ✅ Következő Lépések

1. **Azonnal (1-2 nap):**
   - LLM Cache implementáció
   - Exponential Backoff Retry
   - Centralizált JSON Parser

2. **Rövid táv (1 hét):**
   - Batch Processing
   - Parallel Processing

3. **Középtáv (2 hét):**
   - Token Usage Tracking
   - Centralizált Temperature Config

---

## 📝 Összefoglalás

### Főbb Fejlesztések:
1. ⭐⭐⭐ **LLM Cache** - 50-70% költségcsökkentés
2. ⭐⭐ **Exponential Backoff** - Jobb error recovery
3. ⭐⭐ **Batch Processing** - Kevesebb API hívás
4. ⭐ **Parallel Processing** - Gyorsabb futás
5. ⭐ **Token Tracking** - Monitoring

### Várható Eredmények:
- ✅ **-60-80% API költség**
- ✅ **-40-60% pipeline futási idő**
- ✅ **+30% kód minőség**
- ✅ **Jobb monitoring és analytics**

### Összes Becsült Idő: **5-8 nap**

---

**Megjegyzés:** A fejlesztések fokozatosan implementálhatók, egy komponens egyszerre, hogy ne törjön el a működő rendszer.
