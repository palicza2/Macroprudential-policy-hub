# 🚀 Kritikus Fejlesztési Terv - Macro Policy Hub

## 1. TESZTELÉS ÉS MINŐSÉGBIZTOSÍTÁS

### 1.1 Jelenlegi helyzet
- ❌ Nincs unit teszt
- ❌ Nincs integration teszt
- ❌ Nincs data validation teszt
- ⚠️ Csak manuális tesztelés történik

### 1.2 Célok
- Unit tesztek a kritikus modulokhoz (ETL, LLM wrapper, data parsing)
- Integration tesztek a teljes pipeline-hoz
- Data quality tesztek (anomáliadetektálás, validáció)
- Coverage report (cél: >70%)

### 1.3 Implementációs terv

#### 1.3.1 Projektstruktúra
```
tests/
├── __init__.py
├── conftest.py              # pytest fixtures
├── unit/
│   ├── test_etl.py          # ETL pipeline tesztek
│   ├── test_llm_analysis.py # LLM wrapper tesztek
│   ├── test_data_parsing.py # Rate extraction, regex tesztek
│   └── test_utils.py        # Utility függvények
├── integration/
│   ├── test_pipeline.py     # Teljes pipeline teszt
│   └── test_end_to_end.py   # E2E teszt mock adatokkal
└── data_quality/
    ├── test_data_validation.py  # Adatminőség ellenőrzések
    └── test_anomaly_detection.py # Anomáliadetektálás
```

#### 1.3.2 Példa unit teszt: `test_etl.py`
```python
import pytest
import pandas as pd
from pathlib import Path
from etl import ETLPipeline

class TestETLPipeline:
    """ETL pipeline unit tesztek."""
    
    def test_extract_rate_from_text(self):
        """Teszteljük a rate extraction logikát."""
        etl = ETLPipeline(Path("data"), "url1", "url2")
        
        # Normál esetek
        assert etl._extract_rate_from_text("SRB of 2.5%") == 2.5
        assert etl._extract_rate_from_text("buffer rate: 1.0%") == 1.0
        assert etl._extract_rate_from_text("3% systemic risk buffer") == 3.0
        
        # Edge case-ek
        assert etl._extract_rate_from_text("50% of GDP") == 0.0  # Nem rate
        assert etl._extract_rate_from_text("") == 0.0
        assert etl._extract_rate_from_text(None) == 0.0
        
        # Több rate esetén
        assert etl._extract_rate_from_text("SRB of 2.5%, buffer of 1.0%") == 2.5
    
    def test_process_syrb_empty_file(self):
        """Teszteljük az üres fájl kezelését."""
        etl = ETLPipeline(Path("data"), "url1", "url2")
        result = etl._process_syrb()
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_country_code_conversion(self):
        """Teszteljük az ISO kód konverziót."""
        etl = ETLPipeline(Path("data"), "url1", "url2")
        # Mock dataframe
        df = pd.DataFrame({'country': ['Hungary', 'Germany', 'Invalid']})
        # Teszteljük a konverziót
        # ...
```

#### 1.3.3 Példa integration teszt: `test_pipeline.py`
```python
import pytest
from unittest.mock import Mock, patch
from main import main

class TestPipelineIntegration:
    """Integration tesztek a teljes pipeline-hoz."""
    
    @patch('main.ETLPipeline')
    @patch('main.Visualizer')
    @patch('main.LLMAnalyzer')
    def test_full_pipeline_success(self, mock_llm, mock_viz, mock_etl):
        """Teszteljük a teljes pipeline sikeres futását."""
        # Mock adatok
        mock_etl.return_value.run_pipeline.return_value = {
            'ccyb_df': pd.DataFrame({'country': ['HU'], 'rate': [2.5]}),
            'syrb_df': pd.DataFrame(),
            'bbm_df': pd.DataFrame(),
        }
        # ...
        # Assert-ek
        assert Path("index.html").exists()
    
    def test_pipeline_with_missing_data(self):
        """Teszteljük a hiányzó adatok kezelését."""
        # ...
```

#### 1.3.4 Data quality teszt: `test_data_validation.py`
```python
import pytest
import pandas as pd

class TestDataValidation:
    """Adatminőség validációs tesztek."""
    
    def test_ccyb_rate_range(self):
        """CCyB ráták 0-2.5% között kell legyenek."""
        df = pd.read_parquet("data/processed_ccyb.parquet")
        assert (df['rate'] >= 0).all()
        assert (df['rate'] <= 2.5).all(), "CCyB rate should not exceed 2.5%"
    
    def test_syrb_rate_range(self):
        """SyRB ráták 0-10% között kell legyenek."""
        df = pd.read_parquet("data/processed_syrb.parquet")
        assert (df['rate_numeric'] >= 0).all()
        assert (df['rate_numeric'] <= 10).all(), "SyRB rate should not exceed 10%"
    
    def test_no_duplicate_countries_per_date(self):
        """Egy országnak egy dátumon csak egy rátája lehet."""
        df = pd.read_parquet("data/processed_ccyb.parquet")
        duplicates = df.groupby(['country', 'date']).size()
        assert (duplicates <= 1).all()
    
    def test_date_consistency(self):
        """Dátumok logikusak kell legyenek."""
        df = pd.read_parquet("data/processed_ccyb.parquet")
        assert df['date'].min() >= pd.Timestamp('2010-01-01')
        assert df['date'].max() <= pd.Timestamp.now()
```

#### 1.3.5 Setup: `conftest.py`
```python
import pytest
import pandas as pd
from pathlib import Path

@pytest.fixture
def sample_ccyb_data():
    """Minta CCyB adatok."""
    return pd.DataFrame({
        'country': ['Hungary', 'Germany', 'France'],
        'iso2': ['HU', 'DE', 'FR'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
        'rate': [2.5, 0.5, 1.0],
    })

@pytest.fixture
def temp_data_dir(tmp_path):
    """Ideiglenes data könyvtár."""
    return tmp_path / "data"
```

#### 1.3.6 CI/CD integráció
```yaml
# .github/workflows/tests.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: pip install pytest pytest-cov
      - run: pytest tests/ --cov=. --cov-report=xml
      - uses: codecov/codecov-action@v3
```

### 1.4 Következő lépések
1. ✅ `pytest` és `pytest-cov` hozzáadása `requirements.txt`-hez
2. ✅ `tests/` könyvtár létrehozása
3. ✅ Első unit tesztek írása (ETL rate extraction)
4. ✅ Data validation tesztek
5. ✅ GitHub Actions workflow

---

## 2. TYPE HINTS ÉS DOKUMENTÁCIÓ

### 2.1 Jelenlegi helyzet
- ⚠️ Részleges type hints (pl. `grounding_validator.py`-ban van)
- ❌ Nincs docstring a legtöbb függvényhez
- ❌ Nincs API dokumentáció

### 2.2 Célok
- Teljes type hint coverage a főbb modulokban
- Google/NumPy style docstring-ek
- Type checking `mypy`-vel
- Automatikus API dokumentáció generálás

### 2.3 Implementációs terv

#### 2.3.1 Type hints példa: `etl.py`
```python
from typing import Dict, Optional, Tuple
from pathlib import Path
import pandas as pd

class ETLPipeline:
    def __init__(
        self, 
        data_dir: Path, 
        ccyb_url: str, 
        syrb_url: str, 
        capital_measures_url: Optional[str] = None
    ) -> None:
        """
        ETL Pipeline inicializálása.
        
        Args:
            data_dir: Adatok könyvtára
            ccyb_url: CCyB Excel letöltési URL
            syrb_url: SyRB Excel letöltési URL
            capital_measures_url: Opcionális capital measures URL
        """
        self.data_dir = data_dir
        self.ccyb_url = ccyb_url
        # ...
    
    def _extract_rate_from_text(self, text: Optional[str]) -> float:
        """
        Szövegből rate kinyerése regex-szel.
        
        Args:
            text: Szöveg, ami tartalmazhat rate információt
            
        Returns:
            Kinyert rate (0.0-20.0% között), vagy 0.0 ha nincs találat
            
        Examples:
            >>> etl._extract_rate_from_text("SRB of 2.5%")
            2.5
            >>> etl._extract_rate_from_text("50% of GDP")
            0.0
        """
        if pd.isna(text) or not text:
            return 0.0
        # ...
    
    def run_pipeline(self) -> Dict[str, pd.DataFrame]:
        """
        Teljes ETL pipeline futtatása.
        
        Returns:
            Dictionary a következő kulcsokkal:
            - 'ccyb_df': CCyB adatok
            - 'syrb_df': SyRB adatok
            - 'bbm_df': BBM adatok
            - 'osii_df': O-SII adatok
            - 'latest_ccyb_df': Legfrissebb CCyB snapshot
            - 'latest_syrb_df': Legfrissebb SyRB snapshot
            
        Raises:
            FileNotFoundError: Ha az Excel fájlok nem találhatók
            ValueError: Ha az adatok formátuma hibás
        """
        # ...
```

#### 2.3.2 Type hints példa: `llm_analysis.py`
```python
from typing import List, Dict, Any, Optional
from langchain_google_genai import ChatGoogleGenerativeAI

class LLMAnalyzer:
    def __init__(self, config: Dict[str, Any]) -> None:
        """LLM Analyzer inicializálása."""
        self.config = config
    
    def extract_keywords(
        self, 
        text_list: List[str], 
        context: str = "targeted risk"
    ) -> List[str]:
        """
        Kulcsszavak kinyerése LLM-mel.
        
        Args:
            text_list: Szövegek listája
            context: Kontextus a prompt-hoz
            
        Returns:
            Kulcsszavak listája (egy-egy szöveghez)
        """
        # ...
    
    def run_analysis(
        self,
        inputs: Dict[str, pd.DataFrame],
        plot_paths: Dict[str, Path],
        extra_context: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Teljes AI elemzés futtatása.
        
        Args:
            inputs: Input DataFrame-ek
            plot_paths: Plot fájlok elérési útjai
            extra_context: További kontextus
            
        Returns:
            Dictionary az elemzések eredményeivel
        """
        # ...
```

#### 2.3.3 `mypy` konfiguráció: `mypy.ini`
```ini
[mypy]
python_version = 3.10
warn_return_any = True
warn_unused_configs = True
disallow_untyped_defs = False  # Fokozatosan true-ra állítjuk
disallow_incomplete_defs = True
check_untyped_defs = True
no_implicit_optional = True
warn_redundant_casts = True
warn_unused_ignores = True

[mypy-pandas.*]
ignore_missing_imports = True

[mypy-plotly.*]
ignore_missing_imports = True
```

#### 2.3.4 Pre-commit hook: `.pre-commit-config.yaml`
```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
  
  - repo: https://github.com/psf/black
    rev: 23.3.0
    hooks:
      - id: black
  
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.3.0
    hooks:
      - id: mypy
        additional_dependencies: [types-requests]
```

### 2.4 Következő lépések
1. ✅ `mypy` hozzáadása `requirements.txt`-hez
2. ✅ Type hints hozzáadása a főbb függvényekhez
3. ✅ Docstring-ek írása (Google style)
4. ✅ `mypy.ini` konfiguráció
5. ✅ Pre-commit hookok beállítása

---

## 3. ERROR HANDLING ÉS LOGGING

### 3.1 Jelenlegi helyzet
- ⚠️ Alapvető try-except blokkok vannak
- ⚠️ Egyszerű logging (INFO, WARNING, ERROR)
- ❌ Nincs strukturált logging (JSON)
- ❌ Nincs retry mechanizmus API hívásokhoz
- ❌ Nincs error tracking (Sentry)
- ❌ Nincs graceful degradation

### 3.2 Célok
- Strukturált logging (JSON formátum)
- Retry mechanizmusok (exponential backoff)
- Error tracking integráció
- Graceful degradation (LLM/API hiba esetén)
- Error recovery stratégia

### 3.3 Implementációs terv

#### 3.3.1 Strukturált logging: `logging_config.py`
```python
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

class JSONFormatter(logging.Formatter):
    """JSON formátumú log formatter."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Exception info hozzáadása
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Extra mezők
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)

def setup_logging(log_dir: Path, level: str = "INFO") -> None:
    """Logging beállítása."""
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # File handler (JSON)
    file_handler = logging.FileHandler(log_dir / "pipeline.log")
    file_handler.setFormatter(JSONFormatter())
    
    # Console handler (human-readable)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Noisy library-ek csendesítése
    for lib in ['kaleido', 'urllib3', 'matplotlib']:
        logging.getLogger(lib).setLevel(logging.CRITICAL)
```

#### 3.3.2 Retry mechanizmus: `retry_utils.py`
```python
import time
import logging
from functools import wraps
from typing import Callable, TypeVar, Tuple
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)

T = TypeVar('T')

def retry_with_backoff(
    max_attempts: int = 3,
    initial_wait: float = 1.0,
    max_wait: float = 60.0,
    exceptions: Tuple[Exception, ...] = (Exception,)
):
    """
    Retry decorator exponential backoff-fal.
    
    Args:
        max_attempts: Maximális próbálkozások száma
        initial_wait: Kezdeti várakozási idő (másodperc)
        max_wait: Maximális várakozási idő (másodperc)
        exceptions: Milyen exception-ökre próbáljuk újra
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=initial_wait, max=max_wait),
            retry=retry_if_exception_type(exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Használat:
@retry_with_backoff(max_attempts=3, exceptions=(requests.RequestException,))
def fetch_news(api_key: str, cse_id: str, query: str) -> pd.DataFrame:
    """News fetch retry logikával."""
    # ...
```

#### 3.3.3 Error tracking: `error_tracking.py`
```python
import logging
import os
from typing import Optional

# Sentry integráció (opcionális)
try:
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration
    SENTRY_AVAILABLE = True
except ImportError:
    SENTRY_AVAILABLE = False

def setup_error_tracking(dsn: Optional[str] = None) -> None:
    """Error tracking beállítása (Sentry)."""
    if not SENTRY_AVAILABLE:
        logging.warning("Sentry not available, error tracking disabled")
        return
    
    if not dsn:
        dsn = os.getenv("SENTRY_DSN")
    
    if dsn:
        sentry_sdk.init(
            dsn=dsn,
            integrations=[
                LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
            ],
            traces_sample_rate=0.1,  # 10% of transactions
            environment=os.getenv("ENVIRONMENT", "development"),
        )
        logging.info("Error tracking initialized")
    else:
        logging.warning("SENTRY_DSN not set, error tracking disabled")
```

#### 3.3.4 Graceful degradation: `llm_analysis.py` módosítás
```python
from typing import Optional, List
from enum import Enum

class LLMStatus(Enum):
    """LLM státusz enum."""
    AVAILABLE = "available"
    DEGRADED = "degraded"  # Fallback módban
    UNAVAILABLE = "unavailable"

class LLMAnalyzer:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.status = LLMStatus.AVAILABLE
        self._test_connection()
    
    def _test_connection(self) -> None:
        """LLM kapcsolat tesztelése."""
        try:
            llm = self._get_llm(temperature=0.0)
            # Egyszerű teszt prompt
            test_response = (llm | StrOutputParser()).invoke([
                HumanMessage(content="Say 'OK'")
            ])
            if "OK" in test_response.upper():
                self.status = LLMStatus.AVAILABLE
                logger.info("LLM connection successful")
            else:
                raise ValueError("Unexpected LLM response")
        except Exception as e:
            logger.error(f"LLM connection test failed: {e}")
            self.status = LLMStatus.UNAVAILABLE
    
    def extract_keywords(
        self, 
        text_list: List[str], 
        context: str = "targeted risk"
    ) -> List[str]:
        """
        Kulcsszavak kinyerése, fallback logikával.
        """
        if self.status == LLMStatus.UNAVAILABLE:
            logger.warning("LLM unavailable, using regex fallback")
            return self._extract_keywords_regex(text_list)
        
        try:
            # Normál LLM hívás
            return self._extract_keywords_llm(text_list, context)
        except Exception as e:
            logger.error(f"LLM keyword extraction failed: {e}")
            # Fallback regex-re
            return self._extract_keywords_regex(text_list)
    
    def _extract_keywords_regex(self, text_list: List[str]) -> List[str]:
        """Regex alapú fallback kulcsszó kinyerés."""
        # Egyszerű regex logika
        keywords = []
        for text in text_list:
            found = []
            if "credit" in text.lower() or "lending" in text.lower():
                found.append("Credit Growth")
            if "real estate" in text.lower() or "mortgage" in text.lower():
                found.append("Real Estate")
            # ...
            keywords.append(" | ".join(found) if found else "General Monitoring")
        return keywords
```

#### 3.3.5 Error recovery stratégia: `main.py` módosítás
```python
from enum import Enum
from typing import Dict, Any

class PipelineStatus(Enum):
    """Pipeline státusz."""
    SUCCESS = "success"
    PARTIAL = "partial"  # Néhány komponens sikertelen
    FAILED = "failed"

def main() -> PipelineStatus:
    """Main pipeline graceful degradation-nel."""
    logger.info("STARTING PIPELINE...")
    status = PipelineStatus.SUCCESS
    
    # 1. ETL (kritikus - ha ez sikertelen, leállunk)
    try:
        etl = ETLPipeline(...)
        data = etl.run_pipeline()
    except Exception as e:
        logger.error(f"ETL failed: {e}", extra={"component": "etl"})
        return PipelineStatus.FAILED
    
    # 2. Visualization (nem kritikus)
    try:
        viz = Visualizer(...)
        plots = viz.generate_all_plots(data)
    except Exception as e:
        logger.error(f"Visualization failed: {e}", extra={"component": "visualization"})
        status = PipelineStatus.PARTIAL
        plots = {}  # Üres dict, hogy ne törjön el a render
    
    # 3. LLM Analysis (nem kritikus - fallback van)
    try:
        analyzer = LLMAnalyzer(LLM_CONFIG)
        analyses = analyzer.run_analysis(...)
    except Exception as e:
        logger.error(f"LLM analysis failed: {e}", extra={"component": "llm"})
        status = PipelineStatus.PARTIAL
        # Fallback: üres elemzések
        analyses = {key: "Analysis temporarily unavailable" for key in DEFAULT_ANALYSIS_IDS}
    
    # 4. Render (mindig próbáljuk meg, még ha hiányos is)
    try:
        rendered_html = render_report(...)
        with open("index.html", "w", encoding="utf-8") as f:
            f.write(rendered_html)
        logger.info("Pipeline completed", extra={"status": status.value})
    except Exception as e:
        logger.error(f"Render failed: {e}", extra={"component": "render"})
        return PipelineStatus.FAILED
    
    return status
```

### 3.4 Következő lépések
1. ✅ `tenacity` hozzáadása `requirements.txt`-hez (retry)
2. ✅ Strukturált logging implementálása
3. ✅ Retry mechanizmusok hozzáadása
4. ✅ Graceful degradation LLM-hez
5. ✅ Error recovery stratégia a main pipeline-ban

---

## ÖSSZEFOGLALÁS ÉS PRIORITÁSOK

### Rövid táv (1-2 hét)
1. **Tesztelés**: Unit tesztek ETL-hez és data parsing-hoz
2. **Type hints**: Főbb függvényekhez
3. **Strukturált logging**: JSON formátum

### Közép táv (1 hónap)
1. **Integration tesztek**: Teljes pipeline
2. **Error handling**: Retry mechanizmusok, graceful degradation
3. **Docstring-ek**: Google style dokumentáció

### Hosszú táv (2-3 hónap)
1. **Error tracking**: Sentry integráció
2. **CI/CD**: Automatikus tesztelés
3. **Data validation**: Comprehensive quality checks

---

## HASZNOS LINKek

- [pytest dokumentáció](https://docs.pytest.org/)
- [mypy dokumentáció](https://mypy.readthedocs.io/)
- [tenacity (retry library)](https://tenacity.readthedocs.io/)
- [Sentry Python SDK](https://docs.sentry.io/platforms/python/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
