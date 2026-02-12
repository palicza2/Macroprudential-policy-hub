# 🚀 Macro Policy Hub - Átfogó Fejlesztési Roadmap

## 📋 Tartalomjegyzék

1. [Áttekintés](#áttekintés)
2. [Rövid távú fejlesztések (1-4 hét)](#rövid-távú-fejlesztések)
3. [Közép távú fejlesztések (1-3 hónap)](#közép-távú-fejlesztések)
4. [Hosszú távú fejlesztések (3-6 hónap)](#hosszú-távú-fejlesztések)
5. [Technikai fejlesztések](#technikai-fejlesztések)
6. [Prioritások és időzítés](#prioritások-és-időzítés)

---

## 📊 Áttekintés

### Jelenlegi állapot

**✅ Már implementálva:**
- Moduláris ETL pipeline (CCyB, SyRB, BBM, O-SII)
- AI-driven insights (Gemini 2.5 Flash Lite)
- Country Profiles (interaktív ország-specifikus nézetek)
- Knowledge Graph (alapvető kapcsolatok modellezése)
- Grounded Validation (LangGraph-alapú validáció)
- News enrichment (tagging, summarization)

**⚠️ Fejlesztési lehetőségek:**
- RAG rendszer (Retrieval-Augmented Generation)
- Knowledge Graph vektorizálása
- Knowledge Graph interaktív vizualizáció
- Temporal comparison & change detection
- Predictive analytics
- Agent-based workflows
- Multi-agent collaboration

---

## 🎯 Rövid távú fejlesztések (1-4 hét)

### 1. Knowledge Graph Vektorizálás ⭐ **PRIORITÁS**

**Cél:** A knowledge graph szemantikusan kereshetővé tétele vektor embeddinggel.

**Időtartam:** 1-2 hét

**Költség:** **€0** (teljesen ingyenes - lokális eszközök)

**Technológia:**
- ChromaDB (lokális vector database)
- Sentence Transformers (lokális embedding modell)
- Nincs API költség

**Implementáció:**
```python
# knowledge_graph/vector_retriever.py
from sentence_transformers import SentenceTransformer
import chromadb

class VectorizedKGRetriever:
    def __init__(self):
        self.embeddings = SentenceTransformer('all-MiniLM-L6-v2')
        self.client = chromadb.PersistentClient(path="data/kg_vectorstore")
        self.collection = self.client.get_or_create_collection("kg_chunks")
    
    def build_vector_index(self, graph_data):
        """Graph adatok vektorizálása rich text kontextussal."""
        # Node chunks (bővített szöveggel: justification, recent changes)
        # Edge chunks (kapcsolatok)
        # ChromaDB-be mentés
```

**Előnyök:**
- ✅ Szemantikus keresés (nem csak keyword matching)
- ✅ Rich text kontextus (justification szövegek, recent changes)
- ✅ Azonnali érték
- ✅ Teljesen ingyenes

**Dokumentáció:** `RAG_VECTOR_ANALYSIS.md`

---

### 2. RAG Rendszer Dokumentumokhoz ⭐ **PRIORITÁS**

**Cél:** Korábbi jelentések, policy dokumentumok vektorizálása RAG kontextushoz.

**Időtartam:** 2-3 hét

**Költség:** **€0** (teljesen ingyenes - lokális eszközök)

**Technológia:**
- ChromaDB (lokális)
- Sentence Transformers (lokális)
- LangChain dokumentum loaderek

**Implementáció:**
```python
# rag_system_free.py
from rag_system_free import FreeRAGSystem

rag = FreeRAGSystem()
rag.build_knowledge_base([
    Path("reports/previous_reports/"),  # Korábbi jelentések
    Path("data/justifications/"),      # CCyB/SyRB indoklások
    Path("docs/policy_papers/"),       # ESRB guidelines
])
```

**Előnyök:**
- ✅ Korábbi jelentések kontextusa
- ✅ Policy dokumentumok kontextusa
- ✅ Automatikus anomáliadetektálás
- ✅ Policy konzisztencia ellenőrzés

**Használati esetek:**
- "Hogyan változott a CCyB trend az elmúlt 2 évben?"
- "Konzisztens-e az aktuális döntés a korábbi policy-vel?"
- "Láttunk-e hasonló helyzetet korábban?"

**Dokumentáció:** `RAG_FREE_IMPLEMENTATION.md`, `RAG_BUSINESS_VALUE.md`

---

### 3. Hybrid RAG (Graph + Documents)

**Cél:** Graph és dokumentumok kombinálása egyetlen RAG rendszerben.

**Időtartam:** 1 hét

**Költség:** **€0** (már minden eszköz megvan)

**Implementáció:**
```python
# hybrid_rag.py
class HybridRAG:
    def __init__(self, kg_retriever, doc_retriever):
        self.kg_retriever = kg_retriever
        self.doc_retriever = doc_retriever
    
    def retrieve_context(self, query: str, k: int = 5):
        # Graph kontextus
        kg_results = self.kg_retriever.retrieve_context(query, k=3)
        # Dokumentum kontextus
        doc_results = self.doc_retriever.retrieve_context(query, k=3)
        # Kombináció
        return {'graph_context': kg_results, 'document_context': doc_results}
```

**Előnyök:**
- ✅ Graph struktúra + szöveges kontextus
- ✅ Rich answers
- ✅ Teljes RAG rendszer

---

### 4. Knowledge Graph Interaktív Vizualizáció (Opcionális)

**Cél:** Interaktív graph vizualizáció a dashboard-on.

**Időtartam:** 1-2 hét

**Költség:** **€0** (vis.js vagy Cytoscape.js CDN)

**Technológia:**
- vis.js Network vagy Cytoscape.js (JavaScript)
- Pre-computed JSON (Python generálja)
- Client-side rendering

**Funkciók:**
- Click: Navigáció vagy részletek
- Hover: Tooltip információk
- Filter: Szűrés measure/region szerint
- Search: Gyors keresés
- Zoom & Pan: Nagyítás, mozgatás
- Highlight: Kapcsolódó node-ok kiemelése

**Megjegyzés:** Korábban eltávolítottuk, mert "átláthatatlan" volt. Most feltételes megjelenítéssel és filtered views-szal újra bevezethető.

**Dokumentáció:** `KNOWLEDGE_GRAPH_VISUALIZATION_EXAMPLES.md`

---

## 📈 Közép távú fejlesztések (1-3 hónap)

### 5. Temporal Comparison & Change Detection

**Cél:** Automatikus változás detektálás és korábbi jelentések összehasonlítása.

**Időtartam:** 2-3 hét

**Költség:** **€0** (lokális adatfeldolgozás)

**Implementáció:**
```python
# temporal_comparison.py
class TemporalComparator:
    def compare_reports(
        self,
        current_data: Dict[str, pd.DataFrame],
        comparison_periods: List[str] = ["1M", "3M", "6M", "12M"]
    ) -> Dict[str, Any]:
        # CCyB változások
        # SyRB változások
        # Trend analysis
        # Anomaly detection
```

**Funkciók:**
- "What changed?" dashboard
- Trend alerts (email/Slack értesítések)
- Historical context ("Hasonló helyzet volt-e korábban?")

**Dokumentáció:** `AI_ENHANCEMENT_ROADMAP.md` (3. fejezet)

---

### 6. Anomáliadetektálás

**Cél:** Automatikus anomáliák észlelése rate változásokban.

**Időtartam:** 1 hét

**Költség:** **€0** (scikit-learn, numpy)

**Implementáció:**
```python
# anomaly_detection.py
class AnomalyDetector:
    def detect_rate_anomalies(
        self,
        current_rates: pd.DataFrame,
        historical_rates: pd.DataFrame,
        threshold: float = 2.0  # Z-score threshold
    ) -> List[Dict[str, Any]]:
        # Z-score számítás
        # Anomáliák listázása
```

**Használati esetek:**
- "Netherlands SyRB 50% - valószínűleg hiba (korábbi max: 3%)"
- Automatikus figyelmeztetések

---

### 7. Agent-based Workflows

**Cél:** Több lépéses, autonóm döntéshozatal LangGraph agentekkel.

**Időtartam:** 2-3 hét

**Költség:** **€0** (LangGraph már használjuk)

**Implementáció:**
```python
# agent_system.py
class MacroprudentialAgent:
    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(AgentState)
        workflow.add_node("data_retriever", self._retrieve_data)
        workflow.add_node("analyzer", self._analyze_data)
        workflow.add_node("validator", self._validate_analysis)
        workflow.add_node("refiner", self._refine_analysis)
        workflow.add_node("synthesizer", self._synthesize_results)
        return workflow.compile()
```

**Használati esetek:**
- "Elemezd a CCyB trendet és jelezz anomáliákat"
- "Hasonlítsd össze a jelenlegi helyzetet a 2020-as válsággal"
- Self-correction: Automatikus hibajavítás validáció alapján

**Dokumentáció:** `AI_ENHANCEMENT_ROADMAP.md` (2. fejezet)

---

### 8. Predictive Analytics

**Cél:** Trend előrejelzés és scenario analysis.

**Időtartam:** 2 hét

**Költség:** **€0** (scikit-learn, statsmodels)

**Implementáció:**
```python
# predictive_analytics.py
class PredictiveAnalytics:
    def forecast_ccyb_trends(
        self,
        historical_data: pd.DataFrame,
        forecast_horizon: int = 6  # months
    ) -> Dict[str, Dict[str, float]]:
        # Time series előkészítése
        # Trend fitting (polynomial regression)
        # Forecast generálása
```

**Használati esetek:**
- "Mi várható a következő 6 hónapban?"
- "Mely országokban várható policy változás?"
- Scenario planning: "Mi történne, ha recesszió következne be?"

**Dokumentáció:** `AI_ENHANCEMENT_ROADMAP.md` (4. fejezet)

---

## 🔮 Hosszú távú fejlesztések (3-6 hónap)

### 9. Multi-agent Collaboration

**Cél:** Specialist agentek (CCyB, SyRB, BBM) együttműködő elemzése.

**Időtartam:** 3-4 hét

**Költség:** **€0** (LangGraph már használjuk)

**Implementáció:**
```python
# multi_agent_system.py
class SpecialistAgent:
    def __init__(self, name: str, expertise: str):
        # CCyB expert, SyRB expert, BBM expert
        self.system_prompt = self._build_system_prompt()

class MultiAgentSystem:
    def collaborative_analysis(self, data, question):
        # Minden agent elemzése
        # Coordinator szintetizálja
        # Consensus building
```

**Használati esetek:**
- Expert panel: Több specialist véleménye
- Consensus analysis: Többségi vélemény kialakítása
- Cross-validation: Agentek közötti validáció

**Dokumentáció:** `AI_ENHANCEMENT_ROADMAP.md` (5. fejezet)

---

### 10. Temporal Knowledge Graph

**Cél:** Időbeli változások követése a graphban.

**Időtartam:** 2 hét

**Költség:** **€0**

**Implementáció:**
```python
# Temporal edges
{
    'from': 'HU',
    'to': 'CCyB_HU',
    'label': 'HAS',
    'start_date': '2024-01-15',
    'end_date': None,  # Still active
    'rate_history': [0.0, 1.0, 2.5]  # Historical rates
}
```

**Használat:**
- "Mikor változott utoljára Magyarország CCyB-je?"
- "Mely országok aktiváltak SyRB-t az elmúlt 6 hónapban?"

**Dokumentáció:** `DEVELOPMENT_ROADMAP_V2.md` (3.1 fejezet)

---

### 11. Risk Factor Integration

**Cél:** Risk faktorok hozzáadása a knowledge graphhoz.

**Időtartam:** 1-2 hét

**Költség:** **€0**

**Implementáció:**
```python
# Új node típusok
{
    'id': 'RISK_CreditGrowth',
    'label': 'Credit Growth Risk',
    'group': 'risk_factor',
    'severity': 'high',
    'countries_affected': ['HU', 'PL', 'CZ']
}

# Kapcsolatok
[CCyB: 2.5%] --ADDRESSES--> [Credit Growth Risk]
[Hungary] --EXPOSED_TO--> [Credit Growth Risk]
```

**Dokumentáció:** `DEVELOPMENT_ROADMAP_V2.md` (3.2 fejezet)

---

### 12. Cross-Country Comparison Engine

**Cél:** Automatikus összehasonlítás és benchmarking.

**Időtartam:** 2 hét

**Költség:** **€0**

**Implementáció:**
```python
# comparison_engine.py
class CountryComparator:
    def find_similar_countries(
        self, 
        country: str, 
        criteria: List[str]
    ):
        # Policy mix similarity
        # Capital buffer levels
        # Risk profile
        # Regional proximity
```

**Dokumentáció:** `DEVELOPMENT_ROADMAP_V2.md` (3.3 fejezet)

---

## 🔧 Technikai fejlesztések

### 13. Tesztelés és minőségbiztosítás

**Cél:** Unit tesztek, integration tesztek, data validation tesztek.

**Időtartam:** 2-3 hét

**Költség:** **€0** (pytest, pytest-cov)

**Implementáció:**
```python
# tests/
├── unit/
│   ├── test_etl.py
│   ├── test_llm_analysis.py
│   └── test_data_parsing.py
├── integration/
│   ├── test_pipeline.py
│   └── test_end_to_end.py
└── data_quality/
    ├── test_data_validation.py
    └── test_anomaly_detection.py
```

**Dokumentáció:** `DEVELOPMENT_ROADMAP.md` (1. fejezet)

---

### 14. Type Hints és Dokumentáció

**Cél:** Teljes type hint coverage, docstring-ek, API dokumentáció.

**Időtartam:** 1-2 hét

**Költség:** **€0** (mypy, sphinx)

**Implementáció:**
```python
def _extract_rate_from_text(
    self, 
    text: Optional[str]
) -> float:
    """
    Szövegből rate kinyerése regex-szel.
    
    Args:
        text: Szöveg, ami tartalmazhat rate információt
        
    Returns:
        Kinyert rate (0.0-20.0% között), vagy 0.0 ha nincs találat
    """
```

**Dokumentáció:** `DEVELOPMENT_ROADMAP.md` (2. fejezet)

---

### 15. Error Handling és Logging

**Cél:** Strukturált logging, retry mechanizmusok, graceful degradation.

**Időtartam:** 1-2 hét

**Költség:** **€0** (tenacity, logging)

**Implementáció:**
```python
# logging_config.py
class JSONFormatter(logging.Formatter):
    """JSON formátumú log formatter."""

# retry_utils.py
@retry_with_backoff(max_attempts=3, exceptions=(requests.RequestException,))
def fetch_news(api_key: str, cse_id: str, query: str):
    """News fetch retry logikával."""
```

**Dokumentáció:** `DEVELOPMENT_ROADMAP.md` (3. fejezet)

---

### 16. Caching & Performance

**Cél:** Redis cache, parquet optimalizálás, lazy loading.

**Időtartam:** 1 hét

**Költség:** **€0** (lokális cache) vagy **€5-10/hó** (Redis Cloud)

**Implementáció:**
```python
# caching.py
from functools import lru_cache
import redis

@lru_cache(maxsize=100)
def get_country_profile(country: str):
    """Country profile cache-elése."""
```

---

## 📅 Prioritások és időzítés

### Fázis 1: Foundation (1-2 hét) ⭐ **KÖZVETLEN PRIORITÁS**

1. ✅ **Knowledge Graph vektorizálás** (1-2 hét)
   - Szemantikus keresés
   - Rich text kontextus
   - **Költség: €0**

2. ✅ **RAG rendszer dokumentumokhoz** (2-3 hét)
   - Korábbi jelentések
   - Policy dokumentumok
   - **Költség: €0**

**Összesen: 3-5 hét, €0 költség**

---

### Fázis 2: Enhancement (2-4 hét)

3. ✅ **Hybrid RAG** (1 hét)
   - Graph + dokumentumok kombinációja
   - **Költség: €0**

4. ✅ **Temporal comparison** (2-3 hét)
   - Változás detektálás
   - Trend analysis
   - **Költség: €0**

5. ✅ **Anomáliadetektálás** (1 hét)
   - Automatikus észlelés
   - **Költség: €0**

**Összesen: 4-5 hét, €0 költség**

---

### Fázis 3: Advanced (4-6 hét)

6. ✅ **Agent-based workflows** (2-3 hét)
   - LangGraph agentek
   - **Költség: €0**

7. ✅ **Predictive analytics** (2 hét)
   - Trend előrejelzés
   - **Költség: €0**

8. ✅ **Knowledge Graph vizualizáció** (1-2 hét, opcionális)
   - Interaktív graph
   - **Költség: €0**

**Összesen: 5-7 hét, €0 költség**

---

### Fázis 4: Long-term (6-12 hét)

9. ✅ **Multi-agent collaboration** (3-4 hét)
10. ✅ **Temporal Knowledge Graph** (2 hét)
11. ✅ **Risk Factor Integration** (1-2 hét)
12. ✅ **Cross-Country Comparison Engine** (2 hét)
13. ✅ **Tesztelés** (2-3 hét)
14. ✅ **Type Hints és Dokumentáció** (1-2 hét)
15. ✅ **Error Handling és Logging** (1-2 hét)
16. ✅ **Caching & Performance** (1 hét)

**Összesen: 13-18 hét, €0 költség**

---

## 💰 Költség összefoglalás

### Fejlesztési költség

| Fázis | Időtartam | Költség (ha te fejleszted) | Költség (ha külső fejlesztő) |
|-------|-----------|---------------------------|----------------------------|
| **Fázis 1** | 3-5 hét | **€0** ✅ | €12,000-20,000 |
| **Fázis 2** | 4-5 hét | **€0** ✅ | €16,000-20,000 |
| **Fázis 3** | 5-7 hét | **€0** ✅ | €20,000-28,000 |
| **Fázis 4** | 13-18 hét | **€0** ✅ | €52,000-72,000 |
| **Összesen** | **25-35 hét** | **€0** ✅ | **€100,000-140,000** |

### Működési költség

| Komponens | Költség | Megjegyzés |
|-----------|---------|------------|
| Vector DB (ChromaDB lokális) | **€0/hó** | Ingyenes |
| Embeddings (Sentence Transformers) | **€0/hó** | Lokális, ingyenes |
| LLM (Gemini 2.5 Flash Lite) | **€0/hó** | Már használod, ingyenes tier |
| Storage | **€0/hó** | Lokális fájlrendszer |
| **Összesen** | **€0/hó** | **Teljesen ingyenes** ✅ |

---

## 🎯 ROI Számítás

### Ha TE fejleszted (€0 fejlesztési költség):

**Scenario: Havi 1 jelentés**
- Időmegtakarítás: 12 óra/év × €50/óra = **€600/év**
- Minőség javulás: €600/év
- **Összesen: €1,200/év**
- **ROI: Végtelen** ✅ (€0 befektetés)

**Scenario: Heti 1 jelentés**
- Időmegtakarítás: 52 óra/év × €50/óra = **€2,600/év**
- Minőség javulás: €2,600/év
- **Összesen: €5,200/év**
- **ROI: Végtelen** ✅ (€0 befektetés)

**Scenario: Napi 1 jelentés**
- Időmegtakarítás: 300 óra/év × €50/óra = **€15,000/év**
- Minőség javulás: €15,000/év
- **Összesen: €30,000/év**
- **ROI: Végtelen** ✅ (€0 befektetés)

---

## 📚 Dokumentáció hivatkozások

### RAG és Vektorizálás
- `RAG_VECTOR_ANALYSIS.md` - Részletes elemzés RAG + vektor embedding előnyeiről
- `RAG_FREE_IMPLEMENTATION.md` - Ingyenes RAG implementáció útmutató
- `RAG_BUSINESS_VALUE.md` - Üzleti érték elemzés

### Knowledge Graph
- `KNOWLEDGE_GRAPH_VISUALIZATION_EXAMPLES.md` - Interaktív vizualizációs példák
- `KNOWLEDGE_GRAPH_ANALYSIS.md` - Graph elemzés és használat

### AI Fejlesztések
- `AI_ENHANCEMENT_ROADMAP.md` - 2026-os AI trendek (RAG, Agents, Predictive)
- `DEVELOPMENT_ROADMAP_V2.md` - Knowledge Graph refactoring és RAG integráció
- `DEVELOPMENT_ROADMAP.md` - Technikai fejlesztések (tesztelés, type hints, error handling)

---

## 🚀 Következő lépések

### Azonnali (ez a hét)

1. ✅ **Döntés:** Melyik fázist kezdjük el?
   - **Javaslat:** Fázis 1 - Knowledge Graph vektorizálás (1-2 hét)

2. ✅ **Függőségek telepítése:**
   ```bash
   pip install sentence-transformers chromadb langchain-community
   ```

3. ✅ **Prototípus:** KG vektorizálás implementálása

### Rövid táv (1-2 hét)

4. ✅ **RAG rendszer dokumentumokhoz** implementálása
5. ✅ **Hybrid RAG** integráció
6. ✅ **Tesztelés** és validáció

### Közép táv (1 hónap)

7. ✅ **Temporal comparison** hozzáadása
8. ✅ **Anomáliadetektálás** implementálása
9. ✅ **Agent-based workflows** (ha szükséges)

---

## ✅ Összefoglalás

### Főbb előnyök

1. **Teljesen ingyenes** - €0 fejlesztés + €0/hó működés
2. **Azonnali érték** - Szemantikus keresés, kontextus-aware elemzések
3. **Skálázható** - Könnyen bővíthető később
4. **Modern tech stack** - 2026-os AI trendek (RAG, Agents, Vector DB)

### Prioritások

1. ⭐ **Fázis 1:** Knowledge Graph vektorizálás + RAG dokumentumokhoz (3-5 hét)
2. ⭐ **Fázis 2:** Hybrid RAG + Temporal comparison (4-5 hét)
3. ⭐ **Fázis 3:** Agent-based workflows + Predictive analytics (5-7 hét)

**Készen állsz az implementációra?** 🚀

---

*Utolsó frissítés: 2024*
