# 🚀 Fejlesztési Roadmap v2.0

## 📊 Jelenlegi Állapot Elemzése

### ✅ Erősségek
- **Moduláris ETL pipeline**: Robusztus adatfeldolgozás
- **AI-driven insights**: Gemini 2.5 Flash Lite integráció
- **Country Profiles**: Interaktív ország-specifikus nézetek
- **Knowledge Graph**: Alapvető kapcsolatok modellezése
- **Grounded Validation**: LangGraph-alapú validáció

### ⚠️ Fejlesztési Lehetőségek
1. **Knowledge Graph pozíció**: Jelenleg a Country Profiles blokkban van, de lehetne önálló, központi komponens
2. **RAG hiánya**: Nincs strukturált RAG rendszer a knowledge graph alapján
3. **Temporal tracking**: Időbeli változások követése korlátozott
4. **Multi-modal AI**: Csak szöveg és táblázatok, nincs kép/video elemzés
5. **Predictive analytics**: Nincs előrejelzés vagy trend predikció

---

## 🎯 Prioritásos Fejlesztési Javaslatok

### 1. 🕸️ Knowledge Graph Refactoring & RAG Integration

#### 1.1 Knowledge Graph Áthelyezése (Közvetlen prioritás)

**Jelenlegi helyzet**: A knowledge graph a Country Profiles blokkban van, de logikailag központi komponens.

**Javaslat**: 
- **Önálló "Knowledge Graph" szekció** a dashboard-on
- **Központi pozíció** a mermaid diagramban: `Data Enrichment` → `Knowledge Graph Builder` → **Mindkét irányba** (Country Profiles ÉS AI Analysis)
- **Dual purpose**: 
  - **Vizualizáció** (opcionális, ha kell)
  - **RAG context provider** (fő cél)

**Implementáció**:
```python
# Új struktúra:
country_profiles/
├── profile_generator.py
├── knowledge_graph_builder.py  # ← Központi komponens
└── rag_retriever.py           # ← ÚJ: RAG context retrieval
```

#### 1.2 RAG (Retrieval-Augmented Generation) Implementáció

**Cél**: A knowledge graph használata kontextus retrieval-re az AI elemzésekhez.

**Architektúra**:
```
Knowledge Graph → Vector Store (embeddings) → RAG Retriever → LLM Context
```

**Implementáció lépések**:

1. **Graph-to-Text Conversion**:
   ```python
   # country_profiles/rag_retriever.py
   def graph_to_text_chunks(graph_data):
       """Convert graph nodes/edges to searchable text chunks"""
       chunks = []
       for node in graph_data['nodes']:
           chunk = f"Country: {node['label']}, Type: {node['group']}, "
           chunk += f"Value: {node.get('value', 'N/A')}, Region: {node.get('region', 'N/A')}"
           chunks.append(chunk)
       return chunks
   ```

2. **Vector Embeddings** (Opcionális, de ajánlott):
   - **Ingyenes opció**: `sentence-transformers` (all-MiniLM-L6-v2)
   - **Cloud opció**: Google Vertex AI Embeddings
   - **Storage**: In-memory (faiss) vagy SQLite (chromadb)

3. **RAG Retriever**:
   ```python
   class KnowledgeGraphRAG:
       def retrieve_context(self, query: str, top_k: int = 5):
           """Retrieve relevant graph context for a query"""
           # 1. Query embedding
           # 2. Similarity search in graph chunks
           # 3. Return relevant nodes + edges
           pass
   ```

4. **LLM Integration**:
   ```python
   # llm_analysis.py
   def analyze_with_rag(self, query, graph_data):
       rag = KnowledgeGraphRAG()
       context = rag.retrieve_context(query)
       prompt = f"Context from knowledge graph:\n{context}\n\nQuery: {query}"
       return self.llm.generate(prompt)
   ```

**Használati esetek**:
- ✅ **"Mely országok hasonlóak Magyarországhoz?"** → RAG visszaadja a SIMILAR kapcsolatokat
- ✅ **"Hol van aktív CCyB és SyRB együtt?"** → RAG visszaadja a COEXISTS kapcsolatokat
- ✅ **"Mely régiókban van hasonló policy mix?"** → RAG visszaadja a regionális pattern-eket

**Üzleti érték**:
- 🎯 **Pontosabb AI válaszok**: Kontextus-alapú generálás
- 🎯 **Gyorsabb keresés**: Nem kell teljes graphot átadni
- 🎯 **Skálázható**: Nagy graph esetén is hatékony

---

### 2. 🤖 Advanced AI Features

#### 2.1 Multi-Modal AI Analysis

**Jelenleg**: Csak szöveg és táblázatok

**Javaslat**: 
- **Chart image analysis**: Plotly chart-ok screenshot-jainak elemzése
- **Document parsing**: ESRB PDF-ek automatikus feldolgozása
- **News image analysis**: Hírcikkek képeinek elemzése (ha releváns)

**Implementáció**:
```python
# llm_analysis.py
def analyze_chart_image(self, chart_path: str):
    """Analyze chart using Gemini Vision API"""
    with open(chart_path, 'rb') as f:
        image = f.read()
    
    prompt = "Analyze this macroprudential policy chart. Identify trends, anomalies, and key insights."
    return self.llm.generate_with_image(prompt, image)
```

#### 2.2 Predictive Analytics

**Cél**: Trend előrejelzés és anomália detektálás

**Javaslatok**:
- **Time-series forecasting**: Prophet vagy LSTM modellek
- **Anomaly detection**: Isolation Forest vagy Autoencoders
- **Policy change prediction**: "Mely országok valószínűleg változtatnak CCyB-en?"

**Implementáció**:
```python
# predictive_analytics.py
class PolicyPredictor:
    def predict_ccyb_changes(self, country: str, horizon_months: int = 6):
        """Predict CCyB rate changes for next N months"""
        # 1. Historical data
        # 2. Feature engineering (credit gap, GDP growth, etc.)
        # 3. Model prediction
        pass
```

#### 2.3 Conversational AI Interface

**Cél**: Természetes nyelvű chatbot a dashboard-on

**Javaslat**:
- **Streamlit vagy Gradio** integráció
- **RAG + Knowledge Graph** alapú válaszadás
- **"Mely országok használnak CRE-specifikus SyRB-t?"** típusú kérdések

---

### 3. 📈 Enhanced Data & Analytics

#### 3.1 Temporal Knowledge Graph

**Cél**: Időbeli változások követése a graphban

**Javaslat**:
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

**Használat**:
- **"Mikor változott utoljára Magyarország CCyB-je?"**
- **"Mely országok aktiváltak SyRB-t az elmúlt 6 hónapban?"**

#### 3.2 Risk Factor Integration

**Cél**: Risk faktorok hozzáadása a knowledge graphhoz

**Javaslat**:
```python
# Új node típusok
{
    'id': 'RISK_CreditGrowth',
    'label': 'Credit Growth Risk',
    'group': 'risk_factor',
    'severity': 'high',
    'countries_affected': ['HU', 'PL', 'CZ']
}
```

**Kapcsolatok**:
- `[CCyB: 2.5%] --ADDRESSES--> [Credit Growth Risk]`
- `[Hungary] --EXPOSED_TO--> [Credit Growth Risk]`

#### 3.3 Cross-Country Comparison Engine

**Cél**: Automatikus összehasonlítás és benchmarking

**Javaslat**:
```python
# comparison_engine.py
class CountryComparator:
    def find_similar_countries(self, country: str, criteria: List[str]):
        """Find similar countries based on multiple criteria"""
        # 1. Policy mix similarity
        # 2. Capital buffer levels
        # 3. Risk profile
        # 4. Regional proximity
        pass
```

---

### 4. 🎨 UI/UX Enhancements

#### 4.1 Interactive Knowledge Graph Visualization (Opcionális)

**Megjegyzés**: Korábban eltávolítottuk a vizualizációt, mert "átláthatatlan" volt.

**Javaslat**: 
- **Feltételes megjelenítés**: Csak akkor, ha a felhasználó kéri
- **Filtered views**: Régió, intézkedés típus, stb. szerint szűrt nézetek
- **Minimalist design**: Csak a releváns kapcsolatok

**Alternatíva**: 
- **Table-based view**: Graph adatok táblázatos megjelenítése
- **Tree view**: Hierarchikus struktúra (ország → intézkedések)

#### 4.2 Real-time Updates

**Cél**: Automatikus frissítés új adatok esetén

**Javaslat**:
- **WebSocket** vagy **Server-Sent Events** (SSE)
- **GitHub Actions** trigger új adatok esetén
- **Email/Notification** új policy változásokról

#### 4.3 Export & Reporting

**Cél**: Professzionális jelentések generálása

**Javaslat**:
- **PDF export**: Dashboard → PDF konverzió
- **Excel reports**: Strukturált adatok exportálása
- **PowerPoint**: Automatikus slide generálás

---

### 5. 🔧 Technical Improvements

#### 5.1 Caching & Performance

**Javaslatok**:
- **Redis cache**: API válaszok cache-elése
- **Parquet optimization**: Adatok tömörítése és indexelése
- **Lazy loading**: Csak szükséges adatok betöltése

#### 5.2 Testing & Quality

**Javaslatok**:
- **Unit tests**: pytest framework
- **Integration tests**: ETL pipeline tesztelése
- **AI output validation**: LLM válaszok minőségellenőrzése

#### 5.3 Monitoring & Observability

**Javaslatok**:
- **Logging**: Strukturált logok (JSON formátum)
- **Metrics**: Prometheus/Grafana integráció
- **Error tracking**: Sentry vagy hasonló

---

## 📋 Implementációs Prioritások

### Phase 1: Foundation (1-2 hét)
1. ✅ **Knowledge Graph refactoring**: Áthelyezés központi pozícióba
2. ✅ **RAG retriever**: Alapvető graph-to-text konverzió
3. ✅ **RAG integration**: LLM elemzésekhez integrálás

### Phase 2: Enhancement (2-3 hét)
4. ✅ **Temporal tracking**: Időbeli változások követése
5. ✅ **Risk factors**: Risk faktorok hozzáadása
6. ✅ **Multi-modal AI**: Chart image analysis

### Phase 3: Advanced (3-4 hét)
7. ✅ **Predictive analytics**: Trend előrejelzés
8. ✅ **Conversational AI**: Chatbot interface
9. ✅ **Export & Reporting**: PDF/Excel export

---

## 🎯 Knowledge Graph Pozíció Javaslat

### Jelenlegi Mermaid Diagram:
```
Data Enrichment
├── Country Profile Generator
│   └── Knowledge Graph Builder  ← Itt van most
└── (Country Profiles & Graph Data)
```

### Javasolt Új Struktúra:
```
Data Enrichment
├── Country Profile Generator
│   └── (Country Profiles)
└── Knowledge Graph Builder  ← Központi pozíció
    ├── → Country Profiles (használja)
    ├── → RAG Retriever (új)
    ├── → AI Analysis (használja)
    └── → Grounding Validator (használja)
```

**Előnyök**:
- ✅ **Központi komponens**: Mindenhol elérhető
- ✅ **Reusable**: Több helyen használható
- ✅ **RAG-ready**: Könnyen integrálható RAG-gel
- ✅ **Maintainable**: Egy helyen karbantartható

---

## 💡 Konkrét AI Megoldások

### 1. RAG + Knowledge Graph
- **Cél**: Kontextus-alapú AI válaszok
- **Tech**: sentence-transformers + faiss/chromadb
- **Használat**: LLM elemzésekhez releváns graph kontextus

### 2. Graph Neural Networks (GNN)
- **Cél**: Kapcsolatok tanulása
- **Tech**: PyTorch Geometric vagy DGL
- **Használat**: Policy change prediction, anomaly detection

### 3. Time-Series Forecasting
- **Cél**: Trend előrejelzés
- **Tech**: Prophet, LSTM, Transformer models
- **Használat**: CCyB/SyRB rate predictions

### 4. Multi-Modal AI
- **Cél**: Chart és dokumentum elemzés
- **Tech**: Gemini Vision API, GPT-4 Vision
- **Használat**: Chart image analysis, PDF parsing

### 5. Conversational AI
- **Cél**: Természetes nyelvű interakció
- **Tech**: LangChain + RAG + Knowledge Graph
- **Használat**: Dashboard chatbot

---

## 📊 Várható Üzleti Hatás

### Rövid táv (1-3 hónap):
- 🎯 **30% gyorsabb** információkeresés (RAG)
- 🎯 **50% pontosabb** AI válaszok (graph context)
- 🎯 **Könnyebb karbantartás** (moduláris struktúra)

### Közép táv (3-6 hónap):
- 🎯 **Proaktív monitoring** (predictive analytics)
- 🎯 **Automatizált jelentések** (export features)
- 🎯 **Jobb döntéshozatal** (risk factor integration)

### Hosszú táv (6-12 hónap):
- 🎯 **Teljes automatizálás** (real-time updates)
- 🎯 **AI-powered insights** (GNN, forecasting)
- 🎯 **Scalable architecture** (cloud-ready)

---

## 🚀 Következő Lépések

1. **Döntés**: Knowledge Graph pozíció megerősítése
2. **Prototípus**: RAG retriever implementálása
3. **Tesztelés**: RAG integráció validálása
4. **Iteráció**: Folyamatos fejlesztés felhasználói visszajelzések alapján
