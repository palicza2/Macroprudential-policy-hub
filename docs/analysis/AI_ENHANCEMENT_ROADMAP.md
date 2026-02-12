# 🤖 AI Enhancement Roadmap - 2026 Hot Topics

## Jelenlegi AI Implementáció

### ✅ Már implementálva:
- **Gemini 2.5 Flash Lite**: Chart analysis, summaries, keyword extraction
- **LangGraph**: Grounded validation (data + charts + optional Google Search)
- **Multimodal**: Chart images + data tables
- **Sequential Analysis**: Chart → Section → Global summary
- **News Enrichment**: Tagging, summarization

### ❌ Hiányzó, de 2026-ban kritikus:
- **RAG (Retrieval Augmented Generation)**: Korábbi jelentések, policy dokumentumok
- **Agent-based Workflows**: Több lépéses döntéshozatal
- **Temporal Comparison**: Automatikus összehasonlítás korábbi jelentésekkel
- **Predictive Analytics**: Trend előrejelzés, anomaly detection
- **Vector Databases**: Embedding-based search
- **Multi-agent Collaboration**: Specialist agentek

---

## 1. RAG (RETRIEVAL AUGMENTED GENERATION) 🎯

### 1.1 Miért fontos 2026-ban?
- **Context-aware analysis**: Korábbi jelentések, policy dokumentumok kontextusában
- **Consistency**: Előző elemzésekhez való konzisztencia
- **Domain knowledge**: ESRB guidelines, Basel III dokumentumok
- **Historical patterns**: Hasonló helyzetek korábbi kezelése

### 1.2 Implementációs terv

#### 1.2.1 Vector Database Setup
```python
# rag_system.py
from langchain_community.vectorstores import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFLoader, TextLoader
from pathlib import Path
import pandas as pd

class RAGSystem:
    def __init__(self, config: Dict[str, Any]):
        self.embeddings = GoogleGenerativeAIEmbeddings(
            model="models/embedding-001",
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )
        self.vectorstore = None
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
        )
    
    def build_knowledge_base(self, sources: List[Path]) -> None:
        """
        Knowledge base építése dokumentumokból.
        
        Sources:
        - Korábbi jelentések (HTML/PDF)
        - ESRB policy dokumentumok
        - Basel III guidelines
        - Country-specific policy papers
        """
        documents = []
        
        for source in sources:
            if source.suffix == '.pdf':
                loader = PyPDFLoader(str(source))
                docs = loader.load()
            elif source.suffix in ['.html', '.txt']:
                loader = TextLoader(str(source))
                docs = loader.load()
            else:
                continue
            
            # Metadata hozzáadása
            for doc in docs:
                doc.metadata.update({
                    'source': str(source),
                    'type': self._classify_document(source),
                    'date': self._extract_date(source),
                })
            
            documents.extend(docs)
        
        # Chunking
        chunks = self.text_splitter.split_documents(documents)
        
        # Vector store létrehozása
        self.vectorstore = Chroma.from_documents(
            chunks,
            self.embeddings,
            persist_directory="./data/vectorstore"
        )
    
    def retrieve_context(
        self, 
        query: str, 
        k: int = 5,
        filters: Optional[Dict] = None
    ) -> List[str]:
        """
        Releváns kontextus lekérése.
        
        Args:
            query: Keresési query
            k: Visszaadott dokumentumok száma
            filters: Metadata filterek (pl. date, type)
        """
        if not self.vectorstore:
            return []
        
        # Similarity search
        results = self.vectorstore.similarity_search(
            query,
            k=k,
            filter=filters
        )
        
        return [doc.page_content for doc in results]
    
    def _classify_document(self, path: Path) -> str:
        """Dokumentum típusának meghatározása."""
        if 'ccyb' in path.name.lower():
            return 'ccyb'
        elif 'syrb' in path.name.lower():
            return 'syrb'
        elif 'bbm' in path.name.lower():
            return 'bbm'
        elif 'esrb' in path.name.lower():
            return 'policy'
        return 'general'
    
    def _extract_date(self, path: Path) -> str:
        """Dátum kinyerése fájlnévből vagy metadata-ból."""
        # Regex: YYYY-MM-DD vagy YYYYMMDD
        import re
        match = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2}|\d{8})', path.name)
        return match.group(1) if match else "unknown"
```

#### 1.2.2 RAG integráció LLM analysis-ba
```python
# llm_analysis.py módosítás
class LLMAnalyzer:
    def __init__(self, config: Dict[str, Any], rag_system: Optional[RAGSystem] = None):
        self.config = config
        self.rag_system = rag_system
    
    def run_analysis_with_rag(
        self,
        inputs: Dict[str, pd.DataFrame],
        plot_paths: Dict[str, Path],
        extra_context: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        LLM analysis RAG kontextussal.
        """
        results = {}
        
        # Chart analysis RAG-gel
        for task in build_chart_tasks(...):
            # Releváns kontextus lekérése
            if self.rag_system:
                context_query = f"{task.prompt} {task.data[:500]}"
                relevant_docs = self.rag_system.retrieve_context(
                    context_query,
                    k=3,
                    filters={'type': task.id.split('_')[0]}  # ccyb, syrb, stb.
                )
                context = "\n\n".join(relevant_docs)
            else:
                context = ""
            
            # Enhanced prompt RAG kontextussal
            enhanced_prompt = f"""
{task.prompt}

RELEVANT HISTORICAL CONTEXT:
{context}

CURRENT DATA:
{task.data}
"""
            # LLM hívás
            result = self._invoke_llm(enhanced_prompt, task.temp)
            results[task.id] = result
        
        return results
```

#### 1.2.3 Knowledge Base építése korábbi jelentésekből
```python
# build_rag_knowledge_base.py
from pathlib import Path
from rag_system import RAGSystem

def build_knowledge_base():
    """Knowledge base építése korábbi jelentésekből."""
    rag = RAGSystem(config)
    
    sources = [
        # Korábbi jelentések
        Path("reports/previous_reports/2024-Q1.html"),
        Path("reports/previous_reports/2024-Q2.html"),
        Path("reports/previous_reports/2024-Q3.html"),
        
        # Policy dokumentumok
        Path("docs/ESRB_Guidelines.pdf"),
        Path("docs/Basel_III_Framework.pdf"),
        
        # Country-specific papers
        Path("docs/Hungary_Macroprudential_Strategy.pdf"),
    ]
    
    rag.build_knowledge_base(sources)
    print("Knowledge base built successfully!")
```

### 1.3 Használati esetek
- **Historical comparison**: "Hogyan változott a CCyB trend az elmúlt 2 évben?"
- **Policy consistency**: "Konzisztens-e az aktuális döntés a korábbi policy-vel?"
- **Pattern recognition**: "Láttunk-e hasonló helyzetet korábban?"
- **Context-aware analysis**: Elemzések korábbi jelentések kontextusában

---

## 2. AGENT-BASED WORKFLOWS 🤖

### 2.1 Miért fontos 2026-ban?
- **Multi-step reasoning**: Több lépéses döntéshozatal
- **Tool use**: Adatlekérés, számítások, validáció
- **Autonomous decision-making**: Automatikus következtetések
- **Self-correction**: Hibák észlelése és javítása

### 2.2 Implementációs terv

#### 2.2.1 LangGraph Agent Setup
```python
# agent_system.py
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, AIMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from typing import TypedDict, List, Annotated
import operator

class AgentState(TypedDict):
    messages: Annotated[List, operator.add]
    data_context: Dict[str, Any]
    analysis_results: Dict[str, str]
    validation_status: str

class MacroprudentialAgent:
    def __init__(self, config: Dict[str, Any]):
        self.llm = ChatGoogleGenerativeAI(
            model=config["model_name"],
            temperature=0.3,
        )
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Agent workflow graph építése."""
        workflow = StateGraph(AgentState)
        
        # Nodes
        workflow.add_node("data_retriever", self._retrieve_data)
        workflow.add_node("analyzer", self._analyze_data)
        workflow.add_node("validator", self._validate_analysis)
        workflow.add_node("refiner", self._refine_analysis)
        workflow.add_node("synthesizer", self._synthesize_results)
        
        # Edges
        workflow.set_entry_point("data_retriever")
        workflow.add_edge("data_retriever", "analyzer")
        workflow.add_conditional_edges(
            "analyzer",
            self._should_validate,
            {
                "validate": "validator",
                "skip": "synthesizer"
            }
        )
        workflow.add_conditional_edges(
            "validator",
            self._should_refine,
            {
                "refine": "refiner",
                "accept": "synthesizer"
            }
        )
        workflow.add_edge("refiner", "analyzer")  # Loop back
        workflow.add_edge("synthesizer", END)
        
        return workflow.compile()
    
    def _retrieve_data(self, state: AgentState) -> AgentState:
        """Adatok lekérése és előkészítése."""
        # ETL pipeline futtatása
        # Data validation
        # Context building
        state["data_context"] = {
            "ccyb_df": ...,
            "syrb_df": ...,
            "latest_date": ...,
        }
        return state
    
    def _analyze_data(self, state: AgentState) -> AgentState:
        """Adatok elemzése LLM-mel."""
        prompt = f"""
Analyze the following macroprudential data:
{state['data_context']}

Provide:
1. Key trends
2. Anomalies
3. Policy implications
"""
        response = self.llm.invoke([HumanMessage(content=prompt)])
        state["analysis_results"]["initial"] = response.content
        return state
    
    def _validate_analysis(self, state: AgentState) -> AgentState:
        """Elemzés validálása adatokkal."""
        # Grounding validation
        # Data consistency check
        # Anomaly detection
        state["validation_status"] = "validated"
        return state
    
    def _should_validate(self, state: AgentState) -> str:
        """Validáció szükségességének meghatározása."""
        # Ha van anomália vagy bizonytalanság, validálunk
        if "anomaly" in state["analysis_results"].get("initial", "").lower():
            return "validate"
        return "skip"
    
    def _should_refine(self, state: AgentState) -> str:
        """Finomítás szükségességének meghatározása."""
        if state["validation_status"] == "needs_refinement":
            return "refine"
        return "accept"
    
    def _refine_analysis(self, state: AgentState) -> AgentState:
        """Elemzés finomítása validáció alapján."""
        # Refinement logic
        return state
    
    def _synthesize_results(self, state: AgentState) -> AgentState:
        """Végeredmény szintetizálása."""
        # Final synthesis
        return state
    
    def run(self, query: str) -> Dict[str, Any]:
        """Agent futtatása."""
        initial_state = {
            "messages": [HumanMessage(content=query)],
            "data_context": {},
            "analysis_results": {},
            "validation_status": "pending",
        }
        final_state = self.graph.invoke(initial_state)
        return final_state
```

#### 2.2.2 Tool Use Agent
```python
# tool_agent.py
from langchain.tools import Tool
from langchain.agents import create_react_agent

class ToolAgent:
    def __init__(self):
        self.tools = [
            Tool(
                name="get_ccyb_data",
                func=self._get_ccyb_data,
                description="Retrieves CCyB data for a specific country and date range"
            ),
            Tool(
                name="calculate_diffusion_index",
                func=self._calculate_diffusion_index,
                description="Calculates diffusion index for CCyB adoption"
            ),
            Tool(
                name="compare_with_historical",
                func=self._compare_with_historical,
                description="Compares current data with historical patterns"
            ),
            Tool(
                name="detect_anomalies",
                func=self._detect_anomalies,
                description="Detects anomalies in rate changes"
            ),
        ]
        
        self.agent = create_react_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=self._build_prompt()
        )
    
    def _get_ccyb_data(self, country: str, start_date: str, end_date: str) -> str:
        """CCyB adatok lekérése."""
        # Implementation
        pass
    
    def _calculate_diffusion_index(self, date: str) -> str:
        """Diffusion index számítása."""
        # Implementation
        pass
```

### 2.3 Használati esetek
- **Autonomous analysis**: "Elemezd a CCyB trendet és jelezz anomáliákat"
- **Multi-step reasoning**: "Hasonlítsd össze a jelenlegi helyzetet a 2020-as válsággal"
- **Self-correction**: Automatikus hibajavítás validáció alapján

---

## 3. TEMPORAL COMPARISON & CHANGE DETECTION 📊

### 3.1 Miért fontos 2026-ban?
- **Automated insights**: "What changed?" automatikus detektálás
- **Trend analysis**: Hosszú távú trendek azonosítása
- **Anomaly detection**: Váratlan változások észlelése
- **Historical context**: Korábbi jelentésekhez való összehasonlítás

### 3.2 Implementációs terv

#### 3.2.1 Temporal Comparison System
```python
# temporal_comparison.py
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

class TemporalComparator:
    def __init__(self, historical_reports_dir: Path):
        self.historical_reports_dir = historical_reports_dir
        self.historical_data = self._load_historical_data()
    
    def _load_historical_data(self) -> Dict[str, pd.DataFrame]:
        """Korábbi jelentések betöltése."""
        historical = {}
        for report_file in self.historical_reports_dir.glob("*.parquet"):
            date_str = self._extract_date_from_filename(report_file)
            historical[date_str] = pd.read_parquet(report_file)
        return historical
    
    def compare_reports(
        self,
        current_data: Dict[str, pd.DataFrame],
        comparison_periods: List[str] = ["1M", "3M", "6M", "12M"]
    ) -> Dict[str, Any]:
        """
        Jelenlegi jelentés összehasonlítása korábbiakkal.
        
        Returns:
            Dictionary változásokkal, trendekkel, anomáliákkal
        """
        changes = {}
        
        for period in comparison_periods:
            historical_date = self._get_historical_date(period)
            if historical_date not in self.historical_data:
                continue
            
            historical = self.historical_data[historical_date]
            
            # CCyB változások
            ccyb_changes = self._detect_ccyb_changes(
                current_data["ccyb_df"],
                historical.get("ccyb_df")
            )
            
            # SyRB változások
            syrb_changes = self._detect_syrb_changes(
                current_data["syrb_df"],
                historical.get("syrb_df")
            )
            
            # Trend analysis
            trends = self._analyze_trends(current_data, historical)
            
            changes[period] = {
                "ccyb_changes": ccyb_changes,
                "syrb_changes": syrb_changes,
                "trends": trends,
            }
        
        return changes
    
    def _detect_ccyb_changes(
        self,
        current: pd.DataFrame,
        historical: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """CCyB változások detektálása."""
        changes = []
        
        # Country-level comparison
        for country in current["country"].unique():
            current_rate = current[
                current["country"] == country
            ]["rate"].iloc[-1] if not current.empty else 0
            
            historical_rate = historical[
                historical["country"] == country
            ]["rate"].iloc[-1] if not historical.empty and country in historical["country"].values else 0
            
            if abs(current_rate - historical_rate) > 0.1:  # 0.1% threshold
                changes.append({
                    "country": country,
                    "previous_rate": historical_rate,
                    "current_rate": current_rate,
                    "change": current_rate - historical_rate,
                    "type": "increase" if current_rate > historical_rate else "decrease",
                })
        
        return changes
    
    def generate_change_summary(
        self,
        changes: Dict[str, Any],
        llm_analyzer: LLMAnalyzer
    ) -> str:
        """Változások összefoglalása LLM-mel."""
        prompt = f"""
Summarize the following macroprudential policy changes:

{changes}

Focus on:
1. Most significant changes
2. Trends across countries
3. Policy implications
4. Anomalies or unexpected patterns

Write a concise 2-3 paragraph summary.
"""
        return llm_analyzer._invoke_llm(prompt, temperature=0.3)
```

#### 3.2.2 Anomaly Detection
```python
# anomaly_detection.py
import numpy as np
from scipy import stats
from typing import List, Dict

class AnomalyDetector:
    def detect_rate_anomalies(
        self,
        current_rates: pd.DataFrame,
        historical_rates: pd.DataFrame,
        threshold: float = 2.0  # Z-score threshold
    ) -> List[Dict[str, Any]]:
        """
        Anomáliák detektálása rate változásokban.
        """
        anomalies = []
        
        for country in current_rates["country"].unique():
            country_history = historical_rates[
                historical_rates["country"] == country
            ]["rate"]
            
            if len(country_history) < 5:  # Nincs elég adat
                continue
            
            current_rate = current_rates[
                current_rates["country"] == country
            ]["rate"].iloc[-1]
            
            # Z-score számítás
            mean = country_history.mean()
            std = country_history.std()
            
            if std > 0:
                z_score = abs((current_rate - mean) / std)
                
                if z_score > threshold:
                    anomalies.append({
                        "country": country,
                        "current_rate": current_rate,
                        "historical_mean": mean,
                        "z_score": z_score,
                        "severity": "high" if z_score > 3 else "medium",
                    })
        
        return anomalies
```

### 3.3 Használati esetek
- **"What changed?" dashboard**: Automatikus változások összefoglalása
- **Trend alerts**: Email/Slack értesítések jelentős változásokról
- **Historical context**: "Hasonló helyzet volt-e korábban?"

---

## 4. PREDICTIVE ANALYTICS 🔮

### 4.1 Miért fontos 2026-ban?
- **Proactive policy**: Reaktív helyett proaktív policy elemzés
- **Risk forecasting**: Kockázatok előrejelzése
- **Scenario analysis**: "What-if" elemzések
- **Early warning**: Korai figyelmeztetések

### 4.2 Implementációs terv

#### 4.2.1 Trend Forecasting
```python
# predictive_analytics.py
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import numpy as np
from typing import Dict, Tuple

class PredictiveAnalytics:
    def forecast_ccyb_trends(
        self,
        historical_data: pd.DataFrame,
        forecast_horizon: int = 6  # months
    ) -> Dict[str, Dict[str, float]]:
        """
        CCyB trend előrejelzés országonként.
        """
        forecasts = {}
        
        for country in historical_data["country"].unique():
            country_data = historical_data[
                historical_data["country"] == country
            ].sort_values("date")
            
            if len(country_data) < 12:  # Nincs elég adat
                continue
            
            # Time series előkészítése
            dates = pd.to_datetime(country_data["date"])
            rates = country_data["rate"].values
            
            # Trend fitting
            X = np.arange(len(dates)).reshape(-1, 1)
            y = rates
            
            # Polynomial regression
            poly_features = PolynomialFeatures(degree=2)
            X_poly = poly_features.fit_transform(X)
            
            model = LinearRegression()
            model.fit(X_poly, y)
            
            # Forecast
            future_X = np.arange(len(dates), len(dates) + forecast_horizon).reshape(-1, 1)
            future_X_poly = poly_features.transform(future_X)
            forecast = model.predict(future_X_poly)
            
            forecasts[country] = {
                "current_rate": rates[-1],
                "forecast_6m": forecast[-1],
                "trend": "increasing" if forecast[-1] > rates[-1] else "decreasing",
                "confidence": self._calculate_confidence(model, X_poly, y),
            }
        
        return forecasts
    
    def _calculate_confidence(self, model, X, y) -> float:
        """Előrejelzés megbízhatósága (R² score)."""
        from sklearn.metrics import r2_score
        y_pred = model.predict(X)
        return r2_score(y, y_pred)
```

#### 4.2.2 Scenario Analysis
```python
# scenario_analysis.py
class ScenarioAnalyzer:
    def analyze_scenarios(
        self,
        current_state: Dict[str, pd.DataFrame],
        scenarios: List[Dict[str, Any]],
        llm_analyzer: LLMAnalyzer
    ) -> Dict[str, str]:
        """
        Scenario analysis különböző feltételezésekkel.
        
        Scenarios:
        - "recession": Gazdasági recesszió
        - "credit_boom": Hitelnövekedés
        - "policy_tightening": Szigorítás
        """
        results = {}
        
        for scenario in scenarios:
            # Simulated data generation
            simulated_data = self._simulate_scenario(
                current_state,
                scenario
            )
            
            # Analysis with LLM
            prompt = f"""
Analyze the following scenario:

Scenario: {scenario['name']}
Description: {scenario['description']}
Simulated Data: {simulated_data}

Provide:
1. Expected policy responses
2. Risk implications
3. Recommended actions
"""
            analysis = llm_analyzer._invoke_llm(prompt, temperature=0.4)
            results[scenario['name']] = analysis
        
        return results
```

### 4.3 Használati esetek
- **Forecast dashboard**: "Mi várható a következő 6 hónapban?"
- **Scenario planning**: "Mi történne, ha recesszió következne be?"
- **Early warning**: "Mely országokban várható policy változás?"

---

## 5. MULTI-AGENT COLLABORATION 👥

### 5.1 Miért fontos 2026-ban?
- **Specialized expertise**: Különböző specialisták (CCyB, SyRB, BBM)
- **Collaborative analysis**: Agentek közötti együttműködés
- **Consensus building**: Többségi vélemény kialakítása
- **Error reduction**: Több agent = kevesebb hiba

### 5.2 Implementációs terv

#### 5.2.1 Specialist Agents
```python
# multi_agent_system.py
from langgraph.graph import StateGraph
from typing import Dict, List

class SpecialistAgent:
    def __init__(self, name: str, expertise: str, llm_config: Dict):
        self.name = name
        self.expertise = expertise
        self.llm = ChatGoogleGenerativeAI(**llm_config)
        self.system_prompt = self._build_system_prompt()
    
    def _build_system_prompt(self) -> str:
        """Specialist system prompt."""
        prompts = {
            "ccyb_expert": """
You are a CCyB (Countercyclical Capital Buffer) specialist.
Your expertise includes:
- Credit cycle analysis
- Credit gap interpretation
- Policy rate decisions
- Cross-country comparisons
""",
            "syrb_expert": """
You are a SyRB (Systemic Risk Buffer) specialist.
Your expertise includes:
- Structural risk assessment
- Sectoral exposure analysis
- General vs. sectoral buffers
- Risk concentration identification
""",
            "bbm_expert": """
You are a BBM (Borrower-Based Measures) specialist.
Your expertise includes:
- LTV, DSTI, DTI limits
- Housing market risks
- Affordability analysis
- First-time buyer policies
""",
        }
        return prompts.get(self.expertise, "")
    
    def analyze(self, data: Dict[str, Any], context: str) -> str:
        """Specialist analysis."""
        prompt = f"""
{self.system_prompt}

Context: {context}
Data: {data}

Provide your expert analysis focusing on your area of expertise.
"""
        return self.llm.invoke([HumanMessage(content=prompt)]).content

class MultiAgentSystem:
    def __init__(self, agents: List[SpecialistAgent]):
        self.agents = agents
        self.coordinator = self._build_coordinator()
    
    def collaborative_analysis(
        self,
        data: Dict[str, pd.DataFrame],
        question: str
    ) -> Dict[str, Any]:
        """
        Több agent együttműködő elemzése.
        """
        # 1. Minden agent elemzése
        individual_analyses = {}
        for agent in self.agents:
            relevant_data = self._extract_relevant_data(data, agent.expertise)
            analysis = agent.analyze(relevant_data, question)
            individual_analyses[agent.name] = analysis
        
        # 2. Coordinator szintetizálja az eredményeket
        synthesis = self.coordinator.synthesize(
            question,
            individual_analyses
        )
        
        # 3. Consensus building
        consensus = self._build_consensus(individual_analyses, synthesis)
        
        return {
            "individual_analyses": individual_analyses,
            "synthesis": synthesis,
            "consensus": consensus,
        }
    
    def _build_consensus(
        self,
        analyses: Dict[str, str],
        synthesis: str
    ) -> str:
        """Konszenzus kialakítása."""
        prompt = f"""
Multiple experts provided analyses:

{analyses}

Synthesis: {synthesis}

Build a consensus view that integrates all perspectives.
Identify areas of agreement and disagreement.
"""
        return self.coordinator.llm.invoke([HumanMessage(content=prompt)]).content
```

### 5.3 Használati esetek
- **Expert panel**: Több specialist véleménye egy kérdésről
- **Consensus analysis**: Többségi vélemény kialakítása
- **Cross-validation**: Agentek közötti validáció

---

## 6. IMPLEMENTÁCIÓS PRIORITÁSOK

### Fázis 1 (Q1 2026): Alapok
1. ✅ **RAG System**: Vector database + knowledge base építés
2. ✅ **Temporal Comparison**: Korábbi jelentések összehasonlítása
3. ✅ **Anomaly Detection**: Automatikus anomáliadetektálás

### Fázis 2 (Q2 2026): Fejlett funkciók
1. ✅ **Agent-based Workflows**: LangGraph agentek
2. ✅ **Predictive Analytics**: Trend előrejelzés
3. ✅ **Multi-agent Collaboration**: Specialist agentek

### Fázis 3 (Q3 2026): Optimalizálás
1. ✅ **Fine-tuning**: Saját modell finomhangolása
2. ✅ **Real-time Monitoring**: Alerting rendszer
3. ✅ **Graph RAG**: Kapcsolatok modellezése

---

## 7. TECHNOLÓGIAI STACK

### Új függőségek
```txt
# Vector database
chromadb>=0.4.0
langchain-community>=0.2.0

# Embeddings
langchain-google-genai>=1.0.0  # embedding-001 model

# Agent frameworks
langgraph>=0.2.0  # Már használjuk
langchain-agents>=0.2.0

# Predictive analytics
scikit-learn>=1.3.0
statsmodels>=0.14.0

# Time series
pandas>=2.0.0  # Már van
numpy>=1.24.0
```

---

## 8. KONKRÉT HASZNÁLATI PÉLDÁK

### 8.1 RAG használata
```python
# main.py módosítás
from rag_system import RAGSystem

def main():
    # ...
    
    # RAG system inicializálása
    rag = RAGSystem(config)
    rag.build_knowledge_base([
        Path("reports/previous_reports/"),
        Path("docs/policy_papers/"),
    ])
    
    # LLM analysis RAG-gel
    analyzer = LLMAnalyzer(LLM_CONFIG, rag_system=rag)
    analyses = analyzer.run_analysis_with_rag(...)
```

### 8.2 Temporal comparison
```python
# main.py módosítás
from temporal_comparison import TemporalComparator

def main():
    # ...
    
    # Temporal comparison
    comparator = TemporalComparator(Path("data/historical_reports/"))
    changes = comparator.compare_reports(data, ["1M", "3M", "6M", "12M"])
    
    # Change summary generálása
    change_summary = comparator.generate_change_summary(changes, analyzer)
    
    # Render-ben megjelenítés
    render_report(..., change_summary=change_summary)
```

### 8.3 Predictive analytics
```python
# main.py módosítás
from predictive_analytics import PredictiveAnalytics

def main():
    # ...
    
    # Forecast generálása
    predictor = PredictiveAnalytics()
    forecasts = predictor.forecast_ccyb_trends(
        data["ccyb_df"],
        forecast_horizon=6
    )
    
    # Forecast dashboard generálása
    forecast_plot = viz.generate_forecast_chart(forecasts)
```

---

## ÖSSZEFOGLALÁS

### Legfontosabb 2026-os AI trendek:
1. **RAG** - Kontextus-aware analysis korábbi jelentésekből
2. **Agent-based Workflows** - Több lépéses, autonóm döntéshozatal
3. **Temporal Comparison** - Automatikus változás detektálás
4. **Predictive Analytics** - Trend előrejelzés, scenario analysis
5. **Multi-agent Collaboration** - Specialist agentek együttműködése
6. **Knowledge Graph Visualization** - Interaktív kapcsolat-vizualizáció (lehetséges irány)

### Következő lépések:
1. RAG system implementálása (1-2 hét)
2. Temporal comparison hozzáadása (1 hét)
3. Agent-based workflows (2-3 hét)
4. Predictive analytics (2 hét)

---

## 8. KNOWLEDGE GRAPH VISUALIZATION 🕸️

### 8.1 Miért fontos?
- **Interaktív kapcsolat-vizualizáció**: Országok, intézkedések (CCyB, SyRB, O-SII, BBM) és risk faktok közötti kapcsolatok
- **Felfedezés**: Automatikus hasonlóságok, policy mix összehasonlítás
- **Navigáció**: Kattintással országprofilokra ugrás
- **Modern UI**: Innovatív, interaktív felület

### 8.2 Koncepció

#### Entitások (Nodes):
- **Országok**: Hungary, Poland, Germany, stb.
- **Intézkedések**: CCyB: 2.5%, SyRB: 1.0%, O-SII: 2.0%, BBM: LTV
- **Risk faktok** (opcionális): Credit Growth Risk, Real Estate Risk

#### Kapcsolatok (Edges):
- **HAS**: `[Hungary] --HAS--> [CCyB: 2.5%]`
- **SIMILAR**: `[Hungary] --SIMILAR--> [Poland]` (hasonló tőkepuffer szint)
- **ADDRESSES** (opcionális): `[CCyB: 2.5%] --ADDRESSES--> [Credit Growth Risk]`

### 8.3 Interaktív funkciók
- **Click**: Navigáció vagy részletek
- **Hover**: Tooltip információk
- **Filter**: Szűrés measure/region szerint
- **Search**: Gyors keresés
- **Zoom & Pan**: Nagyítás, mozgatás
- **Highlight**: Kapcsolódó node-ok kiemelése

### 8.4 Technológiai megoldás

#### MVP (Frontend-only):
- **vis.js Network** vagy **Cytoscape.js** (JavaScript library)
- **Pre-computed JSON**: Python generálja a graph adatokat
- **Client-side rendering**: Nincs backend szükség
- **Költség**: Ingyenes (CDN)

#### Későbbi bővítés:
- **Neo4j** vagy **RDF/SPARQL**: Komplex graph analytics
- **Real-time updates**: Dinamikus adatfrissítés
- **Graph query nyelv**: Cypher vagy SPARQL

### 8.5 Implementációs terv

#### Fázis 1: MVP (1 hét)
1. `build_knowledge_graph_data()` metódus implementálása
2. vis.js integrálása (CDN)
3. Graph adatok generálása Python-ban
4. HTML template létrehozása
5. JavaScript interakciók (click, hover)

#### Fázis 2: Bővítések (1 hét)
1. Filtering: Ország típus szerint (CEE, Nordics, stb.)
2. Search: Ország/intézkedés keresés
3. Temporal view: Időbeli változások animációja
4. Export: PNG/SVG export

#### Fázis 3: AI Integration (1 hét)
1. Graph context: LLM kap graph adatokat contextként
2. Relationship inference: LLM következtet új kapcsolatokra
3. Explanatory AI: LLM magyarázatot ad a kapcsolatokra

### 8.6 Dokumentáció
- **Részletes terv**: `KNOWLEDGE_GRAPH_ANALYSIS.md`
- **Vizualizációs példák**: `KNOWLEDGE_GRAPH_VISUALIZATION_EXAMPLES.md`

### 8.7 Üzleti érték
- ✅ **Gyors kontextus**: Egy pillantás alatt látható a teljes policy landscape
- ✅ **Felfedezés**: Automatikus hasonlóságok, kapcsolatok
- ✅ **Navigáció**: Kattintással országprofilokra ugrás
- ✅ **Modern UI**: Innovatív, interaktív felület

**Státusz**: 📋 Lehetséges jövőbeli fejlesztési irány

Melyik funkciót szeretnéd először implementálni?
