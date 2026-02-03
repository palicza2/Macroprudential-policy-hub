# 🆓 Ingyenes RAG Implementáció - Hobbi Verzió

## Áttekintés

Ez egy **teljesen ingyenes** RAG implementáció, amely lokális eszközöket használ. Később könnyen skálázható fel fizetős szolgáltatásokra, ha szükséges.

---

## 1. INGYENES TECHNOLÓGIAI STACK

### Vector Database: **ChromaDB** (100% ingyenes, lokális)
- ✅ Nincs cloud költség
- ✅ Lokális fájlrendszerben tárolódik
- ✅ Könnyen skálázható később (pl. ChromaDB Cloud)

### Embeddings: **Sentence Transformers** (100% ingyenes, lokális)
- ✅ Nincs API költség
- ✅ Lokális modell (CPU/GPU)
- ✅ Alternatíva: Google embedding-001 (ingyenes tier: 1M token/hó)

### LLM: **Gemini 2.5 Flash Lite** (már használod)
- ✅ Ingyenes tier elérhető
- ✅ Már integrálva van

---

## 2. IMPLEMENTÁCIÓS TERV

### 2.1 Függőségek hozzáadása

```txt
# requirements.txt - hozzáadandó sorok
sentence-transformers>=2.2.0
chromadb>=0.4.0
langchain-community>=0.2.0
langchain-sentence-transformers>=0.1.0
```

### 2.2 RAG System implementáció (ingyenes verzió)

```python
# rag_system_free.py
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
import logging

from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
from langchain_community.document_loaders import TextLoader, PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings

logger = logging.getLogger(__name__)

class FreeRAGSystem:
    """
    Ingyenes RAG System lokális eszközökkel.
    
    - Vector DB: ChromaDB (lokális)
    - Embeddings: Sentence Transformers (lokális)
    - Nincs API költség
    """
    
    def __init__(self, vectorstore_dir: Path = None):
        """
        Inicializálás.
        
        Args:
            vectorstore_dir: ChromaDB tárolási könyvtár (default: data/vectorstore)
        """
        if vectorstore_dir is None:
            vectorstore_dir = Path("data/vectorstore")
        
        self.vectorstore_dir = vectorstore_dir
        self.vectorstore_dir.mkdir(parents=True, exist_ok=True)
        
        # Sentence Transformers modell (lokális, ingyenes)
        logger.info("Loading Sentence Transformers model (this may take a minute on first run)...")
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",  # Kicsi, gyors, jó minőség
            model_kwargs={'device': 'cpu'},  # CPU-n fut, GPU opcionális
            encode_kwargs={'normalize_embeddings': True}
        )
        
        # ChromaDB inicializálása (lokális)
        self.client = chromadb.PersistentClient(
            path=str(self.vectorstore_dir),
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True
            )
        )
        
        # Collection létrehozása vagy betöltése
        self.collection = self.client.get_or_create_collection(
            name="macroprudential_knowledge",
            metadata={"hnsw:space": "cosine"}  # Cosine similarity
        )
        
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len,
        )
        
        logger.info("Free RAG System initialized (local, no API costs)")
    
    def build_knowledge_base(self, sources: List[Path]) -> None:
        """
        Knowledge base építése dokumentumokból.
        
        Args:
            sources: Dokumentumok listája (HTML, TXT, PDF)
        """
        if not sources:
            logger.warning("No sources provided for knowledge base")
            return
        
        logger.info(f"Building knowledge base from {len(sources)} sources...")
        
        all_documents = []
        all_metadatas = []
        all_ids = []
        
        doc_id = 0
        
        for source_path in sources:
            if not source_path.exists():
                logger.warning(f"Source not found: {source_path}")
                continue
            
            try:
                # Dokumentum betöltése
                if source_path.suffix == '.pdf':
                    loader = PyPDFLoader(str(source_path))
                    docs = loader.load()
                elif source_path.suffix in ['.html', '.txt']:
                    loader = TextLoader(str(source_path), encoding='utf-8')
                    docs = loader.load()
                else:
                    logger.warning(f"Unsupported file type: {source_path.suffix}")
                    continue
                
                # Dokumentumok chunking
                for doc in docs:
                    chunks = self.text_splitter.split_text(doc.page_content)
                    
                    for i, chunk in enumerate(chunks):
                        if not chunk.strip():
                            continue
                        
                        # Metadata
                        metadata = {
                            'source': str(source_path),
                            'source_name': source_path.name,
                            'doc_type': self._classify_document(source_path),
                            'date': self._extract_date(source_path),
                            'chunk_index': i,
                            'total_chunks': len(chunks),
                        }
                        
                        # Embedding generálása (lokális)
                        embedding = self.embeddings.embed_query(chunk)
                        
                        # ChromaDB-be mentés
                        all_documents.append(chunk)
                        all_metadatas.append(metadata)
                        all_ids.append(f"{source_path.stem}_{doc_id}_{i}")
                        
                        doc_id += 1
                
                logger.info(f"Processed {source_path.name}: {len(docs)} documents, {doc_id} chunks")
            
            except Exception as e:
                logger.error(f"Error processing {source_path}: {e}")
                continue
        
        # Batch insert ChromaDB-be
        if all_documents:
            logger.info(f"Adding {len(all_documents)} chunks to vector store...")
            self.collection.add(
                documents=all_documents,
                metadatas=all_metadatas,
                ids=all_ids
            )
            logger.info(f"Knowledge base built successfully! Total chunks: {len(all_documents)}")
        else:
            logger.warning("No documents to add to knowledge base")
    
    def retrieve_context(
        self,
        query: str,
        k: int = 5,
        filters: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Releváns kontextus lekérése.
        
        Args:
            query: Keresési query
            k: Visszaadott dokumentumok száma
            filters: Metadata filterek (pl. {'doc_type': 'ccyb'})
        
        Returns:
            Lista releváns dokumentumokkal (content + metadata)
        """
        if self.collection.count() == 0:
            logger.warning("Knowledge base is empty. Run build_knowledge_base() first.")
            return []
        
        # Query embedding (lokális)
        query_embedding = self.embeddings.embed_query(query)
        
        # ChromaDB query
        where_clause = filters if filters else None
        
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=k,
            where=where_clause,
            include=['documents', 'metadatas', 'distances']
        )
        
        # Formázás
        retrieved = []
        if results['documents'] and len(results['documents']) > 0:
            for i, doc in enumerate(results['documents'][0]):
                retrieved.append({
                    'content': doc,
                    'metadata': results['metadatas'][0][i] if results['metadatas'] else {},
                    'distance': results['distances'][0][i] if results['distances'] else None,
                    'relevance_score': 1 - results['distances'][0][i] if results['distances'] else None,
                })
        
        return retrieved
    
    def _classify_document(self, path: Path) -> str:
        """Dokumentum típusának meghatározása."""
        name_lower = path.name.lower()
        if 'ccyb' in name_lower or 'countercyclical' in name_lower:
            return 'ccyb'
        elif 'syrb' in name_lower or 'systemic' in name_lower:
            return 'syrb'
        elif 'bbm' in name_lower or 'borrower' in name_lower:
            return 'bbm'
        elif 'esrb' in name_lower or 'policy' in name_lower:
            return 'policy'
        return 'general'
    
    def _extract_date(self, path: Path) -> str:
        """Dátum kinyerése fájlnévből."""
        import re
        # Regex: YYYY-MM-DD vagy YYYYMMDD vagy YYYY-Q1 stb.
        patterns = [
            r'(\d{4}[-/]\d{2}[-/]\d{2})',  # YYYY-MM-DD
            r'(\d{8})',  # YYYYMMDD
            r'(\d{4}-Q[1-4])',  # YYYY-Q1
            r'(\d{4})',  # YYYY
        ]
        
        for pattern in patterns:
            match = re.search(pattern, path.name)
            if match:
                return match.group(1)
        
        return "unknown"
    
    def get_stats(self) -> Dict[str, Any]:
        """Knowledge base statisztikák."""
        count = self.collection.count()
        return {
            'total_chunks': count,
            'vectorstore_path': str(self.vectorstore_dir),
            'model': 'sentence-transformers/all-MiniLM-L6-v2',
            'cost': 'FREE (local)',
        }
```

### 2.3 Integráció az LLM Analysis-ba

```python
# rag_integration_free.py
from typing import Dict, Tuple, Optional
from pathlib import Path
import pandas as pd
from rag_system_free import FreeRAGSystem
from llm_analysis import LLMAnalyzer
from llm_tasks import build_chart_tasks

class RAGEnhancedAnalyzer(LLMAnalyzer):
    """
    LLM Analyzer RAG kontextussal (ingyenes verzió).
    """
    
    def __init__(self, config: Dict, rag_system: Optional[FreeRAGSystem] = None):
        super().__init__(config)
        self.rag_system = rag_system
    
    def run_analysis_with_rag(
        self,
        inputs: Dict[str, pd.DataFrame],
        plot_paths: Dict[str, Path],
        extra_context: Dict[str, Any] = None
    ) -> Tuple[Dict[str, str], Dict[str, Dict]]:
        """
        LLM analysis RAG kontextussal.
        
        Returns:
            - analyses: Elemzések
            - rag_context: RAG kontextus információk
        """
        analyses = {}
        rag_context = {}
        
        # Chart tasks előkészítése
        latest_ccyb_str = self._df_to_string(inputs.get('latest_ccyb_df'))
        # ... (további string konverziók)
        
        chart_tasks = build_chart_tasks(
            latest_ccyb_str=latest_ccyb_str,
            # ... (további paraméterek)
        )
        
        # Minden task-hoz RAG kontextus
        for task in chart_tasks:
            # RAG query építése
            if self.rag_system:
                rag_query = self._build_rag_query(task, inputs)
                
                # Kontextus lekérése
                retrieved = self.rag_system.retrieve_context(
                    rag_query,
                    k=3,
                    filters={'doc_type': self._extract_type(task.id)}
                )
                
                # Kontextus formázása
                if retrieved:
                    context_text = "\n\n".join([
                        f"[{doc['metadata'].get('date', 'Unknown')} - {doc['metadata'].get('source_name', 'Unknown')}]\n"
                        f"{doc['content'][:300]}..."
                        for doc in retrieved
                    ])
                    
                    # Enhanced prompt
                    enhanced_prompt = f"""
{task.prompt}

HISTORICAL CONTEXT (from previous reports):
{context_text}

CURRENT DATA:
{task.data}
"""
                else:
                    enhanced_prompt = task.prompt
                    context_text = ""
            else:
                enhanced_prompt = task.prompt
                context_text = ""
                retrieved = []
            
            # LLM hívás
            result = self._invoke_llm(enhanced_prompt, task.temp)
            analyses[task.id] = result
            
            # RAG kontextus mentése
            rag_context[task.id] = {
                'has_context': len(retrieved) > 0,
                'sources_count': len(retrieved),
                'sources': [
                    {
                        'date': doc['metadata'].get('date', 'Unknown'),
                        'source': doc['metadata'].get('source_name', 'Unknown'),
                        'excerpt': doc['content'][:200],
                        'relevance': doc.get('relevance_score', 0)
                    }
                    for doc in retrieved
                ]
            }
        
        # Section summaries és global summary (ugyanúgy)
        # ...
        
        return analyses, rag_context
    
    def _build_rag_query(self, task, inputs: Dict) -> str:
        """RAG query építése task-ból."""
        # Task prompt + adatok első 500 karaktere
        query = f"{task.prompt} {task.data[:500]}"
        return query
    
    def _extract_type(self, analysis_id: str) -> Optional[str]:
        """Típus kinyerése analysis ID-ból."""
        if 'ccyb' in analysis_id.lower():
            return 'ccyb'
        elif 'syrb' in analysis_id.lower():
            return 'syrb'
        elif 'bbm' in analysis_id.lower():
            return 'bbm'
        return None
```

### 2.4 Main pipeline módosítás

```python
# main.py módosítás (részlet)
from rag_system_free import FreeRAGSystem
from rag_integration_free import RAGEnhancedAnalyzer

def main():
    # ...
    
    # RAG System inicializálása (ingyenes, lokális)
    use_rag = os.getenv("ENABLE_RAG", "false").lower() == "true"
    rag_system = None
    
    if use_rag:
        logger.info("Initializing Free RAG System (local, no API costs)...")
        rag_system = FreeRAGSystem(vectorstore_dir=Path("data/vectorstore"))
        
        # Knowledge base építése (ha még nincs)
        if rag_system.collection.count() == 0:
            logger.info("Building knowledge base from previous reports...")
            previous_reports = list(Path("reports/previous_reports").glob("*.html"))
            if previous_reports:
                rag_system.build_knowledge_base(previous_reports)
            else:
                logger.warning("No previous reports found. RAG will be empty.")
        else:
            logger.info(f"Knowledge base loaded: {rag_system.get_stats()['total_chunks']} chunks")
    
    # LLM Analysis RAG-gel vagy anélkül
    if rag_system:
        analyzer = RAGEnhancedAnalyzer(LLM_CONFIG, rag_system)
        analyses, rag_context = analyzer.run_analysis_with_rag(
            analysis_inputs, paths, {}
        )
    else:
        analyzer = LLMAnalyzer(LLM_CONFIG)
        analyses = analyzer.run_analysis(analysis_inputs, paths, {})
        rag_context = {}
    
    # Render RAG kontextussal
    rendered_html = render_report(
        ...,
        rag_context=rag_context,  # Új paraméter
    )
```

---

## 3. HASZNÁLATI ÚTMUTATÓ

### 3.1 Első lépések

#### 1. Függőségek telepítése
```bash
pip install sentence-transformers chromadb langchain-community langchain-sentence-transformers
```

#### 2. Korábbi jelentések előkészítése
```bash
# Hozz létre egy könyvtárat korábbi jelentésekhez
mkdir -p reports/previous_reports

# Másold be a korábbi jelentéseket (HTML formátumban)
# Pl.:
# reports/previous_reports/2024-Q1.html
# reports/previous_reports/2024-Q2.html
# reports/previous_reports/2024-Q3.html
```

#### 3. Knowledge base építése
```python
# build_knowledge_base.py
from pathlib import Path
from rag_system_free import FreeRAGSystem

def build_kb():
    rag = FreeRAGSystem()
    
    sources = [
        Path("reports/previous_reports/2024-Q1.html"),
        Path("reports/previous_reports/2024-Q2.html"),
        Path("reports/previous_reports/2024-Q3.html"),
        # További jelentések...
    ]
    
    rag.build_knowledge_base(sources)
    print(f"Knowledge base built! Stats: {rag.get_stats()}")

if __name__ == "__main__":
    build_kb()
```

#### 4. RAG engedélyezése
```bash
# .env fájlban
ENABLE_RAG=true
```

#### 5. Pipeline futtatása
```bash
python main.py
```

---

## 4. KÖLTSÉGEK ÖSSZEHASONLÍTÁSA

### Ingyenes verzió (hobbi)
| Komponens | Költség | Megjegyzés |
|-----------|---------|------------|
| **Vector DB** | €0 | ChromaDB lokális |
| **Embeddings** | €0 | Sentence Transformers lokális |
| **LLM** | €0 | Gemini 2.5 Flash Lite ingyenes tier |
| **Storage** | €0 | Lokális fájlrendszer |
| **Összesen** | **€0/hó** | Teljesen ingyenes |

### Fizetős verzió (skálázás esetén)
| Komponens | Költség | Megjegyzés |
|-----------|---------|------------|
| **Vector DB** | €10-50/hó | ChromaDB Cloud vagy Qdrant Cloud |
| **Embeddings** | €20-50/hó | Google embedding-001 API (1M+ token) |
| **LLM** | €0-20/hó | Gemini 2.5 Flash Lite (több hívás) |
| **Storage** | €5-10/hó | Cloud storage |
| **Összesen** | **€35-130/hó** | Skálázás esetén |

---

## 5. SKÁLÁZÁSI ÚTVONAL

### Fázis 1: Ingyenes (hobbi)
- ✅ ChromaDB lokális
- ✅ Sentence Transformers lokális
- ✅ Gemini ingyenes tier
- **Költség: €0/hó**

### Fázis 2: Hibrid (átmenet)
- ✅ ChromaDB lokális (marad)
- ⬆️ Google embedding-001 API (ingyenes tier: 1M token/hó)
- ✅ Gemini ingyenes tier
- **Költség: €0/hó** (ha marad az ingyenes tier-en)

### Fázis 3: Cloud (skálázás)
- ⬆️ ChromaDB Cloud vagy Qdrant Cloud
- ⬆️ Google embedding-001 API (fizetős tier)
- ⬆️ Gemini fizetős tier (ha szükséges)
- **Költség: €35-130/hó**

### Migrációs útmutató
```python
# rag_system.py - Könnyen cserélhető backend
class RAGSystem:
    def __init__(self, backend='local'):
        if backend == 'local':
            # Ingyenes verzió
            self.embeddings = HuggingFaceEmbeddings(...)
            self.client = chromadb.PersistentClient(...)
        elif backend == 'cloud':
            # Fizetős verzió
            self.embeddings = GoogleGenerativeAIEmbeddings(...)
            self.client = chromadb.HttpClient(...)
```

---

## 6. TELJESÍTMÉNY OPTIMALIZÁLÁS

### CPU vs GPU
```python
# CPU (alapértelmezett, minden gépen fut)
self.embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2",
    model_kwargs={'device': 'cpu'}
)

# GPU (ha van CUDA GPU)
self.embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2",
    model_kwargs={'device': 'cuda'}  # 10-50x gyorsabb
)
```

### Modell választás
```python
# Kicsi, gyors (alapértelmezett)
"all-MiniLM-L6-v2"  # 80MB, gyors, jó minőség

# Nagyobb, jobb minőség (ha van GPU)
"all-mpnet-base-v2"  # 420MB, lassabb, jobb minőség
```

---

## 7. PÉLDA HASZNÁLAT

### Knowledge base építése
```python
from rag_system_free import FreeRAGSystem
from pathlib import Path

# Inicializálás
rag = FreeRAGSystem()

# Korábbi jelentések hozzáadása
sources = [
    Path("reports/previous_reports/2024-Q1.html"),
    Path("reports/previous_reports/2024-Q2.html"),
]

rag.build_knowledge_base(sources)
print(rag.get_stats())
# Output: {'total_chunks': 245, 'vectorstore_path': 'data/vectorstore', ...}
```

### Kontextus lekérése
```python
# Query
query = "CCyB adoption trends and credit gap analysis"

# Releváns dokumentumok lekérése
results = rag.retrieve_context(query, k=3)

for result in results:
    print(f"Source: {result['metadata']['source_name']}")
    print(f"Date: {result['metadata']['date']}")
    print(f"Relevance: {result['relevance_score']:.2f}")
    print(f"Content: {result['content'][:200]}...")
    print("---")
```

---

## 8. TROUBLESHOOTING

### Probléma: Lassú első betöltés
**Megoldás:** A Sentence Transformers modell első betöltéskor letöltődik (~80MB). Ez egyszeri művelet.

### Probléma: Nincs elég memória
**Megoldás:** Használj kisebb modellt vagy csökkentsd a chunk size-ot.

### Probléma: Üres knowledge base
**Megoldás:** Ellenőrizd, hogy a forrásfájlok léteznek-e és olvashatók-e.

---

## 9. ÖSSZEFOGLALÁS

### Előnyök (ingyenes verzió):
✅ **Teljesen ingyenes** - nincs API költség
✅ **Lokális** - adatok nálad maradnak
✅ **Könnyen skálázható** - később cloud-re migrálható
✅ **Gyors** - nincs API latency
✅ **Privát** - nincs adatküldés külső szolgáltatásoknak

### Hátrányok:
❌ **Első betöltés lassú** - modell letöltése (~1-2 perc)
❌ **Memória igény** - ~500MB RAM
❌ **CPU használat** - embedding generálás CPU-n (GPU opcionális)

### ROI:
- **Költség: €0/hó**
- **Időmegtakarítás: Ugyanaz mint a fizetős verzió**
- **ROI: Végtelen** (nincs költség) 😊

---

## 10. KÖVETKEZŐ LÉPÉSEK

1. ✅ Függőségek telepítése
2. ✅ `rag_system_free.py` létrehozása
3. ✅ Korábbi jelentések előkészítése
4. ✅ Knowledge base építése
5. ✅ Integráció `main.py`-ba
6. ✅ Tesztelés

**Készen állsz az implementációra?** 🚀
