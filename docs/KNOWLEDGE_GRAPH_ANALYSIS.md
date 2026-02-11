# 🕸️ Knowledge Graph Alkalmazás - Elemzés és Implementációs Terv

## 1. MI AZ A KNOWLEDGE GRAPH?

A **Knowledge Graph** egy olyan adatstruktúra, amely:
- **Entitásokat** (országok, intézkedések, risk faktok) reprezentál
- **Kapcsolatokat** (relationships) modellez közöttük
- **Interaktív vizualizációt** tesz lehetővé
- **Szemantikus keresést** és **inferenciát** támogat

### Példa a macroprudential kontextusban:
```
[Hungary] --HAS_CCyB--> [CCyB: 2.5%]
[Hungary] --HAS_O-SII--> [O-SII: 2.0%]
[Hungary] --HAS_BBM--> [LTV: 80%]
[Hungary] --SIMILAR_TO--> [Poland]
[CCyB: 2.5%] --ADDRESSES_RISK--> [Credit Growth]
[CCyB: 2.5%] --TRIGGERED_BY--> [Credit Gap: 8.5%]
```

---

## 2. HASZNÁLATI ESETEK A DASHBOARDON

### 2.1 Interaktív Kapcsolat Vizualizáció

**Cél**: Országok, intézkedések és risk faktok közötti kapcsolatok vizualizálása

**Példa**:
- **Ország kiválasztása** → Megjelennek a kapcsolódó intézkedések, risk faktok, hasonló országok
- **Intézkedés kiválasztása** → Megjelennek azok az országok, ahol aktív, és a kapcsolódó risk faktok
- **Risk faktor kiválasztása** → Megjelennek azok az intézkedések és országok, ahol releváns

**Üzleti érték**:
- ✅ **Gyors kontextus megértés**: Egy pillantás alatt látható, hogy egy ország milyen intézkedéseket használ és miért
- ✅ **Kapcsolatok felfedezése**: Automatikus felfedezés, hogy mely országok hasonló policy mixet használnak
- ✅ **Risk propagation**: Látható, hogy egy risk faktor hogyan propagálódik intézkedéseken keresztül

### 2.2 Szemantikus Keresés

**Cél**: Természetes nyelvű keresés a knowledge graphban

**Példa**:
- "Mely országok használnak CRE-specifikus SyRB-t?"
- "Hol van aktív CCyB és O-SII együtt?"
- "Mely országok hasonlóak Magyarországhoz?"

**Üzleti érték**:
- ✅ **Gyors információkeresés**: Nem kell táblázatokat böngészni
- ✅ **Komplex lekérdezések**: Több feltétel együttes keresése

### 2.3 Trend és Anomália Detektálás

**Cél**: Kapcsolatok változásainak követése

**Példa**:
- **Temporal edges**: `[Hungary] --HAD_CCyB_AT--> [2024-01-15: 2.5%]`
- **Change detection**: Ha egy ország CCyB-je változik, automatikusan frissül a graph
- **Anomaly detection**: Ha egy ország hirtelen eltér a regionális trendtől, jelzés

**Üzleti érték**:
- ✅ **Proaktív monitoring**: Korai jelzések változásokról
- ✅ **Pattern recognition**: Trendek automatikus felismerése

### 2.4 AI-Enhanced Insights

**Cél**: LLM + Knowledge Graph kombináció

**Példa**:
- **Graph traversal**: LLM végigjárja a graphot, hogy kontextust kapjon
- **Relationship inference**: LLM következtet új kapcsolatokra (pl. "Magyarország és Lengyelország hasonló policy mixet használnak")
- **Explanatory AI**: LLM magyarázatot ad a kapcsolatokra

**Üzleti érték**:
- ✅ **Deeper insights**: Többdimenziós elemzés
- ✅ **Explainability**: Transzparens AI döntések

---

## 3. TECHNOLÓGIAI MEGOLDÁSOK

### 3.1 Frontend-only (JavaScript) - ⭐ AJÁNLOTT MVP

**Technológia**: 
- **vis.js Network** (https://visjs.github.io/vis-network/docs/network/)
- **Cytoscape.js** (https://js.cytoscape.org/)
- **D3.js Force Graph** (https://observablehq.com/@d3/force-directed-graph)

**Előnyök**:
- ✅ **Ingyenes**: Nincs backend szükség
- ✅ **Gyors**: Client-side rendering
- ✅ **Könnyű integráció**: JavaScript library, beilleszthető a meglévő HTML-be
- ✅ **Interaktív**: Zoom, pan, click events

**Hátrányok**:
- ❌ **Statikus adatok**: Csak előre generált adatokkal működik
- ❌ **Korlátozott lekérdezés**: Nincs komplex graph query nyelv
- ❌ **Skálázhatóság**: Max ~1000 node-ig jól működik

**Implementáció**:
```javascript
// app.js - hozzáadandó
function initKnowledgeGraph(data) {
    const nodes = [
        {id: 'HU', label: 'Hungary', group: 'country', color: '#3b82f6'},
        {id: 'CCyB_HU', label: 'CCyB: 2.5%', group: 'measure', color: '#10b981'},
        {id: 'O-SII_HU', label: 'O-SII: 2.0%', group: 'measure', color: '#f59e0b'},
        {id: 'Credit_Growth', label: 'Credit Growth', group: 'risk', color: '#ef4444'},
    ];
    
    const edges = [
        {from: 'HU', to: 'CCyB_HU', label: 'HAS', color: '#64748b'},
        {from: 'HU', to: 'O-SII_HU', label: 'HAS', color: '#64748b'},
        {from: 'CCyB_HU', to: 'Credit_Growth', label: 'ADDRESSES', color: '#ef4444'},
    ];
    
    const container = document.getElementById('knowledge-graph');
    const data = {nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges)};
    const options = {
        nodes: {
            shape: 'dot',
            size: 20,
            font: {size: 14},
        },
        edges: {
            arrows: {to: {enabled: true}},
            font: {size: 12, align: 'middle'},
        },
        physics: {
            enabled: true,
            stabilization: {iterations: 200},
        },
        interaction: {
            hover: true,
            tooltipDelay: 100,
        },
    };
    
    const network = new vis.Network(container, data, options);
    
    // Click event: ország kiválasztása
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            // Navigate to country profile
            window.location.hash = `country=${nodeId}`;
        }
    });
}
```

**Adatstruktúra**:
```python
# country_profiles.py - hozzáadandó metódus
def build_knowledge_graph_data(
    self,
    countries: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Knowledge graph adatok generálása.
    
    Returns:
        {
            'nodes': [
                {'id': 'HU', 'label': 'Hungary', 'group': 'country', ...},
                {'id': 'CCyB_HU', 'label': 'CCyB: 2.5%', 'group': 'measure', ...},
            ],
            'edges': [
                {'from': 'HU', 'to': 'CCyB_HU', 'label': 'HAS', ...},
            ]
        }
    """
    nodes = []
    edges = []
    
    # Országok
    target_countries = countries or self.countries
    for country in target_countries:
        profile = self.get_country_profile(country)
        iso2 = profile.get('iso2') or country[:2].upper()
        
        nodes.append({
            'id': iso2,
            'label': country,
            'group': 'country',
            'title': f"{country} - Click for details",
            'value': profile.get('current_status', {}).get('total_capital', {}).get('total', 0),
        })
        
        # CCyB kapcsolat
        ccyb = profile.get('current_status', {}).get('ccyb')
        if ccyb and ccyb.get('rate', 0) > 0:
            node_id = f"CCyB_{iso2}"
            nodes.append({
                'id': node_id,
                'label': f"CCyB: {ccyb['rate']}%",
                'group': 'ccyb',
                'title': f"CCyB rate: {ccyb['rate']}% (Effective: {ccyb.get('date', 'N/A')})",
            })
            edges.append({
                'from': iso2,
                'to': node_id,
                'label': 'HAS',
                'title': 'Has active CCyB',
            })
        
        # SyRB kapcsolat
        syrb = profile.get('current_status', {}).get('syrb')
        if syrb and syrb.get('rate', 0) > 0:
            node_id = f"SyRB_{iso2}"
            nodes.append({
                'id': node_id,
                'label': f"SyRB: {syrb['rate']}% ({syrb.get('type', 'General')})",
                'group': 'syrb',
                'title': f"SyRB rate: {syrb['rate']}% - {syrb.get('type', 'General')}",
            })
            edges.append({
                'from': iso2,
                'to': node_id,
                'label': 'HAS',
                'title': 'Has active SyRB',
            })
        
        # O-SII kapcsolat
        osii = profile.get('current_status', {}).get('osii')
        if osii and osii.get('rate', 0) > 0:
            node_id = f"O-SII_{iso2}"
            nodes.append({
                'id': node_id,
                'label': f"O-SII: {osii['rate']}%",
                'group': 'osii',
                'title': f"O-SII rate: {osii['rate']}%",
            })
            edges.append({
                'from': iso2,
                'to': node_id,
                'label': 'HAS',
                'title': 'Has active O-SII buffer',
            })
        
        # BBM kapcsolat
        bbm = profile.get('current_status', {}).get('bbm', [])
        if bbm:
            for measure_type in bbm:
                node_id = f"BBM_{iso2}_{measure_type}"
                nodes.append({
                    'id': node_id,
                    'label': f"BBM: {measure_type}",
                    'group': 'bbm',
                    'title': f"Borrower-based measure: {measure_type}",
                })
                edges.append({
                    'from': iso2,
                    'to': node_id,
                    'label': 'HAS',
                    'title': f'Has active {measure_type}',
                })
    
    # Hasonló országok kapcsolatai
    for country in target_countries:
        profile = self.get_country_profile(country)
        comparison = profile.get('comparison', {})
        similar = comparison.get('similar_countries', [])
        
        iso2 = profile.get('iso2') or country[:2].upper()
        for similar_country in similar:
            similar_iso2 = self._get_iso2(similar_country.get('COUNTRY', '')) or similar_country.get('COUNTRY', '')[:2].upper()
            if similar_iso2 and similar_iso2 != iso2:
                edges.append({
                    'from': iso2,
                    'to': similar_iso2,
                    'label': 'SIMILAR',
                    'title': f'Similar capital buffer level: {similar_country.get("Total", 0)}%',
                    'dashes': True,  # Szaggatott vonal
                })
    
    # Deduplikáció
    nodes = list({node['id']: node for node in nodes}.values())
    
    return {
        'nodes': nodes,
        'edges': edges,
    }
```

### 3.2 Backend Knowledge Graph (Neo4j / RDF) - ⭐ KÉSŐBBI BŐVÍTÉS

**Technológia**:
- **Neo4j** (https://neo4j.com/) - Graph database
- **RDF/SPARQL** (https://www.w3.org/TR/sparql11-query/) - Szemantikus web standard
- **Apache Jena** (https://jena.apache.org/) - RDF framework

**Előnyök**:
- ✅ **Komplex lekérdezések**: Cypher (Neo4j) vagy SPARQL nyelv
- ✅ **Skálázhatóság**: Milliós node-ok kezelése
- ✅ **Real-time updates**: Dinamikus adatfrissítés
- ✅ **Inferencia**: Automatikus kapcsolat következtetés

**Hátrányok**:
- ❌ **Backend szükség**: Szerver, adatbázis
- ❌ **Költség**: Neo4j Cloud vagy self-hosted
- ❌ **Komplexitás**: Több komponens karbantartása

**Használati eset**: Ha később komplex graph analytics-re van szükség (pl. path finding, community detection, stb.)

### 3.3 Hybrid (Pre-computed + Interactive) - ⭐ OPTIMÁLIS

**Koncepció**:
- **Backend**: Python script generálja a graph adatokat (JSON formátumban)
- **Frontend**: JavaScript library (vis.js) rendereli interaktívan
- **Update**: Új adatok esetén újragenerálás

**Előnyök**:
- ✅ **Ingyenes**: Nincs backend szükség
- ✅ **Gyors**: Pre-computed adatok
- ✅ **Interaktív**: Client-side rendering
- ✅ **Skálázható**: Később könnyen átmigrálható Neo4j-re

**Implementáció**:
```python
# main.py - hozzáadandó
from country_profiles import CountryProfileGenerator

# Graph adatok generálása
profile_gen = CountryProfileGenerator({
    'ccyb_df': ccyb_df,
    'syrb_df': syrb_df,
    'bbm_df': bbm_df,
    'osii_df': osii_df,
    'capital_overall_df': capital_overall_df,
})

graph_data = profile_gen.build_knowledge_graph_data()
graph_json = json.dumps(graph_data)

# HTML-be beillesztés
tables_html['knowledge_graph_data'] = graph_json
```

```html
<!-- report_template.html - hozzáadandó -->
<section id="tab-knowledge-graph" class="tab-content">
    <h1>Knowledge Graph</h1>
    
    <div class="card">
        <div class="card-title">🕸️ Macroprudential Policy Network</div>
        <div id="knowledge-graph-container" style="width: 100%; height: 800px; border: 1px solid #e2e8f0; border-radius: 8px;"></div>
        <div class="graph-legend">
            <div class="legend-item">
                <span class="legend-color" style="background: #3b82f6;"></span>
                <span>Countries</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background: #10b981;"></span>
                <span>CCyB</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background: #f59e0b;"></span>
                <span>SyRB</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background: #ef4444;"></span>
                <span>O-SII</span>
            </div>
            <div class="legend-item">
                <span class="legend-color" style="background: #8b5cf6;"></span>
                <span>BBM</span>
            </div>
        </div>
    </div>
    
    <div class="card">
        <div class="card-title">ℹ️ How to Use</div>
        <ul>
            <li><strong>Click</strong> on a country node to view its profile</li>
            <li><strong>Hover</strong> over nodes to see details</li>
            <li><strong>Drag</strong> nodes to rearrange the layout</li>
            <li><strong>Zoom</strong> with mouse wheel or pinch gesture</li>
            <li><strong>Dashed lines</strong> indicate similar countries</li>
        </ul>
    </div>
</section>
```

```javascript
// app.js - hozzáadandó
function initKnowledgeGraph() {
    // Graph adatok betöltése (JSON formátumban)
    const graphData = JSON.parse(document.getElementById('knowledge-graph-data').textContent);
    
    // vis.js Network inicializálása
    const container = document.getElementById('knowledge-graph-container');
    const data = {
        nodes: new vis.DataSet(graphData.nodes),
        edges: new vis.DataSet(graphData.edges),
    };
    
    const options = {
        nodes: {
            shape: 'dot',
            size: 25,
            font: {
                size: 14,
                face: 'Inter, sans-serif',
            },
            borderWidth: 2,
            shadow: true,
        },
        edges: {
            arrows: {
                to: {
                    enabled: true,
                    scaleFactor: 0.8,
                },
            },
            font: {
                size: 11,
                align: 'middle',
                face: 'Inter, sans-serif',
            },
            width: 2,
            smooth: {
                type: 'continuous',
            },
        },
        groups: {
            country: {
                color: {background: '#3b82f6', border: '#2563eb'},
                font: {color: '#ffffff', size: 16, face: 'Inter, sans-serif'},
            },
            ccyb: {
                color: {background: '#10b981', border: '#059669'},
                font: {color: '#ffffff', size: 12},
            },
            syrb: {
                color: {background: '#f59e0b', border: '#d97706'},
                font: {color: '#ffffff', size: 12},
            },
            osii: {
                color: {background: '#ef4444', border: '#dc2626'},
                font: {color: '#ffffff', size: 12},
            },
            bbm: {
                color: {background: '#8b5cf6', border: '#7c3aed'},
                font: {color: '#ffffff', size: 12},
            },
        },
        physics: {
            enabled: true,
            stabilization: {
                iterations: 200,
            },
            barnesHut: {
                gravitationalConstant: -2000,
                centralGravity: 0.1,
                springLength: 200,
                springConstant: 0.04,
                damping: 0.09,
            },
        },
        interaction: {
            hover: true,
            tooltipDelay: 100,
            zoomView: true,
            dragView: true,
        },
        layout: {
            improvedLayout: true,
        },
    };
    
    const network = new vis.Network(container, data, options);
    
    // Click event: ország kiválasztása
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            const node = data.nodes.get(nodeId);
            
            if (node.group === 'country') {
                // Navigate to country profile
                window.location.hash = `country=${encodeURIComponent(node.label)}`;
            } else {
                // Show measure details
                showMeasureDetails(node);
            }
        }
    });
    
    // Double-click: zoom to node
    network.on('doubleClick', function(params) {
        if (params.nodes.length > 0) {
            network.focus(params.nodes[0], {
                scale: 1.5,
                animation: true,
            });
        }
    });
    
    // Hover: highlight connected nodes
    network.on('hoverNode', function(params) {
        const nodeId = params.node;
        const connectedNodes = network.getConnectedNodes(nodeId);
        
        // Highlight connected nodes
        const updateNodes = data.nodes.map(node => {
            if (node.id === nodeId) {
                return {id: node.id, borderWidth: 4};
            } else if (connectedNodes.includes(node.id)) {
                return {id: node.id, opacity: 0.8};
            } else {
                return {id: node.id, opacity: 0.3};
            }
        });
        data.nodes.update(updateNodes);
    });
    
    network.on('blurNode', function(params) {
        // Reset opacity
        const updateNodes = data.nodes.map(node => ({
            id: node.id,
            opacity: 1,
        }));
        data.nodes.update(updateNodes);
    });
}

// Graph inicializálása oldal betöltésekor
document.addEventListener('DOMContentLoaded', function() {
    if (document.getElementById('knowledge-graph-container')) {
        initKnowledgeGraph();
    }
});
```

---

## 4. ÜZLETI ÉRTÉK

### 4.1 Felhasználói Élmény
- ✅ **Vizuális kontextus**: Egy pillantás alatt látható a teljes policy landscape
- ✅ **Interaktív felfedezés**: Kattintással navigálás országok és intézkedések között
- ✅ **Kapcsolatok felfedezése**: Automatikus felfedezés, hogy mely országok hasonlóak

### 4.2 Elemzési Lehetőségek
- ✅ **Pattern recognition**: Trendek automatikus felismerése
- ✅ **Anomaly detection**: Eltérések azonosítása
- ✅ **Comparative analysis**: Országok közötti összehasonlítás

### 4.3 AI Integration
- ✅ **Graph-enhanced LLM**: LLM kap contextet a graphból
- ✅ **Relationship inference**: Automatikus kapcsolat következtetés
- ✅ **Explanatory AI**: Transzparens magyarázatok

---

## 5. IMPLEMENTÁCIÓS TERV

### Fázis 1: MVP (1 hét) - ⭐ AJÁNLOTT

**Cél**: Alapvető knowledge graph vizualizáció

**Lépések**:
1. ✅ `build_knowledge_graph_data()` metódus implementálása
2. ✅ vis.js integrálása (CDN)
3. ✅ Graph adatok generálása Python-ban
4. ✅ HTML template létrehozása
5. ✅ JavaScript interakciók (click, hover)

**Kimenet**:
- Interaktív graph a dashboardon
- Országok, intézkedések, kapcsolatok
- Click → Country Profile navigáció

### Fázis 2: Bővítések (1 hét)

**Cél**: További funkciók

**Lépések**:
1. ✅ **Filtering**: Ország típus szerint (CEE, Nordics, stb.)
2. ✅ **Search**: Ország/intézkedés keresés
3. ✅ **Temporal view**: Időbeli változások animációja
4. ✅ **Export**: PNG/SVG export

### Fázis 3: AI Integration (1 hét)

**Cél**: LLM + Knowledge Graph

**Lépések**:
1. ✅ **Graph context**: LLM kap graph adatokat contextként
2. ✅ **Relationship inference**: LLM következtet új kapcsolatokra
3. ✅ **Explanatory AI**: LLM magyarázatot ad a kapcsolatokra

---

## 6. PÉLDA ADATSTRUKTÚRA

```json
{
  "nodes": [
    {
      "id": "HU",
      "label": "Hungary",
      "group": "country",
      "title": "Hungary - Total Capital: 4.5%",
      "value": 4.5
    },
    {
      "id": "CCyB_HU",
      "label": "CCyB: 2.5%",
      "group": "ccyb",
      "title": "CCyB rate: 2.5% (Effective: 2024-01-15)"
    },
    {
      "id": "O-SII_HU",
      "label": "O-SII: 2.0%",
      "group": "osii",
      "title": "O-SII rate: 2.0%"
    },
    {
      "id": "BBM_HU_LTV",
      "label": "BBM: LTV",
      "group": "bbm",
      "title": "Borrower-based measure: LTV"
    }
  ],
  "edges": [
    {
      "from": "HU",
      "to": "CCyB_HU",
      "label": "HAS",
      "title": "Has active CCyB"
    },
    {
      "from": "HU",
      "to": "O-SII_HU",
      "label": "HAS",
      "title": "Has active O-SII buffer"
    },
    {
      "from": "HU",
      "to": "BBM_HU_LTV",
      "label": "HAS",
      "title": "Has active LTV measure"
    },
    {
      "from": "HU",
      "to": "PL",
      "label": "SIMILAR",
      "title": "Similar capital buffer level: 4.2%",
      "dashes": true
    }
  ]
}
```

---

## 7. ÖSSZEFOGLALÁS

### ✅ Érdemes-e implementálni?

**IGEN** - Ha:
- ✅ Interaktív vizualizációt szeretnél
- ✅ Kapcsolatok felfedezését szeretnéd támogatni
- ✅ Modern, innovatív UI-t szeretnél
- ✅ Később AI-t szeretnél integrálni

**NEM** - Ha:
- ❌ Egyszerű táblázatok elégnek
- ❌ Nincs idő/erőforrás a fejlesztésre
- ❌ A felhasználók nem használnák

### 🎯 Ajánlás

**MVP implementáció (Fázis 1)**:
- ⏱️ **Idő**: 1 hét
- 💰 **Költség**: Ingyenes (vis.js CDN)
- 🎨 **Érték**: Magas (interaktív, modern UI)
- 📈 **Skálázhatóság**: Később könnyen bővíthető

**Kezdjük el az MVP-t?** 🚀
