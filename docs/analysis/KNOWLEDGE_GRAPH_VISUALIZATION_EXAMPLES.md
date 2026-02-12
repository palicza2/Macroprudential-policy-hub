# 🕸️ Knowledge Graph - Konkrét Vizualizációs Példák

## 1. MIT ÉRTÜNK "INTERAKTÍV VIZUALIZÁCIÓ" ALATT?

Az **interaktív vizualizáció** azt jelenti, hogy:
- ✅ **Kattintható elemek**: Node-okra kattintva részletek jelennek meg
- ✅ **Hover tooltip**: Egérrel való rámutatáskor információkat mutat
- ✅ **Zoom & Pan**: Nagyítás/kicsinyítés, húzás
- ✅ **Filtering**: Szűrés ország/típus szerint
- ✅ **Highlighting**: Kapcsolódó node-ok kiemelése
- ✅ **Animáció**: Smooth transitions, physics simulation

**Példa**: Google Knowledge Graph, LinkedIn kapcsolati háló, GitHub dependency graph

---

## 2. MIT LEHETNE VIZUALIZÁLNI?

### 2.1 Alapvető Entitások (Nodes)

#### A) Országok
```
[Hungary] 🇭🇺
[Poland] 🇵🇱
[Germany] 🇩🇪
[Sweden] 🇸🇪
...
```

**Tulajdonságok**:
- **Szín**: Region szerint (CEE: kék, Nordics: zöld, stb.)
- **Méret**: Total capital buffer szerint (nagyobb = több tőkepuffer)
- **Forma**: Kör (country node)

#### B) Intézkedések (Measures)
```
[CCyB: 2.5%] - Hungary
[CCyB: 0.5%] - Germany
[SyRB: 1.0%] - Sweden
[O-SII: 2.0%] - Hungary
[BBM: LTV] - Hungary
[BBM: DSTI] - Hungary
```

**Tulajdonságok**:
- **Szín**: Intézkedés típusa szerint
  - CCyB: 🔵 Kék
  - SyRB: 🟠 Narancs
  - O-SII: 🔴 Piros
  - BBM: 🟣 Lila
- **Méret**: Ráta szerint (nagyobb = magasabb ráta)
- **Forma**: Négyzet vagy háromszög (measure node)

#### C) Risk Faktok (Opcionális)
```
[Credit Growth Risk]
[Real Estate Risk]
[Household Indebtedness]
[Systemic Risk]
```

**Tulajdonságok**:
- **Szín**: Risk szint szerint (magas: piros, alacsony: zöld)
- **Forma**: Gyémánt (risk node)

---

### 2.2 Kapcsolatok (Edges)

#### A) HAS kapcsolat
```
[Hungary] --HAS--> [CCyB: 2.5%]
[Hungary] --HAS--> [O-SII: 2.0%]
[Hungary] --HAS--> [BBM: LTV]
```

**Jelentés**: Egy országnak van aktív intézkedése

**Vizualizáció**:
- **Szín**: Szürke vagy intézkedés színe
- **Vastagság**: Ráta szerint (magasabb ráta = vastagabb vonal)
- **Nyíl**: Irányított (ország → intézkedés)

#### B) SIMILAR kapcsolat
```
[Hungary] --SIMILAR--> [Poland]
[Hungary] --SIMILAR--> [Czech Republic]
[Sweden] --SIMILAR--> [Norway]
```

**Jelentés**: Hasonló tőkepuffer szint vagy policy mix

**Vizualizáció**:
- **Szín**: Kék vagy zöld (pozitív kapcsolat)
- **Stílus**: Szaggatott vonal (dashed)
- **Vastagság**: Hasonlóság mértéke szerint

#### C) ADDRESSES kapcsolat (Opcionális)
```
[CCyB: 2.5%] --ADDRESSES--> [Credit Growth Risk]
[SyRB: 1.0%] --ADDRESSES--> [Real Estate Risk]
```

**Jelentés**: Egy intézkedés egy adott risk faktort kezel

**Vizualizáció**:
- **Szín**: Piros (risk kapcsolat)
- **Stílus**: Vastag vonal

#### D) TEMPORAL kapcsolat (Opcionális)
```
[Hungary] --HAD_CCyB_AT--> [2024-01-15: 2.5%]
[Hungary] --HAD_CCyB_AT--> [2023-06-01: 2.0%]
```

**Jelentés**: Időbeli változások

**Vizualizáció**:
- **Szín**: Szürke (temporal)
- **Stílus**: Vékony vonal

---

## 3. KONKRÉT VIZUALIZÁCIÓS PÉLDÁK

### Példa 1: Egyszerű Ország-Intézkedés Graph

```
        [Hungary] 🇭🇺
           /  |  \
          /   |   \
    [CCyB] [O-SII] [BBM]
    2.5%    2.0%   LTV
```

**Interaktivitás**:
- **Click [Hungary]**: Navigate to Country Profile
- **Hover [CCyB: 2.5%]**: Tooltip: "CCyB rate: 2.5%, Effective: 2024-01-15, Credit Gap: 8.5%"
- **Click [CCyB: 2.5%]**: Show all countries with similar CCyB rates

**Vizualizáció**:
- **Layout**: Force-directed (fizikai szimuláció, node-ok "taszítják" egymást)
- **Színek**: 
  - Hungary: 🔵 Kék (country)
  - CCyB: 🟢 Zöld (measure)
  - O-SII: 🟠 Narancs (measure)
  - BBM: 🟣 Lila (measure)

---

### Példa 2: Regionális Hasonlóság Graph

```
    [Hungary] 🇭🇺
        | \
        |  \---SIMILAR---[Poland] 🇵🇱
        |                    |
    [CCyB: 2.5%]        [CCyB: 2.0%]
        |                    |
        +---SIMILAR---+
```

**Interaktivitás**:
- **Click [Hungary]**: Highlight connected nodes (Poland, measures)
- **Hover [SIMILAR edge]**: Tooltip: "Similar capital buffer: 4.5% vs 4.2%"
- **Double-click [Poland]**: Zoom to Poland and its measures

**Vizualizáció**:
- **Layout**: Hierarchical vagy force-directed
- **SIMILAR edges**: Szaggatott vonal, kék szín
- **Node grouping**: Region szerint (CEE: bal oldal, Nordics: jobb oldal)

---

### Példa 3: Teljes Policy Landscape (Minden Ország)

```
    [HU] --HAS--> [CCyB: 2.5%]
    [HU] --HAS--> [O-SII: 2.0%]
    [HU] --HAS--> [BBM: LTV]
    [HU] --SIMILAR--> [PL]
    
    [PL] --HAS--> [CCyB: 2.0%]
    [PL] --HAS--> [BBM: LTV]
    [PL] --SIMILAR--> [CZ]
    
    [DE] --HAS--> [CCyB: 0.5%]
    [DE] --SIMILAR--> [NL]
    
    [SE] --HAS--> [CCyB: 2.0%]
    [SE] --HAS--> [SyRB: 1.0%]
    [SE] --SIMILAR--> [NO]
```

**Interaktivitás**:
- **Filter by measure**: Csak CCyB node-ok megjelenítése
- **Filter by region**: Csak CEE országok
- **Search**: "Hungary" keresés → highlight Hungary és kapcsolatai
- **Cluster view**: Országok csoportosítása region/measure szerint

**Vizualizáció**:
- **Layout**: Force-directed (node-ok automatikusan elrendeződnek)
- **Physics**: 
  - Node-ok "taszítják" egymást (repulsion)
  - Edges "vonzzák" a kapcsolódó node-okat (attraction)
  - Stabilizáció után smooth layout

---

### Példa 4: Temporal Evolution (Időbeli Változások)

```
    [Hungary] 
        |
        +--HAD_CCyB_AT--> [2024-01-15: 2.5%]
        |
        +--HAD_CCyB_AT--> [2023-06-01: 2.0%]
        |
        +--HAD_CCyB_AT--> [2022-01-01: 1.5%]
```

**Interaktivitás**:
- **Timeline slider**: Választott dátum szerint szűrés
- **Animation**: Időbeli változások animálása
- **Hover temporal node**: Tooltip: "CCyB rate on 2024-01-15: 2.5%"

**Vizualizáció**:
- **Layout**: Timeline-based (balról jobbra: idő)
- **Temporal nodes**: Kisebb, szürke node-ok
- **Edges**: Vékony, szürke vonalak

---

## 4. INTERAKTÍV FUNKCIÓK RÉSZLETESEN

### 4.1 Click Events

#### A) Ország Node Click
```javascript
network.on('click', function(params) {
    if (params.nodes.length > 0) {
        const nodeId = params.nodes[0];
        const node = data.nodes.get(nodeId);
        
        if (node.group === 'country') {
            // Navigate to country profile
            window.location.hash = `country=${encodeURIComponent(node.label)}`;
            
            // VAGY: Modal megnyitása ország részletekkel
            showCountryModal(node.label);
        }
    }
});
```

**Eredmény**: 
- Navigálás az országprofil oldalra
- VAGY: Modal ablak megnyitása ország részletekkel

#### B) Measure Node Click
```javascript
if (node.group === 'ccyb' || node.group === 'syrb' || node.group === 'osii' || node.group === 'bbm') {
    // Show all countries with this measure
    const connectedCountries = network.getConnectedNodes(nodeId)
        .filter(id => data.nodes.get(id).group === 'country');
    
    // Highlight connected countries
    highlightNodes(connectedCountries);
    
    // Show measure details
    showMeasureDetails(node);
}
```

**Eredmény**:
- Kapcsolódó országok kiemelése
- Intézkedés részleteinek megjelenítése

---

### 4.2 Hover Events

#### A) Node Hover
```javascript
network.on('hoverNode', function(params) {
    const nodeId = params.node;
    const node = data.nodes.get(nodeId);
    
    // Show tooltip
    showTooltip(node);
    
    // Highlight connected nodes
    const connectedNodes = network.getConnectedNodes(nodeId);
    highlightNodes(connectedNodes);
    
    // Dim other nodes
    dimOtherNodes(nodeId, connectedNodes);
});
```

**Eredmény**:
- Tooltip megjelenítése (pl. "Hungary - Total Capital: 4.5%, CCyB: 2.5%, O-SII: 2.0%")
- Kapcsolódó node-ok kiemelése (opacity: 1.0)
- Egyéb node-ok elhalványítása (opacity: 0.3)

#### B) Edge Hover
```javascript
network.on('hoverEdge', function(params) {
    const edgeId = params.edge;
    const edge = data.edges.get(edgeId);
    
    // Show edge tooltip
    showEdgeTooltip(edge);
    
    // Highlight edge
    data.edges.update({id: edgeId, width: 5});
});
```

**Eredmény**:
- Edge tooltip (pl. "HAS - CCyB rate: 2.5%, Effective: 2024-01-15")
- Edge vastagságának növelése

---

### 4.3 Filtering

#### A) Measure Type Filter
```javascript
function filterByMeasureType(measureType) {
    // Hide all nodes
    const allNodes = data.nodes.getIds();
    data.nodes.update(allNodes.map(id => ({id, hidden: true})));
    
    // Show countries
    const countryNodes = data.nodes.get({
        filter: node => node.group === 'country'
    });
    data.nodes.update(countryNodes.map(node => ({id: node.id, hidden: false})));
    
    // Show selected measure type
    const measureNodes = data.nodes.get({
        filter: node => node.group === measureType
    });
    data.nodes.update(measureNodes.map(node => ({id: node.id, hidden: false})));
    
    // Show connected edges
    const visibleNodeIds = [...countryNodes.map(n => n.id), ...measureNodes.map(n => n.id)];
    const connectedEdges = data.edges.get({
        filter: edge => 
            visibleNodeIds.includes(edge.from) && 
            visibleNodeIds.includes(edge.to)
    });
    data.edges.update(connectedEdges.map(edge => ({id: edge.id, hidden: false})));
}
```

**Használat**:
- Dropdown: "Show only CCyB measures"
- Checkbox: "Show SyRB", "Show O-SII", stb.

#### B) Region Filter
```javascript
function filterByRegion(region) {
    const regionCountries = getCountriesByRegion(region); // ['Hungary', 'Poland', ...]
    
    // Hide all nodes
    const allNodes = data.nodes.getIds();
    data.nodes.update(allNodes.map(id => ({id, hidden: true})));
    
    // Show region countries and their measures
    const visibleNodes = [];
    for (const country of regionCountries) {
        const countryNode = data.nodes.get({
            filter: node => node.label === country
        })[0];
        if (countryNode) {
            visibleNodes.push(countryNode.id);
            const measures = network.getConnectedNodes(countryNode.id);
            visibleNodes.push(...measures);
        }
    }
    
    data.nodes.update(visibleNodes.map(id => ({id, hidden: false})));
}
```

**Használat**:
- Dropdown: "CEE", "Nordics", "Western Europe", stb.

---

### 4.4 Search

```javascript
function searchNodes(query) {
    const matchingNodes = data.nodes.get({
        filter: node => 
            node.label.toLowerCase().includes(query.toLowerCase())
    });
    
    if (matchingNodes.length > 0) {
        // Focus on first match
        network.focus(matchingNodes[0].id, {
            scale: 1.5,
            animation: true,
        });
        
        // Highlight all matches
        highlightNodes(matchingNodes.map(n => n.id));
    }
}
```

**Használat**:
- Search box: "Hungary" → Focus on Hungary node
- Auto-complete: "Hu..." → Suggestions: "Hungary", "Hungary CCyB", stb.

---

### 4.5 Zoom & Pan

```javascript
// Mouse wheel zoom
network.on('wheel', function(params) {
    // Zoom in/out
});

// Drag to pan
network.on('dragStart', function(params) {
    // Start dragging
});

// Double-click to zoom
network.on('doubleClick', function(params) {
    if (params.nodes.length > 0) {
        network.focus(params.nodes[0], {
            scale: 2.0,
            animation: true,
        });
    }
});
```

**Használat**:
- **Mouse wheel**: Zoom in/out
- **Click + drag**: Pan (mozgatás)
- **Double-click node**: Zoom to node
- **Fit button**: Reset zoom, show all nodes

---

## 5. KONKRÉT UI ELEMEK

### 5.1 Graph Container

```html
<div class="knowledge-graph-section">
    <!-- Controls -->
    <div class="graph-controls">
        <div class="control-group">
            <label>Filter by Measure:</label>
            <select id="measure-filter">
                <option value="all">All Measures</option>
                <option value="ccyb">CCyB only</option>
                <option value="syrb">SyRB only</option>
                <option value="osii">O-SII only</option>
                <option value="bbm">BBM only</option>
            </select>
        </div>
        
        <div class="control-group">
            <label>Filter by Region:</label>
            <select id="region-filter">
                <option value="all">All Regions</option>
                <option value="cee">CEE</option>
                <option value="nordics">Nordics</option>
                <option value="western">Western Europe</option>
            </select>
        </div>
        
        <div class="control-group">
            <input type="text" id="node-search" placeholder="Search country or measure...">
        </div>
        
        <button id="reset-view">Reset View</button>
        <button id="export-png">Export PNG</button>
    </div>
    
    <!-- Graph Canvas -->
    <div id="knowledge-graph-container" style="width: 100%; height: 800px; border: 1px solid #e2e8f0; border-radius: 8px;"></div>
    
    <!-- Legend -->
    <div class="graph-legend">
        <div class="legend-item">
            <span class="legend-node country"></span>
            <span>Countries</span>
        </div>
        <div class="legend-item">
            <span class="legend-node ccyb"></span>
            <span>CCyB</span>
        </div>
        <div class="legend-item">
            <span class="legend-node syrb"></span>
            <span>SyRB</span>
        </div>
        <div class="legend-item">
            <span class="legend-node osii"></span>
            <span>O-SII</span>
        </div>
        <div class="legend-item">
            <span class="legend-node bbm"></span>
            <span>BBM</span>
        </div>
        <div class="legend-item">
            <span class="legend-edge has"></span>
            <span>HAS (solid)</span>
        </div>
        <div class="legend-item">
            <span class="legend-edge similar"></span>
            <span>SIMILAR (dashed)</span>
        </div>
    </div>
</div>
```

### 5.2 Tooltip

```html
<div id="graph-tooltip" class="graph-tooltip" style="display: none;">
    <div class="tooltip-title">Hungary</div>
    <div class="tooltip-content">
        <div><strong>Total Capital:</strong> 4.5%</div>
        <div><strong>CCyB:</strong> 2.5% (Active)</div>
        <div><strong>O-SII:</strong> 2.0% (Active)</div>
        <div><strong>BBM:</strong> LTV, DSTI</div>
        <div><strong>Similar to:</strong> Poland, Czech Republic</div>
    </div>
    <div class="tooltip-actions">
        <button onclick="navigateToCountry('Hungary')">View Profile</button>
    </div>
</div>
```

---

## 6. PÉLDA ADATSTRUKTÚRA (JSON)

```json
{
  "nodes": [
    {
      "id": "HU",
      "label": "Hungary",
      "group": "country",
      "title": "Hungary - Total Capital: 4.5%",
      "value": 4.5,
      "region": "CEE",
      "color": "#3b82f6"
    },
    {
      "id": "CCyB_HU",
      "label": "CCyB: 2.5%",
      "group": "ccyb",
      "title": "CCyB rate: 2.5%, Effective: 2024-01-15, Credit Gap: 8.5%",
      "value": 2.5,
      "color": "#10b981"
    },
    {
      "id": "O-SII_HU",
      "label": "O-SII: 2.0%",
      "group": "osii",
      "title": "O-SII rate: 2.0%",
      "value": 2.0,
      "color": "#ef4444"
    },
    {
      "id": "BBM_HU_LTV",
      "label": "BBM: LTV",
      "group": "bbm",
      "title": "Borrower-based measure: LTV (80% for FTB: 90%)",
      "color": "#8b5cf6"
    },
    {
      "id": "PL",
      "label": "Poland",
      "group": "country",
      "title": "Poland - Total Capital: 4.2%",
      "value": 4.2,
      "region": "CEE",
      "color": "#3b82f6"
    },
    {
      "id": "CCyB_PL",
      "label": "CCyB: 2.0%",
      "group": "ccyb",
      "title": "CCyB rate: 2.0%, Effective: 2023-12-01",
      "value": 2.0,
      "color": "#10b981"
    }
  ],
  "edges": [
    {
      "from": "HU",
      "to": "CCyB_HU",
      "label": "HAS",
      "title": "Has active CCyB",
      "color": "#64748b",
      "width": 3
    },
    {
      "from": "HU",
      "to": "O-SII_HU",
      "label": "HAS",
      "title": "Has active O-SII buffer",
      "color": "#64748b",
      "width": 2
    },
    {
      "from": "HU",
      "to": "BBM_HU_LTV",
      "label": "HAS",
      "title": "Has active LTV measure",
      "color": "#64748b",
      "width": 2
    },
    {
      "from": "HU",
      "to": "PL",
      "label": "SIMILAR",
      "title": "Similar capital buffer level: 4.5% vs 4.2%",
      "color": "#3b82f6",
      "dashes": true,
      "width": 2
    },
    {
      "from": "PL",
      "to": "CCyB_PL",
      "label": "HAS",
      "title": "Has active CCyB",
      "color": "#64748b",
      "width": 3
    }
  ]
}
```

---

## 7. ÖSSZEFOGLALÁS

### Mit lehet vizualizálni?

1. **Országok** → Intézkedések kapcsolatai
2. **Hasonló országok** → Policy mix összehasonlítás
3. **Időbeli változások** → Trendek követése
4. **Regionális mintázatok** → CEE vs Nordics vs Western Europe

### Interaktivitás

1. **Click** → Navigáció vagy részletek
2. **Hover** → Tooltip információk
3. **Filter** → Szűrés measure/region szerint
4. **Search** → Gyors keresés
5. **Zoom & Pan** → Nagyítás, mozgatás
6. **Highlight** → Kapcsolódó node-ok kiemelése

### Üzleti Érték

- ✅ **Gyors kontextus**: Egy pillantás alatt látható a teljes policy landscape
- ✅ **Felfedezés**: Automatikus hasonlóságok, kapcsolatok
- ✅ **Navigáció**: Kattintással országprofilokra ugrás
- ✅ **Modern UI**: Innovatív, interaktív felület

**Kezdjük el az implementációt?** 🚀
