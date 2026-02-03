# 🔧 Refaktorálási Javaslatok

## Legnagyobb Fájlok Elemzése

### Python Fájlok (sorok száma szerint)

1. **country_profiles.py** - 681 sor, 31.3 KB
2. **etl.py** - 498 sor, 27.47 KB
3. **main.py** - 396 sor, 18.18 KB
4. **grounding_validator.py** - 368 sor, 17 KB
5. **llm_analysis.py** - 328 sor, 15.04 KB

### Egyéb Nagy Fájlok

- **index.html** - 618 KB (generált, nem kell refaktorálni)
- **assets/app.js** - 686 sor, 26.06 KB
- **assets/styles.css** - 896 sor, 18.1 KB
- **report_template.html** - 500 sor, 26.66 KB

---

## 🎯 Refaktorálási Prioritások

### 1. **country_profiles.py** (681 sor) - ⭐ LEGFONTOSABB

**Probléma:**
- Egyetlen nagy osztály (`CountryProfileGenerator`) tartalmazza:
  - Profil generálást
  - Knowledge graph építést
  - Adat aggregációt
  - Regionális kategorizálást

**Javaslat:**
```
country_profiles/
├── __init__.py
├── profile_generator.py      # CountryProfileGenerator osztály
├── knowledge_graph_builder.py # build_knowledge_graph_data logika
├── data_aggregators.py       # _get_current_status, _get_historical_evolution, stb.
└── region_mapper.py          # _get_region, _get_iso2 helper függvények
```

**Előnyök:**
- Kisebb, fókuszált modulok
- Könnyebb tesztelhetőség
- Jobb karbantarthatóság
- Parallel development lehetőség

---

### 2. **etl.py** (498 sor) - ⭐ FONTOS

**Probléma:**
- `ETLPipeline` osztály tartalmazza:
  - CCyB feldolgozást
  - SyRB feldolgozást
  - BBM feldolgozást
  - O-SII feldolgozást
  - Rate extraction logikát
  - Date parsing logikát

**Javaslat:**
```
etl/
├── __init__.py
├── pipeline.py              # Fő ETLPipeline orchestrator
├── extractors/
│   ├── ccyb_extractor.py    # CCyB specifikus logika
│   ├── syrb_extractor.py    # SyRB specifikus logika
│   ├── bbm_extractor.py     # BBM specifikus logika
│   └── osii_extractor.py    # O-SII specifikus logika
├── parsers/
│   ├── rate_parser.py       # Rate extraction logika
│   ├── date_parser.py       # Date parsing logika
│   └── text_cleaner.py      # Text cleaning utilities
└── validators/
    └── data_validator.py    # Data validation logika
```

**Előnyök:**
- Moduláris extractorok (könnyű új adatforrás hozzáadása)
- Újrafelhasználható parserek
- Jobb error handling lehetőség

---

### 3. **main.py** (396 sor) - ⭐ FONTOS

**Probléma:**
- Egyetlen `main()` függvény orchestrál mindent
- Helper függvények a fájlban (`serialize_profile`, `format_profile_for_llm`)
- Hosszú, nehezen követhető flow

**Javaslat:**
```
pipeline/
├── __init__.py
├── orchestrator.py          # Fő main() logika
├── stages/
│   ├── data_stage.py        # ETL stage
│   ├── visualization_stage.py # Visualization stage
│   ├── ai_stage.py          # AI analysis stage
│   ├── profile_stage.py     # Country profiles stage
│   └── render_stage.py      # Rendering stage
└── serializers/
    ├── profile_serializer.py # serialize_profile
    └── llm_formatter.py     # format_profile_for_llm
```

**Előnyök:**
- Tiszta stage-based architecture
- Könnyebb debugging
- Parallel stage execution lehetőség
- Jobb error recovery

---

### 4. **assets/app.js** (686 sor) - ⭐ KÖZEPES

**Probléma:**
- Egyetlen fájl tartalmazza:
  - Tab management
  - Chart initialization
  - Country profiles logic
  - Knowledge graph logic (most már nem használt)
  - Filtering logic
  - Event handlers

**Javaslat:**
```
assets/js/
├── app.js                   # Main entry point
├── tabs.js                  # Tab management
├── charts/
│   ├── ccyb-charts.js      # CCyB chart initialization
│   ├── syrb-charts.js      # SyRB chart initialization
│   └── bbm-charts.js       # BBM chart initialization
├── country-profiles.js      # Country profile logic
└── filters.js              # Filtering logic
```

**Előnyök:**
- Moduláris JavaScript
- Könnyebb maintenance
- Jobb performance (lazy loading)

---

### 5. **grounding_validator.py** (368 sor) - ⭐ KÖZEPES

**Probléma:**
- LangGraph state management
- Google Search integration
- Data context building
- Chart context building
- Minden egy helyen

**Javaslat:**
```
grounding/
├── __init__.py
├── validator.py             # Fő GroundingValidator
├── state.py                 # ValidatorState dataclass
├── context_builders/
│   ├── data_context.py     # _build_data_context
│   ├── chart_context.py    # _build_chart_context
│   └── graph_context.py    # Knowledge graph context (új)
└── search/
    └── google_search.py     # _google_search logika
```

---

### 6. **llm_analysis.py** (328 sor) - ⭐ ALACSONY

**Javaslat:**
```
llm/
├── __init__.py
├── analyzer.py              # Fő LLMAnalyzer osztály
├── extractors/
│   ├── keyword_extractor.py
│   ├── rate_extractor.py
│   └── tag_classifier.py
├── formatters/
│   └── text_cleaner.py     # _clean_text logika
└── graph_analyzer.py        # analyze_knowledge_graph (új)
```

---

## 🗑️ Tisztítási Javaslatok

### Debug Fájlok Törlése
```
debug_syrb.py
debug_syrb_v2.py
debug_syrb_v3.py
debug_syrb_v4.py
debug_syrb_v5.py
```
**Javaslat:** Töröljük vagy helyezzük `archive/` mappába

### Dokumentációs Fájlok Konszolidálása
- Több nagy MD fájl van (AI_ENHANCEMENT_ROADMAP.md, DEVELOPMENT_ROADMAP.md, stb.)
- **Javaslat:** Konszolidáljuk `docs/` mappába és indexeljük

---

## 📊 Refaktorálási Prioritás Rangsor

1. **country_profiles.py** - Legnagyobb fájl, legfontosabb refaktorálás
2. **etl.py** - Kritikus komponens, modulárisabbá tehető
3. **main.py** - Orchestrator, stage-based architecture-ra refaktorálható
4. **assets/app.js** - Frontend modularizálás
5. **grounding_validator.py** - Context builders kiszervezése
6. **llm_analysis.py** - Extractors és formatters kiszervezése

---

## 🎯 Következő Lépések

1. **Phase 1:** `country_profiles.py` refaktorálás
   - Knowledge graph builder kiszervezése
   - Region mapper kiszervezése
   - Tesztek írása

2. **Phase 2:** `etl.py` refaktorálás
   - Extractorok kiszervezése
   - Parser utilities kiszervezése

3. **Phase 3:** `main.py` refaktorálás
   - Stage-based architecture
   - Serializers kiszervezése

4. **Phase 4:** Frontend modularizálás
   - JavaScript modulok
   - CSS organization

---

## ✅ Refaktorálás Előnyei

- **Karbantarthatóság:** Kisebb, fókuszált fájlok
- **Tesztelhetőség:** Unit tesztek írhatók modulonként
- **Skálázhatóság:** Könnyebb új funkciók hozzáadása
- **Csapatmunka:** Parallel development lehetőség
- **Performance:** Lazy loading, jobb caching
- **Dokumentáció:** Modulonkénti dokumentáció
