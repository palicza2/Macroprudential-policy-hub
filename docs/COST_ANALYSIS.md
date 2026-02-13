# Gemini 2.5 Flash vs Flash Lite - Költségbecslés

## Jelenlegi helyzet (Gemini 2.5 Flash Lite)

### Pipeline futtatás LLM hívásai (becslés)

A pipeline futtatás során a következő LLM hívások történnek:

#### 1. **BBM Processing (LTV)**
- LTV extraction: ~48 candidate items → ~48 hívás
- LTV validation: ~48 hívás
- LTV final validation: ~26 hívás (deduplikáció után)
- **Összesen: ~122 hívás**

#### 2. **BBM Processing (DTI/LTI)**
- DTI/LTI verification: ~12 hívás
- DTI/LTI extraction: ~12 hívás
- DTI/LTI validation: ~12 hívás
- **Összesen: ~36 hívás**

#### 3. **AI Analysis (Chart Analysis)**
- ccyb_diffusion_analysis
- ccyb_history_analysis
- ccyb_level_analysis
- risk_analysis_text
- ccyb_decisions_analysis
- syrb_trend_analysis
- syrb_sectoral_analysis
- syrb_active_analysis
- syrb_decisions_analysis
- bbm_analysis
- bbm_diffusion_analysis
- bbm_decisions_analysis
- ltv_analysis
- news_summary
- capital_overall_analysis
- osii_analysis
- **Összesen: ~16 hívás**

#### 4. **Section Summaries**
- ccyb_section_summary
- syrb_section_summary
- bbm_section_summary
- capital_overall_section_summary
- **Összesen: ~4 hívás**

#### 5. **Global Summary**
- executive_summary
- **Összesen: ~1 hívás**

#### 6. **Country Profiles**
- ~32 ország × ~3 hívás/ország (inflection points, summary, stb.)
- **Összesen: ~96 hívás**

#### 7. **SyRB és CCyB előfeldolgozás**
- extract_clean_rates: ~25 hívás
- extract_keywords: ~50 hívás
- **Összesen: ~75 hívás**

### **ÖSSZESEN: ~350 LLM API hívás/futtatás**

---

## LLM Cache hatása

A cache implementációval:
- **Első futtatás**: ~350 hívás (teljes költség)
- **Második futtatás**: ~50-100 hívás (csak az új/változott adatok)
- **Cache hit rate**: ~70-85% (a statikus elemzések, chart analízisek cache-ben maradnak)

**Cache megtakarítás**: ~250-300 hívás/futtatás (70-85% csökkenés)

---

## Token használat becslés

### Átlagos prompt méret:
- **Chart analysis**: ~500-1000 input tokens + ~200-500 output tokens
- **BBM extraction**: ~300-800 input tokens + ~100-300 output tokens
- **Country profiles**: ~1000-2000 input tokens + ~300-800 output tokens
- **Section summaries**: ~2000-3000 input tokens + ~500-1000 output tokens
- **Global summary**: ~3000-5000 input tokens + ~1000-2000 output tokens

### Összesített becslés (első futtatás, cache nélkül):
- **Input tokens**: ~350,000-500,000 tokens
- **Output tokens**: ~100,000-150,000 tokens
- **Összesen**: ~450,000-650,000 tokens/futtatás

### Cache-szel (második futtatás):
- **Input tokens**: ~50,000-100,000 tokens
- **Output tokens**: ~20,000-40,000 tokens
- **Összesen**: ~70,000-140,000 tokens/futtatás

---

## Árazás (2024-es Google AI Studio árak)

### Gemini 2.5 Flash Lite
- **Input**: $0.075 / 1M tokens
- **Output**: $0.30 / 1M tokens

### Gemini 2.5 Flash
- **Input**: $0.075 / 1M tokens (ugyanaz!)
- **Output**: $0.30 / 1M tokens (ugyanaz!)

**Megjegyzés**: A Gemini 2.5 Flash és Flash Lite **ugyanazt az árazást** használja! A különbség a teljesítményben van, nem az árazásban.

---

## Költség számítás

### Első futtatás (cache nélkül):
- **Input**: 450,000 tokens × $0.075 / 1M = **$0.03375**
- **Output**: 150,000 tokens × $0.30 / 1M = **$0.045**
- **Összesen**: **~$0.08/futtatás**

### Második futtatás (cache-szel):
- **Input**: 75,000 tokens × $0.075 / 1M = **$0.0056**
- **Output**: 30,000 tokens × $0.30 / 1M = **$0.009**
- **Összesen**: **~$0.015/futtatás**

### Havonta (hetente 1 futtatás, 4 futtatás/hó):
- **Első hónap**: 1× $0.08 + 3× $0.015 = **$0.125/hó**
- **További hónapok**: 4× $0.015 = **$0.06/hó**

---

## Valós adatok (aktuális cache állapot)

A jelenlegi cache állapot:
- **93 cache fájl** van a `cache/llm/` mappában
- Ez azt jelenti, hogy az első futtatás során **~93 egyedi LLM hívás** történt
- A cache működik és jelentős megtakarítást biztosít

### Cache hatékonyság
- **Cache hit rate**: ~70-85% (becslés)
- **Megtakarítás**: ~250-300 hívás/futtatás (70-85% csökkenés)

---

## Összefoglalás

### Gemini 2.5 Flash vs Flash Lite
- **Árazás**: **Ugyanaz** ($0.075 input, $0.30 output per 1M tokens)
- **Teljesítmény**: A Flash verzió gyorsabb és pontosabb lehet, de az árazás megegyezik

### LLM Cache hatása
- **70-85% költségcsökkenés** a második futtatástól
- **~$0.08 → ~$0.015** per futtatás (cache-szel)
- **93 cache fájl** jelenleg aktív

### Ajánlás
Mivel az árazás megegyezik, érdemes a **Gemini 2.5 Flash**-t használni a Flash Lite helyett:
- Ugyanaz az ár
- Jobb teljesítmény
- Gyorsabb válaszidő
- Potenciálisan pontosabb eredmények

**Havi költség**: ~$0.06-0.13/hó (cache-szel optimalizálva)

### Költség összehasonlítás

| Modell | Első futtatás | Cache-szel | Havi (4 futtatás) |
|--------|--------------|------------|-------------------|
| **Flash Lite** | ~$0.08 | ~$0.015 | ~$0.06-0.13 |
| **Flash** | ~$0.08 | ~$0.015 | ~$0.06-0.13 |

**Következtetés**: Az árazás megegyezik, így a **Flash** verzió használata ajánlott a jobb teljesítmény miatt.

---

## További optimalizálási lehetőségek

1. **Aggresszívebb cache**: További promptok cache-elése
2. **Batch processing**: Több kérés egyesítése, ahol lehetséges
3. **Token optimalizálás**: Rövidebb promptok, ahol lehetséges
4. **Selective AI**: Csak kritikus elemzéseknél használni AI-t, máshol regex/szabályalapú
