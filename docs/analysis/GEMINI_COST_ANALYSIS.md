# Gemini 2.5 Flash vs Flash Lite - Költség Elemzés

## Modell Összehasonlítás

### Jelenlegi Használat
- **Modell:** `gemini-2.5-flash-lite`
- **Max Output Tokens:** 2000
- **Használat:** Pipeline AI elemzések, BBM rule extraction, validation

### Alternatíva
- **Modell:** `gemini-2.5-flash` (vagy `gemini-2.0-flash-exp`)
- **Max Output Tokens:** 2000 (ugyanaz)
- **Előnyök:** Jobb minőség, pontosabb válaszok, jobb kontextus megértés

## Pipeline Token Használat Becslés

A pipeline futtatása során a logok alapján:

### AI Elemzések (llm_runner.py)
- **Chart Analyses:** ~15-20 hívás (ccyb_diffusion, ccyb_history, ccyb_level, risk_analysis, stb.)
- **Section Summaries:** ~5-6 hívás (ccyb, syrb, bbm, capital_overall section summaries)
- **Global Executive Summary:** 1 hívás
- **News Summarization:** ~10 hívás (ha van news)
- **Country Profile AI Analysis:** ~32 hívás (32 ország)

### BBM Processing
- **LTV Extraction:** ~48 hívás (48 LTV candidate items)
- **LTV Validation:** 1-2 hívás (batch validation)
- **DTI/LTI Confirmation:** ~12 hívás (12 confirmed items)
- **DTI/LTI Extraction:** ~12 hívás
- **DTI/LTI Validation:** 1-2 hívás (batch validation)
- **Final Validation:** 1-2 hívás (external search)

### Összesített Becslés
- **Összes API hívás:** ~140-160 hívás/pipeline run
- **Átlagos input tokens/hívás:** ~2000-3000 tokens (prompt + context + data)
- **Átlagos output tokens/hívás:** ~500-1000 tokens (max 2000, de általában rövidebb)
- **Összes input tokens:** ~280,000 - 480,000 tokens/run
- **Összes output tokens:** ~70,000 - 160,000 tokens/run

## Árazás Becslés (2024 Q4)

### Gemini 2.5 Flash Lite (jelenlegi)
- **Input:** ~$0.075 per 1M tokens
- **Output:** ~$0.30 per 1M tokens

### Gemini 2.5 Flash (alternatíva)
- **Input:** ~$0.075 - $0.15 per 1M tokens (becslés, lehet ugyanaz vagy 2x)
- **Output:** ~$0.30 - $0.60 per 1M tokens (becslés, lehet ugyanaz vagy 2x)

**Megjegyzés:** A pontos árazás változhat, és a Google nem mindig tesz közzé részletes árazást a Flash Lite és Flash között. Általában a Flash Lite 50-70% olcsóbb lehet.

## Költség Számítás

### Jelenlegi (Flash Lite) - 1 Pipeline Run
- **Input költség:** (350,000 / 1,000,000) × $0.075 = **$0.026**
- **Output költség:** (115,000 / 1,000,000) × $0.30 = **$0.035**
- **Összesen:** **~$0.061 per run**

### Alternatíva (Flash) - 1 Pipeline Run
**Scenario 1: Ugyanaz az ár (valószínűtlen)**
- **Input költség:** $0.026
- **Output költség:** $0.035
- **Összesen:** **~$0.061 per run**
- **Növekedés:** 0%

**Scenario 2: 2x ár (valószínűbb)**
- **Input költség:** (350,000 / 1,000,000) × $0.15 = **$0.053**
- **Output költség:** (115,000 / 1,000,000) × $0.60 = **$0.069**
- **Összesen:** **~$0.122 per run**
- **Növekedés:** **~100% (2x)**

**Scenario 3: 1.5x ár (közepes)**
- **Input költség:** (350,000 / 1,000,000) × $0.1125 = **$0.039**
- **Output költség:** (115,000 / 1,000,000) × $0.45 = **$0.052**
- **Összesen:** **~$0.091 per run**
- **Növekedés:** **~50% (1.5x)**

## Havi Költség Becslés

### Heti 1 Pipeline Run
- **Flash Lite:** $0.061 × 4 = **$0.24/hó**
- **Flash (2x):** $0.122 × 4 = **$0.49/hó**
- **Különbség:** **+$0.25/hó**

### Napi 1 Pipeline Run
- **Flash Lite:** $0.061 × 30 = **$1.83/hó**
- **Flash (2x):** $0.122 × 30 = **$3.66/hó**
- **Különbség:** **+$1.83/hó**

### Napi 2 Pipeline Run (multi-user)
- **Flash Lite:** $0.061 × 60 = **$3.66/hó**
- **Flash (2x):** $0.122 × 60 = **$7.32/hó**
- **Különbség:** **+$3.66/hó**

## Minőség vs. Költség Trade-off

### Flash Lite Előnyei
- ✅ **Olcsóbb** (50-70% költségmegtakarítás)
- ✅ **Gyorsabb** válaszidő
- ✅ **Elég jó minőség** a legtöbb feladathoz

### Flash Előnyei
- ✅ **Jobb minőség** (pontosabb válaszok)
- ✅ **Jobb kontextus megértés** (hosszabb, komplexebb promptokhoz)
- ✅ **Kevesebb hallucination** (kisebb valószínűség)
- ✅ **Jobb strukturált output** (JSON, markdown)

## Ajánlás

### Használd Flash Lite-et, ha:
- ✅ A jelenlegi minőség **elégséges**
- ✅ A költség **fontos szempont**
- ✅ A pipeline **gyakran fut** (napi 1+)
- ✅ A válaszidő **fontos**

### Válts Flash-re, ha:
- ✅ **Minőségi problémák** vannak (hallucination, rossz extractions)
- ✅ **Komplex promptok** nem működnek jól Flash Lite-tel
- ✅ A költség **nem kritikus** (például havi <$5)
- ✅ **Kritikus üzleti használat** (például regulatory reporting)

## Tesztelési Javaslat

1. **A/B Teszt:** Futtasd a pipeline-t Flash-re egy hónapig
2. **Minőség mérés:** Összehasonlítsd az extraction accuracy-t
3. **Költség követés:** Figyeld a Google Cloud számlát
4. **Döntés:** Ha a minőség javulás > költség növekedés, válts

## Pontos Árazás Ellenőrzése

A pontos árazást a Google Cloud Console-ban vagy a Gemini API dokumentációban lehet megnézni:
- Google Cloud Console → Vertex AI → Pricing
- Google AI Studio → Pricing

**Fontos:** Az árazás változhat régió és használat szerint!
