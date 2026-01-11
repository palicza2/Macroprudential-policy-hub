# 🇪🇺 Macro Policy Hub (AI-Powered) 🚀

**An automated, AI-driven dashboard for tracking Macroprudential Policy (CCyB & SyRB) across the European Economic Area.**

This repository hosts a sophisticated pipeline that retrieves raw policy data from the **European Systemic Risk Board (ESRB)**, processes it, and generates a professional, mobile-responsive HTML dashboard. It leverages **Google Gemini 2.5 Flash Lite** to provide executive summaries, strategic insights, and smart keyword extraction from complex legal texts.

---

## 🌟 Key Features

### 1. Dual-Pillar Monitoring 🏛️
* **Part I: CCyB Monitor:** Tracks Countercyclical Capital Buffer rates, calculating diffusion indices and analyzing the credit gap vs. rate decoupling.
* **Part II: SyRB Monitor:** A dedicated section for the **Systemic Risk Buffer**, distinguishing between **General** and **Sectoral** measures (e.g., Residential/Commercial Real Estate).

### 2. AI-Driven Intelligence (Gemini 2.5 Flash Lite) 🧠
* **Global Executive Summary:** Generates a 4-5 paragraph strategic overview with bold topic sentences for quick scanning.
* **Section Summaries:** Specific high-level summaries for both CCyB and SyRB chapters.
* **Professional Keyword Extraction:** Automatically converts complex legal descriptions into concise, risk-focused tags (e.g., *"Sectoral systemic risk, mortgage loan portfolios"*), filtered to remove technical noise.
* **Sequential Analysis:** High-level summaries are built upon individual chart analyses for maximum context and accuracy.

### 3. Modern, Mobile-First UI 📱
* **Responsive Design:** Features a "Hamburger" menu on mobile and a professional sidebar on desktop using **Lucide icons**.
* **Interactive Charts:** Zoomable Plotly visualizations (Diffusion Trends, Risk Analysis, Sectoral Focus).
* **Smart Filtering:** Instant JavaScript-based filtering for historical time-series charts.
* **Data Portability:** Integrated download links for processed trend data (CSV/Excel).

### 4. Robust ETL Pipeline ⚙️
* **Lifecycle Tracking:** Advanced SyRB trend calculation that accurately handles activation and deactivation/revocation events.
* **Dynamic Parsing:** Resilient to format changes in ESRB Excel files.
* **Data Cleaning:** Normalizes country names (ISO2/ISO3), dates, and rates.

---

## 📂 Project Structure

    MacroPolicyHub/
    ├── data/                        # Raw Excel downloads & Processed Parquet files
    ├── figures/                     # Static PNG exports for LLM consumption
    ├── templates/
    │   └── report_template.html     # Jinja2 HTML template (Responsive, Lucide Icons)
    ├── etl.py                       # Main ETL: Downloads & Cleans CCyB/SyRB data
    ├── visualizer.py                # Generates interactive Plotly components & PNGs
    ├── llm_analysis.py              # AI Logic: Summaries, Professional Keyword Extraction
    ├── main.py                      # Main orchestrator script
    ├── config.py                    # Centralized configuration (URLs, Model settings)
    ├── utils.py                     # Helper functions
    ├── requirements.txt             # Python dependencies
    └── README.md                    # Project documentation

---

## 🚀 Installation & Usage

### 1. Prerequisites
* Python 3.10+
* A Google Cloud API Key (for Gemini)

### 2. Install Dependencies

    pip install -r requirements.txt

*(Key libraries: `pandas`, `plotly`, `langchain-google-genai`, `jinja2`, `openpyxl`, `country_converter`)*

### 3. Configuration
Create a `.env` file in the root directory and add your API key:

    GOOGLE_API_KEY=your_actual_api_key_here

### 4. Run the Pipeline
To generate the static HTML report:

    python main.py

* **Step 1:** Downloads latest Excel files from ESRB.
* **Step 2:** Processes data, identifying active vs. revoked measures.
* **Step 3:** Generates interactive Plotly charts and static PNGs.
* **Step 4:** Sequential AI analysis: Chart Analysis -> Section Summaries -> Global Executive Summary.
* **Step 5:** Renders the final `index.html`.

---

## 📊 Dashboard Sections

The generated `index.html` includes:

1.  **Global Executive Summary:** Comprehensive synthesis of the macroprudential stance.
2.  **CCyB Section:**
    * *Section Summary:* High-level bullet points on cyclical risks.
    * *Adoption Count:* Cumulative line chart of positive CCyB rates.
    * *Historical Rates:* Filterable time-series of country-specific rates.
    * *Map & Comparative Views:* Geographic and bar chart distribution.
    * *Risk Analysis:* Credit Gap vs. CCyB decoupling.
    * *Latest Decisions:* Table with AI-generated risk keywords.
3.  **SyRB Section:**
    * *Section Summary:* Strategic overview of structural buffers.
    * *Adoption Trend:* Count of countries using General vs. Sectoral SyRB.
    * *Sectoral Focus:* Composition by exposure type (RRE, CRE, etc.).
    * *Active Measures & Latest Decisions:* Detailed tables with AI-cleaned descriptions.

---

## License

This project is open-source, licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)**.
