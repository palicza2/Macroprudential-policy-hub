# 🇪🇺 Macro Policy Hub (AI-Powered) 🚀

**An automated, AI-driven dashboard for tracking Macroprudential Policy (CCyB & SyRB) across the European Economic Area.**

This repository hosts a sophisticated pipeline that retrieves raw policy data from the **European Systemic Risk Board (ESRB)**, processes it, and generates a professional, mobile-responsive HTML dashboard. It leverages **Google Gemini 2.5** to provide executive summaries, strategic insights, and smart keyword extraction from complex legal texts.

---

## 🌟 Key Features

### 1. Dual-Pillar Monitoring 🏛️
* **Part I: CCyB Monitor:** Tracks Countercyclical Capital Buffer rates, calculating diffusion indices and analyzing the credit gap vs. rate decoupling.
* **Part II: SyRB Monitor:** A dedicated section for the **Systemic Risk Buffer**, distinguishing between **General** and **Sectoral** measures (e.g., Residential/Commercial Real Estate).

### 2. AI-Driven Intelligence (Gemini 2.5) 🧠
* **Executive Summaries:** Generates a high-level, bulleted strategic overview of the entire EU landscape.
* **Smart Keyword Extraction:** Automatically converts long, complex legal descriptions (e.g., *"Retail exposures secured by residential property..."*) into punchy tags (e.g., *"Residential Mortgages"*).
* **Trend Analysis:** Interprets visual patterns in diffusion charts to explain *why* policies are tightening or loosening.

### 3. Modern, Mobile-First UI 📱
* **Responsive Design:** Features a "Hamburger" menu on mobile and a sticky sidebar on desktop.
* **Interactive Charts:** Zoomable Plotly visualizations (Diffusion Trends, Risk Analysis, Sectoral Focus).
* **Social Ready:** Includes Open Graph (OG) meta tags for professional previews when sharing via Messenger, LinkedIn, or Teams.

### 4. Robust ETL Pipeline ⚙️
* **Dynamic Parsing:** Automatically detects header rows and sheet names in ESRB Excel files, making it resilient to format changes.
* **Data Cleaning:** Normalizes country names, dates, and rates; handles missing values gracefully.
* **Parquet Storage:** Saves processed data in high-performance `.parquet` format for quick retrieval.

---

## 📂 Project Structure

    MacroPolicyHub/
    ├── data/                        # Raw Excel downloads & Processed Parquet files
    ├── scripts/
    │   ├── etl_process.py           # Robust ETL: Downloads & Cleans CCyB/SyRB data
    │   ├── plot_generator.py        # Generates interactive Plotly HTML components
    │   └── llm_analysis.py          # AI Logic: Summaries, Keyword Extraction
    ├── templates/
    │   └── report_template.html     # Jinja2 HTML template (Responsive, Sidebar)
    ├── run_pipeline.py              # Main orchestrator script
    ├── app.py                       # (Optional) Streamlit Chatbot for Q&A
    ├── requirements.txt             # Python dependencies
    └── README.md                    # Project documentation

---

## 🚀 Installation & Usage

### 1. Prerequisites
* Python 3.10+
* A Google Cloud API Key (for Gemini)

### 2. Install Dependencies

    pip install -r requirements.txt

*(Key libraries: `pandas`, `plotly`, `langchain-google-genai`, `jinja2`, `openpyxl`, `tenacity`)*

### 3. Configuration
Create a `.env` file in the root directory and add your API key:

    GOOGLE_API_KEY=your_actual_api_key_here

### 4. Run the Pipeline
To generate the static HTML report:

    python run_pipeline.py

* **Step 1:** Downloads latest Excel files from ESRB (or uses local cache).
* **Step 2:** Processes data into CCyB and SyRB datasets.
* **Step 3:** Generates interactive charts.
* **Step 4:** Sends data to Gemini AI for analysis and keyword extraction.
* **Step 5:** Renders `index.html`.

### 5. (Beta) Interactive Chatbot
To ask questions about the data (e.g., *"Which countries target CRE risks?"*):

    streamlit run app.py

---

## 📊 Dashboard Sections

The generated `index.html` includes:

1.  **Global Executive Summary:** High-level strategic bullet points.
2.  **CCyB Monitor:**
    * *Recent Decisions:* Table with AI-summarized drivers.
    * *Strategic Diffusion:* Chart showing the number of active countries over time.
    * *Risk Analysis:* Bubble chart comparing Credit Gap vs. Buffer Rates.
3.  **SyRB Monitor:**
    * *Adoption Trends:* Comparison of General vs. Sectoral buffer adoption.
    * *Sectoral Focus:* Bar chart showing targeted exposures (RRE, CRE, Corporate).
    * *Policy Data:* Tables for active measures with **AI-extracted keywords**.

---

## 🛠️ Customization

* **Adjusting AI Tone:** Modify `OUTPUT_INSTRUCTIONS` in `scripts/llm_analysis.py`.
* **Adding New Charts:** Edit `scripts/plot_generator.py` and update the template.
* **Updating Data Sources:** If ESRB URLs change, update `FILES` configuration in `scripts/etl_process.py`.

---

## License

This project is open-source, licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)**.