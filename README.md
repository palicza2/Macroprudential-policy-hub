# Macroprudential Policy Hub (AI-Powered) 🚀

This repository hosts an **automated, AI-driven framework** for tracking and analyzing Countercyclical Capital Buffer (CCyB) rates across the European Union and the European Economic Area (EEA).

The system automatically retrieves the latest data from the European Systemic Risk Board (ESRB), processes it, generates interactive visualizations, and leverages **Google Gemini AI** to produce professional-grade financial stability reports in a modern HTML dashboard.

## 🌟 Key Features

* **Automated ETL Pipeline**: Downloads raw Excel data from the ESRB, cleans and normalizes it, and saves optimized `parquet` files for high-performance access.
* **Interactive Visualization**: Generates dynamic, zoomable charts using `Plotly` (Diffusion Index, Cross-Sectional Snapshots, Risk Analysis Bubble Charts) embedded directly into the report.
* **AI-Driven Analysis (LLM)**: Utilizes the **Google Gemini 2.5 Flash Lite** model to interpret trends, deconstruct policy drivers, and assess risk decoupling. It features built-in "Retry" logic (`tenacity`) to handle API rate limits gracefully.
* **Intelligent Grounding**: The AI is "grounded" with both visual data and raw numeric tables (converted to Markdown) to prevent hallucinations and ensure factual accuracy in the analysis.
* **Modern Dashboard**: The final output is rendered via a `Jinja2` template into a single, standalone `index.html` file that serves as a comprehensive "Executive Dashboard".

## 📂 Project Structure

The project follows a modular architecture for maintainability:

```text
Macroprudential_hub/
├── data/                   # Data storage (Raw Excel and processed Parquet files)
├── output/                 # Temporary artifacts (e.g., base64 images for AI vision)
├── scripts/                # Core logic modules
│   ├── etl_process.py      # Data extraction, transformation, and loading
│   ├── plot_generator.py   # Plotly interactive chart generation
│   └── llm_analysis.py     # LangChain + Google Gemini AI integration
├── templates/              # HTML assets
│   └── report_template.html # Jinja2 report skeleton
├── .env                    # Environment variables (API Keys) - NOT version controlled
├── index.html              # The final generated report
├── run_pipeline.py         # Main entry point / Orchestrator
└── requirements.txt        # Python dependencies
```

## 🛠️ Installation & Setup

Follow these steps to set up the pipeline locally:

### 1. Prerequisites
* **Python 3.10+** installed.
* A Google Cloud Project with a **Google AI Studio API Key** (for Gemini access).

### 2. Clone the Repository
```bash
git clone https://github.com/palicza2/Macroprudential-policy-hub.git
cd Macroprudential-policy-hub
```

### 3. Install Dependencies
It is recommended to use a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```
*Key dependencies: `pandas`, `plotly`, `langchain-google-genai`, `tenacity`, `jinja2`, `python-dotenv`, `openpyxl`, `pyarrow`, `tabulate`.*

### 4. Configure Environment Variables
Create a `.env` file in the root directory and add your Google API key:
```env
GOOGLE_API_KEY=your_actual_api_key_here
```

## 🚀 Usage

To run the full pipeline (Data Update -> Visualization -> AI Analysis -> Report Generation), simply execute the main script:

```bash
python run_pipeline.py
```

### Workflow Overview:
1.  **ETL**: Checks for existing data. If stale or missing, downloads the latest Excel file from ESRB and processes it into DataFrames.
2.  **Visualization**: Generates HTML components for charts and temporary images for the AI's vision capabilities.
3.  **AI Analysis**: The orchestrator sends tasks to Gemini (e.g., "Analyze the diffusion curve"). The system includes throttling (15s delays) to respect Free Tier API rate limits.
4.  **Rendering**: Combines the analytics, charts, and table data into the `report_template.html` and saves the final `index.html`.

## 📊 Outputs

Upon successful execution, the root directory will contain:
* **`index.html`**: A fully interactive web report containing:
    * Executive Summary (AI-synthesized).
    * Interactive Plotly Charts.
    * "Latest Changes" Policy Table.
    * Deep-dive textual analysis for each section.

## 🔧 Customization

* **Prompt Engineering**: You can adjust the analytical depth, tone, or specific questions in the `tasks` list and `OUTPUT_INSTRUCTIONS` within `scripts/llm_analysis.py`.
* **Data Source**: If the ESRB data URL changes, update the `ESRB_URL` constant in `scripts/etl_process.py`.

## License

This project is open-source for educational and personal use, licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0)**.

You are free to:
* **Share** — copy and redistribute the material in any medium or format.
* **Adapt** — remix, transform, and build upon the material.

Under the following terms:
* **Attribution** — You must give appropriate credit to the author.
* **NonCommercial** — You may not use the material for commercial purposes (e.g., internal business tools, paid products).

**For commercial inquiries or licensing, please contact the author.**