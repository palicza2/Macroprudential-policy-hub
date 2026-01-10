import pandas as pd
import base64
import time
import os
import json
import copy
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google.api_core import exceptions

# LangChain imports
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda

load_dotenv()

# --- 1. Konfiguráció és Segédfüggvények ---

def img_to_base64(image_path):
    if not image_path or not os.path.exists(image_path):
        return None
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error encoding image {image_path}: {e}")
        return None

def format_data_to_json(df, max_rows=3000):
    if df is None or df.empty:
        return json.dumps({"info": "No specific numeric data provided for this section."})
    
    display_df = df.head(max_rows).copy()
    for col in display_df.select_dtypes(include=['datetime64']).columns:
        display_df[col] = display_df[col].dt.strftime('%Y-%m-%d')

    try:
        # NaN értékek kezelése JSON-ben
        json_str = display_df.to_json(orient='records', date_format='iso', default_handler=str)
        if len(df) > max_rows:
            data_list = json.loads(json_str)
            data_list.append({"_warning": f"... and {len(df)-max_rows} more rows omitted ..."})
            return json.dumps(data_list)
        return json_str
    except Exception as e:
        return json.dumps({"error": f"Data serialization failed: {str(e)}"})

# --- 2. Prompt Template ---

OUTPUT_INSTRUCTIONS = """
STRICT GUIDELINES:
1. CONCISE: Write exactly ONE dense paragraph (max 6-8 sentences) unless specified otherwise.
2. NO MARKDOWN: Plain text only, unless HTML is requested.
3. DATA USAGE: Cite specific values from the JSON.
4. TONE: Professional financial analyst (ECB/ESRB style).
"""

def build_multimodal_message(inputs):
    text_content = f"""
    ROLE: {inputs.get('system_instructions', 'Expert Financial Stability Analyst')}

    CONTEXT (EXTERNAL):
    {inputs.get('external_context', 'No specific context provided. Use your knowledge base/search tools.')}

    ---
    INTERNAL DATA (JSON FORMAT):
    {inputs.get('data_context', '[]')}
    ---

    TASK:
    {inputs['instruction']}

    GUIDELINES:
    1. Analyze visual patterns in the CHART (if provided).
    2. Use INTERNAL DATA (JSON) to validate arguments.
    3. {OUTPUT_INSTRUCTIONS}
    """
    content_parts = [{"type": "text", "text": text_content}]
    if inputs.get("image_base64"):
        content_parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{inputs['image_base64']}"}
        })
    return [HumanMessage(content=content_parts)]

# --- 3. Pipeline Setup ---

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite", 
    google_api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.3, 
    max_retries=1, 
    model_kwargs={"tools": [{"google_search": {}}]} 
)

analysis_chain = (
    RunnableLambda(build_multimodal_message) 
    | llm 
    | StrOutputParser()
)

@retry(
    retry=retry_if_exception_type(exceptions.ResourceExhausted),
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    reraise=True
)
def invoke_chain_with_retry(chain_input):
    return analysis_chain.invoke(chain_input)

# --- 4. Tasks Definition ---
tasks = [
    # --- CCyB ---
    {
        "key": "ccyb_executive_summary", # <--- ÚJ
        "instruction": "Write a high-level executive summary specifically for the Countercyclical Capital Buffer (CCyB) landscape. Summarize the current tightening/loosening cycle and the main drivers (e.g. credit gap vs positive neutral rate).",
        "image_filter": "ccyb_diffusion",
        "data_key": "latest_ccyb"
    },
    {
        "key": "policy_driver_analysis",
        "instruction": "Analyze recent CCyB decisions up to {latest_date}. Contrast tightening vs. neutral stances.",
        "image_filter": None,
        "data_key": "latest_ccyb_1yr"
    },
    {
        "key": "strategic_diffusion_analysis",
        "instruction": "Evaluate the CCyB adoption curve (chart/data) up to {latest_date}. Explain global drivers (inflation, post-COVID).",
        "image_filter": "ccyb_diffusion",
        "data_key": "agg_trend_df"
    },
    {
        "key": "jurisdictional_heterogeneity",
        "instruction": "Assess the divergence in CCyB rates across Europe. Why do some regions maintain high buffers while others are zero?",
        "image_filter": "cross_section",
        "data_key": "latest_ccyb"
    },
    {
        "key": "risk_decoupling",
        "instruction": "Critique the Credit Gap vs CCyB Rate relationship. Identify 'decoupling' cases.",
        "image_filter": "risk_plot",
        "data_key": "latest_ccyb"
    },
    
    # --- SyRB ---
    {
        "key": "syrb_executive_summary", # <--- ÚJ
        "instruction": "Write a high-level executive summary specifically for the Systemic Risk Buffer (SyRB). Focus on the shift from general to sectoral buffers and the key risks being targeted (Real Estate).",
        "image_filter": "syrb_diffusion",
        "data_key": "syrb_trend_df"
    },
    {
        "key": "syrb_diffusion_analysis",
        "instruction": "Analyze the SyRB trends. Compare 'General' vs 'Sectoral' adoption. Why are sectoral buffers (sSyRB) rising?",
        "image_filter": "syrb_diffusion",
        "data_key": "syrb_trend_df"
    },
    {
        "key": "syrb_sectoral_focus",
        "instruction": "Examine the sectoral focus of SyRB. Which specific risks (RRE, CRE) are targeted?",
        "image_filter": "syrb_sector",
        "data_key": "latest_syrb"
    },
    {
        "key": "syrb_table_analysis", # <--- ÚJ: Táblázat elemzés
        "instruction": "Analyze the 'Latest SyRB Decisions' and 'Active Measures' data provided in JSON. Highlight 2-3 notable recent activations or rate changes and mention which countries have the most complex sectoral setups.",
        "image_filter": None,
        "data_key": "latest_syrb"
    }
]

# --- 5. Main Analysis Logic ---

def perform_llm_analysis(processed_dataframes, plot_paths, one_year_ago, external_contexts):
    analyses = {}
    
    # Data Preparation
    data_map = {
        "agg_trend_df": processed_dataframes.get('agg_trend_df'),
        "latest_ccyb": processed_dataframes.get('latest_country_df'),
        "latest_ccyb_1yr": processed_dataframes.get('latest_country_df')[processed_dataframes.get('latest_country_df')['date'] >= one_year_ago] if processed_dataframes.get('latest_country_df') is not None else None,
        "syrb_trend_df": processed_dataframes.get('syrb_trend_df'),
        "latest_syrb": processed_dataframes.get('latest_syrb_df')
    }

    latest_date_str = datetime.now().strftime("%Y-%m-%d")
    
    print("--- Starting AI Analysis ---")

    # Run Tasks
    for i, task in enumerate(tasks):
        if i > 0: time.sleep(5) # Kis szünet

        df = data_map.get(task.get('data_key'))
        img_path = None
        if task.get('image_filter') and isinstance(plot_paths, list):
             img_path = next((p for p in plot_paths if task['image_filter'] in p), None)

        chain_input = {
            "system_instructions": "You are a senior financial analyst.",
            "instruction": task['instruction'].format(latest_date=latest_date_str),
            "external_context": external_contexts.get(task['key'], 'Use Google Search if needed for recent context.'),
            "data_context": format_data_to_json(df),
            "image_base64": img_to_base64(img_path)
        }
        
        try:
            print(f"Processing: {task['key']}...")
            result = invoke_chain_with_retry(chain_input)
            analyses[task['key']] = result
        except Exception as e:
            print(f"❌ Error in {task['key']}: {e}")
            analyses[task['key']] = "Analysis unavailable."

    # --- GLOBAL EXECUTIVE SUMMARY (BULLET POINTS) ---
    print("Processing: Global Executive Summary...")
    time.sleep(5)
    
    summary_input = "\n".join([f"[{k.upper()}]: {v}" for k, v in analyses.items()])
    
    exec_prompt = {
        "system_instructions": "Senior Chief Risk Officer.",
        "instruction": f"""
        Create a comprehensive Global Executive Summary of the EU Macroprudential landscape based on the findings below.
        
        FORMAT REQUIREMENT:
        - Output MUST be an HTML Unordered List (<ul> ... </ul>).
        - Provide 4-5 key bullet points (<li>).
        - Each bullet point should start with a <strong>bold title</strong> followed by the insight.
        - Cover: Overall Cycle (Tightening/Loosening), CCyB heterogeneity, the rise of Sectoral SyRB, and Emerging Risks (Real Estate).
        
        FINDINGS:
        {summary_input}
        """,
        "external_context": "Focus on high-level strategic direction.",
        "data_context": "See input above.",
        "image_base64": None
    }
    
    try:
        analyses['executive_summary'] = invoke_chain_with_retry(exec_prompt)
    except Exception:
        analyses['executive_summary'] = "<p>Summary unavailable.</p>"

    return analyses

# --- Helper: Categorization ---
def batch_summarize_reasons(justifications):
    """Categorizes policy reasons into short keywords."""
    if not justifications:
        return None

    categories = [
        "Cyclical Risk / Credit Growth",
        "Real Estate Risk (RRE/CRE)", 
        "Building Resilience", 
        "Positive Neutral Rate",
        "Crisis Release / Support",
        "Maintain Status Quo"
    ]

    prompt_text = f"""
    ROLE: Classifier.
    TASK: Map each text to EXACTLY ONE category from this list: {json.dumps(categories)}.
    RULES:
    - "neutral rate" or "standard rate" -> "Positive Neutral Rate"
    - "housing", "mortgage" -> "Real Estate Risk (RRE/CRE)"
    - "shock" or "uncertainty" -> "Building Resilience"
    - OUTPUT: JSON list of strings only.
    
    INPUTS:
    {json.dumps(justifications)}
    """
    
    try:
        response = llm.invoke(prompt_text)
        content = response.content.strip()
        if "```json" in content:
            content = content.replace("```json", "").replace("```", "")
        summaries = json.loads(content)
        if len(summaries) != len(justifications): return None
        return summaries
    except Exception:
        return None