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
1. CONCISE: Write exactly ONE dense paragraph (max 6-8 sentences).
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

# --- 3. Pipeline Setup (KÉT MODELL) ---

# A) KREATÍV MODELL
llm_creative = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite", 
    google_api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.4, 
    max_retries=1, 
    model_kwargs={"tools": [{"google_search": {}}]} 
)

# B) SZIGORÚ MODELL (Adatkinyeréshez)
llm_strict = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite", 
    google_api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.0,
    max_retries=1
)

analysis_chain = (
    RunnableLambda(build_multimodal_message) 
    | llm_creative 
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
        "key": "ccyb_executive_summary",
        "instruction": "Write a high-level executive summary specifically for the Countercyclical Capital Buffer (CCyB) landscape. Summarize the current tightening/loosening cycle and the main drivers.",
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
        "instruction": "Evaluate the CCyB adoption curve (chart/data) up to {latest_date}.",
        "image_filter": "ccyb_diffusion",
        "data_key": "agg_trend_df"
    },
    {
        "key": "jurisdictional_heterogeneity",
        "instruction": "Assess the divergence in CCyB rates across Europe.",
        "image_filter": "cross_section",
        "data_key": "latest_ccyb"
    },
    {
        "key": "risk_decoupling",
        "instruction": "Critique the Credit Gap vs CCyB Rate relationship.",
        "image_filter": "risk_plot",
        "data_key": "latest_ccyb"
    },
    
    # --- SyRB ---
    {
        "key": "syrb_executive_summary",
        "instruction": "Write a high-level executive summary specifically for the Systemic Risk Buffer (SyRB). Focus on the shift from general to sectoral buffers.",
        "image_filter": "syrb_diffusion",
        "data_key": "syrb_trend_df"
    },
    {
        "key": "syrb_diffusion_analysis",
        "instruction": "Analyze the SyRB trends. Compare 'General' vs 'Sectoral' adoption.",
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
        "key": "syrb_table_analysis",
        "instruction": "Analyze the 'Latest SyRB Decisions' and 'Active Measures' data. Highlight notable recent activations and sectoral complexities.",
        "image_filter": None,
        "data_key": "latest_syrb"
    }
]

# --- 5. Main Analysis Logic ---

def perform_llm_analysis(processed_dataframes, plot_paths, one_year_ago, external_contexts):
    analyses = {}
    
    data_map = {
        "agg_trend_df": processed_dataframes.get('agg_trend_df'),
        "latest_ccyb": processed_dataframes.get('latest_country_df'),
        "latest_ccyb_1yr": processed_dataframes.get('latest_country_df')[processed_dataframes.get('latest_country_df')['date'] >= one_year_ago] if processed_dataframes.get('latest_country_df') is not None else None,
        "syrb_trend_df": processed_dataframes.get('syrb_trend_df'),
        "latest_syrb": processed_dataframes.get('latest_syrb_df')
    }
    latest_date_str = datetime.now().strftime("%Y-%m-%d")
    
    print("--- Starting AI Analysis ---")

    for i, task in enumerate(tasks):
        if i > 0: time.sleep(5)

        df = data_map.get(task.get('data_key'))
        img_path = None
        if task.get('image_filter') and isinstance(plot_paths, list):
             img_path = next((p for p in plot_paths if task['image_filter'] in p), None)

        chain_input = {
            "system_instructions": "You are a senior financial analyst.",
            "instruction": task['instruction'].format(latest_date=latest_date_str),
            "external_context": external_contexts.get(task['key'], 'Use Google Search if needed.'),
            "data_context": format_data_to_json(df),
            "image_base64": img_to_base64(img_path)
        }
        
        try:
            print(f"Processing: {task['key']}...")
            result = invoke_chain_with_retry(chain_input)
            analyses[task['key']] = result
        except Exception as e:
            analyses[task['key']] = "Analysis unavailable."

    print("Processing: Global Executive Summary...")
    time.sleep(5)
    
    summary_input = "\n".join([f"[{k.upper()}]: {v}" for k, v in analyses.items()])
    
    exec_prompt = {
        "system_instructions": "Senior Chief Risk Officer.",
        "instruction": f"""
        Create a comprehensive Global Executive Summary of the EU Macroprudential landscape based on the findings below.
        FORMAT REQUIREMENT: HTML Unordered List (<ul><li>). Provide 4-5 key bullet points. Start bullets with <strong>bold title</strong>.
        FINDINGS: {summary_input}
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

# --- Helpers: ROBUST BATCH PROCESSING ---

def process_batch_with_ids(text_list, prompt_template, chunk_size=10):
    if not text_list: return None
    
    final_results = []
    
    for i in range(0, len(text_list), chunk_size):
        chunk = text_list[i : i + chunk_size]
        input_dict = {str(idx): txt for idx, txt in enumerate(chunk)}
        
        # ITT A JAVÍTÁS:
        # A prompt_template itt már egy sima string, amiben {json_input} van.
        # A .format() most már csak a {json_input}-ot cseréli le, 
        # mert a többi kapcsos zárójelet dupláztuk a lenti definíciókban.
        prompt_text = prompt_template.format(json_input=json.dumps(input_dict))
        
        try:
            response = llm_strict.invoke(prompt_text)
            content = response.content.strip()
            if "```json" in content: content = content.replace("```json", "").replace("```", "")
            result_dict = json.loads(content)
            
            chunk_results = []
            for idx in range(len(chunk)):
                key = str(idx)
                if key in result_dict:
                    chunk_results.append(result_dict[key])
                else:
                    chunk_results.append("N/A") 
            
            final_results.extend(chunk_results)
            
        except Exception as e:
            print(f"⚠️ Chunk processing failed: {e}. Using fallback.")
            fallback_chunk = [str(t)[:50] + "..." for t in chunk]
            final_results.extend(fallback_chunk)
            
    return final_results

# KULCS FONTOSSÁGÚ: 
# 1. Nincs 'f' betű a string előtt! (Nem f-string)
# 2. Dupla kapcsos zárójelek {{ }} a JSON példáknál.
# 3. Szimpla kapcsos zárójel { } a json_input-nál.

def batch_summarize_reasons(justifications):
    categories = ["Cyclical Risk / Credit Growth", "Real Estate Risk", "Building Resilience", "Positive Neutral Rate", "Crisis Release", "Maintain Status Quo"]
    
    prompt = """
    ROLE: Classifier.
    TASK: Map each input text in the JSON dictionary to EXACTLY ONE category from: {categories_json}.
    
    INPUT FORMAT: JSON Dictionary with IDs {{ "0": "text...", "1": "text..." }}
    OUTPUT FORMAT: JSON Dictionary with IDs {{ "0": "Category", "1": "Category" }}
    
    CRITICAL RULE: Return a dictionary with the EXACT SAME keys as the input.
    
    INPUTS:
    {json_input}
    """.replace("{categories_json}", json.dumps(categories)) 
    # Trükk: A kategóriákat beletesszük replace-szel, hogy ne zavarja a .format-ot később.
    
    return process_batch_with_ids(justifications, prompt)

def batch_extract_keywords(text_list):
    prompt = """
    ROLE: Financial Data Cleaner.
    TASK: Convert each long description in the JSON dictionary into a short 2-5 word tag.
    
    INPUT FORMAT: JSON Dictionary with IDs {{ "0": "Long text...", "1": "Long text..." }}
    OUTPUT FORMAT: JSON Dictionary with IDs {{ "0": "Short Tag", "1": "Short Tag" }}
    
    CRITICAL RULE: Return a dictionary with the EXACT SAME keys as the input. If text is empty, return "N/A".
    
    EXAMPLES:
    "Retail exposures secured by residential property" -> "Residential Mortgages"
    "All exposures located in the domestic market" -> "Domestic Exposures"
    
    INPUTS:
    {json_input}
    """
    
    return process_batch_with_ids(text_list, prompt)