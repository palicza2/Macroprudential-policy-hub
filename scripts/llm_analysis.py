import pandas as pd
import base64
import time
import os
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

# --- 1. Configuration & Helpers ---

def img_to_base64(image_path):
    if not image_path or not os.path.exists(image_path):
        return None
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error encoding image {image_path}: {e}")
        return None

def format_data_to_markdown(df, max_rows=60):
    if df is None or df.empty:
        return "No specific numeric data provided for this section."
    
    display_df = df.head(max_rows)
    suffix = f"\n... (and {len(df)-max_rows} more rows)" if len(df) > max_rows else ""

    try:
        return display_df.to_markdown(index=False) + suffix
    except ImportError:
        return display_df.to_string(index=False) + suffix
    except Exception as e:
        return f"Data available but formatting failed: {str(e)}"

# --- 2. Prompt Template (STRICT CONSTRAINTS, ENGLISH) ---

OUTPUT_INSTRUCTIONS = """
STRICT FORMATTING & CONTENT GUIDELINES:
1. SINGLE PARAGRAPH ONLY: Your entire response must be exactly ONE dense paragraph. Do not split into multiple paragraphs.
2. NO MARKDOWN: Do NOT use bold (**text**), italics (*text*), headers (##), or bullet points (-). Write plain text only.
3. CONCISE: Keep it under 7-8 sentences.
4. SYNTHESIS: Do not just list the numbers. Explain the trends and drivers (inflation, risks, resilience) connecting the data.
5. TONE: Professional financial analyst style (ECB/ESRB tone).
"""

def build_multimodal_message(inputs):
    text_content = f"""
    ROLE: {inputs.get('system_instructions', 'Expert Financial Stability Analyst')}

    CONTEXT (EXTERNAL):
    {inputs.get('external_context', 'No external context.')}

    ---
    INTERNAL DATA (FACTS):
    {inputs.get('data_context', 'No data provided.')}
    ---

    TASK:
    {inputs['instruction']}

    GUIDELINES:
    1. Analyze visual patterns in the CHART (if provided).
    2. Use INTERNAL DATA to support arguments with specific numbers.
    3. {OUTPUT_INSTRUCTIONS}
    """
    content_parts = [{"type": "text", "text": text_content}]
    if inputs.get("image_base64"):
        content_parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{inputs['image_base64']}"}
        })
    return [HumanMessage(content=content_parts)]

# --- 3. Pipeline Setup (Lite Model + Retry) ---

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

# Retry logic for 429 errors
@retry(
    retry=retry_if_exception_type(exceptions.ResourceExhausted),
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    reraise=True
)
def invoke_chain_with_retry(chain_input):
    return analysis_chain.invoke(chain_input)

# --- Tasks (English, Analytical Focus) ---
tasks = [
    {
        "key": "strategic_diffusion_analysis",
        "instruction": "Evaluate the strategic drivers behind the macroprudential adoption curve shown in the chart and DATA up to {latest_date}. Instead of listing counts, explain how global economic factors (e.g., post-COVID recovery, inflation) triggered these activation waves.",
        "data_df": None, 
        "image_filter": "macroprudential_diffusion.png",
        "external_context_key": "strategic_diffusion_analysis"
    },
    {
        "key": "policy_driver_analysis",
        "instruction": "Deconstruct the economic rationale behind the most recent CCyB decisions up to {latest_date}. Identify if the dominant drivers are cyclical (credit growth) or structural (resilience building), and contrast tightening vs. neutral stances.",
        "data_df": None,
        "image_filter": None,
        "external_context_key": "policy_driver_analysis"
    },
    {
        "key": "jurisdictional_heterogeneity",
        "instruction": "Assess the divergence in CCyB rates across Europe as of {latest_date}. Why do some regions (e.g., Scandinavia) maintain high buffers while others remain at zero? Discuss risk environment differences.",
        "data_df": None,
        "image_filter": "cross_sectional_snapshot.png",
        "external_context_key": "jurisdictional_heterogeneity"
    },
    {
        "key": "long_term_policy_trajectories",
        "instruction": "Interpret the cyclicality of national policies in the historical chart up to {latest_date}. Identify phases like pre-pandemic buildup and crisis release. Which countries show the most active management?",
        "data_df": None,
        "image_filter": "historical_evolution.png",
        "external_context_key": "long_term_policy_trajectories"
    },
     {
        "key": "indicator_decoupling",
        "instruction": "Critique the relationship between Credit Gap and CCyB Rates in the scatter plot. Identify 'decoupling' cases where rates are high despite negative gaps. Why are policymakers disregarding the standard gap (e.g., housing risks)?",
        "data_df": None,
        "image_filter": "risk_analysis.png",
        "external_context_key": "indicator_decoupling"
    }
]

# --- 4. Business Logic ---

def perform_llm_analysis(processed_dataframes, plot_paths, one_year_ago, external_contexts):
    analyses = {}
    
    # Data Extraction
    latest_df = processed_dataframes.get('latest_country_df')
    agg_trend_df = processed_dataframes.get('agg_trend_df')
    full_df = processed_dataframes.get('df')

    # Stats & Dates
    active_buffers, num_countries, latest_date_str = 0, 0, datetime.now().strftime("%Y-%m-%d")
    if latest_df is not None and not latest_df.empty:
        if 'rate' in latest_df.columns: active_buffers = (latest_df['rate'] > 0).sum()
        if 'country' in latest_df.columns: num_countries = latest_df['country'].nunique()
        if 'date' in latest_df.columns: latest_date_str = str(latest_df['date'].max())[:10]

    # Filtering
    latest_df_1yr = pd.DataFrame()
    if latest_df is not None and 'date' in latest_df.columns:
        latest_df_1yr = latest_df[latest_df['date'] >= one_year_ago]
    full_df_1yr = pd.DataFrame()
    if full_df is not None and 'date' in full_df.columns:
        full_df_1yr = full_df[full_df['date'] >= one_year_ago]

    _tasks = copy.deepcopy(tasks)

    print("--- Starting Analysis ---")

    for i, task in enumerate(_tasks):
        # Throttling
        if i > 0: 
            print("  ...Waiting 15s for API limits...")
            time.sleep(15) 

        try:
            task['instruction'] = task['instruction'].format(latest_date=latest_date_str, date=latest_date_str)
        except Exception: pass

        # Data Binding
        if task['key'] == "strategic_diffusion_analysis": task['data_df'] = agg_trend_df
        elif task['key'] == "policy_driver_analysis": task['data_df'] = latest_df_1yr
        elif task['key'] == "jurisdictional_heterogeneity": task['data_df'] = latest_df
        elif task['key'] == "long_term_policy_trajectories": task['data_df'] = full_df
        elif task['key'] == "indicator_decoupling": task['data_df'] = latest_df

        print(f"Processing: {task['key']}...")
        
        img_path = None
        if task.get('image_filter') and isinstance(plot_paths, list):
             img_path = next((p for p in plot_paths if task['image_filter'] in p), None)

        chain_input = {
            "system_instructions": "You are a senior financial analyst.",
            "instruction": task['instruction'],
            "external_context": external_contexts.get(task.get('external_context_key'), ''),
            "data_context": format_data_to_markdown(task['data_df']),
            "image_base64": img_to_base64(img_path)
        }
        
        try:
            result = invoke_chain_with_retry(chain_input)
            analyses[task['key']] = result
        except Exception as e:
            print(f"❌ Error in {task['key']}: {e}")
            analyses[task['key']] = "Analysis unavailable."

    # Executive Summary
    print("  ...Waiting 15s for Summary...")
    time.sleep(15)
    print("Processing: Executive Summary...")
    
    summary_input = ""
    for k, v in analyses.items():
        summary_input += f"[{k.upper()}]: {v}\n\n"

    exec_prompt = {
        "system_instructions": "You are a senior financial analyst.",
        "instruction": f"Write a single, concise Executive Summary paragraph (max 8 sentences) based on the findings below up to {latest_date_str}. Do NOT use any markdown formatting. Focus on the main trends.\n\nINPUT:\n{summary_input}",
        "external_context": f"Active buffers: {active_buffers}/{num_countries}.",
        "data_context": "See input above.",
        "image_base64": None
    }
    
    try:
        analyses['executive_summary'] = invoke_chain_with_retry(exec_prompt)
    except Exception:
        analyses['executive_summary'] = "Summary unavailable."

    return analyses

if __name__ == "__main__":
    pass