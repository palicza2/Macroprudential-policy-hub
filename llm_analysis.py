import base64
import json
import logging
import os
import re
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser
from dotenv import load_dotenv
from config import LLM_CONFIG

load_dotenv()
logger = logging.getLogger(__name__)

def get_base64(path):
    if not path or not path.exists(): return None
    return base64.b64encode(path.read_bytes()).decode('utf-8')

def df_to_string(df, rows=50):
    if df is None or df.empty: return "No numeric data available."
    # Megemeltük a limitet 50-re, hogy minden ország beleférjen
    return df.head(rows).to_markdown(index=False)

class LLMAnalyzer:
    def __init__(self, config):
        self.config = config

    def _get_llm(self, temperature):
        api_key_env = self.config.get("api_key_env", "GOOGLE_API_KEY")
        api_key = os.getenv(api_key_env)
        return ChatGoogleGenerativeAI(
            model=self.config["model_name"], 
            temperature=temperature,
            max_tokens=self.config.get("max_output_tokens", 1000),
            google_api_key=api_key,
        )

    def _clean_text(self, text, is_global=False):
        if not text: return ""
        # Eltávolítjuk a Markdown fejléc jeleket és dollárjeleket
        text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
        text = text.replace('$', '')
        
        # Eltávolítjuk a gyakori AI bevezető sallangokat a fejezet-összefoglalóknál
        if is_global:
            text = re.sub(r'^(Here is|Below is|This is|Here are).*?:', '', text, flags=re.IGNORECASE | re.DOTALL).strip()

        # Félkövér kiemelések átalakítása (AI tételmondatokhoz)
        text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)

        if is_global:
            # Bulletpontok átalakítása HTML listává
            text = re.sub(r'^\s*[-*]\s+(.*)', r'<li>\1</li>', text, flags=re.MULTILINE)
            
            # Bekezdések kezelése: dupla sortörésnél vágunk
            parts = [p.strip() for p in text.split('\n\n') if p.strip()]
            final_parts = []
            
            for p in parts:
                if '<li>' in p:
                    # Ha van benne lista elem, tegyük <ul> közé
                    li_content = re.sub(r'\n+', '', p)
                    # Biztosítsuk, hogy nincs felesleges whitespace a tagek között
                    li_content = li_content.replace('</li><li>', '</li>\n<li>')
                    final_parts.append(f"<ul>{li_content}</ul>")
                else:
                    final_parts.append(f"<p>{p}</p>")
            
            text = "".join(final_parts)
        else:
            text = re.sub(r'\*|_', '', text)
            text = re.sub(r'^\s*[-*]\s+', '', text, flags=re.MULTILINE)
            
        return text.strip()

    def extract_clean_rates(self, text_list):
        if not text_list: return []
        input_text = "\n".join([f"{i+1}. {str(text)[:300]}" for i, text in enumerate(text_list)])
        prompt = f"TASK: Extract the specific SyRB rate or interval. OUTPUT FORMAT: Numbered list. ONLY the rate. INPUT:\n{input_text}"
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            lines = [line.strip() for line in res.split('\n') if line.strip()]
            results = [re.sub(r'^\d+\.?\s*', '', l) for l in lines]
            if len(results) < len(text_list): results.extend(["N/A"]*(len(text_list)-len(results)))
            return results[:len(text_list)]
        except: return ["Error"]*len(text_list)

    def extract_keywords(self, text_list, context="justification"):
        if not text_list: return []
        input_text = "\n".join([f"{i+1}. {str(text)[:500]}" for i, text in enumerate(text_list)])
        
        # Szigorúbb szakmai fókusz
        instr = (
            "Focus ONLY on targeted risks (e.g., credit growth, real estate, cyclical risks) and regulatory intent. "
            "NEVER include technical terms like 'press release', 'notification', 'official', or authority names. "
            "NO generic phrases."
        )
        
        prompt = f"""TASK: Extract 3-4 professional keywords/phrases for each numbered item.
        {instr}
        FORMAT: Return a numbered list matching the input count. Each line should ONLY contain keywords separated by commas.
        INPUT:
        {input_text}"""
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            # Tisztább sorokra bontás
            lines = [l.strip() for l in res.split('\n') if l.strip() and (l.strip()[0].isdigit() or ',' in l)]
            results = [re.sub(r'^\d+[\.\)]\s*', '', l) for l in lines]
            
            # Ellenőrizzük a hosszt
            if len(results) < len(text_list):
                results.extend([""] * (len(text_list) - len(results)))
            return results[:len(text_list)]
        except Exception as e:
            logger.error(f"Error in extract_keywords: {e}")
            return [""] * len(text_list)

    def extract_ltv_fields(self, text_list):
        if not text_list:
            return []
        input_text = "\n".join([f"{i+1}. {str(text)[:800]}" for i, text in enumerate(text_list)])
        prompt = f"""TASK: Extract structured LTV policy details from each item.
OUTPUT: JSON array with one object per item, in the same order.
Each object must contain:
  - limits: list of LTV limit strings with % (e.g., ["80%", "90%"])
  - ftb_flag: "Yes" or "No" if a first-time buyer (FTB) exception exists
  - ftb_details: short phrase describing the FTB exception (or empty string)
  - other_exceptions: short phrase for other exceptions/quotas (or empty string)
Do NOT invent values. Use empty list/strings if not stated.
INPUT:
{input_text}"""
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = None
            try:
                parsed = json.loads(res)
            except Exception:
                match = re.search(r"(\[[\s\S]*\])", res)
                if match:
                    try:
                        parsed = json.loads(match.group(1))
                    except Exception:
                        parsed = None
            if parsed is None:
                return [{} for _ in text_list]
            if not isinstance(parsed, list):
                return [{} for _ in text_list]
            if len(parsed) < len(text_list):
                parsed.extend([{}] * (len(text_list) - len(parsed)))
            return parsed[:len(text_list)]
        except Exception as e:
            logger.error(f"Error in extract_ltv_fields: {e}")
            return [{} for _ in text_list]

    def classify_news_tags(self, text_list):
        if not text_list:
            return []
        allowed = [
            "ccyb", "syrb", "bbm", "ltv", "dsti", "lti", "dti",
            "real-estate", "capital", "reciprocation"
        ]
        allowed_str = ", ".join(allowed)
        input_text = "\n".join([f"{i+1}. {str(text)[:600]}" for i, text in enumerate(text_list)])
        prompt = f"""TASK: Assign zero or more tags to each item from the allowed list.
ALLOWED TAGS: {allowed_str}
RETURN: JSON array, each entry is an array of tag strings for the matching item.
RULES: Only use allowed tags. Use [] if no tags are applicable.
INPUT:
{input_text}"""
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = None
            try:
                parsed = json.loads(res)
            except Exception:
                match = re.search(r"(\[[\s\S]*\])", res)
                if match:
                    try:
                        parsed = json.loads(match.group(1))
                    except Exception:
                        parsed = None
            if not isinstance(parsed, list):
                return [[] for _ in text_list]
            if len(parsed) < len(text_list):
                parsed.extend([[]] * (len(text_list) - len(parsed)))
            normalized = []
            for tags in parsed[:len(text_list)]:
                if not isinstance(tags, list):
                    normalized.append([])
                    continue
                cleaned = [t for t in tags if isinstance(t, str) and t in allowed]
                normalized.append(cleaned)
            return normalized
        except Exception as e:
            logger.error(f"Error in classify_news_tags: {e}")
            return [[] for _ in text_list]

    def summarize_news_items(self, text_list):
        if not text_list:
            return []
        input_text = "\n".join([f"{i+1}. {str(text)[:800]}" for i, text in enumerate(text_list)])
        prompt = """TASK: Summarize each item in 2-3 concise sentences.
RULES: Keep it factual and short (max ~60 words). Do not add new facts.
RETURN: JSON array of strings, in the same order as input.
INPUT:
""" + input_text
        try:
            llm = self._get_llm(temperature=0.2)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = None
            try:
                parsed = json.loads(res)
            except Exception:
                match = re.search(r"(\[[\s\S]*\])", res)
                if match:
                    try:
                        parsed = json.loads(match.group(1))
                    except Exception:
                        parsed = None
            if not isinstance(parsed, list):
                return ["" for _ in text_list]
            if len(parsed) < len(text_list):
                parsed.extend([""] * (len(text_list) - len(parsed)))
            cleaned = [str(s).strip() if isinstance(s, str) else "" for s in parsed[:len(text_list)]]
            return cleaned
        except Exception as e:
            logger.error(f"Error in summarize_news_items: {e}")
            return ["" for _ in text_list]

    def run_analysis(self, data_inputs, plot_paths, contexts):
        latest_ccyb_str = df_to_string(data_inputs.get('latest_ccyb_df'))
        ccyb_decisions_str = df_to_string(data_inputs.get('ccyb_decisions_df'))
        active_syrb_str = df_to_string(data_inputs.get('active_syrb_df'))
        syrb_decisions_str = df_to_string(data_inputs.get('syrb_decisions_df'))
        active_bbm_str = df_to_string(data_inputs.get('active_bbm_df'))
        bbm_decisions_str = df_to_string(data_inputs.get('bbm_decisions_df'))
        ltv_table_str = df_to_string(data_inputs.get('ltv_table_df'))
        news_str = df_to_string(data_inputs.get('news_df'))
        
        system_context = (
            "ROLE: Financial Analyst. STYLE: Professional, concise, analytical. "
            "TIMEFRAME: Focus on developments in the last 12 months; mention older context only briefly. "
            "FOCUS: Emphasize country objectives and the risks being addressed. "
            "AVOID: Explaining what the tools are or their transmission/impact mechanisms. "
            "IMPORTANT: Always use the provided DATA tables as the primary source of truth for numbers and rates."
        )

        # 1. LÉPÉS: Egyedi ábra-elemzések
        chart_tasks = [
            {"id": "ccyb_diffusion_analysis", "img": "ccyb_diffusion", "data": latest_ccyb_str, "temp": 0.2, "prompt": "Analyze CCyB adoption over the last 12 months. Emphasize country objectives and risks addressed (e.g., credit growth, property markets). Avoid tool descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "ccyb_history_analysis", "img": "ccyb_timeseries", "data": latest_ccyb_str, "temp": 0.2, "prompt": "Highlight key CCyB changes in the last 12 months. Emphasize where objectives shifted and what risks authorities cite. Avoid explaining the CCyB mechanism. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "ccyb_level_analysis", "img": "cross_section_bar", "data": latest_ccyb_str, "temp": 0.3, "prompt": "Compare current CCyB levels with emphasis on the last 12 months of changes. Focus on country goals and risks being targeted; avoid general tool descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "risk_analysis_text", "img": "risk_plot", "data": latest_ccyb_str, "temp": 0.3, "prompt": "Interpret Credit Gap vs CCyB with a focus on the last 12 months. Emphasize risk signals and policy objectives across countries; avoid explaining mechanisms. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "ccyb_decisions_analysis", "img": None, "data": ccyb_decisions_str, "temp": 0.2, "prompt": "Summarize CCyB decisions from the last 12 months. Emphasize the risks cited and policy objectives; avoid tool explanations. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            
            {"id": "syrb_trend_analysis", "img": "syrb_counts_trend", "data": "", "temp": 0.2, "prompt": "Describe SyRB trends over the last 12 months. Emphasize objectives and risks (especially sectoral exposures) rather than tool mechanics. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "syrb_sectoral_analysis", "img": "syrb_sector", "data": "", "temp": 0.2, "prompt": "Analyze SyRB sectoral composition with focus on the last 12 months. Highlight country targets and risk pockets; avoid mechanism descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "syrb_active_analysis", "img": None, "data": active_syrb_str, "temp": 0.3, "prompt": "Analyze active SyRB measures from the last 12 months. Emphasize country objectives and risks cited; avoid tool explanations. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "syrb_decisions_analysis", "img": None, "data": syrb_decisions_str, "temp": 0.2, "prompt": "Summarize SyRB decisions from the last 12 months. Emphasize risks addressed and policy objectives; avoid mechanism descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            
            {"id": "bbm_analysis", "img": None, "data": active_bbm_str, "temp": 0.3, "prompt": "Analyze borrower-based measures with focus on the last 12 months. Emphasize country objectives and risks (e.g., housing credit risks), not tool mechanics. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "bbm_diffusion_analysis", "img": "bbm_diffusion", "data": "", "temp": 0.2, "prompt": "Analyze adoption trends of borrower-based measures over the last 12 months. Emphasize what risks countries are targeting; avoid describing tool mechanics. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."},
            {"id": "bbm_decisions_analysis", "img": None, "data": bbm_decisions_str, "temp": 0.2, "prompt": "Summarize borrower-based measure decisions from the last 12 months. Emphasize objectives and risks cited; avoid mechanism descriptions. Start with a strong topic sentence. Write ONE paragraph of 6-7 sentences."}
            ,
            {"id": "ltv_analysis", "img": None, "data": ltv_table_str, "temp": 0.2, "prompt": "Analyze LTV limits and first-time buyer exemptions with focus on the last 12 months. Emphasize objectives and risks, avoid mechanism explanations. Write ONE paragraph of 4-5 sentences."}
            ,
            {"id": "news_summary", "img": None, "data": news_str, "temp": 0.2, "prompt": "Summarize the most important macroprudential news from the last 12 months. Focus on objectives and risks cited. Write ONE paragraph of 4-5 sentences."}
        ]

        results = {}
        for t in chart_tasks:
            logger.info(f"  🧠 Elemzés: {t['id']}...")
            try:
                img_path = plot_paths.get(t['img']) if t['img'] else None
                img_b64 = get_base64(img_path)
                content = [{"type": "text", "text": t['prompt'] + (f"\nDATA:\n{t['data']}" if t['data'] else "")}]
                if img_b64: content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}})
                llm = self._get_llm(temperature=t.get('temp', 0.2))
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=content)])
                results[t['id']] = self._clean_text(res, is_global=False)
            except: results[t['id']] = "N/A"

        # 2. LÉPÉS: Fejezet összefoglalók (már látják a rész-elemzéseket is)
        logger.info("  🧠 Section Summaries...")
        try:
            # CCyB Section Summary
            ccyb_summ_prompt = f"""
            {system_context}
            TASK: Write a SPECIFIC high-level summary of the CCyB section focused on the last 12 months.
            INPUTS (Context from charts):
            - Adoption Trends: {results.get('ccyb_diffusion_analysis')}
            - Current Levels: {results.get('ccyb_level_analysis')}
            - Risks: {results.get('risk_analysis_text')}
            - Decisions: {results.get('ccyb_decisions_analysis')}
            
            STRUCTURE: 1-2 bullet points (HTML <li> tags). 
            REQUIREMENT: Be analytical. Emphasize country objectives and the risks being addressed. Avoid tool descriptions or mechanism explanations.
            """
            res_ccyb = (self._get_llm(0.3) | StrOutputParser()).invoke([HumanMessage(content=ccyb_summ_prompt)])
            results['ccyb_section_summary'] = self._clean_text(res_ccyb, is_global=True)

            # SyRB Section Summary
            syrb_summ_prompt = f"""
            {system_context}
            TASK: Write a SPECIFIC high-level summary of the SyRB section focused on the last 12 months.
            INPUTS (Context from charts):
            - Usage Trends: {results.get('syrb_trend_analysis')}
            - Sectoral Focus: {results.get('syrb_sectoral_analysis')}
            - Active Measures: {results.get('syrb_active_analysis')}
            - Recent Decisions: {results.get('syrb_decisions_analysis')}
            
            STRUCTURE: 1-2 bullet points (HTML <li> tags).
            REQUIREMENT: Be analytical. Emphasize objectives and targeted risks (e.g., sectoral exposures). Avoid tool descriptions or mechanism explanations.
            """
            res_syrb = (self._get_llm(0.3) | StrOutputParser()).invoke([HumanMessage(content=syrb_summ_prompt)])
            results['syrb_section_summary'] = self._clean_text(res_syrb, is_global=True)

            # BBM Section Summary
            bbm_summ_prompt = f"""
            {system_context}
            TASK: Write a SPECIFIC high-level summary of the Borrower-Based Measures (BBM) section focused on the last 12 months.
            INPUTS (Context from analysis):
            - Active BBM Analysis: {results.get('bbm_analysis')}
            
            STRUCTURE: 1-2 bullet points (HTML <li> tags).
            REQUIREMENT: Be analytical. Emphasize objectives and risks (housing leverage, affordability, credit quality). Avoid tool descriptions or mechanism explanations.
            """
            res_bbm = (self._get_llm(0.3) | StrOutputParser()).invoke([HumanMessage(content=bbm_summ_prompt)])
            results['bbm_section_summary'] = self._clean_text(res_bbm, is_global=True)
        except Exception as e:
            logger.error(f"Error in section summaries: {e}")

        # 3. LÉPÉS: Global Executive Summary (már a fejezet-összefoglalókból építkezik)
        logger.info("  🧠 Global Summary...")
        try:
            exec_prompt = f"""
            {system_context}
            TASK: Write a comprehensive Global Executive Summary focused on the last 12 months.
            STRUCTURE: 4-5 paragraphs, each 5-6 sentences long. Each paragraph must start with a <b>bold topic sentence</b>.
            CONTENT: Synthesize the findings. Emphasize country objectives and the risks being addressed, and how recent trends shifted the overall stance. Avoid explaining tool mechanics.
    
    INPUTS:
            CCyB Overview: {results.get('ccyb_section_summary')}
            SyRB Overview: {results.get('syrb_section_summary')}
            BBM Overview: {results.get('bbm_section_summary')}
            """
            res_global = (self._get_llm(0.5) | StrOutputParser()).invoke([HumanMessage(content=exec_prompt)])
            results['executive_summary'] = self._clean_text(res_global, is_global=True)
        except: results['executive_summary'] = "N/A"
        
        return results
        
        return results