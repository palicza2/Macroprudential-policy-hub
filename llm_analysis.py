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
from prompts import SYSTEM_CONTEXT_LAST_12M
from llm_tasks import build_chart_tasks, build_section_tasks, build_global_task
from llm_runner import run_tasks, run_task

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
        capital_overall_str = df_to_string(data_inputs.get('capital_overall_df'))
        
        # 1) Chart/table tasks
        chart_tasks = build_chart_tasks(
            latest_ccyb_str=latest_ccyb_str,
            ccyb_decisions_str=ccyb_decisions_str,
            active_syrb_str=active_syrb_str,
            syrb_decisions_str=syrb_decisions_str,
            active_bbm_str=active_bbm_str,
            bbm_decisions_str=bbm_decisions_str,
            ltv_table_str=ltv_table_str,
            news_str=news_str,
            capital_overall_str=capital_overall_str,
        )
        results = run_tasks(analyzer=self, tasks=chart_tasks, plot_paths=plot_paths)

        # 2) Section summaries
        logger.info("  🧠 Section Summaries...")
        for t in build_section_tasks(results):
            results[t.id] = run_task(analyzer=self, task=t)

        # 3) Global summary
        logger.info("  🧠 Global Summary...")
        exec_task = build_global_task(results)
        results[exec_task.id] = run_task(analyzer=self, task=exec_task)

        return results
    
    def analyze_knowledge_graph(self, graph_data, summary_data):
        """
        Analyze the knowledge graph and compare it with existing data tables.
        
        Args:
            graph_data: Dict with 'nodes' and 'edges' lists
            summary_data: Dict with summary statistics from tables (CCyB, SyRB, BBM counts)
        
        Returns:
            Analysis text comparing graph insights with table data
        """
        if not graph_data or not graph_data.get('nodes'):
            return "Knowledge graph data not available for analysis."
        
        nodes = graph_data.get('nodes', [])
        edges = graph_data.get('edges', [])
        
        # Count nodes by type
        country_count = len([n for n in nodes if n.get('group') == 'country'])
        ccyb_count = len([n for n in nodes if n.get('group') == 'ccyb'])
        syrb_count = len([n for n in nodes if n.get('group') == 'syrb'])
        osii_count = len([n for n in nodes if n.get('group') == 'osii'])
        bbm_count = len([n for n in nodes if n.get('group') == 'bbm'])
        
        # Count edges by type
        has_edges = len([e for e in edges if e.get('label') == 'HAS'])
        similar_edges = len([e for e in edges if e.get('label') == 'SIMILAR'])
        similar_measure_edges = len([e for e in edges if e.get('label') == 'SIMILAR_MEASURE'])
        coexists_edges = len([e for e in edges if e.get('label') == 'COEXISTS'])
        
        # Build summary text
        graph_summary = f"""
Knowledge Graph Statistics:
- Countries: {country_count}
- CCyB measures: {ccyb_count}
- SyRB measures: {syrb_count}
- O-SII measures: {osii_count}
- BBM measures: {bbm_count}
- Total nodes: {len(nodes)}
- Total edges: {len(edges)}
  - HAS (country → measure): {has_edges}
  - SIMILAR (similar countries): {similar_edges}
  - SIMILAR_MEASURE (similar measures): {similar_measure_edges}
  - COEXISTS (measures in same country): {coexists_edges}

Table Data Summary:
- Active CCyB countries: {summary_data.get('active_ccyb', 'N/A')}
- Active SyRB countries: {summary_data.get('active_syrb', 'N/A')}
- Active BBM countries: {summary_data.get('active_bbm', 'N/A')}
"""
        
        prompt = f"""Analyze the knowledge graph data and compare it with the table-based summary data.

TASK: Provide a concise analysis (3-4 paragraphs) that:
1. Identifies key insights from the knowledge graph structure
2. Compares graph statistics with table-based counts (validate consistency)
3. Highlights interesting patterns or relationships visible in the graph
4. Notes any discrepancies or additional insights the graph reveals

Focus on:
- Policy mix patterns (which countries use multiple measures)
- Regional similarities or differences
- Measure adoption patterns
- Any notable clusters or outliers

GRAPH DATA:
{graph_summary}

OUTPUT: Write a professional analysis in 3-4 paragraphs, focusing on actionable insights."""
        
        try:
            llm = self._get_llm(temperature=0.3)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            return self._clean_text(res, is_global=False)
        except Exception as e:
            logger.error(f"Error in analyze_knowledge_graph: {e}")
            return f"Graph analysis unavailable. Graph contains {len(nodes)} nodes and {len(edges)} edges."