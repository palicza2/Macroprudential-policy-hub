import base64
import logging
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
        return ChatGoogleGenerativeAI(
            model=self.config["model_name"], 
            temperature=temperature,
            max_tokens=self.config.get("max_output_tokens", 1000)
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

    def run_analysis(self, data_inputs, plot_paths, contexts):
        latest_ccyb_str = df_to_string(data_inputs.get('latest_ccyb_df'))
        ccyb_decisions_str = df_to_string(data_inputs.get('ccyb_decisions_df'))
        active_syrb_str = df_to_string(data_inputs.get('active_syrb_df'))
        syrb_decisions_str = df_to_string(data_inputs.get('syrb_decisions_df'))
        active_bbm_str = df_to_string(data_inputs.get('active_bbm_df'))
        
        system_context = "ROLE: Financial Analyst. STYLE: Professional, concise. Use HTML tags like <b> for emphasis if needed. IMPORTANT: Always use the provided DATA tables as the primary source of truth for numbers and rates."

        # 1. LÉPÉS: Egyedi ábra-elemzések
        chart_tasks = [
            {"id": "ccyb_diffusion_analysis", "img": "ccyb_diffusion", "data": latest_ccyb_str, "temp": 0.2, "prompt": "Analyze the CCyB adoption trend. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            {"id": "ccyb_history_analysis", "img": "ccyb_timeseries", "data": latest_ccyb_str, "temp": 0.2, "prompt": "Highlight key historical CCyB trajectories. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            {"id": "ccyb_level_analysis", "img": "cross_section_bar", "data": latest_ccyb_str, "temp": 0.3, "prompt": "Compare current CCyB levels. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            {"id": "risk_analysis_text", "img": "risk_plot", "data": latest_ccyb_str, "temp": 0.3, "prompt": "Interpret Credit Gap vs CCyB. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            {"id": "ccyb_decisions_analysis", "img": None, "data": ccyb_decisions_str, "temp": 0.2, "prompt": "Summarize recent CCyB decisions. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            
            {"id": "syrb_trend_analysis", "img": "syrb_counts_trend", "data": "", "temp": 0.2, "prompt": "Describe SyRB usage trends. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            {"id": "syrb_sectoral_analysis", "img": "syrb_sector", "data": "", "temp": 0.2, "prompt": "Analyze SyRB sectoral composition. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            {"id": "syrb_active_analysis", "img": None, "data": active_syrb_str, "temp": 0.3, "prompt": "Analyze active SyRB measures. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            {"id": "syrb_decisions_analysis", "img": None, "data": syrb_decisions_str, "temp": 0.2, "prompt": "Summarize latest SyRB decisions. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."},
            
            {"id": "bbm_analysis", "img": None, "data": active_bbm_str, "temp": 0.3, "prompt": "Analyze Borrower-Based Measures (LTV, DSTI, DTI, etc.) active across Europe. Focus on how these measures target household indebtedness. Start with a strong topic sentence. Write exactly ONE paragraph of 6-7 sentences."}
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
            TASK: Write a SPECIFIC high-level summary of the CCyB (Countercyclical Buffer) section.
            INPUTS (Context from charts):
            - Adoption Trends: {results.get('ccyb_diffusion_analysis')}
            - Current Levels: {results.get('ccyb_level_analysis')}
            - Risks: {results.get('risk_analysis_text')}
            - Decisions: {results.get('ccyb_decisions_analysis')}
            
            STRUCTURE: 1-2 bullet points (HTML <li> tags). 
            REQUIREMENT: Be specific. Mention actual trends, country groups, and risk directions.
            """
            res_ccyb = (self._get_llm(0.3) | StrOutputParser()).invoke([HumanMessage(content=ccyb_summ_prompt)])
            results['ccyb_section_summary'] = self._clean_text(res_ccyb, is_global=True)

            # SyRB Section Summary
            syrb_summ_prompt = f"""
            {system_context}
            TASK: Write a SPECIFIC high-level summary of the SyRB (Systemic Risk Buffer) section.
            INPUTS (Context from charts):
            - Usage Trends: {results.get('syrb_trend_analysis')}
            - Sectoral Focus: {results.get('syrb_sectoral_analysis')}
            - Active Measures: {results.get('syrb_active_analysis')}
            - Recent Decisions: {results.get('syrb_decisions_analysis')}
            
            STRUCTURE: 1-2 bullet points (HTML <li> tags).
            REQUIREMENT: Be specific about real estate (RRE/CRE) focus and country-specific sectoral measures.
            """
            res_syrb = (self._get_llm(0.3) | StrOutputParser()).invoke([HumanMessage(content=syrb_summ_prompt)])
            results['syrb_section_summary'] = self._clean_text(res_syrb, is_global=True)

            # BBM Section Summary
            bbm_summ_prompt = f"""
            {system_context}
            TASK: Write a SPECIFIC high-level summary of the Borrower-Based Measures (BBM) section.
            INPUTS (Context from analysis):
            - Active BBM Analysis: {results.get('bbm_analysis')}
            
            STRUCTURE: 1-2 bullet points (HTML <li> tags).
            REQUIREMENT: Be specific about LTV, DSTI, and other borrower-based constraints applied across countries.
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
            TASK: Write a comprehensive Global Executive Summary.
            STRUCTURE: 4-5 paragraphs, each 5-6 sentences long. Each paragraph must start with a <b>bold topic sentence</b>.
            CONTENT: Synthesize the findings. How are cyclical (CCyB), structural (SyRB), and borrower-based (BBM) tools interacting? What is the overall macroprudential stance in Europe?
            
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