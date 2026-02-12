import base64
import json
import logging
import os
import re
from typing import Dict, Any, Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser
from dotenv import load_dotenv
from config import LLM_CONFIG
from prompts import SYSTEM_CONTEXT_LAST_12M
from llm_tasks import build_chart_tasks, build_section_tasks, build_global_task, build_osii_analysis_task
from llm_runner import run_tasks, run_task
from knowledge_graph import KnowledgeGraphRAG
from llm.cache import LLMCache
from utils.json_parser import safe_json_loads_list, safe_json_loads_dict

load_dotenv()
logger = logging.getLogger(__name__)

# Initialize cache instance
_cache = LLMCache()

def get_base64(path):
    if not path or not path.exists(): return None
    return base64.b64encode(path.read_bytes()).decode('utf-8')

def df_to_string(df, rows=50):
    if df is None or df.empty: return "No numeric data available."
    # Megemeltük a limitet 50-re, hogy minden ország beleférjen
    return df.head(rows).to_markdown(index=False)

class LLMAnalyzer:
    def __init__(self, config, rag_retriever=None):
        self.config = config
        self.rag_retriever = rag_retriever  # Optional RAG retriever for knowledge graph context

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
        
        # Check cache
        model_name = self.config.get("model_name", "")
        cached_response = _cache.get(prompt=prompt, model=model_name, temperature=0.0)
        if cached_response:
            res = cached_response
        else:
            try:
                llm = self._get_llm(temperature=0.0)
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
                # Cache the response
                _cache.set(prompt=prompt, response=res, model=model_name, temperature=0.0)
            except:
                return ["Error"]*len(text_list)
        
        try:
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
        
        # Check cache
        model_name = self.config.get("model_name", "")
        cached_response = _cache.get(prompt=prompt, model=model_name, temperature=0.0)
        if cached_response:
            res = cached_response
        else:
            try:
                llm = self._get_llm(temperature=0.0)
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
                # Cache the response
                _cache.set(prompt=prompt, response=res, model=model_name, temperature=0.0)
            except Exception as e:
                logger.error(f"Error in extract_keywords: {e}")
                return [""] * len(text_list)
        
        try:
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
        
        # Check cache
        model_name = self.config.get("model_name", "")
        cached_response = _cache.get(prompt=prompt, model=model_name, temperature=0.0)
        if cached_response:
            res = cached_response
        else:
            try:
                llm = self._get_llm(temperature=0.0)
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
                # Cache the response
                _cache.set(prompt=prompt, response=res, model=model_name, temperature=0.0)
            except Exception as e:
                logger.error(f"Error in extract_ltv_fields: {e}")
                return [{} for _ in text_list]
        
        try:
            parsed = safe_json_loads_list(res, default=[{} for _ in text_list])
            if len(parsed) < len(text_list):
                parsed.extend([{}] * (len(text_list) - len(parsed)))
            return parsed[:len(text_list)]
        except Exception as e:
            logger.error(f"Error in extract_ltv_fields: {e}")
            return [{} for _ in text_list]

    def extract_dti_lti_fields(self, items: list[dict]) -> list[dict]:
        """
        Extract structured DTI/LTI policy details from ESRB BBM items.
        Each item should include: iso2, country, measure_short, description, related_links (optional).
        Returns list of dicts with keys:
          - country_iso2
          - country_name
          - type (DTI/LTI)
          - numerator
          - denominator
          - limits
          - legal_basis (Mandatory / Guidance / Unknown)
          - evidence_excerpt
          - confidence (high/medium/low)
        """
        if not items:
            return []

        # Keep prompt tight and verification-friendly: require direct quotes as evidence.
        input_text = "\n\n".join(
            [
                "ITEM {i}\nISO2: {iso2}\nCountry: {country}\nMeasure: {measure}\nDescription:\n{desc}\nRelated links:\n{links}".format(
                    i=idx + 1,
                    iso2=str(it.get("iso2", "")),
                    country=str(it.get("country", "")),
                    measure=str(it.get("measure_short", "")),
                    desc=str(it.get("description", "")).strip()[:2000],
                    links=str(it.get("related_links", "") or "").strip()[:500],
                )
                for idx, it in enumerate(items)
            ]
        )

        prompt = f"""TASK: From each item, extract ONLY what is explicitly stated about DTI/LTI limits.
RETURN FORMAT: JSON array with one object per item, same order.

RULES:
- Do NOT invent values.
- Prefer short phrases (max ~12 words) for numerator/denominator.
- IMPORTANT: numerator/denominator must describe the components (e.g., "total debt obligations", "gross annual income").
  Do NOT put the ratio name itself ("DTI" or "LTI") into numerator/denominator.
- limits: include numeric thresholds and any stated exceptions/allowances.
- legal_basis must be one of: "Mandatory", "Guidance", "Unknown".
- evidence_excerpt MUST quote an exact phrase from the Description that supports the limits and type.
- confidence must be one of: "high", "medium", "low".
  - high: type+limits clearly stated in Description and evidence_excerpt quotes them
  - medium: type clear but limits or basis ambiguous
  - low: unclear / not enough evidence
- limit_standard: numeric multiplier (e.g., 4.5 for 4.5x income). Extract from "X times income" or "X:1" or percentage/100.
- limit_ftb: multiplier for First-Time Buyers if explicitly stated (nullable).
- limit_btl: multiplier for Buy-to-Let/Investors if explicitly stated (nullable).
- income_basis: "Gross" (pre-tax) or "Net" (post-tax) if explicitly stated, else "Gross".
- allowance_share: percentage of volume allowed to exceed limit (e.g., "15%") if stated.

FIELDS (all required):
- country_iso2
- country_name
- type
- numerator
- denominator
- limits
- legal_basis
- evidence_excerpt
- confidence
- limit_standard (float or null)
- limit_ftb (float or null)
- limit_btl (float or null)
- income_basis ("Gross" or "Net")
- allowance_share (string like "15%" or empty)

INPUT:
{input_text}"""

        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_list(res, default=[{} for _ in items])
            if len(parsed) < len(items):
                parsed.extend([{}] * (len(items) - len(parsed)))
            return parsed[: len(items)]
        except Exception as e:
            logger.error(f"Error in extract_dti_lti_fields: {e}")
            return [{} for _ in items]

    def verify_dti_lti_fields(self, items: list[dict], extracted: list[dict]) -> list[dict]:
        """
        Second-pass self-check: verify extracted fields against the item Description.
        If any field can't be justified by the Description, blank it and downgrade confidence.
        """
        if not items:
            return []
        if not extracted:
            return [{} for _ in items]

        # Build compact verification input (include related_links for legal basis inference).
        blocks = []
        for idx, (it, ex) in enumerate(zip(items, extracted), start=1):
            blocks.append(
                "ITEM {i}\nISO2: {iso2}\nCountry: {country}\nMeasure: {measure}\nDescription:\n{desc}\nRelated links:\n{links}\nEXTRACTED_JSON:\n{ex}".format(
                    i=idx,
                    iso2=str(it.get("iso2", "")),
                    country=str(it.get("country", "")),
                    measure=str(it.get("measure_short", "")),
                    desc=str(it.get("description", "")).strip()[:2000],
                    links=str(it.get("related_links", "") or "").strip()[:500],
                    ex=json.dumps(ex, ensure_ascii=False),
                )
            )
        input_text = "\n\n".join(blocks)

        prompt = f"""TASK: Verify each EXTRACTED_JSON against the Description for each item.
RETURN FORMAT: JSON array with one object per item, same order.

VERIFICATION RULES:
- Keep a field ONLY if it is explicitly supported by the Description text.
- If not explicitly supported, set it to "" (empty string).
- If numerator/denominator equals "DTI" or "LTI" (or is just the ratio name), set it to "".
- evidence_excerpt must be an exact quote from Description; if not possible, set "".
- confidence must be:
  - "high" only if type + limits + evidence_excerpt are all supported
  - otherwise "medium" if type is supported but some fields missing
  - otherwise "low"
- legal_basis: set "Mandatory" only if Description clearly indicates binding regulation/law/requirement; set "Guidance" if it clearly indicates guidelines/recommendation; else "Unknown".

FIELDS REQUIRED (same keys as before):
- country_iso2
- country_name
- type
- numerator
- denominator
- limits
- legal_basis
- evidence_excerpt
- confidence

INPUT:
{input_text}"""

        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_list(res, default=[{} for _ in items])
            if len(parsed) < len(items):
                parsed.extend([{}] * (len(items) - len(parsed)))
            return parsed[: len(items)]
        except Exception as e:
            logger.error(f"Error in verify_dti_lti_fields: {e}")
            return [{} for _ in items]

    def validate_dti_lti_rules(
        self,
        rules: list[dict],
        use_external_search: bool = False,
        search_config: Optional[Dict[str, Any]] = None
    ) -> list[dict]:
        """
        Validate extracted DTI/LTI rules using AI.
        
        Args:
            rules: List of rule dictionaries with extracted fields
            use_external_search: Whether to use external search for validation
            
        Returns:
            List of validation results with confidence scores
        """
        if not rules:
            return []
        
        input_text = "\n\n".join([
            f"RULE {i+1}\n"
            f"Country: {r.get('country', '')}\n"
            f"Measure: {r.get('measure_code', '')}\n"
            f"Status: {r.get('implementation_status', '')}\n"
            f"Legal Form: {r.get('legal_form', '')}\n"
            f"Limit Standard: {r.get('limit_standard', '') or 'MISSING - TRY TO EXTRACT FROM DESCRIPTION'}\n"
            f"Limit FTB: {r.get('limit_ftb', '') or 'MISSING - TRY TO EXTRACT IF MENTIONED'}\n"
            f"Limit BTL: {r.get('limit_btl', '') or 'MISSING - TRY TO EXTRACT IF MENTIONED'}\n"
            f"Income Basis: {r.get('income_basis', '') or 'MISSING - TRY TO EXTRACT FROM DESCRIPTION'}\n"
            f"Allowance Share: {r.get('allowance_share', '') or 'MISSING - TRY TO EXTRACT IF MENTIONED'}\n"
            f"Regulation URL: {r.get('regulation_url', '') or 'MISSING - TRY TO EXTRACT IF MENTIONED'}\n"
            f"ESRB Description:\n{r.get('description', '')[:2000]}"
            for i, r in enumerate(rules)
        ])
        
        search_note = ""
        search_results_text = ""
        if use_external_search and search_config:
            # Perform external searches for each rule
            try:
                from grounding_validator import _google_search
                for i, rule in enumerate(rules):
                    country = rule.get('country', '')
                    measure = rule.get('measure_code', '')
                    query = f"{country} {measure} DTI LTI macroprudential limit regulation"
                    results = _google_search(query, search_config)
                    if results:
                        search_results_text += f"\n\nRULE {i+1} ({country} {measure}) External Sources:\n"
                        for j, result in enumerate(results[:3], 1):
                            search_results_text += f"{j}. {result.get('title', '')} - {result.get('link', '')}\n{result.get('snippet', '')}\n"
                search_note = "\n\nIMPORTANT: Use external sources provided below to verify facts. Always cite sources."
            except Exception as e:
                logger.warning(f"External search failed: {e}")

        prompt = f"""TASK: Validate and FILL MISSING DATA for each extracted DTI/LTI rule against the ESRB description.
RETURN FORMAT: JSON array with one object per rule, same order.

VALIDATION AND FILLING RULES:
- Verify that each extracted field is explicitly supported by the description.
- If a field is missing (e.g., limit_standard is null/empty), try to extract it from the description.
- If a field cannot be verified or found, keep the original value or mark as null.
- confidence must be one of: "high", "medium", "low"
  - high: All key fields (limit_standard, income_basis, legal_form) are clearly supported or successfully extracted
  - medium: Most fields supported but some ambiguous or partially extracted
  - low: Significant uncertainty or missing key information
- IMPORTANT: Try to fill missing limit_standard values by extracting numeric multipliers from the description (e.g., "4.5x", "6 times", "8-times income").
- If multiple standard limits exist (e.g., ranges like "3-8x" or different limits for different borrower types), extract as list and ensure notes explains what each value means.
- If limit_standard is a list, notes MUST explain what each value represents (e.g., "3x minimum, 8x maximum, decreasing by age").
- If use_external_search is enabled, you may verify against external sources but must cite them.

FIELDS (all required):
- country
- measure_code
- confidence ("high"/"medium"/"low")
- limit_standard (extract and fill if missing, or correct if wrong, or keep original)
- limit_ftb (extract and fill if missing, or null)
- limit_btl (extract and fill if missing, or null)
- income_basis (extract and fill if missing, or correct if wrong, or keep original)
- legal_form (extract and fill if missing, or correct if wrong, or keep original)
- allowance_share (extract and fill if missing, or correct if wrong, or keep original)
- regulation_url (extract URL if mentioned in description, or null)
- corrections (array of strings describing any corrections or fillings made)
- evidence (short quote supporting the validation/filling)

{search_note}
{search_results_text}

INPUT:
{input_text}"""
        
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_list(res, default=[{} for _ in rules])
            if len(parsed) < len(rules):
                parsed.extend([{}] * (len(rules) - len(parsed)))
            return parsed[:len(rules)]
        except Exception as e:
            logger.error(f"Error in validate_dti_lti_rules: {e}")
            return [{} for _ in rules]
    
    def validate_dti_lti_table(
        self,
        table_rows: list[dict],
        use_external_search: bool = True,
        search_config: Optional[Dict[str, Any]] = None
    ) -> list[dict]:
        """
        Final validation pass: validate complete DTI/LTI table with AI and optional external search.
        
        Args:
            table_rows: List of dictionaries representing table rows
            use_external_search: Whether to use external search for validation
            
        Returns:
            List of validation results with confidence scores
        """
        if not table_rows:
            return []
        
        # Convert table to validation format
        table_text = "\n\n".join([
            f"ROW {i+1}\n" + "\n".join([f"{k}: {v}" for k, v in row.items()])
            for i, row in enumerate(table_rows)
        ])
        
        search_note = ""
        search_results_text = ""
        if use_external_search and search_config:
            # Perform external searches for key countries/measures
            try:
                from grounding_validator import _google_search
                seen_queries = set()
                for row in table_rows:
                    country = row.get('Country', '')
                    measure = row.get('Measure_Code', '')
                    query = f"{country} {measure} DTI LTI macroprudential limit regulation"
                    if query not in seen_queries:
                        seen_queries.add(query)
                        results = _google_search(query, search_config)
                        if results:
                            search_results_text += f"\n\n{country} {measure} External Sources:\n"
                            for j, result in enumerate(results[:2], 1):
                                search_results_text += f"{j}. {result.get('title', '')} - {result.get('link', '')}\n{result.get('snippet', '')}\n"
                search_note = "\n\nIMPORTANT: Use external sources provided below to verify facts. Cite sources for any corrections."
            except Exception as e:
                logger.warning(f"External search failed: {e}")

        prompt = f"""TASK: Perform final validation of the complete DTI/LTI table.
RETURN FORMAT: JSON array with one object per row, same order.

VALIDATION RULES:
- Verify each row against ESRB data and external sources (if enabled).
- Check for consistency across countries and measures.
- confidence must be one of: "high", "medium", "low"
  - high: All fields verified and consistent
  - medium: Most fields verified but some uncertainty
  - low: Significant issues or missing verification
- Provide corrections if needed.

FIELDS (all required):
- row_index (0-based)
- confidence ("high"/"medium"/"low")
- is_valid (true/false)
- corrections (array of strings describing corrections needed, empty if valid)
- external_sources (array of source URLs if external search used)
- notes (any additional notes)

{search_note}
{search_results_text}

TABLE:
{table_text}"""
        
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_list(res, default=[{} for _ in table_rows])
            if len(parsed) < len(table_rows):
                parsed.extend([{}] * (len(table_rows) - len(parsed)))
            return parsed[:len(table_rows)]
        except Exception as e:
            logger.error(f"Error in validate_dti_lti_table: {e}")
            return [{} for _ in table_rows]
    
    def confirm_dti_lti_presence(self, items: list[dict]) -> list[dict]:
        """
        Lightweight confirmation: does the Description explicitly state a DTI or LTI limit?
        Returns JSON list with:
          - country_iso2
          - type (DTI/LTI)
          - confirmed ("yes"/"no")
          - evidence_excerpt (exact quote)
          - confidence ("high"/"medium"/"low")
        """
        if not items:
            return []

        input_text = "\n\n".join(
            [
                "ITEM {i}\nISO2: {iso2}\nCountry: {country}\nMeasure: {measure}\nDescription:\n{desc}".format(
                    i=idx + 1,
                    iso2=str(it.get("iso2", "")),
                    country=str(it.get("country", "")),
                    measure=str(it.get("measure_short", "")),
                    desc=str(it.get("description", "")).strip()[:2000],
                )
                for idx, it in enumerate(items)
            ]
        )

        prompt = f"""TASK: For each item, decide if the Description explicitly states a binding DTI/LTI limit (threshold or rule).
RETURN FORMAT: JSON array with one object per item, same order.

RULES:
- confirmed must be "yes" if a DTI/LTI limit/rule is mentioned, even if the exact numeric value is not explicitly stated in the quote.
- Look for: DTI/LTI limits, debt-to-income ratios, loan-to-income ratios, income multiples (e.g., "8-times income", "6 times", "4.5x"), or similar borrower-based restrictions.
- IMPORTANT: If measure_short is "DTI" but description mentions "loan-to-income" or "mortgage", it's likely LTI (Loan-to-Income), not DTI (Debt-to-Income). Set type accordingly.
- evidence_excerpt MUST be an exact quote from Description containing the limit/rule or DTI/LTI mention.
- confidence:
  - high: numeric limit/rule clearly quoted (e.g., "DTI ratio is set at 6", "LTI ... 4 times income", "8-times his/her yearly net disposable income")
  - medium: DTI/LTI limit mentioned but exact numeric value not in quote, or limit described in general terms (e.g., "DTI limit", "loan-to-income restriction")
  - low: unclear or no DTI/LTI mention

IMPORTANT: Be inclusive - if a country has a DTI/LTI measure mentioned in the description, mark it as confirmed="yes" with at least "medium" confidence. Countries like SK, DK, LV should be included if they have DTI/LTI mentions.

FIELDS:
- country_iso2
- type (DTI or LTI - determine from description, not just measure_short)
- confirmed
- evidence_excerpt
- confidence

INPUT:
{input_text}"""

        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_list(res, default=[{} for _ in items])
            if len(parsed) < len(items):
                parsed.extend([{}] * (len(items) - len(parsed)))
            return parsed[: len(items)]
        except Exception as e:
            logger.error(f"Error in confirm_dti_lti_presence: {e}")
            return [{} for _ in items]

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
        
        # Check cache
        model_name = self.config.get("model_name", "")
        cached_response = _cache.get(prompt=prompt, model=model_name, temperature=0.0)
        if cached_response:
            res = cached_response
        else:
            try:
                llm = self._get_llm(temperature=0.0)
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
                # Cache the response
                _cache.set(prompt=prompt, response=res, model=model_name, temperature=0.0)
            except Exception as e:
                logger.error(f"Error in classify_news_tags: {e}")
                return [[] for _ in text_list]
        
        try:
            parsed = safe_json_loads_list(res, default=[[] for _ in text_list])
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

    def summarize_text(self, text: str, instruction: str = "") -> str:
        """
        Summarize a single text using LLM.
        
        Args:
            text: Text to summarize
            instruction: Optional instruction for the summarization task
        
        Returns:
            Summarized text
        """
        if not text:
            return ""
        
        prompt = f"""TASK: {instruction if instruction else "Summarize the following text concisely."}
        
TEXT:
{text[:2000]}

Return a concise summary."""
        
        # Check cache
        model_name = self.config.get("model_name", "")
        cached_response = _cache.get(prompt=prompt, model=model_name, temperature=0.3)
        if cached_response:
            return self._clean_text(cached_response, is_global=False)
        
        try:
            llm = self._get_llm(temperature=0.3)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            # Cache the response
            _cache.set(prompt=prompt, response=res, model=model_name, temperature=0.3)
            return self._clean_text(res, is_global=False)
        except Exception as e:
            logger.error(f"Error in summarize_text: {e}")
            return ""

    def summarize_news_items(self, text_list):
        if not text_list:
            return []
        input_text = "\n".join([f"{i+1}. {str(text)[:800]}" for i, text in enumerate(text_list)])
        prompt = """TASK: Summarize each item in 2-3 concise sentences.
RULES: Keep it factual and short (max ~60 words). Do not add new facts.
RETURN: JSON array of strings, in the same order as input.
INPUT:
""" + input_text
        
        # Check cache
        model_name = self.config.get("model_name", "")
        cached_response = _cache.get(prompt=prompt, model=model_name, temperature=0.2)
        if cached_response:
            res = cached_response
        else:
            try:
                llm = self._get_llm(temperature=0.2)
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
                # Cache the response
                _cache.set(prompt=prompt, response=res, model=model_name, temperature=0.2)
            except Exception as e:
                logger.error(f"Error in summarize_news_items: {e}")
                return [""] * len(text_list)
        
        try:
            parsed = safe_json_loads_list(res, default=["" for _ in text_list])
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
        osii_data_str = df_to_string(data_inputs.get('latest_osii_df'))
        
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
        
        # 1b) OSII/GSII analysis task
        if osii_data_str and osii_data_str != "No numeric data available.":
            logger.info("  🧠 Analysis: osii_analysis...")
            osii_task = build_osii_analysis_task(osii_data_str)
            results[osii_task.id] = run_task(analyzer=self, task=osii_task)

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
        Uses RAG retriever to provide relevant context from the knowledge graph.
        
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
        
        # Use RAG retriever to get relevant context
        rag_context = ""
        if self.rag_retriever:
            try:
                # Retrieve context for key queries
                queries = [
                    "similar countries policy mix",
                    "countries with multiple measures",
                    "regional patterns",
                    "measure adoption trends"
                ]
                all_contexts = []
                for query in queries:
                    contexts = self.rag_retriever.retrieve_context(query, top_k=3)
                    for ctx in contexts:
                        if ctx.get('text') not in [c.get('text') for c in all_contexts]:
                            all_contexts.append(ctx)
                
                if all_contexts:
                    rag_context = "\n\nRelevant Knowledge Graph Context:\n"
                    rag_context += "\n".join([f"- {ctx.get('text')}" for ctx in all_contexts[:10]])
            except Exception as e:
                logger.warning(f"RAG context retrieval failed: {e}")
        
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
{rag_context}

OUTPUT: Write a professional analysis in 3-4 paragraphs, focusing on actionable insights."""
        
        try:
            llm = self._get_llm(temperature=0.3)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            return self._clean_text(res, is_global=False)
        except Exception as e:
            logger.error(f"Error in analyze_knowledge_graph: {e}")
            return f"Graph analysis unavailable. Graph contains {len(nodes)} nodes and {len(edges)} edges."
    
    def extract_ltv_rule_ai(self, description: str, country: str) -> Optional[Dict[str, Any]]:
        """
        Extract LTV rule from description using AI.
        
        Args:
            description: Measure description
            country: Country ISO2 code
            
        Returns:
            Dictionary with extracted fields or None
        """
        prompt = f"""TASK: Extract LTV (Loan-to-Value) rule details from the description.
RETURN FORMAT: JSON object.

EXTRACTION RULES:
- limit_standard: Standard LTV limit (0-100, e.g., 80.0 for 80%). If multiple standard limits exist (e.g., "80% for owner-occupied, 70% for investment"), return as list [80.0, 70.0]
- limit_ftb: First-Time Buyer limit if mentioned (0-100, nullable)
- limit_btl: Buy-to-Let/Investor limit if mentioned (0-100, nullable)
- exception_quota: Speed limit - percentage of volume allowed to exceed (e.g., "15% of volume")
- notes: Specific conditions or clarifications. IMPORTANT: If limit_standard is a list, notes MUST explain what each value means (e.g., "80% for owner-occupied properties, 70% for investment properties")

FIELDS (all required, use null for missing):
- limit_standard (float, list of floats, or null)
- limit_ftb (float or null)
- limit_btl (float or null)
- exception_quota (string or null)
- notes (string or null - MUST explain list meanings if limit_standard is a list)

INPUT:
Country: {country}
Description: {description[:2000]}"""
        
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_dict(res)
            return parsed if parsed else None
        except Exception as e:
            logger.warning(f"Error in extract_ltv_rule_ai: {e}")
            return None
    
    def validate_ltv_rules(
        self,
        rules: list[dict],
        descriptions: list[str],
        use_external_search: bool = False,
        search_config: Optional[Dict[str, Any]] = None
    ) -> list[dict]:
        """
        Validate extracted LTV rules using AI.
        
        Args:
            rules: List of rule dictionaries with extracted fields
            descriptions: List of ESRB descriptions (for context)
            use_external_search: Whether to use external search for validation
            
        Returns:
            List of validation results with confidence scores
        """
        if not rules:
            return []
        
        input_text = "\n\n".join([
            f"RULE {i+1}\n"
            f"Country: {r.get('country', '')}\n"
            f"Status: {r.get('implementation_status', '')}\n"
            f"Legal Form: {r.get('legal_form', '')}\n"
            f"Limit Standard: {r.get('limit_standard', '') or 'MISSING - TRY TO EXTRACT FROM DESCRIPTION'}\n"
            f"Limit FTB: {r.get('limit_ftb', '') or 'MISSING - TRY TO EXTRACT IF MENTIONED'}\n"
            f"Limit BTL: {r.get('limit_btl', '') or 'MISSING - TRY TO EXTRACT IF MENTIONED'}\n"
            f"Exception Quota: {r.get('exception_quota', '') or 'MISSING - TRY TO EXTRACT IF MENTIONED'}\n"
            f"ESRB Description:\n{descriptions[i] if i < len(descriptions) else ''}"
            for i, r in enumerate(rules)
        ])
        
        search_note = ""
        search_results_text = ""
        if use_external_search and search_config:
            try:
                from grounding_validator import _google_search
                for i, rule in enumerate(rules):
                    country = rule.get('country', '')
                    query = f"{country} LTV loan-to-value macroprudential limit regulation"
                    results = _google_search(query, search_config)
                    if results:
                        search_results_text += f"\n\nRULE {i+1} ({country}) External Sources:\n"
                        for j, result in enumerate(results[:3], 1):
                            search_results_text += f"{j}. {result.get('title', '')} - {result.get('link', '')}\n{result.get('snippet', '')}\n"
                search_note = "\n\nIMPORTANT: Use external sources provided below to verify facts. Always cite sources."
            except Exception as e:
                logger.warning(f"External search failed: {e}")
        
        prompt = f"""TASK: Validate and FILL MISSING DATA for each extracted LTV rule against the ESRB description.
RETURN FORMAT: JSON array with one object per rule, same order.

VALIDATION AND FILLING RULES:
- Verify that each extracted field is explicitly supported by the description.
- If a field is missing (e.g., limit_standard is null/empty), try to extract it from the description.
- If a field cannot be verified or found, keep the original value or mark as null.
- confidence must be one of: "high", "medium", "low"
  - high: All key fields (limit_standard, legal_form) are clearly supported or successfully extracted
  - medium: Most fields supported but some ambiguous or partially extracted
  - low: Significant uncertainty or missing key information
- IMPORTANT: Try to fill missing limit_standard values by extracting percentages from the description (e.g., "80%", "90% LTV").
- If use_external_search is enabled, you may verify against external sources but must cite them.

FIELDS (all required):
- country
- confidence ("high"/"medium"/"low")
- limit_standard (extract and fill if missing, or correct if wrong, or keep original)
- limit_ftb (extract and fill if missing, or null)
- limit_btl (extract and fill if missing, or null)
- exception_quota (extract and fill if missing, or null)
- legal_form (extract and fill if missing, or correct if wrong, or keep original)
- notes (extract and fill if missing, or null)
- corrections (array of strings describing any corrections or fillings made)
- evidence (short quote supporting the validation/filling)

{search_note}
{search_results_text}

INPUT:
{input_text}"""
        
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_list(res, default=[{} for _ in rules])
            if len(parsed) < len(rules):
                parsed.extend([{}] * (len(rules) - len(parsed)))
            return parsed[:len(rules)]
        except Exception as e:
            logger.error(f"Error in validate_ltv_rules: {e}")
            return [{} for _ in rules]
    
    def validate_ltv_table(
        self,
        table_rows: list[dict],
        use_external_search: bool = True,
        search_config: Optional[Dict[str, Any]] = None
    ) -> list[dict]:
        """
        Final validation pass: validate complete LTV table with AI and optional external search.
        
        Args:
            table_rows: List of dictionaries representing table rows
            use_external_search: Whether to use external search for validation
            
        Returns:
            List of validation results with confidence scores
        """
        if not table_rows:
            return []
        
        # Convert table to validation format
        table_text = "\n\n".join([
            f"ROW {i+1}\n" + "\n".join([f"{k}: {v}" for k, v in row.items()])
            for i, row in enumerate(table_rows)
        ])
        
        search_note = ""
        search_results_text = ""
        if use_external_search and search_config:
            try:
                from grounding_validator import _google_search
                seen_queries = set()
                for row in table_rows:
                    country = row.get('Country', '')
                    query = f"{country} LTV loan-to-value macroprudential limit regulation"
                    if query not in seen_queries:
                        seen_queries.add(query)
                        results = _google_search(query, search_config)
                        if results:
                            search_results_text += f"\n\n{country} External Sources:\n"
                            for j, result in enumerate(results[:2], 1):
                                search_results_text += f"{j}. {result.get('title', '')} - {result.get('link', '')}\n{result.get('snippet', '')}\n"
                search_note = "\n\nIMPORTANT: Use external sources provided below to verify facts. Cite sources for any corrections."
            except Exception as e:
                logger.warning(f"External search failed: {e}")
        
        prompt = f"""TASK: Perform final validation of the complete LTV table.
RETURN FORMAT: JSON array with one object per row, same order.

VALIDATION RULES:
- Verify each row against ESRB data and external sources (if enabled).
- Check for consistency across countries.
- confidence must be one of: "high", "medium", "low"
  - high: All fields verified and consistent
  - medium: Most fields verified but some uncertainty
  - low: Significant uncertainty or inconsistencies
- Update any fields that can be corrected or filled based on external sources.

FIELDS (all required):
- Country
- confidence ("high"/"medium"/"low")
- Limit_Standard (verify or correct)
- Limit_FTB (verify or correct, or null)
- Limit_BTL (verify or correct, or null)
- Exception_Quota (verify or correct, or null)
- Legal_Form (verify or correct)
- Implementation_Status (verify or correct)
- Notes (verify or correct, or null)
- corrections (array of strings describing any corrections made)
- evidence (short quote supporting the validation)

{search_note}
{search_results_text}

INPUT:
{table_text}"""
        
        try:
            llm = self._get_llm(temperature=0.0)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=prompt)])
            parsed = safe_json_loads_list(res, default=[{} for _ in table_rows])
            if len(parsed) < len(table_rows):
                parsed.extend([{}] * (len(table_rows) - len(parsed)))
            return parsed[:len(table_rows)]
        except Exception as e:
            logger.error(f"Error in validate_ltv_table: {e}")
            return [{} for _ in table_rows]
    
    def get_rag_context(self, query: str, top_k: int = 5) -> str:
        """
        Get RAG context from knowledge graph for a query.
        
        Args:
            query: Search query string
            top_k: Number of top results to return
        
        Returns:
            Formatted context string for LLM prompts
        """
        if not self.rag_retriever:
            return ""
        
        try:
            contexts = self.rag_retriever.retrieve_context(query, top_k=top_k)
            if not contexts:
                return ""
            
            context_text = "\n".join([f"- {ctx.get('text')}" for ctx in contexts])
            return f"\nKnowledge Graph Context:\n{context_text}\n"
        except Exception as e:
            logger.warning(f"RAG context retrieval failed for query '{query}': {e}")
            return ""