from __future__ import annotations

import logging
from typing import Dict

from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser

from llm_tasks import LLMTask
from llm.cache import LLMCache
from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

# Initialize cache instance
_cache = LLMCache()


def run_tasks(
    *,
    analyzer,
    tasks,
    plot_paths: Dict[str, object],
    default_retries: int = 2,
    sleep_s: float = 1.0,
) -> Dict[str, str]:
    results: Dict[str, str] = {}
    model_name = analyzer.config.get("model_name", "")
    
    for t in tasks:
        logger.info(f"  🧠 Elemzés: {t.id}...")
        
        # Build prompt with data
        full_prompt = t.prompt + (f"\nDATA:\n{t.data}" if t.data else "")
        img_key = t.img or ""
        
        # Check cache first
        cached_response = _cache.get(
            prompt=t.prompt,
            data=t.data,
            img_key=img_key,
            model=model_name,
            temperature=t.temp
        )
        
        if cached_response:
            logger.debug(f"  ✅ Cache hit for {t.id}")
            results[t.id] = analyzer._clean_text(cached_response, is_global=t.clean_global)
            continue
        
        # Cache miss - make API call with exponential backoff retry
        @retry_with_backoff(
            max_retries=default_retries + 1,  # +1 because first attempt is not a retry
            initial_delay=sleep_s,
            max_delay=60.0,
            exceptions=(Exception,)
        )
        def _call_llm():
            img_path = plot_paths.get(t.img) if t.img else None
            img_b64 = None
            if img_path and hasattr(img_path, "exists") and img_path.exists():
                try:
                    img_b64 = img_path.read_bytes()
                    import base64
                    img_b64 = base64.b64encode(img_b64).decode("utf-8")
                except Exception:
                    img_b64 = None

            content = [{"type": "text", "text": full_prompt}]
            if img_b64:
                content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}})

            llm = analyzer._get_llm(temperature=t.temp)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=content)])
            
            # Cache the response
            _cache.set(
                prompt=t.prompt,
                response=res,
                data=t.data,
                img_key=img_key,
                model=model_name,
                temperature=t.temp
            )
            
            return analyzer._clean_text(res, is_global=t.clean_global)
        
        try:
            results[t.id] = _call_llm()
        except Exception as exc:
            logger.warning(f"LLM task failed ({t.id}) after retries: {exc}")
            results[t.id] = "N/A"
    return results


def run_task(*, analyzer, task: LLMTask, default_retries: int = 2, sleep_s: float = 1.0) -> str:
    model_name = analyzer.config.get("model_name", "")
    
    # Check cache first
    cached_response = _cache.get(
        prompt=task.prompt,
        data=task.data,
        img_key=task.img or "",
        model=model_name,
        temperature=task.temp
    )
    
    if cached_response:
        logger.debug(f"  ✅ Cache hit for {task.id}")
        return analyzer._clean_text(cached_response, is_global=task.clean_global)
    
    # Cache miss - make API call with exponential backoff retry
    @retry_with_backoff(
        max_retries=default_retries + 1,  # +1 because first attempt is not a retry
        initial_delay=sleep_s,
        max_delay=60.0,
        exceptions=(Exception,)
    )
    def _call_llm():
        llm = analyzer._get_llm(temperature=task.temp)
        res = (llm | StrOutputParser()).invoke([HumanMessage(content=task.prompt)])
        
        # Cache the response
        _cache.set(
            prompt=task.prompt,
            response=res,
            data=task.data,
            img_key=task.img or "",
            model=model_name,
            temperature=task.temp
        )
        
        return analyzer._clean_text(res, is_global=task.clean_global)
    
    try:
        return _call_llm()
    except Exception as exc:
        logger.warning(f"LLM task failed ({task.id}) after retries: {exc}")
        return "N/A"

