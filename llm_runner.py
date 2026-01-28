from __future__ import annotations

import logging
import time
from typing import Dict

from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser

from llm_tasks import LLMTask

logger = logging.getLogger(__name__)


def run_tasks(
    *,
    analyzer,
    tasks,
    plot_paths: Dict[str, object],
    default_retries: int = 2,
    sleep_s: float = 1.0,
) -> Dict[str, str]:
    results: Dict[str, str] = {}
    for t in tasks:
        logger.info(f"  🧠 Elemzés: {t.id}...")
        attempt = 0
        while True:
            try:
                img_path = plot_paths.get(t.img) if t.img else None
                img_b64 = None
                if img_path and hasattr(img_path, "exists") and img_path.exists():
                    try:
                        img_b64 = img_path.read_bytes()
                        import base64
                        img_b64 = base64.b64encode(img_b64).decode("utf-8")
                    except Exception:
                        img_b64 = None

                content = [{"type": "text", "text": t.prompt + (f"\nDATA:\n{t.data}" if t.data else "")}]
                if img_b64:
                    content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}})

                llm = analyzer._get_llm(temperature=t.temp)
                res = (llm | StrOutputParser()).invoke([HumanMessage(content=content)])
                results[t.id] = analyzer._clean_text(res, is_global=t.clean_global)
                break
            except Exception as exc:
                attempt += 1
                if attempt > default_retries:
                    logger.warning(f"LLM task failed ({t.id}): {exc}")
                    results[t.id] = "N/A"
                    break
                time.sleep(sleep_s)
    return results


def run_task(*, analyzer, task: LLMTask, default_retries: int = 2, sleep_s: float = 1.0) -> str:
    attempt = 0
    while True:
        try:
            llm = analyzer._get_llm(temperature=task.temp)
            res = (llm | StrOutputParser()).invoke([HumanMessage(content=task.prompt)])
            return analyzer._clean_text(res, is_global=task.clean_global)
        except Exception as exc:
            attempt += 1
            if attempt > default_retries:
                logger.warning(f"LLM task failed ({task.id}): {exc}")
                return "N/A"
            time.sleep(sleep_s)

