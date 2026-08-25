"""
Pipeline manifest: content hashes that decide which stages can be skipped.

Bronze (Excel) + parser code fingerprint → silver skip
Silver + viz code → plot skip
Silver + figures + llm code → Gemini skip
Silver → Supabase upsert skip
News uses a separate TTL, not the Excel hash.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger(__name__)

MANIFEST_VERSION = 1

PARSER_RELATIVE = (
    "etl.py",
    "capital_overall.py",
    "reciprocation.py",
    "utils/dataframe.py",
)

VIZ_RELATIVE = (
    "visualizer.py",
)

LLM_RELATIVE = (
    "llm_analysis.py",
    "prompts.py",
    "llm_tasks.py",
    "ccyb.py",
    "syrb.py",
    "country_profiles/profile_generator.py",
    "country_profiles/data_aggregators.py",
)

PLOT_KEYS = (
    "ccyb_diffusion",
    "cross_section_map",
    "cross_section_bar",
    "risk_plot",
    "syrb_counts_trend",
    "syrb_sector",
    "bbm_diffusion",
    "capital_overall_buffers",
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def sha256_file(path: Path) -> Optional[str]:
    if not path or not Path(path).exists() or not Path(path).is_file():
        return None
    return sha256_bytes(Path(path).read_bytes())


def sha256_files(base_dir: Path, relative: Iterable[str]) -> str:
    """Fingerprint a set of source files; missing files contribute a placeholder."""
    parts = []
    for rel in relative:
        path = base_dir / rel
        digest = sha256_file(path) or "missing"
        parts.append(f"{rel}:{digest}")
    return sha256_bytes("\n".join(parts).encode("utf-8"))


def sha256_paths(paths: Iterable[Path]) -> str:
    parts = []
    for path in sorted(Path(p) for p in paths):
        digest = sha256_file(path) or "missing"
        parts.append(f"{path.name}:{digest}")
    return sha256_bytes("\n".join(parts).encode("utf-8"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read %s: %s", path, exc)
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


@dataclass
class SkipPlan:
    etl: bool = False
    ccyb: bool = False
    measures: bool = False
    capital: bool = False
    viz: bool = False
    llm: bool = False
    supabase: bool = False
    news: bool = False
    reasons: Dict[str, str] = field(default_factory=dict)

    def log(self) -> None:
        logger.info(
            "Manifest skip: etl=%s (ccyb=%s measures=%s capital=%s) viz=%s llm=%s supabase=%s news=%s",
            self.etl, self.ccyb, self.measures, self.capital,
            self.viz, self.llm, self.supabase, self.news,
        )
        for key, reason in self.reasons.items():
            logger.info("   -> %s: %s", key, reason)


class PipelineManifest:
    """Read/write data/pipeline_manifest.json and compute skip flags."""

    def __init__(self, data_dir: Path, base_dir: Path, reports_dir: Path, figures_dir: Path):
        self.data_dir = Path(data_dir)
        self.base_dir = Path(base_dir)
        self.reports_dir = Path(reports_dir)
        self.figures_dir = Path(figures_dir)
        self.path = self.data_dir / "pipeline_manifest.json"
        self.analyses_path = self.data_dir / "analyses_cache.json"
        self.countries_path = self.data_dir / "countries_data.json"
        self.plots_inline_path = self.data_dir / "plots_inline_cache.json"
        self.news_path = self.data_dir / "news_cache.parquet"
        self.ccyb_decisions_path = self.data_dir / "gold_ccyb_decisions.parquet"
        self.syrb_active_path = self.data_dir / "gold_syrb_active.parquet"
        self.syrb_decisions_path = self.data_dir / "gold_syrb_decisions.parquet"

    def load(self) -> Dict[str, Any]:
        data = load_json(self.path, default={}) or {}
        if not isinstance(data, dict):
            return {"version": MANIFEST_VERSION}
        data.setdefault("version", MANIFEST_VERSION)
        data.setdefault("bronze", {})
        data.setdefault("skipped", {})
        return data

    def bronze_hashes(self) -> Dict[str, Optional[str]]:
        from config import FILES
        return {
            "ccyb": sha256_file(FILES["ccyb_source"]),
            "measures": sha256_file(FILES["syrb_source"]),
            "capital": sha256_file(FILES["capital_measures_source"]),
        }

    def parser_fingerprint(self) -> str:
        return sha256_files(self.base_dir, PARSER_RELATIVE)

    def viz_fingerprint(self) -> str:
        return sha256_files(self.base_dir, VIZ_RELATIVE)

    def llm_fingerprint(self) -> str:
        return sha256_files(self.base_dir, LLM_RELATIVE)

    def silver_fingerprint(self) -> str:
        from config import FILES
        return sha256_paths([
            FILES["ccyb_processed"],
            FILES["syrb_processed"],
            FILES["bbm_processed"],
            FILES["osii_processed"],
            FILES["latest_ccyb"],
            FILES["latest_syrb"],
            FILES["latest_bbm"],
            FILES["latest_osii"],
        ])

    def figures_fingerprint(self) -> str:
        if not self.figures_dir.exists():
            return "missing"
        pngs = sorted(self.figures_dir.glob("*.png"))
        if not pngs:
            return "missing"
        return sha256_paths(pngs)

    def plots_exist(self) -> bool:
        plots_dir = self.reports_dir / "plots"
        return any((plots_dir / f"{key}.html").exists() for key in PLOT_KEYS)

    def llm_cache_exists(self) -> bool:
        return self.analyses_path.exists() and self.countries_path.exists()

    def silver_ready(self, skip_ccyb: bool, skip_measures: bool, skip_capital: bool) -> bool:
        from config import FILES
        needed = []
        if skip_ccyb:
            needed.append(FILES["ccyb_processed"])
        if skip_measures:
            needed.extend([FILES["syrb_processed"], FILES["bbm_processed"]])
        if skip_capital:
            needed.append(FILES["osii_processed"])
        return all(p.exists() for p in needed) if needed else True

    def plan(self, force: bool = False, news_ttl_days: int = 7) -> SkipPlan:
        plan = SkipPlan()
        if force:
            plan.reasons["force"] = "FORCE_REBUILD set"
            return plan

        prev = self.load()
        bronze = self.bronze_hashes()
        parser = self.parser_fingerprint()
        prev_bronze = prev.get("bronze") or {}
        parser_ok = parser == prev.get("parser") and bool(prev.get("parser"))

        def bronze_ok(key: str) -> bool:
            return bool(bronze.get(key)) and bronze.get(key) == prev_bronze.get(key)

        plan.ccyb = bronze_ok("ccyb") and parser_ok
        plan.measures = bronze_ok("measures") and parser_ok
        plan.capital = bronze_ok("capital") and parser_ok
        if not parser_ok:
            plan.reasons["parser"] = "parser fingerprint changed or first run"
        if not bronze_ok("ccyb"):
            plan.reasons["ccyb"] = "CCyB Excel changed or missing"
        if not bronze_ok("measures"):
            plan.reasons["measures"] = "measures Excel changed or missing"
        if not bronze_ok("capital"):
            plan.reasons["capital"] = "capital Excel changed or missing"

        if not self.silver_ready(plan.ccyb, plan.measures, plan.capital):
            plan.reasons["silver"] = "required parquet missing; will parse"
            plan.ccyb = False
            plan.measures = False
            plan.capital = False

        plan.etl = plan.ccyb and plan.measures and plan.capital
        if plan.etl:
            plan.reasons["etl"] = "bronze + parser unchanged; load parquet"

        # Viz / LLM / Supabase depend on the silver that will exist after DataStage.
        # If any parse will run, silver will change (or first run) → do not skip those yet.
        # Final viz/llm/supabase flags are refined after DataStage via refine_after_silver().
        return plan

    def refine_after_silver(self, plan: SkipPlan, force: bool = False, news_ttl_days: int = 7) -> SkipPlan:
        if force:
            return plan
        prev = self.load()
        silver = self.silver_fingerprint()
        viz_code = self.viz_fingerprint()
        llm_code = self.llm_fingerprint()
        figures = self.figures_fingerprint()

        plan.viz = (
            silver == prev.get("silver")
            and viz_code == prev.get("viz_code")
            and self.plots_exist()
            and bool(prev.get("silver"))
        )
        if plan.viz:
            plan.reasons["viz"] = "silver + visualizer.py unchanged; reuse plots"
        else:
            plan.reasons["viz"] = "silver, viz code, or plot files changed"

        plan.llm = (
            silver == prev.get("silver")
            and llm_code == prev.get("llm_code")
            and figures == prev.get("figures")
            and self.llm_cache_exists()
            and bool(prev.get("silver"))
        )
        if plan.llm:
            plan.reasons["llm"] = "silver + figures + LLM code unchanged; reuse analyses"
        else:
            plan.reasons["llm"] = "silver, figures, LLM code, or analyses cache missing"

        plan.supabase = bool(prev.get("silver")) and silver == prev.get("supabase_silver")
        if plan.supabase:
            plan.reasons["supabase"] = "silver unchanged since last upsert"
        else:
            plan.reasons["supabase"] = "silver changed or no prior upsert"

        plan.news = self._news_fresh(prev, news_ttl_days)
        if plan.news:
            plan.reasons["news"] = f"news cache younger than {news_ttl_days} days"
        else:
            plan.reasons["news"] = "news TTL expired or no cache"
        return plan

    def _news_fresh(self, prev: Dict[str, Any], ttl_days: int) -> bool:
        if not self.news_path.exists():
            return False
        stamp = prev.get("news_fetched_at")
        if not stamp:
            return False
        try:
            fetched = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        except ValueError:
            return False
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - fetched
        return age.total_seconds() < ttl_days * 86400

    def write(
        self,
        *,
        plan: SkipPlan,
        news_fetched: bool,
        supabase_wrote: bool,
    ) -> Dict[str, Any]:
        prev = self.load()
        silver = self.silver_fingerprint()
        if supabase_wrote:
            supabase_silver = silver
        else:
            supabase_silver = prev.get("supabase_silver")
        out = {
            "version": MANIFEST_VERSION,
            "updated_at": now_iso(),
            "bronze": self.bronze_hashes(),
            "parser": self.parser_fingerprint(),
            "silver": silver,
            "viz_code": self.viz_fingerprint(),
            "llm_code": self.llm_fingerprint(),
            "figures": self.figures_fingerprint(),
            "supabase_silver": supabase_silver,
            "news_fetched_at": now_iso() if news_fetched else prev.get("news_fetched_at"),
            "skipped": {
                "etl": plan.etl,
                "viz": plan.viz,
                "llm": plan.llm,
                "supabase": plan.supabase,
                "news": plan.news,
            },
        }
        save_json(self.path, out)
        logger.info("Wrote pipeline manifest %s", self.path)
        return out

    def save_llm_cache(
        self,
        analyses: Dict[str, Any],
        countries_data: Dict[str, Any],
        ccyb_decisions,
        active_syrb,
        syrb_decisions,
    ) -> None:
        save_json(self.analyses_path, analyses or {})
        save_json(self.countries_path, countries_data or {})
        self._write_df(ccyb_decisions, self.ccyb_decisions_path)
        self._write_df(active_syrb, self.syrb_active_path)
        self._write_df(syrb_decisions, self.syrb_decisions_path)

    def load_llm_cache(self) -> Dict[str, Any]:
        return {
            "analyses": load_json(self.analyses_path, default={}) or {},
            "countries_data": load_json(self.countries_path, default={}) or {},
            "ccyb_decisions": self._read_df(self.ccyb_decisions_path),
            "active_syrb": self._read_df(self.syrb_active_path),
            "syrb_decisions": self._read_df(self.syrb_decisions_path),
        }

    @staticmethod
    def _write_df(df, path: Path) -> None:
        if df is None:
            return
        try:
            import pandas as pd
            if not isinstance(df, pd.DataFrame) or df.empty:
                return
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path)
        except Exception as exc:
            logger.warning("Could not write %s: %s", path, exc)

    @staticmethod
    def _read_df(path: Path):
        import pandas as pd
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            logger.warning("Could not read %s: %s", path, exc)
            return pd.DataFrame()
