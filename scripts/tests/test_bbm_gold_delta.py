"""Unit tests for BBM gold + delta classification (no LLM, no ESRB download)."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bbm.delta_checker import check_gold_deltas
from bbm.gold import (
    FAMILY_DTI,
    GoldPaths,
    KIND_CHANGED,
    KIND_NEW,
    KIND_UNCHANGED,
    KIND_WITHDRAWN,
    apply_state_updates,
    classify_family,
    collapse_items,
    description_hash,
    dti_gold_index,
    gold_fingerprint,
    load_state,
    normalize_iso2,
    save_state,
    should_skip_new_extract,
    state_key,
)


def _dti_gold_row(country="IE", measure="LTI", limit="3.5x"):
    return {
        "Country": country,
        "Type": measure,
        "Standard Limit": limit,
        "Legal Form": "Binding",
        "Income Basis": "Gross",
    }


class GoldDeltaTests(unittest.TestCase):
    def test_hash_ignores_whitespace(self):
        self.assertEqual(
            description_hash("  3.5 times   income\n"),
            description_hash("3.5 times income"),
        )

    def test_gb_maps_to_uk(self):
        self.assertEqual(normalize_iso2("GB"), "UK")
        self.assertEqual(state_key(FAMILY_DTI, "GB", "LTI"), "dti:UK:LTI")

    def test_collapse_joins_duplicate_keys(self):
        items = [
            {"iso2": "IE", "measure_short": "LTI", "description": "short"},
            {"iso2": "IE", "measure_short": "LTI", "description": "a much longer LTI description"},
        ]
        collapsed = collapse_items(items, FAMILY_DTI)
        self.assertIn("dti:UK:LTI".replace("UK", "IE"), collapsed)
        self.assertIn("longer", collapsed["dti:IE:LTI"]["description"])

    def test_first_run_trusts_gold(self):
        gold = dti_gold_index(
            __import__("pandas").DataFrame([_dti_gold_row()])
        )
        items = [{
            "iso2": "IE",
            "measure_short": "LTI",
            "description": "LTI limit of 3.5 times income",
        }]
        result = classify_family(FAMILY_DTI, items, gold, {"entries": {}})
        kinds = {r.kind for r in result}
        self.assertEqual(kinds, {KIND_UNCHANGED})
        self.assertEqual(result[0].current_hash, description_hash(items[0]["description"]))

    def test_unchanged_description_skips(self):
        desc = "LTI limit of 3.5 times income"
        gold_row = _dti_gold_row()
        key = "dti:IE:LTI"
        state = {
            "entries": {
                key: {
                    "approved_hash": description_hash(desc),
                    "gold_fingerprint": gold_fingerprint(gold_row),
                    "status": "ok",
                }
            }
        }
        result = classify_family(
            FAMILY_DTI,
            [{"iso2": "IE", "measure_short": "LTI", "description": desc}],
            {key: gold_row},
            state,
        )
        self.assertEqual(result[0].kind, KIND_UNCHANGED)
        self.assertEqual(result[0].reason, "ESRB description unchanged")

    def test_changed_description_flags(self):
        gold_row = _dti_gold_row()
        key = "dti:IE:LTI"
        state = {
            "entries": {
                key: {
                    "approved_hash": description_hash("old text"),
                    "gold_fingerprint": gold_fingerprint(gold_row),
                    "status": "ok",
                }
            }
        }
        result = classify_family(
            FAMILY_DTI,
            [{"iso2": "IE", "measure_short": "LTI", "description": "new limit of 4.0 times income"}],
            {key: gold_row},
            state,
        )
        self.assertEqual(result[0].kind, KIND_CHANGED)

    def test_gold_edit_accepts_current_text(self):
        old_row = _dti_gold_row(limit="3.5x")
        new_row = _dti_gold_row(limit="4.0x")
        key = "dti:IE:LTI"
        desc = "same esrb text"
        state = {
            "entries": {
                key: {
                    "approved_hash": description_hash(desc),
                    "gold_fingerprint": gold_fingerprint(old_row),
                    "status": "ok",
                }
            }
        }
        result = classify_family(
            FAMILY_DTI,
            [{"iso2": "IE", "measure_short": "LTI", "description": desc}],
            {key: new_row},
            state,
        )
        self.assertEqual(result[0].kind, KIND_UNCHANGED)
        self.assertIn("edited", result[0].reason.lower())

    def test_new_and_withdrawn(self):
        gold_row = _dti_gold_row(country="NO", measure="DTI")
        gold = {"dti:NO:DTI": gold_row}
        items = [{"iso2": "PT", "measure_short": "DTI", "description": "DTI 8 times"}]
        result = classify_family(FAMILY_DTI, items, gold, {"entries": {}})
        kinds = {r.country: r.kind for r in result}
        self.assertEqual(kinds["PT"], KIND_NEW)
        self.assertEqual(kinds["NO"], KIND_WITHDRAWN)

    def test_match_advances_hash_conflict_does_not(self):
        gold_row = _dti_gold_row()
        key = "dti:IE:LTI"
        old_hash = description_hash("old")
        new_hash = description_hash("new")
        item_changed = classify_family(
            FAMILY_DTI,
            [{"iso2": "IE", "measure_short": "LTI", "description": "new"}],
            {key: gold_row},
            {"entries": {key: {
                "approved_hash": old_hash,
                "gold_fingerprint": gold_fingerprint(gold_row),
            }}},
        )[0]
        self.assertEqual(item_changed.kind, KIND_CHANGED)

        after_match = apply_state_updates(
            {"entries": {key: {"approved_hash": old_hash}}},
            [item_changed],
            {key: {"verdict": "match"}},
        )
        self.assertEqual(after_match["entries"][key]["approved_hash"], new_hash)
        self.assertEqual(after_match["entries"][key]["status"], "ok")

        after_conflict = apply_state_updates(
            {"entries": {key: {"approved_hash": old_hash}}},
            [item_changed],
            {key: {"verdict": "conflict"}},
        )
        self.assertEqual(after_conflict["entries"][key]["approved_hash"], old_hash)
        self.assertEqual(after_conflict["entries"][key]["status"], "conflict")

    def test_skip_reextract_when_proposal_hash_matches(self):
        gold_row = _dti_gold_row(country="PT", measure="DTI")
        items = [{"iso2": "PT", "measure_short": "DTI", "description": "DTI 8x"}]
        classified = classify_family(FAMILY_DTI, items, {}, {"entries": {}})
        self.assertEqual(classified[0].kind, KIND_NEW)
        state = apply_state_updates({"entries": {}}, classified, {})
        self.assertTrue(should_skip_new_extract(state, classified[0]))

    def test_state_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bbm_gold_state.json"
            save_state(path, {"entries": {"dti:IE:LTI": {"approved_hash": "abc", "status": "ok"}}})
            loaded = load_state(path)
            self.assertEqual(loaded["entries"]["dti:IE:LTI"]["approved_hash"], "abc")
            self.assertTrue(path.exists())
            json.loads(path.read_text(encoding="utf-8"))

    def test_delta_without_analyzer_is_unclear(self):
        gold_row = _dti_gold_row()
        key = "dti:IE:LTI"
        item = classify_family(
            FAMILY_DTI,
            [{"iso2": "IE", "measure_short": "LTI", "description": "new text"}],
            {key: gold_row},
            {"entries": {key: {
                "approved_hash": description_hash("old"),
                "gold_fingerprint": gold_fingerprint(gold_row),
            }}},
        )[0]
        verdicts = check_gold_deltas([item], analyzer=None)
        self.assertEqual(verdicts[key]["verdict"], "unclear")

    def test_gold_paths(self):
        paths = GoldPaths(Path("/tmp/bbm-gold"))
        self.assertEqual(paths.dti_csv.name, "dti_expert_table.csv")
        self.assertEqual(paths.ltv_csv.name, "ltv_gold.csv")

    def test_stage_reads_gold_without_llm(self):
        import pandas as pd
        from pipeline.stages.bbm_stage import BBMStage

        class _NoSb:
            def is_enabled(self):
                return False

        with tempfile.TemporaryDirectory() as tmp:
            paths = GoldPaths(Path(tmp))
            pd.DataFrame([_dti_gold_row()]).to_csv(paths.dti_csv, index=False)
            bbm = pd.DataFrame([{
                "active_status": "Active",
                "status": "Active",
                "measure_type": "Loan-to-income (LTI)",
                "iso2": "IE",
                "country": "Ireland",
                "description": "The LTI limit is 3.5 times gross income.",
                "date": pd.Timestamp("2024-01-15"),
            }])
            out = BBMStage(analyzer=None, supabase_writer=_NoSb(), paths=paths).process(bbm)
            self.assertFalse(out["dti_expert_table"].empty)
            self.assertEqual(out["bbm_delta_report"]["dti"]["unchanged"], 1)
            self.assertEqual(out["bbm_delta_report"]["dti"]["changed"], 0)
            self.assertTrue(paths.state_json.exists())
            self.assertTrue(paths.report_json.exists())


if __name__ == "__main__":
    unittest.main()
