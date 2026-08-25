"""Tests for pipeline manifest skip logic and hash-before-replace download."""
from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.manifest import PipelineManifest, sha256_file, sha256_files
from utils.download import replace_if_changed


class HashBeforeReplaceTests(unittest.TestCase):
    def test_identical_bytes_not_replaced(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "a.xlsx"
            temp = Path(tmp) / "a.xlsx.tmp"
            target.write_bytes(b"hello-excel-content")
            temp.write_bytes(b"hello-excel-content")
            mtime = target.stat().st_mtime
            changed = replace_if_changed(temp, target)
            self.assertFalse(changed)
            self.assertFalse(temp.exists())
            self.assertEqual(target.read_bytes(), b"hello-excel-content")
            self.assertEqual(target.stat().st_mtime, mtime)

    def test_changed_bytes_replaced(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "a.xlsx"
            temp = Path(tmp) / "a.xlsx.tmp"
            target.write_bytes(b"old-content-here")
            temp.write_bytes(b"new-content-here")
            self.assertTrue(replace_if_changed(temp, target))
            self.assertEqual(target.read_bytes(), b"new-content-here")


class ManifestFingerprintTests(unittest.TestCase):
    def test_sha256_file_stable(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "f.txt"
            p.write_text("abc", encoding="utf-8")
            self.assertEqual(sha256_file(p), sha256_file(p))
            self.assertIsNone(sha256_file(Path(tmp) / "missing"))

    def test_parser_fingerprint_changes_with_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "etl.py").write_text("v1", encoding="utf-8")
            (base / "capital_overall.py").write_text("x", encoding="utf-8")
            a = sha256_files(base, ["etl.py", "capital_overall.py"])
            (base / "etl.py").write_text("v2", encoding="utf-8")
            b = sha256_files(base, ["etl.py", "capital_overall.py"])
            self.assertNotEqual(a, b)

    def test_news_ttl(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = PipelineManifest(root, root, root, root)
            m.news_path.write_bytes(b"cache")
            now = datetime.now(timezone.utc)
            self.assertTrue(m._news_fresh({"news_fetched_at": now.isoformat()}, 7))
            old = (now - timedelta(days=8)).isoformat()
            self.assertFalse(m._news_fresh({"news_fetched_at": old}, 7))
            self.assertFalse(m._news_fresh({}, 7))

    def test_force_plan_skips_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = PipelineManifest(root, root, root, root)
            plan = m.plan(force=True)
            self.assertFalse(plan.etl)
            self.assertFalse(plan.viz)
            self.assertFalse(plan.llm)
            self.assertIn("force", plan.reasons)


if __name__ == "__main__":
    unittest.main()
