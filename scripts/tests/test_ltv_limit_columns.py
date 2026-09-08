"""Tests for LTV FTB/OOO vs SSB/BTL columns (no LLM)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bbm.ltv_extractor import extract_exception_quota_regex, extract_ltv_limits_regex
from bbm.ltv_model import (
    LABEL_BTL,
    LABEL_FTB,
    LABEL_OOO,
    LABEL_SSB,
    format_labeled_limits,
    migrate_legacy_ltv_columns,
)
from bbm.ltv_renderer import render_ltv_table_html


class LtvLimitColumnTests(unittest.TestCase):
    def test_unlabeled_residential_is_ooo(self):
        primary, secondary, other = extract_ltv_limits_regex(
            "The maximum LTV ratio is 95%."
        )
        self.assertEqual([(p.value, p.label) for p in primary], [(95.0, LABEL_OOO)])
        self.assertEqual(secondary, [])
        self.assertEqual(other, "")

    def test_ftb_ooo_and_btl_split(self):
        text = (
            "First-time buyers (FTB) are subject to a 90% LTV limit. "
            "Buy-to-let (BTL) mortgages are limited to 70% LTV."
        )
        primary, secondary, other = extract_ltv_limits_regex(text)
        self.assertIn((90.0, LABEL_FTB), [(p.value, p.label) for p in primary])
        self.assertIn((70.0, LABEL_BTL), [(p.value, p.label) for p in secondary])
        self.assertEqual(other, "")

    def test_owner_occupied_vs_investment(self):
        text = (
            "The LTV limit is 80% for owner-occupied properties and "
            "70% for investment properties."
        )
        primary, secondary, other = extract_ltv_limits_regex(text)
        self.assertIn((80.0, LABEL_OOO), [(p.value, p.label) for p in primary])
        self.assertIn((70.0, LABEL_BTL), [(p.value, p.label) for p in secondary])

    def test_ssb_not_dumped_as_standard(self):
        text = (
            "The LTV limit is 90% for first-time buyers and 90% for "
            "second and subsequent buyers (SSB). Buy-to-let is 70%."
        )
        primary, secondary, _ = extract_ltv_limits_regex(text)
        self.assertIn((90.0, LABEL_FTB), [(p.value, p.label) for p in primary])
        labels_sec = {(p.value, p.label) for p in secondary}
        self.assertIn((90.0, LABEL_SSB), labels_sec)
        self.assertIn((70.0, LABEL_BTL), labels_sec)

    def test_quota_not_treated_as_ltv(self):
        text = (
            "The LTV limit is 90% for owner-occupied housing. "
            "Up to 15% of new lending can take place above the limits."
        )
        primary, secondary, other = extract_ltv_limits_regex(text)
        values = [p.value for p in primary + secondary]
        self.assertIn(90.0, values)
        self.assertNotIn(15.0, values)
        quota = extract_exception_quota_regex(text)
        self.assertIsNotNone(quota)
        self.assertIn("15", quota)

    def test_green_goes_to_other(self):
        text = (
            "The LTV limit is 80% for owner-occupied properties. "
            "A 90% LTV applies to energy-efficient (green) mortgages."
        )
        primary, secondary, other = extract_ltv_limits_regex(text)
        self.assertIn((80.0, LABEL_OOO), [(p.value, p.label) for p in primary])
        self.assertEqual(secondary, [])
        self.assertIn("90%", other)
        self.assertIn("green", other.lower())

    def test_migrate_drops_percentage_soup(self):
        df = pd.DataFrame([{
            "Country": "BE",
            "Implementation_Status": "Active",
            "Legal_Form": "Binding",
            "Limit_Standard": "0.0%, 5.0%, 10.0%, 50.0%, 90.0%, 100.0%",
            "Limit_FTB": 35.0,
            "Limit_BTL": 80.0,
            "Exception_Quota": "",
            "Notes": "",
        }])
        out = migrate_legacy_ltv_columns(df)
        row = out.iloc[0]
        self.assertEqual(row["Limit_FTB_OOO"], "")  # 35% is not a plausible LTV
        self.assertEqual(row["Limit_SSB_BTL"], "80% (BTL)")
        self.assertNotIn("0.0%", str(row["Limit_FTB_OOO"]))

    def test_migrate_single_standard_becomes_ooo(self):
        df = pd.DataFrame([{
            "Country": "DK",
            "Status": "Active",
            "Legal Form": "Binding",
            "Standard Limit": "95.0%",
            "FTB Limit": "—",
            "BTL Limit": "—",
            "Exception Quota": "—",
            "Notes": "95% is the maximum LTV",
        }])
        out = migrate_legacy_ltv_columns(df)
        self.assertEqual(out.iloc[0]["Limit_FTB_OOO"], "95% (OOO)")
        self.assertEqual(out.iloc[0]["Limit_SSB_BTL"], "")

    def test_renderer_headers(self):
        df = pd.DataFrame([{
            "Country": "IE",
            "Implementation_Status": "Active",
            "Legal_Form": "Binding",
            "Limit_FTB_OOO": "90% (FTB)",
            "Limit_SSB_BTL": "90% (SSB); 70% (BTL)",
            "Other_Limits": "",
            "Exception_Quota": "15% of FTB lending",
            "Notes": "",
        }])
        html = render_ltv_table_html(df)
        self.assertIn("FTB / OOO", html)
        self.assertIn("SSB / BTL", html)
        self.assertIn("Other limits", html)
        self.assertNotIn("Standard Limit", html)
        self.assertIn("90% (FTB)", html)

    def test_format_labeled(self):
        self.assertEqual(
            format_labeled_limits([]),
            "",
        )


if __name__ == "__main__":
    unittest.main()
