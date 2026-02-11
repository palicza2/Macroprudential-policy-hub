-- ============================================
-- Re-enable RLS after migration
-- Migration: 009_re_enable_rls_after_migration.sql
-- ============================================
-- 
-- NOTE: Run this AFTER migration is complete

-- Re-enable RLS on all tables
ALTER TABLE countries ENABLE ROW LEVEL SECURITY;
ALTER TABLE ccyb_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE syrb_measures ENABLE ROW LEVEL SECURITY;
ALTER TABLE bbm_measures ENABLE ROW LEVEL SECURITY;
ALTER TABLE ltv_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE dti_lti_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE osii_banks ENABLE ROW LEVEL SECURITY;
ALTER TABLE latest_ccyb_snapshot ENABLE ROW LEVEL SECURITY;
ALTER TABLE latest_syrb_snapshot ENABLE ROW LEVEL SECURITY;
ALTER TABLE latest_osii_snapshot ENABLE ROW LEVEL SECURITY;
ALTER TABLE ccyb_diffusion_trend ENABLE ROW LEVEL SECURITY;
ALTER TABLE syrb_trend ENABLE ROW LEVEL SECURITY;
ALTER TABLE bbm_diffusion_trend ENABLE ROW LEVEL SECURITY;
