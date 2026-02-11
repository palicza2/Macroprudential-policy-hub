-- ============================================
-- Temporarily disable RLS for migration
-- Migration: 008_temporarily_disable_rls_for_migration.sql
-- ============================================
-- 
-- NOTE: Run this BEFORE migration, then run 009 to re-enable
-- Or use service role key which should bypass RLS

-- Temporarily disable RLS on all tables
ALTER TABLE countries DISABLE ROW LEVEL SECURITY;
ALTER TABLE ccyb_decisions DISABLE ROW LEVEL SECURITY;
ALTER TABLE syrb_measures DISABLE ROW LEVEL SECURITY;
ALTER TABLE bbm_measures DISABLE ROW LEVEL SECURITY;
ALTER TABLE ltv_rules DISABLE ROW LEVEL SECURITY;
ALTER TABLE dti_lti_rules DISABLE ROW LEVEL SECURITY;
ALTER TABLE osii_banks DISABLE ROW LEVEL SECURITY;
ALTER TABLE latest_ccyb_snapshot DISABLE ROW LEVEL SECURITY;
ALTER TABLE latest_syrb_snapshot DISABLE ROW LEVEL SECURITY;
ALTER TABLE latest_osii_snapshot DISABLE ROW LEVEL SECURITY;
ALTER TABLE ccyb_diffusion_trend DISABLE ROW LEVEL SECURITY;
ALTER TABLE syrb_trend DISABLE ROW LEVEL SECURITY;
ALTER TABLE bbm_diffusion_trend DISABLE ROW LEVEL SECURITY;
