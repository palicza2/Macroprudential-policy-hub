-- ============================================
-- Drop Old Snapshot and Trend Tables
-- Migration: 012_drop_old_snapshot_trend_tables.sql
-- ============================================
--
-- Drops the old snapshot and trend tables after migration to Materialized Views
-- WARNING: Only run this after verifying Materialized Views work correctly!

-- ============================================
-- 1. Drop RLS Policies (if they exist)
-- ============================================

-- Drop policies for snapshot tables
DROP POLICY IF EXISTS "Public read access for latest_ccyb_snapshot" ON latest_ccyb_snapshot;
DROP POLICY IF EXISTS "Service role write access for latest_ccyb_snapshot" ON latest_ccyb_snapshot;
DROP POLICY IF EXISTS "Public read access for latest_syrb_snapshot" ON latest_syrb_snapshot;
DROP POLICY IF EXISTS "Service role write access for latest_syrb_snapshot" ON latest_syrb_snapshot;
DROP POLICY IF EXISTS "Public read access for latest_osii_snapshot" ON latest_osii_snapshot;
DROP POLICY IF EXISTS "Service role write access for latest_osii_snapshot" ON latest_osii_snapshot;

-- Drop policies for trend tables
DROP POLICY IF EXISTS "Public read access for ccyb_diffusion_trend" ON ccyb_diffusion_trend;
DROP POLICY IF EXISTS "Service role write access for ccyb_diffusion_trend" ON ccyb_diffusion_trend;
DROP POLICY IF EXISTS "Public read access for syrb_trend" ON syrb_trend;
DROP POLICY IF EXISTS "Service role write access for syrb_trend" ON syrb_trend;
DROP POLICY IF EXISTS "Public read access for bbm_diffusion_trend" ON bbm_diffusion_trend;
DROP POLICY IF EXISTS "Service role write access for bbm_diffusion_trend" ON bbm_diffusion_trend;

-- ============================================
-- 2. Drop Old Tables
-- ============================================

-- Drop snapshot tables
DROP TABLE IF EXISTS latest_ccyb_snapshot CASCADE;
DROP TABLE IF EXISTS latest_syrb_snapshot CASCADE;
DROP TABLE IF EXISTS latest_osii_snapshot CASCADE;

-- Drop trend tables
DROP TABLE IF EXISTS ccyb_diffusion_trend CASCADE;
DROP TABLE IF EXISTS syrb_trend CASCADE;
DROP TABLE IF EXISTS bbm_diffusion_trend CASCADE;

-- ============================================
-- 3. Create RLS Policies for Materialized Views
-- ============================================

-- Note: Materialized Views don't support RLS directly, but we can create policies
-- on the underlying tables. However, for read-only access, we typically don't need
-- RLS on Materialized Views since they're derived from source tables.

-- If you need to restrict access to Materialized Views, you can:
-- 1. Create a function that checks permissions before refreshing
-- 2. Use views on top of Materialized Views with RLS
-- 3. Grant/revoke SELECT permissions directly on the Materialized Views

-- For now, we'll grant public read access to Materialized Views
-- (This is safe since they're read-only and derived from source data)

GRANT SELECT ON mv_latest_ccyb_snapshot TO anon, authenticated;
GRANT SELECT ON mv_latest_syrb_snapshot TO anon, authenticated;
GRANT SELECT ON mv_latest_osii_snapshot TO anon, authenticated;
GRANT SELECT ON mv_ccyb_diffusion_trend TO anon, authenticated;
GRANT SELECT ON mv_syrb_trend TO anon, authenticated;
GRANT SELECT ON mv_bbm_diffusion_trend TO anon, authenticated;
