-- ============================================
-- Fix O-SII Snapshot Materialized View Aggregation
-- Migration: 014_fix_osii_snapshot_aggregation.sql
-- ============================================
--
-- This fixes the mv_latest_osii_snapshot Materialized View to correctly
-- aggregate O-SII/GSII rates by taking the MAX rate per country (not summing all banks).
--
-- Logic:
-- - O-SII/GSII rates should be the MAXIMUM rate per country (not sum of all banks)
-- - This matches the Python logic in capital_overall.py which uses: df.groupby("iso2")["rate_numeric"].max()
-- - Rates should be between 0-5% (cap at 5% to prevent data errors)
--
-- The issue: The old view was using SUM() which incorrectly added up all bank rates,
-- resulting in values > 5% for countries with multiple banks.

-- Drop the old Materialized View
DROP MATERIALIZED VIEW IF EXISTS mv_latest_osii_snapshot CASCADE;

-- Create the fixed Materialized View with correct aggregation
CREATE MATERIALIZED VIEW mv_latest_osii_snapshot AS
SELECT 
    country_iso2,
    COALESCE(MAX(CASE 
        WHEN status = 'Active' 
         AND rate > 0 
         AND rate <= 5.0  -- Cap at 5% to prevent data errors
        THEN rate ELSE 0 
    END), 0) as total_rate,
    COUNT(CASE WHEN status = 'Active' AND buffer_type = 'O-SII' THEN 1 END) as osii_count,
    COUNT(CASE WHEN status = 'Active' AND buffer_type = 'G-SII' THEN 1 END) as gsii_count,
    NOW() as updated_at
FROM osii_banks
WHERE status = 'Active'
GROUP BY country_iso2;

-- Create unique index
CREATE UNIQUE INDEX idx_mv_latest_osii_country 
    ON mv_latest_osii_snapshot(country_iso2);

-- Refresh the Materialized View
REFRESH MATERIALIZED VIEW mv_latest_osii_snapshot;

-- Grant permissions
GRANT SELECT ON mv_latest_osii_snapshot TO anon, authenticated;
