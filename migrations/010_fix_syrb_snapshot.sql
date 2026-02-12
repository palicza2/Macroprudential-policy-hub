-- ============================================
-- Fix SyRB Snapshot Materialized View
-- Migration: 010_fix_syrb_snapshot.sql
-- ============================================
--
-- This fixes the mv_latest_syrb_snapshot Materialized View
-- to use the same logic as the Python code (case-insensitive matching)

-- Drop the old Materialized View
DROP MATERIALIZED VIEW IF EXISTS mv_latest_syrb_snapshot CASCADE;

-- Create the fixed Materialized View
CREATE MATERIALIZED VIEW mv_latest_syrb_snapshot AS
SELECT 
    country_iso2,
    COALESCE(SUM(CASE 
        WHEN (status ILIKE '%active%' OR status ILIKE '%applicable%')
         AND status NOT ILIKE '%inactive%'
         AND status NOT ILIKE '%revoked%'
         AND status NOT ILIKE '%deactivated%'
         AND status NOT ILIKE '%no longer%'
        THEN rate ELSE 0 
    END), 0) as total_rate,
    COALESCE(SUM(CASE 
        WHEN (status ILIKE '%active%' OR status ILIKE '%applicable%')
         AND status NOT ILIKE '%inactive%'
         AND status NOT ILIKE '%revoked%'
         AND status NOT ILIKE '%deactivated%'
         AND status NOT ILIKE '%no longer%'
         AND measure_type = 'General' 
        THEN rate ELSE 0 
    END), 0) as general_rate,
    COALESCE(SUM(CASE 
        WHEN (status ILIKE '%active%' OR status ILIKE '%applicable%')
         AND status NOT ILIKE '%inactive%'
         AND status NOT ILIKE '%revoked%'
         AND status NOT ILIKE '%deactivated%'
         AND status NOT ILIKE '%no longer%'
         AND measure_type = 'Sectoral' 
        THEN rate ELSE 0 
    END), 0) as sectoral_rate,
    NOW() as updated_at
FROM syrb_measures
WHERE (status ILIKE '%active%' OR status ILIKE '%applicable%')
  AND status NOT ILIKE '%inactive%'
  AND status NOT ILIKE '%revoked%'
  AND status NOT ILIKE '%deactivated%'
  AND status NOT ILIKE '%no longer%'
GROUP BY country_iso2;

-- Create unique index
CREATE UNIQUE INDEX idx_mv_latest_syrb_country 
    ON mv_latest_syrb_snapshot(country_iso2);

-- Refresh the Materialized View
REFRESH MATERIALIZED VIEW mv_latest_syrb_snapshot;
