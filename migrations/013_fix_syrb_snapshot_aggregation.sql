-- ============================================
-- Fix SyRB Snapshot Materialized View Aggregation
-- Migration: 013_fix_syrb_snapshot_aggregation.sql
-- ============================================
--
-- This fixes the mv_latest_syrb_snapshot Materialized View to correctly
-- aggregate SyRB rates by taking the LATEST rate per country (not summing all historical records).
--
-- Logic:
-- - General SyRB: Take the latest (most recent date) General rate per country
-- - Sectoral SyRB: For each exposure_type, take the latest rate, then sum those latest rates
-- - Total Rate: General rate + sum of latest sectoral rates per exposure_type
--
-- This matches the Python logic in country_profiles/data_aggregators.py

-- Drop the old Materialized View
DROP MATERIALIZED VIEW IF EXISTS mv_latest_syrb_snapshot CASCADE;

-- Create the fixed Materialized View with correct aggregation
CREATE MATERIALIZED VIEW mv_latest_syrb_snapshot AS
WITH active_syrb AS (
    -- Filter to only active records with valid rates (0-10%)
    SELECT 
        country_iso2,
        measure_type,
        sector,
        rate,
        effective_date,
        status
    FROM syrb_measures
    WHERE (status ILIKE '%active%' OR status ILIKE '%applicable%')
      AND status NOT ILIKE '%inactive%'
      AND status NOT ILIKE '%revoked%'
      AND status NOT ILIKE '%deactivated%'
      AND status NOT ILIKE '%no longer%'
      AND rate > 0
      AND rate <= 10.0  -- Cap at 10% to avoid data errors
),
latest_general AS (
    -- Get the latest General SyRB rate per country (most recent date)
    SELECT DISTINCT ON (country_iso2)
        country_iso2,
        rate as general_rate
    FROM active_syrb
    WHERE measure_type = 'General'
    ORDER BY country_iso2, effective_date DESC NULLS LAST
),
latest_sectoral_per_sector AS (
    -- Get the latest rate per sector per country
    SELECT DISTINCT ON (country_iso2, sector)
        country_iso2,
        sector,
        rate
    FROM active_syrb
    WHERE measure_type = 'Sectoral'
    ORDER BY country_iso2, sector, effective_date DESC NULLS LAST
),
latest_sectoral AS (
    -- Sum the latest rates per sector for each country
    SELECT 
        country_iso2,
        COALESCE(SUM(rate), 0) as sectoral_rate
    FROM latest_sectoral_per_sector
    GROUP BY country_iso2
)
SELECT 
    COALESCE(g.country_iso2, s.country_iso2) as country_iso2,
    COALESCE(g.general_rate, 0) as general_rate,
    COALESCE(s.sectoral_rate, 0) as sectoral_rate,
    COALESCE(g.general_rate, 0) + COALESCE(s.sectoral_rate, 0) as total_rate,
    NOW() as updated_at
FROM latest_general g
FULL OUTER JOIN latest_sectoral s ON g.country_iso2 = s.country_iso2;

-- Create unique index
CREATE UNIQUE INDEX idx_mv_latest_syrb_country 
    ON mv_latest_syrb_snapshot(country_iso2);

-- Refresh the Materialized View
REFRESH MATERIALIZED VIEW mv_latest_syrb_snapshot;

-- Grant permissions
GRANT SELECT ON mv_latest_syrb_snapshot TO anon, authenticated;
