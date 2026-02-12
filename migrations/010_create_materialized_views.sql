-- ============================================
-- Materialized Views Migration
-- Migration: 010_create_materialized_views.sql
-- ============================================
-- 
-- This migration converts snapshot and trend tables to Materialized Views
-- to reduce redundant data storage by 40-70%
--
-- Snapshot tables → Materialized Views:
--   - latest_ccyb_snapshot → mv_latest_ccyb_snapshot
--   - latest_syrb_snapshot → mv_latest_syrb_snapshot
--   - latest_osii_snapshot → mv_latest_osii_snapshot
--
-- Trend tables → Materialized Views:
--   - ccyb_diffusion_trend → mv_ccyb_diffusion_trend
--   - syrb_trend → mv_syrb_trend
--   - bbm_diffusion_trend → mv_bbm_diffusion_trend

-- ============================================
-- 1. Create Materialized Views for Snapshots
-- ============================================

-- Latest CCyB Snapshot Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_ccyb_snapshot AS
SELECT DISTINCT ON (country_iso2)
    country_iso2,
    rate,
    effective_date,
    credit_gap,
    credit_to_gdp,
    NOW() as updated_at
FROM ccyb_decisions
WHERE rate IS NOT NULL
ORDER BY country_iso2, effective_date DESC, decision_date DESC NULLS LAST;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_latest_ccyb_country 
    ON mv_latest_ccyb_snapshot(country_iso2);

-- Latest SyRB Snapshot Materialized View
-- Note: Uses ILIKE to match status values like "Active", "Active (applicable)", etc.
-- Excludes "Deactivated", "Revoked", "No longer" statuses
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_syrb_snapshot AS
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_latest_syrb_country 
    ON mv_latest_syrb_snapshot(country_iso2);

-- Latest OSII Snapshot Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_osii_snapshot AS
SELECT 
    country_iso2,
    COALESCE(SUM(CASE WHEN status = 'Active' THEN rate ELSE 0 END), 0) as total_rate,
    COUNT(CASE WHEN status = 'Active' AND buffer_type = 'OSII' THEN 1 END) as osii_count,
    COUNT(CASE WHEN status = 'Active' AND buffer_type = 'GSII' THEN 1 END) as gsii_count,
    NOW() as updated_at
FROM osii_banks
WHERE status = 'Active'
GROUP BY country_iso2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_latest_osii_country 
    ON mv_latest_osii_snapshot(country_iso2);

-- ============================================
-- 2. Create Materialized Views for Trends
-- ============================================

-- CCyB Diffusion Trend Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ccyb_diffusion_trend AS
SELECT 
    effective_date as date,
    COUNT(DISTINCT country_iso2) FILTER (WHERE rate > 0) as countries_with_buffer,
    ROUND(AVG(rate) FILTER (WHERE rate > 0), 2) as avg_rate,
    ROUND(MAX(rate), 2) as max_rate,
    ROUND(MIN(rate) FILTER (WHERE rate > 0), 2) as min_rate,
    NOW() as updated_at
FROM ccyb_decisions
WHERE rate IS NOT NULL
GROUP BY effective_date
ORDER BY effective_date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_ccyb_trend_date 
    ON mv_ccyb_diffusion_trend(date);

-- SyRB Trend Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_syrb_trend AS
SELECT 
    effective_date as date,
    COUNT(DISTINCT country_iso2) FILTER (WHERE status = 'Active' AND measure_type = 'General') as countries_with_general,
    COUNT(DISTINCT country_iso2) FILTER (WHERE status = 'Active' AND measure_type = 'Sectoral') as countries_with_sectoral,
    ROUND(AVG(rate) FILTER (WHERE status = 'Active' AND measure_type = 'General'), 2) as avg_general_rate,
    ROUND(AVG(rate) FILTER (WHERE status = 'Active' AND measure_type = 'Sectoral'), 2) as avg_sectoral_rate,
    NOW() as updated_at
FROM syrb_measures
WHERE status = 'Active' AND effective_date IS NOT NULL
GROUP BY effective_date
ORDER BY effective_date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_syrb_trend_date 
    ON mv_syrb_trend(date);

-- BBM Diffusion Trend Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_bbm_diffusion_trend AS
SELECT 
    effective_date as date,
    COUNT(DISTINCT country_iso2) FILTER (WHERE active_status = 'Active') as countries_with_bbm,
    COUNT(DISTINCT country_iso2) FILTER (WHERE active_status = 'Active' AND measure_short = 'LTV') as ltv_count,
    COUNT(DISTINCT country_iso2) FILTER (WHERE active_status = 'Active' AND measure_short IN ('DTI', 'LTI')) as dti_lti_count,
    COUNT(DISTINCT country_iso2) FILTER (WHERE active_status = 'Active' AND measure_short = 'DSTI') as dsti_count,
    NOW() as updated_at
FROM bbm_measures
WHERE active_status = 'Active' AND effective_date IS NOT NULL
GROUP BY effective_date
ORDER BY effective_date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_bbm_trend_date 
    ON mv_bbm_diffusion_trend(date);

-- ============================================
-- 3. Create Refresh Trigger Function
-- ============================================

CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    -- Note: CONCURRENTLY refresh requires unique indexes and cannot be used in triggers
    -- We use regular refresh which locks the view but is safe in triggers
    -- For high-traffic scenarios, consider using pg_cron or a background job instead
    
    -- Refresh snapshot views
    REFRESH MATERIALIZED VIEW mv_latest_ccyb_snapshot;
    REFRESH MATERIALIZED VIEW mv_latest_syrb_snapshot;
    REFRESH MATERIALIZED VIEW mv_latest_osii_snapshot;
    
    -- Refresh trend views
    REFRESH MATERIALIZED VIEW mv_ccyb_diffusion_trend;
    REFRESH MATERIALIZED VIEW mv_syrb_trend;
    REFRESH MATERIALIZED VIEW mv_bbm_diffusion_trend;
    
    RETURN NULL;
END;
$$;

-- ============================================
-- 4. Create Triggers for Automatic Refresh
-- ============================================

-- Trigger on ccyb_decisions (affects latest_ccyb_snapshot and ccyb_diffusion_trend)
DROP TRIGGER IF EXISTS trigger_refresh_views_on_ccyb ON ccyb_decisions;
CREATE TRIGGER trigger_refresh_views_on_ccyb
    AFTER INSERT OR UPDATE OR DELETE ON ccyb_decisions
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_materialized_views();

-- Trigger on syrb_measures (affects latest_syrb_snapshot and syrb_trend)
DROP TRIGGER IF EXISTS trigger_refresh_views_on_syrb ON syrb_measures;
CREATE TRIGGER trigger_refresh_views_on_syrb
    AFTER INSERT OR UPDATE OR DELETE ON syrb_measures
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_materialized_views();

-- Trigger on osii_banks (affects latest_osii_snapshot)
DROP TRIGGER IF EXISTS trigger_refresh_views_on_osii ON osii_banks;
CREATE TRIGGER trigger_refresh_views_on_osii
    AFTER INSERT OR UPDATE OR DELETE ON osii_banks
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_materialized_views();

-- Trigger on bbm_measures (affects bbm_diffusion_trend)
DROP TRIGGER IF EXISTS trigger_refresh_views_on_bbm ON bbm_measures;
CREATE TRIGGER trigger_refresh_views_on_bbm
    AFTER INSERT OR UPDATE OR DELETE ON bbm_measures
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_materialized_views();

-- ============================================
-- 5. Initial Refresh
-- ============================================

-- Perform initial refresh of all materialized views
REFRESH MATERIALIZED VIEW mv_latest_ccyb_snapshot;
REFRESH MATERIALIZED VIEW mv_latest_syrb_snapshot;
REFRESH MATERIALIZED VIEW mv_latest_osii_snapshot;
REFRESH MATERIALIZED VIEW mv_ccyb_diffusion_trend;
REFRESH MATERIALIZED VIEW mv_syrb_trend;
REFRESH MATERIALIZED VIEW mv_bbm_diffusion_trend;
