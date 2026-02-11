-- ============================================
-- Enable Row Level Security (RLS) on all tables
-- Migration: 005_enable_rls.sql
-- ============================================

-- Enable RLS on all tables
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

-- ============================================
-- Create policies for public read access
-- ============================================

-- Countries: Public read access
CREATE POLICY "Public read access for countries"
    ON countries FOR SELECT
    USING (true);

-- CCyB Decisions: Public read access
CREATE POLICY "Public read access for ccyb_decisions"
    ON ccyb_decisions FOR SELECT
    USING (true);

-- SyRB Measures: Public read access
CREATE POLICY "Public read access for syrb_measures"
    ON syrb_measures FOR SELECT
    USING (true);

-- BBM Measures: Public read access
CREATE POLICY "Public read access for bbm_measures"
    ON bbm_measures FOR SELECT
    USING (true);

-- LTV Rules: Public read access
CREATE POLICY "Public read access for ltv_rules"
    ON ltv_rules FOR SELECT
    USING (true);

-- DTI/LTI Rules: Public read access
CREATE POLICY "Public read access for dti_lti_rules"
    ON dti_lti_rules FOR SELECT
    USING (true);

-- OSII Banks: Public read access
CREATE POLICY "Public read access for osii_banks"
    ON osii_banks FOR SELECT
    USING (true);

-- Latest CCyB Snapshot: Public read access
CREATE POLICY "Public read access for latest_ccyb_snapshot"
    ON latest_ccyb_snapshot FOR SELECT
    USING (true);

-- Latest SyRB Snapshot: Public read access
CREATE POLICY "Public read access for latest_syrb_snapshot"
    ON latest_syrb_snapshot FOR SELECT
    USING (true);

-- Latest OSII Snapshot: Public read access
CREATE POLICY "Public read access for latest_osii_snapshot"
    ON latest_osii_snapshot FOR SELECT
    USING (true);

-- CCyB Diffusion Trend: Public read access
CREATE POLICY "Public read access for ccyb_diffusion_trend"
    ON ccyb_diffusion_trend FOR SELECT
    USING (true);

-- SyRB Trend: Public read access
CREATE POLICY "Public read access for syrb_trend"
    ON syrb_trend FOR SELECT
    USING (true);

-- BBM Diffusion Trend: Public read access
CREATE POLICY "Public read access for bbm_diffusion_trend"
    ON bbm_diffusion_trend FOR SELECT
    USING (true);

-- ============================================
-- Create policies for service role write access
-- ============================================

-- Service role can insert/update/delete (for migrations)
-- Note: These policies allow the service role (used by migrations) to write data
-- The anon key (public API) can only read

-- Countries: Service role write access
CREATE POLICY "Service role write access for countries"
    ON countries FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- CCyB Decisions: Service role write access
CREATE POLICY "Service role write access for ccyb_decisions"
    ON ccyb_decisions FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- SyRB Measures: Service role write access
CREATE POLICY "Service role write access for syrb_measures"
    ON syrb_measures FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- BBM Measures: Service role write access
CREATE POLICY "Service role write access for bbm_measures"
    ON bbm_measures FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- LTV Rules: Service role write access
CREATE POLICY "Service role write access for ltv_rules"
    ON ltv_rules FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- DTI/LTI Rules: Service role write access
CREATE POLICY "Service role write access for dti_lti_rules"
    ON dti_lti_rules FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- OSII Banks: Service role write access
CREATE POLICY "Service role write access for osii_banks"
    ON osii_banks FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Latest CCyB Snapshot: Service role write access
CREATE POLICY "Service role write access for latest_ccyb_snapshot"
    ON latest_ccyb_snapshot FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Latest SyRB Snapshot: Service role write access
CREATE POLICY "Service role write access for latest_syrb_snapshot"
    ON latest_syrb_snapshot FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Latest OSII Snapshot: Service role write access
CREATE POLICY "Service role write access for latest_osii_snapshot"
    ON latest_osii_snapshot FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- CCyB Diffusion Trend: Service role write access
CREATE POLICY "Service role write access for ccyb_diffusion_trend"
    ON ccyb_diffusion_trend FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- SyRB Trend: Service role write access
CREATE POLICY "Service role write access for syrb_trend"
    ON syrb_trend FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- BBM Diffusion Trend: Service role write access
CREATE POLICY "Service role write access for bbm_diffusion_trend"
    ON bbm_diffusion_trend FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');
