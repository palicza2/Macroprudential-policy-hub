-- ============================================
-- Add service role write policies (if not exist)
-- Migration: 007_add_service_role_policies.sql
-- ============================================

-- Drop existing service role policies if they exist, then recreate
-- This ensures we have the correct policies

-- Countries: Service role write access
DROP POLICY IF EXISTS "Service role write access for countries" ON countries;
CREATE POLICY "Service role write access for countries"
    ON countries FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- CCyB Decisions: Service role write access
DROP POLICY IF EXISTS "Service role write access for ccyb_decisions" ON ccyb_decisions;
CREATE POLICY "Service role write access for ccyb_decisions"
    ON ccyb_decisions FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- SyRB Measures: Service role write access
DROP POLICY IF EXISTS "Service role write access for syrb_measures" ON syrb_measures;
CREATE POLICY "Service role write access for syrb_measures"
    ON syrb_measures FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- BBM Measures: Service role write access
DROP POLICY IF EXISTS "Service role write access for bbm_measures" ON bbm_measures;
CREATE POLICY "Service role write access for bbm_measures"
    ON bbm_measures FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- LTV Rules: Service role write access
DROP POLICY IF EXISTS "Service role write access for ltv_rules" ON ltv_rules;
CREATE POLICY "Service role write access for ltv_rules"
    ON ltv_rules FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- DTI/LTI Rules: Service role write access
DROP POLICY IF EXISTS "Service role write access for dti_lti_rules" ON dti_lti_rules;
CREATE POLICY "Service role write access for dti_lti_rules"
    ON dti_lti_rules FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- OSII Banks: Service role write access
DROP POLICY IF EXISTS "Service role write access for osii_banks" ON osii_banks;
CREATE POLICY "Service role write access for osii_banks"
    ON osii_banks FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Latest CCyB Snapshot: Service role write access
DROP POLICY IF EXISTS "Service role write access for latest_ccyb_snapshot" ON latest_ccyb_snapshot;
CREATE POLICY "Service role write access for latest_ccyb_snapshot"
    ON latest_ccyb_snapshot FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Latest SyRB Snapshot: Service role write access
DROP POLICY IF EXISTS "Service role write access for latest_syrb_snapshot" ON latest_syrb_snapshot;
CREATE POLICY "Service role write access for latest_syrb_snapshot"
    ON latest_syrb_snapshot FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Latest OSII Snapshot: Service role write access
DROP POLICY IF EXISTS "Service role write access for latest_osii_snapshot" ON latest_osii_snapshot;
CREATE POLICY "Service role write access for latest_osii_snapshot"
    ON latest_osii_snapshot FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- CCyB Diffusion Trend: Service role write access
DROP POLICY IF EXISTS "Service role write access for ccyb_diffusion_trend" ON ccyb_diffusion_trend;
CREATE POLICY "Service role write access for ccyb_diffusion_trend"
    ON ccyb_diffusion_trend FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- SyRB Trend: Service role write access
DROP POLICY IF EXISTS "Service role write access for syrb_trend" ON syrb_trend;
CREATE POLICY "Service role write access for syrb_trend"
    ON syrb_trend FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- BBM Diffusion Trend: Service role write access
DROP POLICY IF EXISTS "Service role write access for bbm_diffusion_trend" ON bbm_diffusion_trend;
CREATE POLICY "Service role write access for bbm_diffusion_trend"
    ON bbm_diffusion_trend FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');
