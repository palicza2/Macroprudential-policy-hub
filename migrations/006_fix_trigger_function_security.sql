-- ============================================
-- Fix trigger function security (search_path)
-- Migration: 006_fix_trigger_function_security.sql
-- ============================================

-- Drop and recreate the function with fixed search_path
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- Recreate with SECURITY DEFINER and fixed search_path
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Recreate all triggers
CREATE TRIGGER update_countries_updated_at BEFORE UPDATE ON countries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ccyb_decisions_updated_at BEFORE UPDATE ON ccyb_decisions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_syrb_measures_updated_at BEFORE UPDATE ON syrb_measures
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_bbm_measures_updated_at BEFORE UPDATE ON bbm_measures
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ltv_rules_updated_at BEFORE UPDATE ON ltv_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dti_lti_rules_updated_at BEFORE UPDATE ON dti_lti_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_osii_banks_updated_at BEFORE UPDATE ON osii_banks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
