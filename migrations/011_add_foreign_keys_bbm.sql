-- ============================================
-- Add Foreign Keys for BBM Measures
-- Migration: 011_add_foreign_keys_bbm.sql
-- ============================================
--
-- Adds foreign key relationships:
--   - ltv_rules.bbm_measure_id → bbm_measures.id
--   - dti_lti_rules.bbm_measure_id → bbm_measures.id

-- ============================================
-- 1. Add bbm_measure_id columns if they don't exist
-- ============================================

-- Add bbm_measure_id to ltv_rules
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'ltv_rules' AND column_name = 'bbm_measure_id'
    ) THEN
        ALTER TABLE ltv_rules 
        ADD COLUMN bbm_measure_id BIGINT REFERENCES bbm_measures(id) ON DELETE SET NULL;
        
        CREATE INDEX IF NOT EXISTS idx_ltv_bbm_measure 
            ON ltv_rules(bbm_measure_id);
    END IF;
END $$;

-- Add bbm_measure_id to dti_lti_rules
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'dti_lti_rules' AND column_name = 'bbm_measure_id'
    ) THEN
        ALTER TABLE dti_lti_rules 
        ADD COLUMN bbm_measure_id BIGINT REFERENCES bbm_measures(id) ON DELETE SET NULL;
        
        CREATE INDEX IF NOT EXISTS idx_dti_lti_bbm_measure 
            ON dti_lti_rules(bbm_measure_id);
    END IF;
END $$;

-- ============================================
-- 2. Populate foreign keys based on country and measure type
-- ============================================

-- Populate ltv_rules.bbm_measure_id
UPDATE ltv_rules lr
SET bbm_measure_id = (
    SELECT bm.id
    FROM bbm_measures bm
    WHERE bm.country_iso2 = lr.country_iso2
      AND bm.measure_short = 'LTV'
      AND bm.active_status = 'Active'
    ORDER BY bm.effective_date DESC NULLS LAST
    LIMIT 1
)
WHERE bbm_measure_id IS NULL;

-- Populate dti_lti_rules.bbm_measure_id
UPDATE dti_lti_rules dlr
SET bbm_measure_id = (
    SELECT bm.id
    FROM bbm_measures bm
    WHERE bm.country_iso2 = dlr.country_iso2
      AND bm.measure_short = dlr.measure_code
      AND bm.active_status = 'Active'
    ORDER BY bm.effective_date DESC NULLS LAST
    LIMIT 1
)
WHERE bbm_measure_id IS NULL;
