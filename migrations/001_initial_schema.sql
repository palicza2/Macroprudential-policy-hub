-- ============================================
-- Supabase Initial Schema for Macroprudential Hub
-- Migration: 001_initial_schema.sql
-- ============================================

-- 1. Countries Lookup Table
CREATE TABLE IF NOT EXISTS countries (
    iso2 CHAR(2) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    iso3 CHAR(3),
    region VARCHAR(50),
    eea_member BOOLEAN DEFAULT FALSE,
    eu_member BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_countries_region ON countries(region);

-- 2. CCyB Decisions (Időszoros)
CREATE TABLE IF NOT EXISTS ccyb_decisions (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    effective_date DATE NOT NULL,
    decision_date DATE,
    announcement_date DATE,
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 20),
    status VARCHAR(50), -- "Confirmation", "Increase", "Decrease", etc.
    credit_gap DECIMAL(5,2),
    credit_to_gdp DECIMAL(8,2),
    buffer_guide DECIMAL(5,2),
    justification TEXT,
    justification_exceptional TEXT,
    link TEXT,
    reference_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_ccyb_country_date ON ccyb_decisions(country_iso2, effective_date);
CREATE INDEX IF NOT EXISTS idx_ccyb_date ON ccyb_decisions(effective_date);
CREATE INDEX IF NOT EXISTS idx_ccyb_status ON ccyb_decisions(status);

-- 3. SyRB Measures (Időszoros)
CREATE TABLE IF NOT EXISTS syrb_measures (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_type VARCHAR(50), -- "General", "Sectoral"
    sector VARCHAR(50), -- "Residential RE", "Commercial RE", NULL
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 20),
    effective_date DATE,
    decision_date DATE,
    status VARCHAR(50), -- "Active", "Withdrawn", "Announced", "Revoked"
    description TEXT,
    basis_in_union_law TEXT,
    related_links TEXT,
    revocation_date DATE,
    revocation_note TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_syrb_country ON syrb_measures(country_iso2);
CREATE INDEX IF NOT EXISTS idx_syrb_status ON syrb_measures(status) WHERE status = 'Active';
CREATE INDEX IF NOT EXISTS idx_syrb_type ON syrb_measures(measure_type);

-- 4. BBM Measures (Raw ESRB Data)
CREATE TABLE IF NOT EXISTS bbm_measures (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_type VARCHAR(50), -- "Loan-to-value (LTV)", "Debt-to-income (DTI)", etc.
    measure_short VARCHAR(10), -- "LTV", "DTI", "LTI", "DSTI"
    status VARCHAR(20), -- "Active", "Withdrawn", "Announced"
    active_status VARCHAR(20), -- "Active", "Inactive"
    description TEXT,
    intermediate_objective TEXT,
    basis_in_union_law TEXT,
    effective_date DATE,
    decision_date DATE,
    authority VARCHAR(200),
    year_initiative INTEGER,
    parent_measure VARCHAR(200),
    has_been_revoked BOOLEAN DEFAULT FALSE,
    revocation_date DATE,
    revocation_note TEXT,
    related_links TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bbm_country_type ON bbm_measures(country_iso2, measure_short);
CREATE INDEX IF NOT EXISTS idx_bbm_status ON bbm_measures(active_status) WHERE active_status = 'Active';
CREATE INDEX IF NOT EXISTS idx_bbm_measure_short ON bbm_measures(measure_short);

-- 5. LTV Rules (Strukturált) ⭐ ÚJ
CREATE TABLE IF NOT EXISTS ltv_rules (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    implementation_status VARCHAR(20) CHECK (implementation_status IN ('Active', 'Inactive', 'Announced')),
    legal_form VARCHAR(20) CHECK (legal_form IN ('Binding', 'Recommendation')),
    limit_standard TEXT, -- Can be single value (e.g., "80.0%") or list (e.g., "80.0%, 90.0%")
    limit_ftb DECIMAL(5,2) CHECK (limit_ftb >= 0 AND limit_ftb <= 100),
    limit_btl DECIMAL(5,2) CHECK (limit_btl >= 0 AND limit_btl <= 100),
    exception_quota VARCHAR(100), -- e.g., "15% of volume"
    notes TEXT, -- Explains what list values mean (e.g., "80% for owner-occupied, 70% for investment")
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2)
);

CREATE INDEX IF NOT EXISTS idx_ltv_country ON ltv_rules(country_iso2);
CREATE INDEX IF NOT EXISTS idx_ltv_status ON ltv_rules(implementation_status) WHERE implementation_status = 'Active';

-- 6. DTI/LTI Rules (Strukturált) - FRISSÍTVE
CREATE TABLE IF NOT EXISTS dti_lti_rules (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    measure_code VARCHAR(3) CHECK (measure_code IN ('DTI', 'LTI')),
    implementation_status VARCHAR(20) CHECK (implementation_status IN ('Active', 'Withdrawn', 'Announced')),
    legal_form VARCHAR(20) CHECK (legal_form IN ('Binding', 'Recommendation')),
    limit_standard TEXT, -- Can be single value (e.g., "4.5x") or list (e.g., "3.0x, 8.0x")
    limit_ftb DECIMAL(4,2),
    limit_btl DECIMAL(4,2),
    limit_green DECIMAL(4,2), -- Green/sustainable mortgage limit (e.g., for LV)
    income_basis VARCHAR(10) CHECK (income_basis IN ('Gross', 'Net', 'Unknown')),
    allowance_share VARCHAR(10), -- "15%"
    regulation_url TEXT,
    notes TEXT, -- Explains what list values mean (e.g., "Decreasing by age" for SK's 3-8x range)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_iso2, measure_code)
);

CREATE INDEX IF NOT EXISTS idx_dti_lti_country ON dti_lti_rules(country_iso2);
CREATE INDEX IF NOT EXISTS idx_dti_lti_status ON dti_lti_rules(implementation_status) WHERE implementation_status = 'Active';
CREATE INDEX IF NOT EXISTS idx_dti_lti_measure ON dti_lti_rules(measure_code);

-- 7. OSII/GSII Banks
CREATE TABLE IF NOT EXISTS osii_banks (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) REFERENCES countries(iso2) ON DELETE CASCADE,
    bank_name VARCHAR(200) NOT NULL,
    lei_code VARCHAR(20),
    buffer_type VARCHAR(20), -- "OSII", "GSII"
    rate DECIMAL(5,2) CHECK (rate >= 0 AND rate <= 5),
    effective_date DATE,
    status VARCHAR(20), -- "Active", "Inactive"
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_osii_country ON osii_banks(country_iso2);
CREATE INDEX IF NOT EXISTS idx_osii_status ON osii_banks(status) WHERE status = 'Active';
CREATE INDEX IF NOT EXISTS idx_osii_type ON osii_banks(buffer_type);

-- 8. Latest Snapshots
CREATE TABLE IF NOT EXISTS latest_ccyb_snapshot (
    country_iso2 CHAR(2) PRIMARY KEY REFERENCES countries(iso2),
    rate DECIMAL(5,2),
    effective_date DATE,
    credit_gap DECIMAL(5,2),
    credit_to_gdp DECIMAL(8,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS latest_syrb_snapshot (
    country_iso2 CHAR(2) PRIMARY KEY REFERENCES countries(iso2),
    total_rate DECIMAL(5,2), -- Sum of all active SyRB rates
    general_rate DECIMAL(5,2),
    sectoral_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS latest_osii_snapshot (
    country_iso2 CHAR(2) PRIMARY KEY REFERENCES countries(iso2),
    total_rate DECIMAL(5,2), -- Sum of all active OSII/GSII rates
    osii_count INTEGER,
    gsii_count INTEGER,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 9. Aggregated Trends
CREATE TABLE IF NOT EXISTS ccyb_diffusion_trend (
    date DATE PRIMARY KEY,
    countries_with_buffer INTEGER,
    avg_rate DECIMAL(5,2),
    max_rate DECIMAL(5,2),
    min_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS syrb_trend (
    date DATE PRIMARY KEY,
    countries_with_general INTEGER,
    countries_with_sectoral INTEGER,
    avg_general_rate DECIMAL(5,2),
    avg_sectoral_rate DECIMAL(5,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bbm_diffusion_trend (
    date DATE PRIMARY KEY,
    countries_with_bbm INTEGER,
    ltv_count INTEGER,
    dti_lti_count INTEGER,
    dsti_count INTEGER,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- Triggers for updated_at timestamps
-- ============================================

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

-- Apply triggers to all tables with updated_at
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
