-- ============================================
-- Fix OSII column lengths
-- Migration: 004_fix_osii_status_length.sql
-- ============================================

-- OSII lei_code column needs to be longer (max value is 21 chars)
ALTER TABLE osii_banks ALTER COLUMN lei_code TYPE VARCHAR(25);
ALTER TABLE osii_banks ALTER COLUMN status TYPE VARCHAR(50);
-- OSII bank_name column needs to be much longer (max value is 2072 chars)
ALTER TABLE osii_banks ALTER COLUMN bank_name TYPE TEXT;
