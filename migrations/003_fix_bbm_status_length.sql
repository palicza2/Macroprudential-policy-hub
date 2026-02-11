-- ============================================
-- Fix BBM status column lengths
-- Migration: 003_fix_bbm_status_length.sql
-- ============================================

-- BBM status and active_status columns may need to be longer
ALTER TABLE bbm_measures ALTER COLUMN status TYPE VARCHAR(50);
ALTER TABLE bbm_measures ALTER COLUMN active_status TYPE VARCHAR(50);
