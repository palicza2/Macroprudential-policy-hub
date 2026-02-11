-- ============================================
-- Fix SyRB status column length
-- Migration: 002_fix_syrb_status_length.sql
-- ============================================

-- SyRB status column needs to be longer (max value is 23 chars: "Is currently applicable")
ALTER TABLE syrb_measures ALTER COLUMN status TYPE VARCHAR(50);
