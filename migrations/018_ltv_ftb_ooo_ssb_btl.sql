-- LTV limits: FTB/OOO and SSB/BTL instead of standard/FTB/BTL.
ALTER TABLE ltv_rules ADD COLUMN IF NOT EXISTS limit_ftb_ooo TEXT;
ALTER TABLE ltv_rules ADD COLUMN IF NOT EXISTS limit_ssb_btl TEXT;
ALTER TABLE ltv_rules ADD COLUMN IF NOT EXISTS other_limits TEXT;

COMMENT ON COLUMN ltv_rules.limit_ftb_ooo IS 'First-time buyer (FTB) and/or owner-occupied (OOO) LTV caps, e.g. 90% (FTB); 80% (OOO)';
COMMENT ON COLUMN ltv_rules.limit_ssb_btl IS 'Second/subsequent buyer (SSB) and/or buy-to-let (BTL) LTV caps';
COMMENT ON COLUMN ltv_rules.other_limits IS 'Other differentiations: green, secondary home, FX, etc.';
