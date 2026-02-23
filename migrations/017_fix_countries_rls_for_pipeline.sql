-- ============================================
-- Fix Countries RLS for Pipeline Writes
-- Migration: 017_fix_countries_rls_for_pipeline.sql
-- ============================================
-- Ensures the pipeline (using service role or anon key) can upsert countries.
-- The service_role key normally bypasses RLS; this policy helps when the
-- JWT role is passed differently or for SQL migrations run as postgres.

-- Recreate policy with multiple role checks (Supabase service_role bypasses RLS,
-- but some clients/configs may evaluate policies; this ensures writes succeed)
DROP POLICY IF EXISTS "Service role write access for countries" ON countries;
CREATE POLICY "Service role write access for countries"
    ON countries FOR ALL
    USING (
        auth.role() = 'service_role'
        OR (auth.jwt() ->> 'role') = 'service_role'
    )
    WITH CHECK (
        auth.role() = 'service_role'
        OR (auth.jwt() ->> 'role') = 'service_role'
    );
