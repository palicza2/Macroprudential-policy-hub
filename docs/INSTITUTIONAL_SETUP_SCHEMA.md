# Institutional Setup Schema

This document describes the `institutional_setup` table used for the Country Profiles institutional setup section.

## Overview

The institutional setup section displays the framework of macroprudential policy per country: National Macroprudential Authority (NMA), National Designated Authority (NDA), legal basis, and AI-generated descriptions with confidence/grounding metadata.

## Data Sources

- **ESRB:** [List of National Macroprudential Authorities and National Designated Authorities in EEA Member States](https://www.esrb.europa.eu/national_policy/shared/pdf/esrb.191125_list_national_macroprudential_authorities_and_national_designated_authorities_in_EEA_Member_States.en.pdf)
- **National sources:** Central bank websites, legal databases, financial stability reports

## Supabase Table Schema

```sql
CREATE TABLE institutional_setup (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) NOT NULL REFERENCES countries(iso2) ON DELETE CASCADE,
    
    -- Structured data
    macroprudential_authority VARCHAR(300),
    designated_authority VARCHAR(300),
    institutional_model VARCHAR(50),   -- 'unified' | 'separate' | 'central_bank_led'
    legal_basis TEXT,
    decision_making_body VARCHAR(300),
    relationship_to_cb VARCHAR(200),
    key_regulations TEXT[],
    source_url TEXT,
    
    -- AI-generated (optional; can be filled by pipeline)
    ai_description TEXT,
    ai_confidence_score DECIMAL(3,2) CHECK (ai_confidence_score >= 0 AND ai_confidence_score <= 1),
    ai_grounding_notes TEXT,
    ai_sources_cited TEXT[],
    ai_generated_at TIMESTAMPTZ,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(country_iso2)
);
```

## Fallback: `data/institutional_setup.json`

When the Supabase table is empty or not used, the pipeline loads from `data/institutional_setup.json`, keyed by ISO2. The file includes all 30 EEA countries (EU-27 + Iceland, Liechtenstein, Norway).

```json
{
  "AT": {
    "macroprudential_authority": "Oesterreichische Nationalbank (OeNB)",
    "designated_authority": "Oesterreichische Nationalbank (OeNB)",
    "institutional_model": "unified",
    "legal_basis": "Nationalbank Act; Financial Market Stability Act (FMStG)",
    "decision_making_body": "Governing Council of OeNB",
    "relationship_to_cb": "Central bank acts as both NMA and NDA",
    "key_regulations": ["FMStG", "Nationalbank Act"],
    "source_url": "https://www.esrb.europa.eu/national_policy/"
  }
}
```

## AI Description & Grounding

For each country with institutional setup data, the pipeline generates:

- **ai_description:** 2–3 paragraph prose describing the institutional framework
- **ai_confidence_score:** 0.0–1.0 (higher = more grounded in structured data)
- **ai_grounding_notes:** Brief explanation of sources/facts used
- **ai_sources_cited:** Array of source references

The UI shows a confidence badge (high/medium/low) and grounding notes.

## Migration

**015_institutional_setup.sql** – Creates the `institutional_setup` table and seeds all 30 EEA countries (ensures countries exist, then inserts institutional data). Safe to re-run (uses ON CONFLICT).
