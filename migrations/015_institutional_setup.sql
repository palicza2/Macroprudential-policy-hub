-- ============================================
-- Institutional Setup for Macroprudential Policy
-- Migration: 015_institutional_setup.sql
-- ============================================
-- Creates the institutional_setup table and seeds all 30 EEA countries.
-- Safe to re-run (uses ON CONFLICT for countries and institutional_setup).

-- 1. Create table
CREATE TABLE IF NOT EXISTS institutional_setup (
    id BIGSERIAL PRIMARY KEY,
    country_iso2 CHAR(2) NOT NULL REFERENCES countries(iso2) ON DELETE CASCADE,
    
    -- Structured data (from ESRB list, manual curation, or official sources)
    macroprudential_authority VARCHAR(300),
    designated_authority VARCHAR(300),
    institutional_model VARCHAR(50),  -- 'unified' | 'separate' | 'central_bank_led'
    legal_basis TEXT,
    decision_making_body VARCHAR(300),
    relationship_to_cb VARCHAR(200),
    key_regulations TEXT[],
    source_url TEXT,
    
    -- AI-generated content with grounding
    ai_description TEXT,
    ai_confidence_score DECIMAL(3,2) CHECK (ai_confidence_score >= 0 AND ai_confidence_score <= 1),
    ai_grounding_notes TEXT,
    ai_sources_cited TEXT[],
    ai_generated_at TIMESTAMPTZ,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(country_iso2)
);

CREATE INDEX IF NOT EXISTS idx_institutional_setup_country ON institutional_setup(country_iso2);
CREATE INDEX IF NOT EXISTS idx_institutional_setup_model ON institutional_setup(institutional_model);

CREATE TRIGGER update_institutional_setup_updated_at
    BEFORE UPDATE ON institutional_setup
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE institutional_setup IS 'Institutional framework of macroprudential policy per country (NMA, NDA, legal basis) with AI-generated descriptions and grounding metadata.';

-- 2. Ensure EEA countries exist (for FK)
INSERT INTO countries (iso2, country_name, iso3, eea_member, eu_member) VALUES
    ('AT','Austria','AUT',true,true),('BE','Belgium','BEL',true,true),('BG','Bulgaria','BGR',true,true),
    ('HR','Croatia','HRV',true,true),('CY','Cyprus','CYP',true,true),('CZ','Czechia','CZE',true,true),
    ('DK','Denmark','DNK',true,true),('EE','Estonia','EST',true,true),('FI','Finland','FIN',true,true),
    ('FR','France','FRA',true,true),('DE','Germany','DEU',true,true),('GR','Greece','GRC',true,true),
    ('HU','Hungary','HUN',true,true),('IE','Ireland','IRL',true,true),('IT','Italy','ITA',true,true),
    ('LV','Latvia','LVA',true,true),('LT','Lithuania','LTU',true,true),('LU','Luxembourg','LUX',true,true),
    ('MT','Malta','MLT',true,true),('NL','Netherlands','NLD',true,true),('PL','Poland','POL',true,true),
    ('PT','Portugal','PRT',true,true),('RO','Romania','ROU',true,true),('SK','Slovakia','SVK',true,true),
    ('SI','Slovenia','SVN',true,true),('ES','Spain','ESP',true,true),('SE','Sweden','SWE',true,true),
    ('IS','Iceland','ISL',true,false),('LI','Liechtenstein','LIE',true,false),('NO','Norway','NOR',true,false)
ON CONFLICT (iso2) DO UPDATE SET country_name = EXCLUDED.country_name, eea_member = true, updated_at = NOW();

-- 3. Seed institutional setup for all EEA countries
INSERT INTO institutional_setup (
    country_iso2,
    macroprudential_authority,
    designated_authority,
    institutional_model,
    legal_basis,
    decision_making_body,
    relationship_to_cb,
    key_regulations,
    source_url
) VALUES
    ('AT', 'Financial Market Stability Board (FMSB)', 'Financial Market Authority (FMA)', 'separate', 'Financial Market Stability Act (FMStG); Nationalbank Act', 'FMSB (links OeNB monitoring with FMA supervision)', 'OeNB provides analysis and FMSB secretariat; FMA implements instruments', ARRAY['FMStG','Nationalbank Act','CRR/CRD IV'], 'https://www.fmsg.at/en/'),
    ('BE', 'Nationale Bank van België / Banque Nationale de Belgique (NBB)', 'Nationale Bank van België / Banque Nationale de Belgique (NBB)', 'unified', 'Banking Law; NBB Organic Law', 'Board of Directors of the NBB', 'Central bank acts as both NMA and NDA', ARRAY['Banking Law','NBB Organic Law','CRR/CRD IV'], 'https://www.nbb.be/'),
    ('BG', 'Bulgarian National Bank (BNB)', 'Bulgarian National Bank (BNB)', 'unified', 'Law on the BNB; Credit Institutions Act', 'Governing Council of the BNB', 'Central bank acts as both NMA and NDA', ARRAY['Law on BNB','Credit Institutions Act','CRR/CRD IV'], 'https://www.bnb.bg/'),
    ('HR', 'Hrvatska narodna banka (HNB)', 'Hrvatska narodna banka (HNB)', 'unified', 'HNB Act; Credit Institutions Act', 'Governor and Council of the HNB', 'Central bank acts as both NMA and NDA', ARRAY['HNB Act','Credit Institutions Act','CRR/CRD IV'], 'https://www.hnb.hr/'),
    ('CY', 'Central Bank of Cyprus (CBC)', 'Central Bank of Cyprus (CBC)', 'unified', 'Central Bank of Cyprus Laws; Banking Law', 'Board of the CBC', 'Central bank acts as both NMA and NDA', ARRAY['CBC Laws','Banking Law','CRR/CRD IV'], 'https://www.centralbank.cy/'),
    ('CZ', 'Česká národní banka (CNB)', 'Česká národní banka (CNB)', 'unified', 'Act on the CNB; Banking Act', 'Bank Board of the CNB', 'Central bank acts as both NMA and NDA', ARRAY['Act on CNB','Banking Act','CRR/CRD IV'], 'https://www.cnb.cz/'),
    ('DK', 'Finansiel Stabilitet; Danmarks Nationalbank', 'Finanstilsynet (Danish FSA)', 'separate', 'Financial Business Act; Act on the Danmarks Nationalbank', 'Finansiel Stabilitet; Finanstilsynet implements', 'Nationalbank advises; FSA designated for instruments', ARRAY['Financial Business Act','CRR/CRD IV'], 'https://www.nationalbanken.dk/'),
    ('EE', 'Eesti Pank', 'Finantsinspektsioon (Estonian FSA)', 'separate', 'Eesti Pank Act; Credit Institutions Act', 'Board of Eesti Pank; Finantsinspektsioon implements', 'Eesti Pank monitors; FSA designated', ARRAY['Eesti Pank Act','Credit Institutions Act','CRR/CRD IV'], 'https://www.eestipank.ee/'),
    ('FI', 'Suomen Pankki (Bank of Finland) / Finanssivalvonta (FIVA)', 'Finanssivalvonta (FIVA)', 'unified', 'Act on the Bank of Finland; Act on Financial Supervisory Authority', 'Board of Finanssivalvonta', 'Bank of Finland in ECB; FIVA is national authority', ARRAY['Act on FIVA','Credit Institutions Act','CRR/CRD IV'], 'https://www.finanssivalvonta.fi/'),
    ('FR', 'Haut Conseil de Stabilité Financière (HCSF)', 'Haut Conseil de Stabilité Financière (HCSF)', 'unified', 'Monetary and Financial Code; ORDINANCE on HCSF', 'HCSF (chaired by Minister of Finance)', 'Banque de France provides secretariat; ACPR implements', ARRAY['Monetary and Financial Code','CRR/CRD IV'], 'https://www.tresor.economie.gouv.fr/'),
    ('DE', 'Ausschuss für Finanzstabilität (Financial Stability Committee)', 'Deutsche Bundesbank', 'separate', 'Financial Stability Act (FinStabG); Kreditwesengesetz (KWG)', 'Ausschuss für Finanzstabilität; BaFin and Bundesbank', 'Bundesbank (designated); BaFin in committee', ARRAY['FinStabG','KWG','CRR/CRD IV'], 'https://www.bundesbank.de/'),
    ('GR', 'Bank of Greece (BoG)', 'Bank of Greece (BoG)', 'unified', 'Bank of Greece Statute; Banking Law', 'Governor and General Council of the BoG', 'Central bank acts as both NMA and NDA', ARRAY['BoG Statute','Banking Law','CRR/CRD IV'], 'https://www.bankofgreece.gr/'),
    ('HU', 'Magyar Nemzeti Bank (MNB)', 'Magyar Nemzeti Bank (MNB)', 'unified', 'Act CCXXXVII of 2013 on the Magyar Nemzeti Bank', 'Monetary Council of the MNB', 'Central bank acts as both NMA and NDA', ARRAY['MNB Act','CRR/CRD IV'], 'https://www.mnb.hu/'),
    ('IE', 'Central Bank of Ireland (CBI)', 'Central Bank of Ireland (CBI)', 'unified', 'Central Bank Act 1942; European Union (Capital Requirements) Regulations', 'Commission of the CBI; Governor', 'Central bank acts as both NMA and NDA', ARRAY['Central Bank Act','CRR/CRD IV'], 'https://www.centralbank.ie/'),
    ('IT', 'Comitato per le Politiche Macroprudenziali (CMP)', 'Banca d''Italia', 'separate', 'Banking Act; Legislative Decree 385/1993', 'CMP (chaired by BoI Governor); Banca d''Italia implements', 'Banca d''Italia is designated; CMP coordinates', ARRAY['Banking Act','CRR/CRD IV'], 'https://www.bancaditalia.it/'),
    ('LV', 'Latvijas Banka', 'Latvijas Banka', 'unified', 'Law on the Bank of Latvia; Credit Institution Law', 'Council of Latvijas Banka', 'Central bank acts as both NMA and NDA', ARRAY['Law on Bank of Latvia','Credit Institution Law','CRR/CRD IV'], 'https://www.bank.lv/'),
    ('LT', 'Lietuvos bankas', 'Lietuvos bankas', 'unified', 'Law on the Bank of Lithuania; Law on Financial Institutions', 'Board of Lietuvos bankas', 'Central bank acts as both NMA and NDA', ARRAY['Law on Bank of Lithuania','CRR/CRD IV'], 'https://www.lb.lt/'),
    ('LU', 'Commission de Surveillance du Secteur Financier (CSSF)', 'Commission de Surveillance du Secteur Financier (CSSF)', 'unified', 'Law on the CSSF; Law of 5 April 1993 on the financial sector', 'Board of the CSSF', 'CSSF (supervisor) acts as NMA and NDA; BCL advises', ARRAY['CSSF Law','Banking Law','CRR/CRD IV'], 'https://www.cssf.lu/'),
    ('MT', 'Central Bank of Malta (CBM)', 'Central Bank of Malta (CBM)', 'unified', 'Central Bank of Malta Act; Banking Act', 'Board of Directors of the CBM', 'Central bank acts as both NMA and NDA', ARRAY['CBM Act','Banking Act','CRR/CRD IV'], 'https://www.centralbankmalta.org/'),
    ('NL', 'Comité Financiële Stabiliteit (CFS) / Financial Stability Committee', 'De Nederlandsche Bank (DNB)', 'separate', 'Financial Supervision Act; DNB Act', 'CFS; DNB implements macroprudential instruments', 'DNB (central bank) is designated; CFS coordinates', ARRAY['Financial Supervision Act','DNB Act','CRR/CRD IV'], 'https://www.dnb.nl/'),
    ('PL', 'Komisja Nadzoru Finansowego (KNF)', 'Narodowy Bank Polski (NBP)', 'separate', 'Banking Law; Act on the NBP; Act on KNF', 'KNF for supervision; NBP for CCyB and designated instruments', 'NBP (designated for CCyB); KNF (macroprudential supervisor)', ARRAY['Banking Law','Act on NBP','Act on KNF','CRR/CRD IV'], 'https://www.nbpl.pl/'),
    ('PT', 'Banco de Portugal (BdP)', 'Banco de Portugal (BdP)', 'unified', 'Legal Framework of Credit Institutions; Organic Law of BdP', 'Board of Directors of BdP', 'Central bank acts as both NMA and NDA', ARRAY['Organic Law BdP','Legal Framework CIs','CRR/CRD IV'], 'https://www.bportugal.pt/'),
    ('RO', 'Banca Națională a României (BNR)', 'Banca Națională a României (BNR)', 'unified', 'Statute of the BNR; Banking Law', 'Board of the BNR', 'Central bank acts as both NMA and NDA', ARRAY['Statute of BNR','Banking Law','CRR/CRD IV'], 'https://www.bnr.ro/'),
    ('SK', 'Národná banka Slovenska (NBS)', 'Národná banka Slovenska (NBS)', 'unified', 'Act on the NBS; Act on Financial Market Supervision', 'Bank Board of the NBS', 'Central bank acts as both NMA and NDA', ARRAY['Act on NBS','CRR/CRD IV'], 'https://www.nbs.sk/'),
    ('SI', 'Banka Slovenije', 'Banka Slovenije', 'unified', 'Banka Slovenije Act; Banking Act', 'Governing Board of Banka Slovenije', 'Central bank acts as both NMA and NDA', ARRAY['Banka Slovenije Act','Banking Act','CRR/CRD IV'], 'https://www.bsi.si/'),
    ('ES', 'Consejo de Estabilidad Financiera (CEF)', 'Banco de España', 'separate', 'Law on the CEF; Law on Banco de España', 'CEF; Banco de España implements', 'Banco de España (designated); CEF coordinates', ARRAY['CEF Law','Law on Banco de España','CRR/CRD IV'], 'https://www.bde.es/'),
    ('SE', 'Finansinspektionen (FI)', 'Finansinspektionen (FI)', 'unified', 'Act on Finansinspektionen; Banking and Financing Business Act', 'Board of Finansinspektionen', 'FI (FSA) acts as both NMA and NDA; Riksbank advises', ARRAY['Act on FI','Banking and Financing Business Act','CRR/CRD IV'], 'https://www.fi.se/'),
    ('IS', 'Seðlabanki Íslands (Central Bank of Iceland)', 'Fjármálaeftirlitið (FME)', 'separate', 'Act on the Central Bank of Iceland; Act on Financial Undertakings', 'Governor of CBI; FME implements', 'CBI monitors; FME designated for instruments', ARRAY['CBI Act','Act on Financial Undertakings'], 'https://www.cb.is/'),
    ('LI', 'Finanzmarktaufsicht Liechtenstein (FMA)', 'Finanzmarktaufsicht Liechtenstein (FMA)', 'unified', 'Banking Act; FMA Act', 'Board of the FMA', 'FMA (no national central bank) acts as NMA and NDA', ARRAY['Banking Act','FMA Act','CRR/CRD IV'], 'https://www.fma-li.li/'),
    ('NO', 'Finanstilsynet (Financial Supervisory Authority of Norway)', 'Finanstilsynet', 'unified', 'Financial Institutions Act; Norges Bank Act', 'Board of Finanstilsynet; Norges Bank advises', 'Finanstilsynet designated; Norges Bank provides analysis', ARRAY['Financial Institutions Act','CRR/CRD IV'], 'https://www.finanstilsynet.no/')
ON CONFLICT (country_iso2) DO UPDATE SET
    macroprudential_authority = EXCLUDED.macroprudential_authority,
    designated_authority = EXCLUDED.designated_authority,
    institutional_model = EXCLUDED.institutional_model,
    legal_basis = EXCLUDED.legal_basis,
    decision_making_body = EXCLUDED.decision_making_body,
    relationship_to_cb = EXCLUDED.relationship_to_cb,
    key_regulations = EXCLUDED.key_regulations,
    source_url = EXCLUDED.source_url,
    updated_at = NOW();
