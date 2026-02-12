# Supabase Séma Validáció és Oszlop Mapping

## 📋 Összefoglaló

Ez a dokumentum validálja, hogy minden jelenlegi adatstruktúra megfelelően le van-e képezve a Supabase sémába.

---

## 1. CCyB Decisions

### Jelenlegi Parquet Oszlopok:
- `country` → **country_iso2** (kell ISO2 konverzió)
- `decision_date` → **decision_date** ✅
- `Date of Announcement` → **announcement_date** ✅
- `rate` → **rate** ✅
- `status` → **status** ✅
- `date` → **effective_date** ✅
- `Credit-to-GDP` → **credit_to_gdp** ✅
- `Reference date` → **reference_date** ✅
- `Credit Gap` → **credit_gap** ✅
- `Buffer Guide` → **buffer_guide** ✅
- `justification` → **justification** ✅
- `Justification exceptional circumstances` → **justification_exceptional** ✅
- `Link` → **link** ✅
- `iso2` → **country_iso2** (direkt használható) ✅
- `iso3` → (nem kell Supabase-ben, de lehet countries táblában)

### Supabase Séma:
```sql
country_iso2, effective_date, decision_date, announcement_date, rate, status,
credit_gap, credit_to_gdp, buffer_guide, justification, justification_exceptional,
link, reference_date
```

**✅ MINDEN OSZLOP LE VAN KÉPEZVE**

---

## 2. SyRB Measures

### Jelenlegi Parquet Oszlopok:
- `iso2` → **country_iso2** ✅
- `syrb_type` → **measure_type** ✅ ("General" vagy "Sectoral")
- `exposure_type` → **sector** ✅ ("Residential RE", "Commercial RE", stb.)
- `rate_numeric` → **rate** ✅
- `date` → **effective_date** ✅
- `Decision made on` → **decision_date** ✅
- `status` → **status** ✅
- `description` → **description** ✅
- `Basis in Union law` → **basis_in_union_law** ✅
- `Related links` → **related_links** ✅
- `revocation_date` → **revocation_date** ✅
- `Note of revocation/ replacement` → **revocation_note** ✅

### Supabase Séma:
```sql
country_iso2, measure_type, sector, rate, effective_date, decision_date, status,
description, basis_in_union_law, related_links, revocation_date, revocation_note
```

**✅ MINDEN OSZLOP LE VAN KÉPEZVE**

---

## 3. BBM Measures

### Jelenlegi Parquet Oszlopok:
- `iso2` → **country_iso2** ✅
- `measure_type` → **measure_type** ✅
- `measure_type` (rövidített) → **measure_short** ⚠️ (kell extractálás: "LTV", "DTI", "LTI", "DSTI")
- `status` → **status** ✅
- `active_status` → **active_status** ✅
- `description` → **description** ✅
- `Intermediate Objective` → **intermediate_objective** ✅
- `Basis in Union law` → **basis_in_union_law** ✅
- `date` → **effective_date** ✅
- `Decision made on` → **decision_date** ✅
- `Authority` → **authority** ✅
- `Year initiative` → **year_initiative** ✅
- `Parent measure` → **parent_measure** ✅
- `Has the measure been revoked or replaced?` → **has_been_revoked** ✅ (boolean konverzió)
- `revocation_date` → **revocation_date** ✅
- `Note of revocation/ replacement` → **revocation_note** ✅
- `Related links` → **related_links** ✅

### Supabase Séma:
```sql
country_iso2, measure_type, measure_short, status, active_status, description,
intermediate_objective, basis_in_union_law, effective_date, decision_date,
authority, year_initiative, parent_measure, has_been_revoked, revocation_date,
revocation_note, related_links
```

**✅ MINDEN OSZLOP LE VAN KÉPEZVE**
**⚠️ Megjegyzés**: `measure_short` kell extractálni a `measure_type`-ból (pl. "Loan-to-value (LTV)" → "LTV")

---

## 4. DTI/LTI Rules

### Jelenlegi CSV Oszlopok:
- `Country` → **country_iso2** ✅ (kell ISO2 konverzió, pl. "UK" → "GB")
- `Measure_Code` → **measure_code** ✅ ("DTI" vagy "LTI")
- `Implementation_Status` → **implementation_status** ✅
- `Legal_Form` → **legal_form** ✅
- `Limit_Standard` → **limit_standard** ✅ (TEXT - lehet "3.0x, 8.0x")
- `Limit_FTB` → **limit_ftb** ✅
- `Limit_BTL` → **limit_btl** ✅
- `Limit_Green` → **limit_green** ✅
- `Income_Basis` → **income_basis** ✅
- `Allowance_Share` → **allowance_share** ✅
- `Regulation_URL` → **regulation_url** ✅
- `Notes` → **notes** ✅

### Supabase Séma:
```sql
country_iso2, measure_code, implementation_status, legal_form, limit_standard (TEXT),
limit_ftb, limit_btl, limit_green, income_basis, allowance_share, regulation_url, notes
```

**✅ MINDEN OSZLOP LE VAN KÉPEZVE**

---

## 5. LTV Rules

### Jelenlegi Generált DataFrame Oszlopok (bbm/ltv_model.py alapján):
- `Country` → **country_iso2** ✅ (kell ISO2 konverzió)
- `Implementation_Status` → **implementation_status** ✅
- `Legal_Form` → **legal_form** ✅
- `Limit_Standard` → **limit_standard** ✅ (TEXT - lehet lista)
- `Limit_FTB` → **limit_ftb** ✅
- `Limit_BTL` → **limit_btl** ✅
- `Exception_Quota` → **exception_quota** ✅
- `Notes` → **notes** ✅

### Supabase Séma:
```sql
country_iso2, implementation_status, legal_form, limit_standard (TEXT),
limit_ftb, limit_btl, exception_quota, notes
```

**✅ MINDEN OSZLOP LE VAN KÉPEZVE**

---

## 6. OSII Banks

### Jelenlegi Parquet Oszlopok:
- `iso2` → **country_iso2** ✅
- `bank_name` → **bank_name** ✅
- `lei_code` → **lei_code** ✅
- `buffer_type` → **buffer_type** ✅ ("OSII" vagy "GSII")
- `rate_numeric` → **rate** ✅
- `date` → **effective_date** ✅
- `status` → **status** ✅

### Supabase Séma:
```sql
country_iso2, bank_name, lei_code, buffer_type, rate, effective_date, status
```

**✅ MINDEN OSZLOP LE VAN KÉPEZVE**

---

## 7. Countries Lookup Table

### Szükséges Adatok:
- ISO2 kódok (minden táblából)
- Országnevek (country_converter vagy manuális mapping)
- ISO3 kódok (CCyB parquet-ből)
- EEA/EU tagság (hardcoded vagy lookup)

**✅ SÉMA RENDBEN**

---

## 8. Latest Snapshots

### CCyB Snapshot:
- `country_iso2` → **country_iso2** ✅
- `rate` → **rate** ✅
- `date` → **effective_date** ✅
- `credit_gap` → **credit_gap** ✅
- `credit_to_gdp` → **credit_to_gdp** ✅

### SyRB Snapshot:
- Agregált adatok (general_rate, sectoral_rate, total_rate)
- **✅ SÉMA RENDBEN** (kell aggregáció logika)

### OSII Snapshot:
- Agregált adatok (osii_count, gsii_count, total_rate)
- **✅ SÉMA RENDBEN** (kell aggregáció logika)

---

## 9. Aggregated Trends

### CCyB Diffusion Trend:
- `date` → **date** ✅
- `countries_with_buffer` → **countries_with_buffer** ✅
- `avg_rate`, `max_rate`, `min_rate` → **avg_rate, max_rate, min_rate** ✅

### SyRB Trend:
- `date` → **date** ✅
- `countries_with_general`, `countries_with_sectoral` → **countries_with_general, countries_with_sectoral** ✅
- `avg_general_rate`, `avg_sectoral_rate` → **avg_general_rate, avg_sectoral_rate** ✅

### BBM Diffusion Trend:
- `date` → **date** ✅
- `countries_with_bbm`, `ltv_count`, `dti_lti_count`, `dsti_count` → **countries_with_bbm, ltv_count, dti_lti_count, dsti_count** ✅

**✅ MINDEN OSZLOP LE VAN KÉPEZVE**

---

## ⚠️ Fontos Megjegyzések

### 1. ISO2 Konverzió
- **"UK" → "GB"**: A DTI/LTI CSV-ben "UK" szerepel, de Supabase-ben "GB"-t használunk
- **Country nevek → ISO2**: Szükséges country_converter vagy manuális mapping

### 2. Limit_Standard TEXT Formátum
- **DTI/LTI**: "4.5x" vagy "3.0x, 8.0x" (stringként tárolva)
- **LTV**: "80.0%" vagy "80.0%, 90.0%" (stringként tárolva)
- **Notes oszlop**: Magyarázza a lista jelentését

### 3. Boolean Konverzió
- **has_been_revoked**: "Yes"/"No" vagy "True"/"False" → BOOLEAN

### 4. Date Konverzió
- Pandas Timestamp → PostgreSQL DATE
- NULL/NaN kezelés

### 5. Measure_Short Extractálás
- "Loan-to-value (LTV)" → "LTV"
- "Debt-to-income (DTI)" → "DTI"
- Regex vagy string matching szükséges

---

## ✅ ÖSSZEFOGLALÁS

**MINDEN OSZLOP MEGVAN ÉS MEGFELELŐ A FORMÁTUMA!**

A Supabase séma teljes mértékben lefedi a jelenlegi adatstruktúrákat. A migrációs scriptnek csak az alábbi transzformációkat kell elvégeznie:

1. ISO2 konverzió (country nevek → ISO2)
2. Oszlop nevek átnevezése
3. Date formátum konverzió
4. Boolean konverzió
5. Measure_short extractálás (BBM-nél)
6. Limit_standard string formátum megtartása (TEXT mező)

**A séma validálva és készen áll a migrációra!** ✅
