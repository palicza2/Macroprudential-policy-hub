/**
 * Supabase Client for Frontend
 * Handles data fetching from Supabase REST API
 */

/**
 * Get Supabase configuration from window variables
 */
function getSupabaseConfig() {
    return {
        url: window.SUPABASE_URL || null,
        key: window.SUPABASE_KEY || null, // Anon key for read-only access
    };
}

/**
 * Check if Supabase is enabled
 */
function isSupabaseEnabled() {
    const config = getSupabaseConfig();
    return !!(config.url && config.key);
}

/**
 * Initialize Supabase client
 */
function initSupabaseClient() {
    const config = getSupabaseConfig();
    if (!config.key || !config.url) {
        return null;
    }
    
    // Simple fetch-based client (no external dependencies)
    return {
        url: config.url,
        key: config.key,
    };
}

/**
 * Fetch data from Supabase table
 * @param {string} table - Table name
 * @param {Object} options - Query options (select, filter, order, limit)
 * @returns {Promise<Array>} Array of records
 */
async function fetchFromSupabase(table, options = {}) {
    const client = initSupabaseClient();
    if (!client) {
        return null;
    }
    
    try {
        let url = `${client.url}/rest/v1/${table}`;
        const params = new URLSearchParams();
        
        // Add select parameter
        if (options.select) {
            params.append('select', options.select);
        }
        
        // Add filter parameters (eq, gte, lte, etc.)
        if (options.filter) {
            Object.entries(options.filter).forEach(([key, value]) => {
                if (value !== null && value !== undefined) {
                    // Supabase PostgREST format: column=eq.value
                    params.append(key, `eq.${encodeURIComponent(value)}`);
                }
            });
        }
        
        // Add order parameter
        if (options.order) {
            params.append('order', options.order);
        }
        
        // Add limit parameter
        if (options.limit) {
            params.append('limit', options.limit.toString());
        }
        
        if (params.toString()) {
            url += '?' + params.toString();
        }
        
        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'apikey': client.key,
                'Authorization': `Bearer ${client.key}`,
                'Content-Type': 'application/json',
            },
        });
        
        if (!response.ok) {
            throw new Error(`Supabase API error: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data;
    } catch (error) {
        console.error(`Error fetching from Supabase table ${table}:`, error);
        return null;
    }
}

/**
 * Fetch CCyB decisions for a specific country
 * @param {string} countryIso2 - ISO2 country code
 * @param {number} limit - Maximum number of records
 * @returns {Promise<Array>} Array of CCyB decisions
 */
async function fetchCCyBDecisions(countryIso2 = null, limit = 10) {
    const options = {
        select: '*',
        order: 'effective_date.desc',
        limit: limit,
    };
    
    if (countryIso2) {
        options.filter = { country_iso2: countryIso2 };
    }
    
    return await fetchFromSupabase('ccyb_decisions', options);
}

/**
 * Fetch latest CCyB snapshot
 * @param {string} countryIso2 - Optional ISO2 country code
 * @returns {Promise<Array>} Array of snapshot records
 */
async function fetchLatestCCyBSnapshot(countryIso2 = null) {
    const options = {
        select: '*',
    };
    
    if (countryIso2) {
        options.filter = { country_iso2: countryIso2 };
    }
    
    return await fetchFromSupabase('mv_latest_ccyb_snapshot', options);
}

/**
 * Fetch DTI/LTI rules
 * @param {string} countryIso2 - Optional ISO2 country code
 * @returns {Promise<Array>} Array of DTI/LTI rules
 */
async function fetchDTILTIRules(countryIso2 = null) {
    const options = {
        select: '*',
    };
    
    if (countryIso2) {
        options.filter = { country_iso2: countryIso2 };
    }
    
    return await fetchFromSupabase('dti_lti_rules', options);
}

/**
 * Fetch LTV rules
 * @param {string} countryIso2 - Optional ISO2 country code
 * @returns {Promise<Array>} Array of LTV rules
 */
async function fetchLTVRules(countryIso2 = null) {
    const options = {
        select: '*',
    };
    
    if (countryIso2) {
        options.filter = { country_iso2: countryIso2 };
    }
    
    return await fetchFromSupabase('ltv_rules', options);
}

/**
 * Fetch CCyB trend data
 * @param {number} limit - Maximum number of records
 * @returns {Promise<Array>} Array of trend records
 */
async function fetchCCyBTrend(limit = 30) {
    const options = {
        select: '*',
        order: 'date.desc',
        limit: limit,
    };
    
    return await fetchFromSupabase('mv_ccyb_diffusion_trend', options);
}

/**
 * Fetch complete country profile from Supabase
 * @param {string} countryIso2 - ISO2 country code
 * @returns {Promise<Object>} Country profile data
 */
async function fetchCountryProfile(countryIso2) {
    if (!isSupabaseEnabled()) {
        return null;
    }
    
    try {
        const client = initSupabaseClient();
        if (!client) {
            return null;
        }
        
        // Fetch all data in parallel (using Materialized Views for snapshots)
        const instFetch = fetchFromSupabase('institutional_setup', { filter: { country_iso2: countryIso2 }, limit: 1 }).catch(() => []);
        const [country, ccybSnapshot, syrbSnapshot, osiiSnapshot, ccybDecisions, syrbMeasures, bbmMeasures, instResult] = await Promise.all([
            fetchFromSupabase('countries', { filter: { iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('mv_latest_ccyb_snapshot', { filter: { country_iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('mv_latest_syrb_snapshot', { filter: { country_iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('mv_latest_osii_snapshot', { filter: { country_iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('ccyb_decisions', { filter: { country_iso2: countryIso2 }, order: 'effective_date.asc' }),
            fetchFromSupabase('syrb_measures', { filter: { country_iso2: countryIso2 }, order: 'effective_date.asc' }),
            fetchFromSupabase('bbm_measures', { filter: { country_iso2: countryIso2 }, order: 'effective_date.asc' }),
            instFetch,
        ]);
        
        if (!country || country.length === 0) {
            return null;
        }
        
        const countryInfo = country[0];
        const countryName = countryInfo.country_name || countryInfo.name || '';
        
        const ccyb_snap = ccybSnapshot && ccybSnapshot.length > 0 ? ccybSnapshot[0] : null;
        const syrb_snap = syrbSnapshot && syrbSnapshot.length > 0 ? syrbSnapshot[0] : null;
        const osii_snap = osiiSnapshot && osiiSnapshot.length > 0 ? osiiSnapshot[0] : null;

        const RENAME_MAP = {
            'Loan-to-value (LTV)': 'LTV',
            'Debt-service-to-income (DSTI)': 'DSTI',
            'Loan-to-income (LTI)': 'LTI',
            'DTI': 'DTI',
            'LTI': 'LTI',
            'Loan maturity': 'Maturity',
            'Loan amortisation': 'Amort.',
            'Flexibility quota': 'Flex.',
            'Stress test / sensitivity test': 'Stress T.',
        };
        function isBbmRowActive(m) {
            var statusText = ((m.active_status || '') + ' ' + (m.status || '')).toLowerCase();
            if (!(statusText.indexOf('active') !== -1 || statusText.indexOf('applicable') !== -1)) return false;
            if (['not active', 'inactive', 'revoked', 'deactivated', 'expired'].some(function(x) { return statusText.indexOf(x) !== -1; })) return false;
            return true;
        }
        function bbmShort(measureType) {
            if (!measureType) return '';
            return RENAME_MAP[measureType] || measureType;
        }

        var bbmTypes = [];
        var activeBbmList = [];
        (bbmMeasures || []).filter(isBbmRowActive).forEach(function(m) {
            var short = bbmShort(m.measure_type) || m.measure_type;
            if (short && bbmTypes.indexOf(short) === -1) bbmTypes.push(short);
            activeBbmList.push({
                type: short || m.measure_type,
                status: 'Active',
                date: m.effective_date || m.date,
                description: m.description || '',
            });
        });

        var ccybRate = ccyb_snap ? (parseFloat(ccyb_snap.rate) || 0) : 0;
        var syrbTotal = syrb_snap ? (parseFloat(syrb_snap.total_rate) || 0) : 0;
        var osiiTotal = osii_snap ? (parseFloat(osii_snap.total_rate) || parseFloat(osii_snap.rate) || 0) : 0;
        if (osiiTotal > 0 && osiiTotal < 1) osiiTotal = osiiTotal * 100;

        const currentStatus = {
            ccyb: ccyb_snap ? {
                rate: ccybRate,
                date: ccyb_snap.effective_date || ccyb_snap.date || '',
                status: ccybRate > 0 ? 'Active' : 'Inactive',
            } : null,
            syrb: syrb_snap ? {
                rate: syrbTotal,
                date: '',
                type: (parseFloat(syrb_snap.general_rate) || 0) > 0 ? 'General' : ((parseFloat(syrb_snap.sectoral_rate) || 0) > 0 ? 'Sectoral' : 'General'),
                status: syrbTotal > 0 ? 'Active' : 'Inactive',
            } : null,
            osii: osii_snap ? {
                rate: osiiTotal,
                rate_min: osiiTotal,
                rate_max: osiiTotal,
                rate_display: osiiTotal > 0 ? (osiiTotal === parseInt(osiiTotal, 10) ? (parseInt(osiiTotal, 10) + '%') : osiiTotal.toFixed(2) + '%') : '0%',
                status: osiiTotal > 0 ? 'Active' : 'Inactive',
            } : null,
            bbm: bbmTypes,
            total_capital: null,
        };

        const ccybHistory = (ccybDecisions || []).map(function(d) {
            return {
                date: d.effective_date || d.date || '',
                rate: parseFloat(d.rate) || 0,
                credit_gap: d.credit_gap != null ? parseFloat(d.credit_gap) : null,
            };
        });
        const syrbHistory = (syrbMeasures || []).map(function(m) {
            return {
                date: m.effective_date || m.date || '',
                rate_numeric: parseFloat(m.rate) || 0,
            };
        });

        const activeMeasures = {
            ccyb: currentStatus.ccyb,
            syrb: (syrbMeasures || []).filter(function(m) {
                var s = ((m.active_status || '') + ' ' + (m.status || '')).toLowerCase();
                return s.indexOf('active') !== -1 && s.indexOf('inactive') === -1;
            }),
            bbm: activeBbmList,
            osii: currentStatus.osii,
        };
        
        const instRow = instResult && instResult.length > 0 ? instResult[0] : null;
        const institutionalSetup = instRow ? {
            macroprudential_authority: instRow.macroprudential_authority,
            designated_authority: instRow.designated_authority,
            institutional_model: instRow.institutional_model,
            legal_basis: instRow.legal_basis,
            decision_making_body: instRow.decision_making_body,
            relationship_to_cb: instRow.relationship_to_cb,
            key_regulations: instRow.key_regulations || [],
            ai_description: instRow.ai_description,
            ai_confidence_score: instRow.ai_confidence_score != null ? parseFloat(instRow.ai_confidence_score) : null,
            ai_grounding_notes: instRow.ai_grounding_notes,
            ai_sources_cited: instRow.ai_sources_cited || [],
        } : null;

        return {
            country: countryName,
            iso2: countryIso2,
            current_status: currentStatus,
            institutional_setup: institutionalSetup,
            historical_evolution: { ccyb: ccybHistory, syrb: syrbHistory },
            recent_changes: [],
            active_measures: activeMeasures,
            comparison: {
                regional_average: null,
                similar_countries: [],
            },
            ai_analysis: '',
        };
    } catch (error) {
        console.error('Error fetching country profile from Supabase:', error);
        return null;
    }
}

// Export functions for use in other scripts
window.SupabaseClient = {
    init: initSupabaseClient,
    fetch: fetchFromSupabase,
    fetchCCyBDecisions: fetchCCyBDecisions,
    fetchLatestCCyBSnapshot: fetchLatestCCyBSnapshot,
    fetchDTILTIRules: fetchDTILTIRules,
    fetchLTVRules: fetchLTVRules,
    fetchCCyBTrend: fetchCCyBTrend,
    fetchCountryProfile: fetchCountryProfile,
    isEnabled: isSupabaseEnabled,
};
