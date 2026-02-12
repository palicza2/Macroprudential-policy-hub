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
        const [country, ccybSnapshot, syrbSnapshot, osiiSnapshot, ccybDecisions, syrbMeasures, bbmMeasures] = await Promise.all([
            fetchFromSupabase('countries', { filter: { iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('mv_latest_ccyb_snapshot', { filter: { country_iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('mv_latest_syrb_snapshot', { filter: { country_iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('mv_latest_osii_snapshot', { filter: { country_iso2: countryIso2 }, limit: 1 }),
            fetchFromSupabase('ccyb_decisions', { filter: { country_iso2: countryIso2 }, order: 'effective_date.asc' }),
            fetchFromSupabase('syrb_measures', { filter: { country_iso2: countryIso2 }, order: 'effective_date.asc' }),
            fetchFromSupabase('bbm_measures', { filter: { country_iso2: countryIso2 }, order: 'effective_date.asc' }),
        ]);
        
        if (!country || country.length === 0) {
            return null;
        }
        
        const countryInfo = country[0];
        const countryName = countryInfo.country_name || countryInfo.name || '';
        
        // Transform to countries_data format
        const ccyb_snap = ccybSnapshot && ccybSnapshot.length > 0 ? ccybSnapshot[0] : null;
        const syrb_snap = syrbSnapshot && syrbSnapshot.length > 0 ? syrbSnapshot[0] : null;
        const osii_snap = osiiSnapshot && osiiSnapshot.length > 0 ? osiiSnapshot[0] : null;
        
        // BBM types
        const bbmTypes = [];
        if (bbmMeasures) {
            const activeBBM = bbmMeasures.filter(m => 
                m.active_status === 'Active' || m.status === 'Active'
            );
            activeBBM.forEach(m => {
                const measureType = m.measure_type;
                if (measureType && !bbmTypes.includes(measureType)) {
                    bbmTypes.push(measureType);
                }
            });
        }
        
        // Current status (Materialized Views use different column names)
        const currentStatus = {
            ccyb: ccyb_snap ? {
                rate: parseFloat(ccyb_snap.rate) || 0.0,
                date: ccyb_snap.effective_date || '',  // Materialized View uses effective_date
                status: (parseFloat(ccyb_snap.rate) || 0) > 0 ? 'Active' : 'Inactive',
            } : null,
            syrb: syrb_snap ? {
                rate: parseFloat(syrb_snap.total_rate) || 0.0,  // Materialized View uses total_rate
                date: '',  // Materialized View doesn't have date field
                type: (parseFloat(syrb_snap.general_rate) || 0) > 0 ? 'General' : ((parseFloat(syrb_snap.sectoral_rate) || 0) > 0 ? 'Sectoral' : 'General'),
                status: (parseFloat(syrb_snap.total_rate) || 0) > 0 ? 'Active' : 'Inactive',
            } : null,
            osii: osii_snap ? {
                rate: parseFloat(osii_snap.total_rate) || 0.0,  // Materialized View uses total_rate
                status: (parseFloat(osii_snap.total_rate) || 0) > 0 ? 'Active' : 'Inactive',
            } : null,
            bbm: bbmTypes,
            total_capital: null,
        };
        
        // Historical evolution
        const ccybHistory = (ccybDecisions || []).map(d => ({
            date: d.effective_date || '',
            rate: parseFloat(d.rate) || 0.0,
            credit_gap: d.credit_gap ? parseFloat(d.credit_gap) : null,
        }));
        
        const syrbHistory = (syrbMeasures || []).map(m => ({
            date: m.effective_date || '',
            rate_numeric: parseFloat(m.rate) || 0.0,
        }));
        
        const historicalEvolution = {
            ccyb: ccybHistory,
            syrb: syrbHistory,
        };
        
        // Active measures
        const activeMeasures = {
            ccyb: currentStatus.ccyb,
            syrb: (syrbMeasures || []).filter(m => 
                m.active_status === 'Active' || m.status === 'Active'
            ),
            bbm: (bbmMeasures || []).filter(m => 
                m.active_status === 'Active' || m.status === 'Active'
            ),
            osii: currentStatus.osii,
        };
        
        return {
            country: countryName,
            iso2: countryIso2,
            current_status: currentStatus,
            historical_evolution: historicalEvolution,
            recent_changes: [], // TODO: Calculate from historical data
            active_measures: activeMeasures,
            comparison: {
                regional_average: null,
                similar_countries: [],
            },
            ai_analysis: '', // Would need separate fetch
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
