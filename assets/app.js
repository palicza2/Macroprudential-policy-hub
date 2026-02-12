function toggleSidebar() {
    var sidebar = document.getElementById('sidebar');
    var overlay = document.querySelector('.overlay');
    if (sidebar) sidebar.classList.toggle('active');
    if (overlay) overlay.classList.toggle('active');
}

function activateTab(tabName, updateHash, clickedLink) {
    // updateHash defaults to true
    if (updateHash === undefined) updateHash = true;
    
    var buttons = document.querySelectorAll('.tab-btn');
    var contents = document.querySelectorAll('.tab-content');
    var navLinks = document.querySelectorAll('.nav-link[data-tab]');
    buttons.forEach(function(btn) {
        var isActive = btn.dataset.tab === tabName;
        btn.classList.toggle('active', isActive);
        btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
        btn.setAttribute('tabindex', isActive ? '0' : '-1');
    });
    contents.forEach(function(section) {
        section.classList.toggle('active', section.id === 'tab-' + tabName);
    });
    
    // Only activate the clicked link, not all links with the same data-tab
    // If clickedLink is provided, only activate that specific link
    // Otherwise, activate only the main tab link (not sub-navs)
    var hasActiveSubNav = false;
    navLinks.forEach(function(link) {
        var isActive = false;
        if (clickedLink && link === clickedLink) {
            // This is the clicked link, activate it
            isActive = true;
            // Check if this is a sub-nav
            if (link.classList.contains('sub-nav')) {
                hasActiveSubNav = true;
            }
        } else if (!clickedLink) {
            // No specific link clicked, activate only main tab links (not sub-navs)
            isActive = link.dataset.tab === tabName && !link.classList.contains('sub-nav');
        }
        link.classList.toggle('active', isActive);
    });
    
    // Handle collapsible sections for capital and borrower measures
    // If no sub-nav is active, collapse the section
    var sections = ['capital', 'borrower'];
    sections.forEach(function(sectionName) {
        var section = document.querySelector('.nav-section[data-section="' + sectionName + '"]');
        var header = document.querySelector('.nav-section-header[data-section="' + sectionName + '"]');
        if (section && header) {
            if (tabName === sectionName && hasActiveSubNav) {
                // Expand section if this tab is active and has active sub-nav
                section.classList.remove('collapsed');
                header.classList.remove('collapsed');
            } else if (tabName !== sectionName || !hasActiveSubNav) {
                // Collapse section if different tab or no active sub-nav
                section.classList.add('collapsed');
                header.classList.add('collapsed');
            }
        }
    });
    
    // Update URL hash for shareable links
    if (updateHash) {
        var currentHash = window.location.hash;
        var newHash = '#' + tabName;
        
        // Preserve country parameter if exists
        if (currentHash && currentHash.indexOf('country=') !== -1) {
            var countryMatch = currentHash.match(/country=([^&]+)/);
            if (countryMatch) {
                newHash += '&country=' + countryMatch[1];
            }
        }
        
        // Use replaceState to avoid adding to history
        if (window.location.hash !== newHash) {
            window.history.replaceState(null, '', newHash);
        }
    }
    
    // Update page title based on active tab
    var titleMap = {
        'overview': 'Overview - EU Macroprudential Dashboard',
        'news': 'Latest News - EU Macroprudential Dashboard',
        'capital': 'Capital Measures - EU Macroprudential Dashboard',
        'borrower': 'Borrower Measures - EU Macroprudential Dashboard',
        'country-profiles': 'Country Profiles - EU Macroprudential Dashboard',
        'knowledge-graph': 'Knowledge Graph - EU Macroprudential Dashboard',
        'about': 'About - EU Macroprudential Dashboard'
    };
    
    var newTitle = titleMap[tabName] || 'EU Macroprudential Dashboard';
    document.title = newTitle;
    
    // Re-render Mermaid diagrams when About tab becomes active
    if (tabName === 'about' && typeof mermaid !== 'undefined') {
        setTimeout(function() {
            var mermaidElements = document.querySelectorAll('.mermaid');
            mermaidElements.forEach(function(element) {
                // Remove any existing processed markers
                element.removeAttribute('data-processed');
                // Get the original text content
                var graphDefinition = element.textContent.trim();
                // Clear and reset
                element.innerHTML = '';
                element.textContent = graphDefinition;
            });
            // Re-initialize Mermaid
            try {
                mermaid.run({
                    querySelector: '.mermaid'
                });
            } catch (error) {
                console.error('Mermaid rendering error:', error);
            }
        }, 200);
    }
}

function initTabs() {
    var tabList = document.querySelector('.tabs');
    if (tabList) tabList.setAttribute('role', 'tablist');

    var buttons = document.querySelectorAll('.tab-btn');
    buttons.forEach(function(btn) {
        btn.setAttribute('role', 'tab');
        btn.setAttribute('aria-selected', btn.classList.contains('active') ? 'true' : 'false');
        btn.addEventListener('click', function() {
            activateTab(btn.dataset.tab);
        });
    });

    var navLinks = document.querySelectorAll('.nav-link[data-tab]');
    navLinks.forEach(function(link) {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            // Handle section header clicks (toggle collapse)
            if (link.classList.contains('nav-section-header')) {
                var sectionName = link.dataset.section;
                var section = document.querySelector('.nav-section[data-section="' + sectionName + '"]');
                if (section) {
                    var isCollapsed = section.classList.contains('collapsed');
                    if (isCollapsed) {
                        section.classList.remove('collapsed');
                        link.classList.remove('collapsed');
                        // Activate the first sub-nav if section is expanded
                        var firstSubNav = section.querySelector('.sub-nav');
                        if (firstSubNav) {
                            activateTab(link.dataset.tab, true, firstSubNav);
                        } else {
                            activateTab(link.dataset.tab, true, link);
                        }
                    } else {
                        // Collapse section
                        section.classList.add('collapsed');
                        link.classList.add('collapsed');
                        activateTab(link.dataset.tab, true, link);
                    }
                }
            } else {
                // Pass the clicked link to activateTab so only this link becomes active
                activateTab(link.dataset.tab, true, link);
            }

            // Optional in-page anchor jump (e.g., #ccyb-section under Capital)
            var href = link.getAttribute('href') || '';
            if (href && href.indexOf('#') === 0 && href.length > 1) {
                var target = document.querySelector(href);
                if (target) {
                    // wait a tick so the tab content is visible
                    setTimeout(function() {
                        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    }, 0);
                }
            }
        });
    });
    
    // Handle URL hash on page load
    function handleInitialHash() {
        var hash = window.location.hash;
        if (hash) {
            // Check for section hash (e.g., #ccyb-section, #syrb-section, etc.)
            var sectionMatch = hash.match(/^#([^-]+)-section/);
            if (sectionMatch) {
                var sectionName = sectionMatch[1];
                // Find the nav-link that matches this section
                var matchingLink = Array.from(navLinks).find(function(link) {
                    var href = link.getAttribute('href') || '';
                    return href === hash || href === '#' + sectionName + '-section';
                });
                if (matchingLink) {
                    activateTab(matchingLink.dataset.tab, false, matchingLink);
                    return;
                }
            }
            
            // Check for tab name in hash (e.g., #capital, #news, etc.)
            var tabMatch = hash.match(/^#([^&]+)/);
            if (tabMatch) {
                var tabName = tabMatch[1];
                // Find the main nav-link for this tab (not sub-navs)
                var mainLink = Array.from(navLinks).find(function(link) {
                    return link.dataset.tab === tabName && !link.classList.contains('sub-nav');
                });
                if (mainLink) {
                    activateTab(tabName, false, mainLink);
                } else {
                    // Fallback: activate tab without specific link
                    activateTab(tabName, false);
                }
            }
        }
    }
    
    // Handle hash changes (back/forward button)
    window.addEventListener('hashchange', function() {
        handleInitialHash();
    });
    
    // Initial load
    handleInitialHash();
}

function initPlotFilter() {
    var input = document.getElementById('ccyb-country-filter');
    if (!input) return;

    input.addEventListener('input', function(e) {
        var plot = document.getElementById('ccyb_ts_plot');
        if (!plot || !window.Plotly || !plot.data) return;

        var tokens = e.target.value.toUpperCase().trim().split(/[\s,]+/).filter(Boolean);
        var visibility = plot.data.map(function(trace) {
            if (tokens.length === 0) return true;
            var name = (trace.name || '').toUpperCase();
            return tokens.some(function(token) { return name.indexOf(token) !== -1; }) ? true : 'legendonly';
        });

        window.Plotly.restyle(plot, { visible: visibility });
    });
}

function initResize() {
    window.addEventListener('resize', function() {
        var plot = document.getElementById('ccyb_ts_plot');
        if (plot && window.Plotly) {
            window.Plotly.Plots.resize(plot);
        }
    });
}

function initNewsFilters() {
    var search = document.getElementById('news-search');
    var clearBtn = document.getElementById('news-clear');
    var checkboxes = document.querySelectorAll('.filter-option input[data-filter]');
    var cards = document.querySelectorAll('.news-card');
    var tagPills = document.querySelectorAll('.tag-pill[data-tag]');

    if (!cards.length) return;

    function getSelectedTags() {
        return Array.from(checkboxes)
            .filter(function(cb) { return cb.checked; })
            .map(function(cb) { return cb.dataset.filter; });
    }

    function applyFilters() {
        var query = (search ? search.value : '').toLowerCase().trim();
        var selected = getSelectedTags();

        cards.forEach(function(card) {
            var tags = (card.dataset.tags || '').split(/\s+/).filter(Boolean);
            var text = (card.dataset.search || '').toLowerCase();

            var matchesQuery = !query || text.indexOf(query) !== -1;
            var matchesTags = selected.length === 0 || selected.some(function(tag) { return tags.indexOf(tag) !== -1; });

            card.style.display = (matchesQuery && matchesTags) ? 'flex' : 'none';
        });
    }

    if (search) {
        search.addEventListener('input', applyFilters);
    }

    checkboxes.forEach(function(cb) {
        cb.addEventListener('change', applyFilters);
    });

    if (clearBtn) {
        clearBtn.addEventListener('click', function() {
            if (search) search.value = '';
            checkboxes.forEach(function(cb) { cb.checked = false; });
            applyFilters();
        });
    }

    tagPills.forEach(function(pill) {
        pill.addEventListener('click', function() {
            var tag = pill.dataset.tag;
            var target = Array.from(checkboxes).find(function(cb) { return cb.dataset.filter === tag; });
            if (target) {
                target.checked = true;
                applyFilters();
            } else if (search) {
                search.value = tag;
                applyFilters();
            }
        });
    });
}

function initCountryProfiles() {
    var selector = document.getElementById('country-selector');
    var content = document.getElementById('country-profile-content');
    
    if (!selector || !content) {
        console.warn('Country profiles elements not found');
        return;
    }
    
    // Load countries list from embedded data
    var countriesData = window.countriesData || {};
    console.log('Countries data loaded:', Object.keys(countriesData).length, 'countries');
    
    if (!countriesData || Object.keys(countriesData).length === 0) {
        console.warn('No countries data available');
        selector.innerHTML = '<option value="">No countries data available. Please regenerate the report.</option>';
        return;
    }
    
    var countries = Object.keys(countriesData).sort();
    
    // Populate selector
    countries.forEach(function(country) {
        var option = document.createElement('option');
        option.value = country;
        option.textContent = country;
        selector.appendChild(option);
    });
    
    // Default country: Austria (or first country if Austria not available)
    var defaultCountry = 'Austria';
    if (countries.indexOf(defaultCountry) === -1 && countries.length > 0) {
        defaultCountry = countries[0];
    }
    
    // Handle country selection
    selector.addEventListener('change', async function(e) {
        var country = e.target.value;
        if (country) {
            // Try to load from Supabase first, fallback to static data
            var profileData = countriesData[country] || null;
            await loadCountryProfile(country, profileData);
            content.style.display = 'block';
            
            // Update URL hash with country parameter
            var currentHash = window.location.hash;
            var tabName = 'country-profiles';
            var newHash = '#' + tabName + '&country=' + encodeURIComponent(country);
            
            if (window.location.hash !== newHash) {
                window.history.replaceState(null, '', newHash);
            }
        } else {
            content.style.display = 'none';
            // Remove country from hash if no country selected
            var currentHash = window.location.hash;
            if (currentHash && currentHash.indexOf('&country=') !== -1) {
                var newHash = currentHash.replace(/&country=[^&]*/, '');
                window.history.replaceState(null, '', newHash);
            }
        }
    });
    
    // Check URL hash for country parameter
    async function checkHashForCountry() {
        var hash = window.location.hash;
        var selectedCountry = null;
        
        if (hash) {
            // Check for country parameter in hash (e.g., #country-profiles&country=Hungary)
            var countryMatch = hash.match(/country=([^&]+)/);
            if (countryMatch) {
                var countryFromHash = decodeURIComponent(countryMatch[1]);
                // Accept country even if not in static countriesData (will load from Supabase)
                selectedCountry = countryFromHash;
                
                // Ensure country-profiles tab is active
                var tabMatch = hash.match(/^#([^&]+)/);
                if (tabMatch && tabMatch[1] !== 'country-profiles') {
                    var mainLink = Array.from(document.querySelectorAll('.nav-link[data-tab]')).find(function(link) {
                        return link.dataset.tab === 'country-profiles' && !link.classList.contains('sub-nav');
                    });
                    activateTab('country-profiles', false, mainLink);
                }
            }
        }
        
        // If no country from hash, use default
        if (!selectedCountry && defaultCountry) {
            selectedCountry = defaultCountry;
        }
        
        // Load selected/default country
        if (selectedCountry) {
            selector.value = selectedCountry;
            var profileData = countriesData[selectedCountry] || null;
            await loadCountryProfile(selectedCountry, profileData);
            content.style.display = 'block';
        }
    }
    
    // Check on initial load
    checkHashForCountry();
    
    // Check on hash change
    window.addEventListener('hashchange', checkHashForCountry);
}

async function loadCountryProfile(country, profileData) {
    // Try to load from Supabase if enabled and profileData is not provided
    if (window.useSupabase && window.SupabaseClient && window.SupabaseClient.isEnabled()) {
        if (!profileData || Object.keys(profileData).length === 0) {
            try {
                // Get ISO2 from profileData or countriesData
                var iso2 = null;
                if (profileData && profileData.iso2) {
                    iso2 = profileData.iso2;
                } else if (window.countriesData && window.countriesData[country] && window.countriesData[country].iso2) {
                    iso2 = window.countriesData[country].iso2;
                }
                
                if (iso2) {
                    console.log('Loading country profile from Supabase for', country, '(', iso2, ')');
                    var supabaseProfile = await window.SupabaseClient.fetchCountryProfile(iso2);
                    if (supabaseProfile) {
                        profileData = supabaseProfile;
                        // Cache in window.countriesData for future use
                        if (!window.countriesData) {
                            window.countriesData = {};
                        }
                        window.countriesData[country] = profileData;
                        console.log('Loaded country profile from Supabase');
                    }
                }
            } catch (error) {
                console.error('Error loading country profile from Supabase:', error);
                // Fall through to use provided profileData or static data
            }
        }
    }
    
    if (!profileData) {
        console.warn('No profile data available for', country);
        return;
    }
    
    // Update current status
    renderCurrentStatus(profileData.current_status || {});
    
    // Update historical evolution
    renderHistoricalEvolution(country, profileData.historical_evolution || {});
    
    // Update recent changes
    renderRecentChanges(profileData.recent_changes || []);
    
    // Update active measures (tab-based)
    renderActiveMeasuresTabbed(profileData.active_measures || {});
    
    // Update AI inflection points
    renderAIInflectionPoints(profileData.ai_inflection_points || []);
    
    // Update AI analysis
    renderAIAnalysis(profileData.ai_analysis || '');
    
    // Update comparison
    renderComparison(profileData.comparison || {});
}

function renderCurrentStatus(status) {
    var grid = document.getElementById('key-measures-grid');
    if (!grid) return;
    
    grid.innerHTML = '';
    
    // CCyB Card
    var ccybCard = createKeyMeasureCard({
        icon: '🏦',
        label: 'CCyB',
        description: 'Countercyclical Capital Buffer',
        rate: status.ccyb?.rate || 0,
        status: status.ccyb?.status || 'Inactive',
        color: '#3b82f6'
    });
    grid.appendChild(ccybCard);
    
    // SyRB Card - General + Sectoral (sSyRB)
    var syrbRate = status.syrb?.rate || 0;
    var ssyrbList = status.syrb?.ssyrb || [];
    var syrbContent = '';
    if (syrbRate > 0 || ssyrbList.length > 0) {
        syrbContent = '<div class="syrb-details-list">';
        if (syrbRate > 0) {
            syrbContent += '<div class="syrb-item-line">SyRB: ' + syrbRate.toFixed(2) + '%</div>';
        }
        ssyrbList.forEach(function(ssyrb) {
            var exposureLabel = ssyrb.exposure || 'Sectoral';
            syrbContent += '<div class="syrb-item-line">' + exposureLabel + ' sSyRB: ' + ssyrb.rate.toFixed(2) + '%</div>';
        });
        syrbContent += '</div>';
    }
    
    var syrbCard = createKeyMeasureCard({
        icon: '🛡️',
        label: 'SyRB',
        description: 'Systemic Risk Buffer',
        rate: syrbRate,
        status: (syrbRate > 0 || ssyrbList.length > 0) ? 'Active' : 'Inactive',
        color: '#10b981',
        customContent: syrbContent
    });
    grid.appendChild(syrbCard);
    
    // O-SII Card - min-max intervallum
    var osiiRateDisplay = '0%';
    if (status.osii && status.osii.rate_min !== undefined && status.osii.rate_max !== undefined) {
        if (status.osii.rate_max > 0) {
            if (status.osii.rate_min === status.osii.rate_max) {
                osiiRateDisplay = status.osii.rate_max.toFixed(2) + '%';
            } else {
                osiiRateDisplay = status.osii.rate_min.toFixed(2) + '-' + status.osii.rate_max.toFixed(2) + '%';
            }
        }
    } else if (status.osii?.rate) {
        osiiRateDisplay = status.osii.rate.toFixed(2) + '%';
    }
    
    var osiiCard = createKeyMeasureCard({
        icon: '🏛️',
        label: 'O-SII',
        description: 'Other Systemically Important Inst.',
        rate: null,  // Nem használjuk a rate mezőt, mert saját display-tel rendelkezünk
        rateDisplay: osiiRateDisplay,  // Egyedi rate display
        status: status.osii?.status || 'Inactive',
        color: '#ef4444'
    });
    grid.appendChild(osiiCard);
    
    // BBM Card
    var bbmActive = status.bbm && status.bbm.length > 0;
    var bbmCard = createKeyMeasureCard({
        icon: '📋',
        label: 'BBM',
        description: 'Borrower-Based Measures',
        rate: null,
        status: bbmActive ? 'Active' : 'Inactive',
        color: '#8b5cf6',
        bbmTypes: bbmActive ? status.bbm : []
    });
    grid.appendChild(bbmCard);
}

function createKeyMeasureCard(config) {
    var card = document.createElement('div');
    card.className = 'key-measure-card';
    card.style.borderLeftColor = config.color;
    
    var statusClass = config.status === 'Active' ? 'active' : 'inactive';
    
    // Rate display: ha van egyedi rateDisplay, azt használjuk, különben számoljuk
    var rateDisplay;
    if (config.rateDisplay !== undefined) {
        rateDisplay = config.rateDisplay;
    } else if (config.rate !== null && config.rate !== undefined) {
        rateDisplay = config.rate.toFixed(2) + '%';
    } else if (config.bbmTypes && config.bbmTypes.length > 0) {
        // BBM esetén ne mutassunk "Yes"-t, csak a listát
        rateDisplay = '';  // Üres, mert a lista alatt lesz
    } else {
        rateDisplay = '0%';
    }
    
    var bbmContent = '';
    if (config.bbmTypes && config.bbmTypes.length > 0) {
        bbmContent = '<div class="bbm-types-list">' +
                    config.bbmTypes.map(function(type) {
                        return '<span class="bbm-type-check">✔ ' + type + '</span>';
                    }).join('') +
                    '</div>';
    }
    
    var customContent = config.customContent || '';
    
    card.innerHTML = '<div class="key-measure-header">' +
                    '<span class="key-measure-icon">' + config.icon + '</span>' +
                    '<div class="key-measure-info">' +
                    '<div class="key-measure-label">' + config.label + '</div>' +
                    '<div class="key-measure-desc">' + config.description + '</div>' +
                    '</div>' +
                    '</div>' +
                    '<div class="key-measure-body">' +
                    (rateDisplay ? '<div class="key-measure-rate">' + rateDisplay + '</div>' : '') +
                    '<span class="key-measure-status-badge ' + statusClass + '">' + config.status + '</span>' +
                    '</div>' +
                    bbmContent +
                    customContent;
    
    return card;
}

// Store evolution data globally for period filtering
window.countryEvolutionData = null;
window.currentCountry = null;

function renderHistoricalEvolution(country, evolution) {
    var chartDiv = document.getElementById('country-evolution-chart');
    if (!chartDiv || !window.Plotly) return;
    
    // Store data globally
    window.countryEvolutionData = evolution;
    window.currentCountry = country;
    
    // Initial render with 5Y period
    updateChartPeriod('5Y');
}

function updateChartPeriod(period) {
    var chartDiv = document.getElementById('country-evolution-chart');
    if (!chartDiv || !window.Plotly || !window.countryEvolutionData) return;
    
    var evolution = window.countryEvolutionData;
    var country = window.currentCountry;
    var traces = [];
    var allDates = [];
    
    // Collect all dates
    if (evolution.ccyb && evolution.ccyb.length > 0) {
        evolution.ccyb.forEach(function(d) {
            if (d.date) allDates.push(new Date(d.date));
        });
    }
    if (evolution.syrb && evolution.syrb.length > 0) {
        evolution.syrb.forEach(function(d) {
            if (d.date) allDates.push(new Date(d.date));
        });
    }
    
    if (allDates.length === 0) {
        chartDiv.innerHTML = '<p style="color: #64748b; padding: 20px;">No historical data available.</p>';
        return;
    }
    
    // Calculate date range based on period
    var now = new Date();
    var minDate = new Date(now);
    
    if (period === '1Y') {
        minDate.setFullYear(now.getFullYear() - 1);
    } else if (period === '5Y') {
        minDate.setFullYear(now.getFullYear() - 5);
    } else {
        // Max - use earliest date
        minDate = new Date(Math.min.apply(null, allDates));
    }
    
    // Filter CCyB data
    if (evolution.ccyb && evolution.ccyb.length > 0) {
        var ccybData = evolution.ccyb.filter(function(d) {
            if (!d.date) return false;
            var date = new Date(d.date);
            return date >= minDate;
        });
        
        if (ccybData.length > 0) {
            traces.push({
                x: ccybData.map(function(d) { return d.date; }),
                y: ccybData.map(function(d) { return d.rate || 0; }),
                name: 'CCyB',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#3b82f6', width: 2 }
            });
        }
    }
    
    // Filter SyRB data
    if (evolution.syrb && evolution.syrb.length > 0) {
        var syrbData = evolution.syrb.filter(function(d) {
            if (!d.date) return false;
            var date = new Date(d.date);
            return date >= minDate;
        });
        
        if (syrbData.length > 0) {
            traces.push({
                x: syrbData.map(function(d) { return d.date; }),
                y: syrbData.map(function(d) { return d.rate_numeric || 0; }),
                name: 'SyRB',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#10b981', width: 2 }
            });
        }
    }
    
    if (traces.length === 0) {
        chartDiv.innerHTML = '<p style="color: #64748b; padding: 20px;">No data available for selected period.</p>';
        return;
    }
    
    var layout = {
        title: 'Rate progression for CCyB vs SyRB (' + (period === 'max' ? 'All Time' : period) + ')',
        xaxis: { title: 'Date' },
        yaxis: { title: 'Rate (%)' },
        hovermode: 'x unified',
        height: 400,
        margin: { t: 50, r: 20, b: 50, l: 60 },
        legend: { x: 0, y: 1 }
    };
    
    Plotly.newPlot(chartDiv, traces, layout, { responsive: true, displayModeBar: false });
    
    // Update active period button
    var periodButtons = document.querySelectorAll('.period-btn');
    periodButtons.forEach(function(btn) {
        if (btn.dataset.period === period) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
}

// Initialize period selector
function initChartPeriodSelector() {
    var periodButtons = document.querySelectorAll('.period-btn');
    periodButtons.forEach(function(btn) {
        btn.addEventListener('click', function() {
            var period = this.dataset.period;
            updateChartPeriod(period);
        });
    });
}

function renderRecentChanges(changes) {
    var list = document.getElementById('recent-changes-list');
    if (!list) return;
    
    list.innerHTML = '';
    
    if (changes.length === 0) {
        list.innerHTML = '<p style="color: #64748b; padding: 20px;">No recent changes in the last 12 months.</p>';
        return;
    }
    
    changes.forEach(function(change) {
        var div = document.createElement('div');
        div.className = 'change-item';
        var dateStr = change.date ? (typeof change.date === 'string' ? change.date : change.date.toISOString().split('T')[0]) : 'N/A';
        div.innerHTML = '<span class="change-date">' + dateStr + '</span>' +
                       '<span class="change-type">' + change.type + '</span>' +
                       '<span class="change-detail">' + change.change + '</span>';
        list.appendChild(div);
    });
}

function renderActiveMeasuresTabbed(measures) {
    // Borrower-Based tab
    var borrowerContainer = document.getElementById('active-measures-borrower');
    if (borrowerContainer) {
        renderBBMMeasures(measures.bbm || [], borrowerContainer);
    }
    
    // Capital-Based tab
    var capitalContainer = document.getElementById('active-measures-capital');
    if (capitalContainer) {
        renderCapitalMeasures(measures, capitalContainer);
    }
    
    // Initialize tabs
    initMeasuresTabs();
}

function renderBBMMeasures(bbmMeasures, container) {
    container.innerHTML = '';
    
    // Szűrjük ki a nem aktív eszközöket
    var activeBBM = bbmMeasures.filter(function(bbm) {
        var status = (bbm.status || '').toString().toLowerCase();
        // Kihagyjuk a "not active", "inactive", "revoked", stb. státuszú eszközöket
        return !status || (!status.includes('not active') && 
                          !status.includes('inactive') && 
                          !status.includes('revoked') && 
                          !status.includes('deactivated') &&
                          !status.includes('expired'));
    });
    
    if (!activeBBM || activeBBM.length === 0) {
        container.innerHTML = '<p style="color: #64748b; padding: 20px; text-align: center;">No active BBM measures available.</p>';
        return;
    }
    
    // Csoportosítás típus szerint
    var groupedByType = {};
    activeBBM.forEach(function(bbm) {
        var type = bbm.type || 'Other';
        if (!groupedByType[type]) {
            groupedByType[type] = [];
        }
        groupedByType[type].push(bbm);
    });
    
    // Map BBM types to icons and labels
    var bbmTypeMap = {
        'LTV': { icon: '🏠', label: 'LTV Limits (Loan-to-Value)' },
        'DTI': { icon: '📊', label: 'DTI Ratio (Debt-to-Income)' },
        'LTI': { icon: '📊', label: 'LTI Ratio (Loan-to-Income)' },
        'DSTI': { icon: '📊', label: 'DSTI Ratio (Debt-Service-to-Income)' },
        'Maturity': { icon: '⏰', label: 'Maturity Limits (Loan Duration)' },
        'Other': { icon: '📋', label: 'Other BBM' }
    };
    
    // Minden típusra egy kártya, összevont leírásokkal
    Object.keys(groupedByType).forEach(function(type) {
        var typeMeasures = groupedByType[type];
        var typeInfo = bbmTypeMap[type] || bbmTypeMap['Other'];
        
        // Összevonjuk a leírásokat - kivonjuk a lényeges információkat
        var summary = extractBBMSummary(typeMeasures);
        
        var card = document.createElement('div');
        card.className = 'bbm-measure-card';
        card.innerHTML = '<div class="bbm-measure-header">' +
                        '<div class="bbm-measure-title">' +
                        '<span class="bbm-measure-icon">' + typeInfo.icon + '</span>' +
                        '<span class="bbm-measure-name">' + typeInfo.label + '</span>' +
                        '</div>' +
                        '<span class="bbm-measure-status-badge active">ACTIVE</span>' +
                        '</div>' +
                        '<div class="bbm-measure-description">' +
                        '<div class="bbm-summary">' + summary + '</div>' +
                        (typeMeasures.length > 1 ? '<div class="bbm-count">(' + typeMeasures.length + ' active measure' + (typeMeasures.length > 1 ? 's' : '') + ')</div>' : '') +
                        '</div>';
        container.appendChild(card);
    });
}

function extractBBMSummary(measures) {
    if (!measures || measures.length === 0) return 'No details available.';
    
    // Ha csak egy measure van, használjuk annak leírását (rövidítve)
    if (measures.length === 1) {
        var desc = measures[0].description || '';
        // Kivonjuk a lényeges részeket (számok, százalékok, kulcsszavak)
        var summary = extractKeyInfo(desc);
        return summary || (desc.length > 200 ? desc.substring(0, 200) + '...' : desc);
    }
    
    // Több measure esetén összevonjuk
    var keyPoints = [];
    var allDescriptions = measures.map(function(m) { return m.description || ''; }).join(' ');
    
    // Kivonjuk a kulcsszavakat és számokat
    var summary = extractKeyInfo(allDescriptions);
    
    // Ha van dátum információ, hozzáadjuk
    var dates = measures.map(function(m) { return m.date; }).filter(Boolean);
    if (dates.length > 0) {
        var latestDate = dates.sort().reverse()[0];
        summary += ' (Effective: ' + latestDate + ')';
    }
    
    return summary || 'Multiple active ' + (measures[0].type || 'BBM') + ' measures in place.';
}

function extractKeyInfo(description) {
    if (!description) return '';
    
    // Kivonjuk a számokat és százalékokat
    var numbers = description.match(/\d+(?:\.\d+)?%?/g);
    var keyNumbers = numbers ? numbers.slice(0, 3).join(', ') : '';
    
    // Kivonjuk a kulcsszavakat
    var keywords = [];
    var lowerDesc = description.toLowerCase();
    
    if (lowerDesc.includes('ltv') || lowerDesc.includes('loan-to-value')) keywords.push('LTV');
    if (lowerDesc.includes('dti') || lowerDesc.includes('debt-to-income')) keywords.push('DTI');
    if (lowerDesc.includes('lti') || lowerDesc.includes('loan-to-income')) keywords.push('LTI');
    if (lowerDesc.includes('dsti') || lowerDesc.includes('debt-service')) keywords.push('DSTI');
    if (lowerDesc.includes('maturity') || lowerDesc.includes('duration')) keywords.push('Maturity');
    if (lowerDesc.includes('limit')) keywords.push('Limits');
    if (lowerDesc.includes('ratio')) keywords.push('Ratios');
    
    // Összeállítjuk a summary-t
    var summary = '';
    if (keywords.length > 0) {
        summary = keywords.join(', ') + ' measures';
    }
    if (keyNumbers) {
        summary += (summary ? ': ' : '') + keyNumbers;
    }
    
    // Ha nincs semmi, akkor az első mondatot vesszük
    if (!summary) {
        var firstSentence = description.split(/[.!?]/)[0];
        summary = firstSentence.length > 150 ? firstSentence.substring(0, 150) + '...' : firstSentence;
    }
    
    return summary || description.substring(0, 150) + '...';
}

function renderCapitalMeasures(measures, container) {
    container.innerHTML = '';
    
    var hasCapital = false;
    
    // CCyB
    if (measures.ccyb && measures.ccyb.rate > 0) {
        hasCapital = true;
        var ccybCard = createCapitalMeasureCard({
            icon: '🏦',
            name: 'Countercyclical Capital Buffer (CCyB)',
            rate: measures.ccyb.rate,
            date: measures.ccyb.date,
            creditGap: measures.ccyb.credit_gap
        });
        container.appendChild(ccybCard);
    }
    
    // SyRB - General + Sectoral (sSyRB)
    if (measures.syrb && measures.syrb.length > 0) {
        hasCapital = true;
        // Szeparáljuk a General és Sectoral SyRB-ket
        var generalSyRB = measures.syrb.filter(function(s) { return s.type === 'General' || s.exposure === 'General'; });
        var sectoralSyRB = measures.syrb.filter(function(s) { return s.type === 'Sectoral' || (s.exposure && s.exposure !== 'General'); });
        
        var generalRate = generalSyRB.length > 0 ? generalSyRB[0].rate : 0;
        var totalRate = generalRate;
        sectoralSyRB.forEach(function(s) { totalRate += (s.rate || 0); });
        
        // Részletek listája
        var syrbDetails = '<div class="syrb-measures-list">';
        if (generalRate > 0) {
            syrbDetails += '<div class="syrb-measure-item"><span class="syrb-measure-label">SyRB:</span><span class="syrb-measure-value">' + generalRate.toFixed(2) + '%</span></div>';
        }
        sectoralSyRB.forEach(function(s) {
            var exposureLabel = s.exposure || 'Sectoral';
            syrbDetails += '<div class="syrb-measure-item"><span class="syrb-measure-label">' + exposureLabel + ' sSyRB:</span><span class="syrb-measure-value">' + (s.rate || 0).toFixed(2) + '%</span></div>';
        });
        syrbDetails += '</div>';
        
        var syrbCard = createCapitalMeasureCard({
            icon: '🛡️',
            name: 'Systemic Risk Buffer (SyRB)',
            rate: totalRate,
            count: measures.syrb.length,
            customContent: syrbDetails
        });
        container.appendChild(syrbCard);
    }
    
    // O-SII - min-max intervallum + bankok listája
    if (measures.osii && measures.osii.rate_max > 0) {
        hasCapital = true;
        var osiiRateDisplay = measures.osii.rate_min === measures.osii.rate_max 
            ? measures.osii.rate_max.toFixed(2) + '%'
            : measures.osii.rate_min.toFixed(2) + '-' + measures.osii.rate_max.toFixed(2) + '%';
        
        // Bankok listája
        var banksContent = '';
        if (measures.osii.banks && measures.osii.banks.length > 0) {
            banksContent = '<div class="osii-banks-list">';
            measures.osii.banks.forEach(function(bank) {
                banksContent += '<div class="osii-bank-item">' +
                              '<span class="osii-bank-name">' + (bank.name || 'N/A') + '</span>' +
                              '<span class="osii-bank-rate">' + (bank.rate || 0).toFixed(2) + '%</span>' +
                              (bank.buffer_type ? '<span class="osii-bank-type">(' + bank.buffer_type + ')</span>' : '') +
                              '</div>';
            });
            banksContent += '</div>';
        }
        
        var osiiCard = createCapitalMeasureCard({
            icon: '🏛️',
            name: 'Other Systemically Important Institutions (O-SII)',
            rate: measures.osii.rate_max,  // A max értéket használjuk a display-hez
            rateDisplay: osiiRateDisplay,  // Egyedi rate display
            count: measures.osii.count,
            customContent: banksContent
        });
        container.appendChild(osiiCard);
    }
    
    if (!hasCapital) {
        container.innerHTML = '<p style="color: #64748b; padding: 20px; text-align: center;">No active capital-based measures available.</p>';
    }
}

function createCapitalMeasureCard(config) {
    var card = document.createElement('div');
    card.className = 'capital-measure-card';
    
    // Rate display: ha van egyedi rateDisplay, azt használjuk, különben a rate-t
    var rateDisplay = config.rateDisplay || (config.rate ? config.rate.toFixed(2) + '%' : '0%');
    
    var customContent = config.customContent || '';
    
    card.innerHTML = '<div class="capital-measure-header">' +
                    '<span class="capital-measure-icon">' + config.icon + '</span>' +
                    '<div class="capital-measure-info">' +
                    '<div class="capital-measure-name">' + config.name + '</div>' +
                    '<div class="capital-measure-rate">' + rateDisplay + '</div>' +
                    '</div>' +
                    '</div>' +
                    (config.date ? '<div class="capital-measure-detail">Effective Date: ' + config.date + '</div>' : '') +
                    (config.creditGap !== null && config.creditGap !== undefined ? '<div class="capital-measure-detail">Credit Gap: ' + config.creditGap.toFixed(2) + '%</div>' : '') +
                    (config.count ? '<div class="capital-measure-detail">Active Measures: ' + config.count + '</div>' : '') +
                    customContent;
    return card;
}

function initMeasuresTabs() {
    var tabs = document.querySelectorAll('.measure-tab');
    var panes = document.querySelectorAll('.tab-pane');
    
    tabs.forEach(function(tab) {
        tab.addEventListener('click', function() {
            var targetTab = this.dataset.tab;
            
            // Update active tab
            tabs.forEach(function(t) { t.classList.remove('active'); });
            this.classList.add('active');
            
            // Update active pane
            panes.forEach(function(pane) {
                pane.classList.remove('active');
                if (pane.id === 'active-measures-' + targetTab) {
                    pane.classList.add('active');
                }
            });
        });
    });
}

function renderAIInflectionPoints(points) {
    var container = document.getElementById('ai-inflection-points');
    var card = document.getElementById('ai-inflection-points-card');
    
    if (!container || !card) return;
    
    if (!points || points.length === 0) {
        card.style.display = 'none';
        return;
    }
    
    card.style.display = 'block';
    container.innerHTML = '';
    
    points.forEach(function(point) {
        var pointDiv = document.createElement('div');
        pointDiv.className = 'inflection-point';
        pointDiv.innerHTML = '<div class="inflection-point-header">' +
                            '<span class="inflection-point-date">' + point.date + '</span>' +
                            '<span class="inflection-point-title">' + point.title + '</span>' +
                            '</div>' +
                            '<div class="inflection-point-description">' + point.description + '</div>';
        container.appendChild(pointDiv);
    });
}

function renderActiveMeasures(measures) {
    // Legacy function - redirect to tabbed version
    renderActiveMeasuresTabbed(measures);
    
    // CCyB Section - csak 1 card, ha aktív
    if (measures.ccyb && measures.ccyb.rate > 0) {
        hasMeasures = true;
        var ccybCard = document.createElement('div');
        ccybCard.className = 'measure-card measure-ccyb';
        ccybCard.innerHTML = '<div class="measure-header">' +
                          '<div class="measure-title">' +
                          '<span class="measure-icon">🏦</span>' +
                          '<span class="measure-name">Countercyclical Capital Buffer (CCyB)</span>' +
                          '</div>' +
                          '<div class="measure-rate">' + (measures.ccyb.rate || 0).toFixed(2) + '%</div>' +
                          '</div>' +
                          '<div class="measure-body">' +
                          '<div class="measure-row"><span class="measure-label">Effective Date:</span><span class="measure-value">' + (measures.ccyb.date || 'N/A') + '</span></div>' +
                          (measures.ccyb.credit_gap !== null && measures.ccyb.credit_gap !== undefined ? 
                           '<div class="measure-row"><span class="measure-label">Credit Gap:</span><span class="measure-value">' + measures.ccyb.credit_gap.toFixed(2) + '%</span></div>' : '') +
                          (measures.ccyb.justification ? '<div class="measure-row"><span class="measure-label">Justification:</span><span class="measure-value">' + measures.ccyb.justification + '</span></div>' : '') +
                          '</div>';
        container.appendChild(ccybCard);
    }
    
    // SyRB Section - 1 card az összes aktív SyRB-vel
    if (measures.syrb && measures.syrb.length > 0) {
        hasMeasures = true;
        var syrbCard = document.createElement('div');
        syrbCard.className = 'measure-card measure-syrb';
        
        // Számítsuk ki az összesített rate-et
        var totalRate = measures.syrb.reduce(function(sum, syrb) {
            return sum + (syrb.rate || 0);
        }, 0);
        
        var syrbId = 'syrb-details';
        var header = document.createElement('div');
        header.className = 'measure-header measure-header-collapsible';
        header.setAttribute('data-target', syrbId);
        header.innerHTML = '<div class="measure-title">' +
                          '<span class="measure-icon">🛡️</span>' +
                          '<span class="measure-name">Systemic Risk Buffer (SyRB)</span>' +
                          '<span class="measure-count">(' + measures.syrb.length + ' active measure' + (measures.syrb.length > 1 ? 's' : '') + ')</span>' +
                          '</div>' +
                          '<div class="measure-actions">' +
                          '<div class="measure-rate">' + totalRate.toFixed(2) + '%</div>' +
                          '<span class="collapse-icon">▼</span>' +
                          '</div>';
        syrbCard.appendChild(header);
        
        var body = document.createElement('div');
        body.className = 'measure-body measure-body-collapsible';
        body.id = syrbId;
        
        var itemsContainer = document.createElement('div');
        itemsContainer.className = 'syrb-items';
        
        measures.syrb.forEach(function(syrb, index) {
            var itemDiv = document.createElement('div');
            itemDiv.className = 'syrb-item';
            itemDiv.innerHTML = '<div class="syrb-item-header">' +
                              '<span class="syrb-item-number">#' + (index + 1) + '</span>' +
                              '<span class="syrb-item-type">' + (syrb.type || 'General') + '</span>' +
                              '<span class="syrb-item-rate">' + (syrb.rate || 0).toFixed(2) + '%</span>' +
                              '</div>' +
                              '<div class="syrb-item-details">' +
                              (syrb.exposure ? '<div class="measure-row"><span class="measure-label">Exposure Type:</span><span class="measure-value">' + syrb.exposure + '</span></div>' : '') +
                              (syrb.date ? '<div class="measure-row"><span class="measure-label">Effective Date:</span><span class="measure-value">' + syrb.date + '</span></div>' : '') +
                              (syrb.description ? '<div class="measure-row"><span class="measure-label">Description:</span><span class="measure-value">' + syrb.description + '</span></div>' : '') +
                              '</div>';
            itemsContainer.appendChild(itemDiv);
        });
        
        body.appendChild(itemsContainer);
        syrbCard.appendChild(body);
        container.appendChild(syrbCard);
    }
    
    // BBM Section - 1 card az összes aktív BBM-mel
    if (measures.bbm && measures.bbm.length > 0) {
        hasMeasures = true;
        var bbmCard = document.createElement('div');
        bbmCard.className = 'measure-card measure-bbm';
        var bbmId = 'bbm-details';
        
        var header = document.createElement('div');
        header.className = 'measure-header measure-header-collapsible';
        header.setAttribute('data-target', bbmId);
        header.innerHTML = '<div class="measure-title">' +
                          '<span class="measure-icon">📋</span>' +
                          '<span class="measure-name">Borrower-Based Measures (BBM)</span>' +
                          '<span class="measure-count">(' + measures.bbm.length + ' active measure' + (measures.bbm.length > 1 ? 's' : '') + ')</span>' +
                          '</div>' +
                          '<div class="measure-actions">' +
                          '<span class="measure-status-badge active">Active</span>' +
                          '<span class="collapse-icon">▼</span>' +
                          '</div>';
        bbmCard.appendChild(header);
        
        var body = document.createElement('div');
        body.className = 'measure-body measure-body-collapsible';
        body.id = bbmId;
        
        var itemsContainer = document.createElement('div');
        itemsContainer.className = 'bbm-items';
        
        measures.bbm.forEach(function(bbm, itemIndex) {
            var itemDiv = document.createElement('div');
            itemDiv.className = 'bbm-item';
            var fullDesc = bbm.description || '';
            var isLong = fullDesc.length > 200;
            var displayDesc = isLong ? fullDesc.substring(0, 200) + '...' : fullDesc;
            var descId = bbmId + '-desc-' + itemIndex;
            
            var itemHeader = document.createElement('div');
            itemHeader.className = 'bbm-item-header';
            itemHeader.innerHTML = '<span class="bbm-item-number">#' + (itemIndex + 1) + '</span>' +
                                  '<span class="bbm-item-type">' + (bbm.type || 'BBM') + '</span>' +
                                  (bbm.date ? '<span class="bbm-item-date">' + bbm.date + '</span>' : '');
            itemDiv.appendChild(itemHeader);
            
            var itemDesc = document.createElement('div');
            itemDesc.className = 'bbm-item-description';
            
            var shortDescDiv = document.createElement('div');
            shortDescDiv.className = 'bbm-desc-short';
            shortDescDiv.id = descId + '-short';
            shortDescDiv.textContent = displayDesc;
            itemDesc.appendChild(shortDescDiv);
            
            if (isLong) {
                var fullDescDiv = document.createElement('div');
                fullDescDiv.className = 'bbm-desc-full';
                fullDescDiv.id = descId + '-full';
                fullDescDiv.style.display = 'none';
                fullDescDiv.textContent = fullDesc;
                itemDesc.appendChild(fullDescDiv);
                
                var expandBtn = document.createElement('button');
                expandBtn.className = 'bbm-expand-btn';
                expandBtn.setAttribute('data-target', descId);
                expandBtn.textContent = 'Show more';
                itemDesc.appendChild(expandBtn);
            }
            
            itemDiv.appendChild(itemDesc);
            itemsContainer.appendChild(itemDiv);
        });
        
        body.appendChild(itemsContainer);
        bbmCard.appendChild(body);
        container.appendChild(bbmCard);
    }
    
    // O-SII Section - csak 1 card, ha aktív
    if (measures.osii && measures.osii.rate > 0) {
        hasMeasures = true;
        var osiiCard = document.createElement('div');
        osiiCard.className = 'measure-card measure-osii';
        osiiCard.innerHTML = '<div class="measure-header">' +
                            '<div class="measure-title">' +
                            '<span class="measure-icon">🏛️</span>' +
                            '<span class="measure-name">Other Systemically Important Institutions (O-SII)</span>' +
                            (measures.osii.count ? '<span class="measure-count">(' + measures.osii.count + ' institution' + (measures.osii.count > 1 ? 's' : '') + ')</span>' : '') +
                            '</div>' +
                            '<div class="measure-rate">' + (measures.osii.rate || 0).toFixed(2) + '%</div>' +
                            '</div>' +
                            '<div class="measure-body">' +
                            '<div class="measure-row"><span class="measure-label">Status:</span><span class="measure-value">' + (measures.osii.status || 'Active') + '</span></div>' +
                            '</div>';
        container.appendChild(osiiCard);
    }
    
    if (!hasMeasures) {
        container.innerHTML = '<p style="color: #64748b; padding: 20px; text-align: center;">No active measures details available.</p>';
    } else {
        // Initialize collapsible functionality
        initMeasureCollapsibles();
        initBBMExpandButtons();
    }
}

function initMeasureCollapsibles() {
    var collapsibleHeaders = document.querySelectorAll('.measure-header-collapsible');
    collapsibleHeaders.forEach(function(header) {
        header.addEventListener('click', function() {
            var targetId = this.dataset.target;
            var body = document.getElementById(targetId);
            var icon = this.querySelector('.collapse-icon');
            
            if (body) {
                if (body.style.display === 'none') {
                    body.style.display = 'block';
                    icon.textContent = '▼';
                    icon.style.transform = 'rotate(0deg)';
                } else {
                    body.style.display = 'none';
                    icon.textContent = '▶';
                    icon.style.transform = 'rotate(-90deg)';
                }
            }
        });
    });
}

function initBBMExpandButtons() {
    var expandButtons = document.querySelectorAll('.bbm-expand-btn');
    expandButtons.forEach(function(btn) {
        btn.addEventListener('click', function(e) {
            e.stopPropagation();
            var targetId = this.dataset.target;
            var shortDesc = document.getElementById(targetId + '-short');
            var fullDesc = document.getElementById(targetId + '-full');
            
            if (fullDesc && fullDesc.style.display === 'none') {
                fullDesc.style.display = 'block';
                shortDesc.style.display = 'none';
                this.textContent = 'Show less';
            } else if (fullDesc) {
                fullDesc.style.display = 'none';
                shortDesc.style.display = 'block';
                this.textContent = 'Show more';
            }
        });
    });
}

function renderAIAnalysis(analysis) {
    var container = document.getElementById('country-ai-analysis');
    if (!container) return;
    
    if (analysis) {
        container.innerHTML = '<p>' + analysis + '</p>';
    } else {
        container.innerHTML = '<p style="color: #64748b;">AI analysis will be generated for this country profile.</p>';
    }
}

function renderComparison(comparison) {
    var container = document.getElementById('country-comparison');
    if (!container) return;
    
    container.innerHTML = '';
    
    if (comparison.similar_countries && comparison.similar_countries.length > 0) {
        var div = document.createElement('div');
        div.innerHTML = '<h4 style="margin-top: 0;">Similar Countries (by Total Capital Buffer)</h4>';
        
        comparison.similar_countries.forEach(function(item) {
            var countryDiv = document.createElement('div');
            countryDiv.className = 'comparison-item';
            var countryName = item.COUNTRY || item.country || 'Unknown';
            var total = item.Total || item.total || 0;
            countryDiv.innerHTML = '<strong>' + countryName + ':</strong> ' + total.toFixed(2) + '%';
            div.appendChild(countryDiv);
        });
        
        container.appendChild(div);
    } else {
        container.innerHTML = '<p style="color: #64748b; padding: 20px;">Comparison data not available.</p>';
    }
}

function initKnowledgeGraph() {
    var container = document.getElementById('knowledge-graph-container');
    if (!container || !window.vis) {
        console.warn('Knowledge graph container or vis.js not found');
        return;
    }
    
    // Graph adatok betöltése
    var graphDataScript = document.getElementById('knowledge-graph-data');
    if (!graphDataScript) {
        console.warn('Knowledge graph data not found');
        return;
    }
    
    var graphData;
    try {
        graphData = JSON.parse(graphDataScript.textContent);
    } catch (e) {
        console.error('Failed to parse knowledge graph data:', e);
        return;
    }
    
    if (!graphData.nodes || !graphData.edges) {
        console.warn('Invalid knowledge graph data structure');
        return;
    }
    
    console.log('Knowledge graph loaded:', graphData.nodes.length, 'nodes,', graphData.edges.length, 'edges');
    
    // vis.js DataSet-ek létrehozása
    var nodes = new vis.DataSet(graphData.nodes);
    var edges = new vis.DataSet(graphData.edges);
    
    // Színek csoportok szerint
    var nodeColors = {
        'country': {background: '#3b82f6', border: '#2563eb'},
        'ccyb': {background: '#10b981', border: '#059669'},
        'syrb': {background: '#f59e0b', border: '#d97706'},
        'osii': {background: '#ef4444', border: '#dc2626'},
        'bbm': {background: '#8b5cf6', border: '#7c3aed'},
        'bank': {background: '#ec4899', border: '#db2777'},
    };
    
    // Node-ok színezése
    var coloredNodes = nodes.map(function(node) {
        var color = nodeColors[node.group] || {background: '#64748b', border: '#475569'};
        return {
            id: node.id,
            label: node.label,
            group: node.group,
            title: node.title || node.label,
            value: node.value || 10,
            color: color,
            font: {
                color: node.group === 'country' ? '#ffffff' : '#ffffff',
                size: node.group === 'country' ? 16 : 12,
                face: 'Inter, sans-serif',
            },
            borderWidth: 2,
            shadow: true,
            shape: node.group === 'country' ? 'dot' : 'box',
        };
    });
    
    nodes.clear();
    nodes.add(coloredNodes);
    
    // Edge-ek formázása
    var formattedEdges = edges.map(function(edge) {
        return {
            from: edge.from,
            to: edge.to,
            label: edge.label || '',
            title: edge.title || '',
            color: edge.color || {color: '#64748b'},
            width: edge.width || 2,
            dashes: edge.dashes || false,
            arrows: {
                to: {
                    enabled: true,
                    scaleFactor: 0.8,
                },
            },
            font: {
                size: 11,
                align: 'middle',
                face: 'Inter, sans-serif',
            },
            smooth: {
                type: 'continuous',
            },
        };
    });
    
    edges.clear();
    edges.add(formattedEdges);
    
    var data = {nodes: nodes, edges: edges};
    
    var options = {
        nodes: {
            shape: 'dot',
            size: 25,
            font: {
                size: 14,
                face: 'Inter, sans-serif',
            },
            borderWidth: 2,
            shadow: true,
        },
        edges: {
            arrows: {
                to: {
                    enabled: true,
                    scaleFactor: 0.8,
                },
            },
            font: {
                size: 11,
                align: 'middle',
                face: 'Inter, sans-serif',
            },
            width: 2,
            smooth: {
                type: 'continuous',
            },
        },
        physics: {
            enabled: true,
            stabilization: {
                iterations: 200,
            },
            barnesHut: {
                gravitationalConstant: -2000,
                centralGravity: 0.1,
                springLength: 200,
                springConstant: 0.04,
                damping: 0.09,
            },
        },
        interaction: {
            hover: true,
            tooltipDelay: 100,
            zoomView: true,
            dragView: true,
        },
        layout: {
            improvedLayout: true,
        },
    };
    
    var network = new vis.Network(container, data, options);
    
    // Click event: ország kiválasztása
    network.on('click', function(params) {
        if (params.nodes.length > 0) {
            var nodeId = params.nodes[0];
            var node = nodes.get(nodeId);
            
            if (node && node.group === 'country') {
                // Navigate to country profile
                window.location.hash = 'country=' + encodeURIComponent(node.label);
                // Switch to country profiles tab
                var mainLink = Array.from(document.querySelectorAll('.nav-link[data-tab]')).find(function(link) {
                    return link.dataset.tab === 'country-profiles' && !link.classList.contains('sub-nav');
                });
                activateTab('country-profiles', true, mainLink);
            }
        }
    });
    
    // Double-click: zoom to node
    network.on('doubleClick', function(params) {
        if (params.nodes.length > 0) {
            network.focus(params.nodes[0], {
                scale: 1.5,
                animation: true,
            });
        }
    });
    
    // Hover: highlight connected nodes
    network.on('hoverNode', function(params) {
        var nodeId = params.node;
        var connectedNodes = network.getConnectedNodes(nodeId);
        
        // Update node opacity
        var updateNodes = nodes.getIds().map(function(id) {
            if (id === nodeId) {
                return {id: id, borderWidth: 4};
            } else if (connectedNodes.indexOf(id) !== -1) {
                return {id: id, opacity: 0.8};
            } else {
                return {id: id, opacity: 0.3};
            }
        });
        nodes.update(updateNodes);
    });
    
    network.on('blurNode', function(params) {
        // Reset opacity
        var updateNodes = nodes.getIds().map(function(id) {
            return {id: id, opacity: 1, borderWidth: 2};
        });
        nodes.update(updateNodes);
    });
    
    // Filtering
    var measureFilter = document.getElementById('kg-measure-filter');
    var regionFilter = document.getElementById('kg-region-filter');
    var searchInput = document.getElementById('kg-node-search');
    var resetBtn = document.getElementById('kg-reset-view');
    
    function applyFilters() {
        var measureType = measureFilter ? measureFilter.value : 'all';
        var region = regionFilter ? regionFilter.value : 'all';
        var searchQuery = searchInput ? searchInput.value.toLowerCase().trim() : '';
        
        var allNodeIds = nodes.getIds();
        var visibleNodeIds = [];
        var visibleEdgeIds = [];
        
        // Filter nodes
        allNodeIds.forEach(function(nodeId) {
            var node = nodes.get(nodeId);
            if (!node) return;
            
            var matchesMeasure = measureType === 'all' || node.group === measureType || node.group === 'country';
            var matchesRegion = region === 'all' || node.region === region || node.group !== 'country';
            var matchesSearch = !searchQuery || node.label.toLowerCase().indexOf(searchQuery) !== -1;
            
            if (matchesMeasure && matchesRegion && matchesSearch) {
                visibleNodeIds.push(nodeId);
            }
        });
        
        // Filter edges (only show edges between visible nodes)
        edges.getIds().forEach(function(edgeId) {
            var edge = edges.get(edgeId);
            if (visibleNodeIds.indexOf(edge.from) !== -1 && visibleNodeIds.indexOf(edge.to) !== -1) {
                visibleEdgeIds.push(edgeId);
            }
        });
        
        // Update visibility
        nodes.update(allNodeIds.map(function(id) {
            return {id: id, hidden: visibleNodeIds.indexOf(id) === -1};
        }));
        
        edges.update(edges.getIds().map(function(id) {
            return {id: id, hidden: visibleEdgeIds.indexOf(id) === -1};
        }));
        
        // If search, focus on first match
        if (searchQuery && visibleNodeIds.length > 0) {
            network.focus(visibleNodeIds[0], {
                scale: 1.5,
                animation: true,
            });
        }
    }
    
    if (measureFilter) {
        measureFilter.addEventListener('change', applyFilters);
    }
    if (regionFilter) {
        regionFilter.addEventListener('change', applyFilters);
    }
    if (searchInput) {
        searchInput.addEventListener('input', applyFilters);
    }
    if (resetBtn) {
        resetBtn.addEventListener('click', function() {
            if (measureFilter) measureFilter.value = 'all';
            if (regionFilter) regionFilter.value = 'all';
            if (searchInput) searchInput.value = '';
            applyFilters();
            network.fit({
                animation: true,
            });
        });
    }
}

function initOSII() {
    var selector = document.getElementById('osii-country-selector');
    var container = document.getElementById('osii-table-container');
    
    if (!selector || !container) {
        return;
    }
    
    // Get OSII data from window (set by template)
    var osiiData = window.osiiByCountry || {};
    
    function updateOSIITable(country) {
        if (!osiiData[country]) {
            container.innerHTML = '<div class="empty-state">No OSII/GSII data available for ' + country + '.</div>';
            return;
        }
        
        var data = osiiData[country];
        if (!data || data.length === 0) {
            container.innerHTML = '<div class="empty-state">No OSII/GSII data available for ' + country + '.</div>';
            return;
        }
        
        // Build table HTML - check if we have individual bank data
        var hasBankNames = data.length > 0 && data[0].bank_name;
        
        if (hasBankNames) {
            // Filter to only show active banks
            var activeData = data.filter(function(row) {
                return !row.status || row.status === 'Active';
            });
            
            // Individual bank table (without Total Rate and Status columns)
            var html = '<table class="data-table"><thead><tr><th>Bank Name</th><th>LEI Code</th><th>Buffer Type</th><th>G-SII Rate</th><th>O-SII Rate</th></tr></thead><tbody>';
            
            for (var i = 0; i < activeData.length; i++) {
                var row = activeData[i];
                var gsiiDisplay = (row.gsii_rate && row.gsii_rate > 0) ? row.gsii_rate.toFixed(2) + '%' : '-';
                var osiiDisplay = (row.osii_rate && row.osii_rate > 0) ? row.osii_rate.toFixed(2) + '%' : '-';
                
                html += '<tr>';
                html += '<td><strong>' + (row.bank_name || '') + '</strong></td>';
                html += '<td>' + (row.lei_code || '-') + '</td>';
                html += '<td>' + (row.buffer_type || 'N/A') + '</td>';
                html += '<td>' + gsiiDisplay + '</td>';
                html += '<td>' + osiiDisplay + '</td>';
                html += '</tr>';
            }
            
            html += '</tbody></table>';
            container.innerHTML = html;
        } else {
            // Aggregate table (fallback)
            var html = '<table class="data-table"><thead><tr><th>Country</th><th>Number of Banks</th><th>Rate Range</th><th>Maximum Rate</th><th>Status</th><th>Description</th></tr></thead><tbody>';
            
            for (var i = 0; i < data.length; i++) {
                var row = data[i];
                html += '<tr>';
                html += '<td><strong>' + (row.country || country) + '</strong> (' + (row.iso2 || '') + ')</td>';
                html += '<td>' + (row.bank_count || 0) + '</td>';
                html += '<td>' + (row.rate_range || 'N/A') + '</td>';
                html += '<td><strong>' + (row.max_rate_numeric ? row.max_rate_numeric.toFixed(2) + '%' : 'N/A') + '</strong></td>';
                html += '<td>' + (row.status || 'Active') + '</td>';
                html += '<td>' + (row.description || '') + '</td>';
                html += '</tr>';
            }
            
            html += '</tbody></table>';
            container.innerHTML = html;
        }
    }
    
    // Handle country selection
    selector.addEventListener('change', function(e) {
        var country = e.target.value;
        if (country) {
            updateOSIITable(country);
        }
    });
    
    // Initialize with default country (Austria)
    var defaultCountry = selector.value || 'Austria';
    if (defaultCountry && osiiData[defaultCountry]) {
        updateOSIITable(defaultCountry);
    }
}

document.addEventListener('DOMContentLoaded', function() {
    if (window.lucide) {
        window.lucide.createIcons();
    }
    initTabs();
    initPlotFilter();
    initResize();
    initNewsFilters();
    initCountryProfiles();
    initOSII();
    initChartPeriodSelector();
    // Knowledge graph visualization removed - data is used for AI analysis only
});
