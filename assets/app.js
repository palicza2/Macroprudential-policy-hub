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
    
    // Re-render Mermaid diagrams when About tab becomes active
    if (tabName === 'about' && typeof mermaid !== 'undefined') {
        setTimeout(function() {
            var mermaidElements = document.querySelectorAll('.mermaid');
            mermaidElements.forEach(function(element) {
                if (element.getAttribute('data-processed') === 'true') {
                    // Remove processed attribute to allow re-rendering
                    element.removeAttribute('data-processed');
                    // Clear the content and re-initialize
                    var graphDefinition = element.textContent;
                    element.textContent = graphDefinition;
                }
            });
            mermaid.init(undefined, '.mermaid');
        }, 100);
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
    selector.addEventListener('change', function(e) {
        var country = e.target.value;
        if (country && countriesData[country]) {
            loadCountryProfile(country, countriesData[country]);
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
    function checkHashForCountry() {
        var hash = window.location.hash;
        var selectedCountry = null;
        
        if (hash) {
            // Check for country parameter in hash (e.g., #country-profiles&country=Hungary)
            var countryMatch = hash.match(/country=([^&]+)/);
            if (countryMatch) {
                var countryFromHash = decodeURIComponent(countryMatch[1]);
                if (countriesData[countryFromHash]) {
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
        }
        
        // If no country from hash, use default
        if (!selectedCountry && defaultCountry && countriesData[defaultCountry]) {
            selectedCountry = defaultCountry;
        }
        
        // Load selected/default country
        if (selectedCountry) {
            selector.value = selectedCountry;
            loadCountryProfile(selectedCountry, countriesData[selectedCountry]);
            content.style.display = 'block';
        }
    }
    
    // Check on initial load
    checkHashForCountry();
    
    // Check on hash change
    window.addEventListener('hashchange', checkHashForCountry);
}

function loadCountryProfile(country, profileData) {
    if (!profileData) return;
    
    // Update current status
    renderCurrentStatus(profileData.current_status || {});
    
    // Update historical evolution
    renderHistoricalEvolution(country, profileData.historical_evolution || {});
    
    // Update recent changes
    renderRecentChanges(profileData.recent_changes || []);
    
    // Update active measures
    renderActiveMeasures(profileData.active_measures || {});
    
    // Update AI analysis
    renderAIAnalysis(profileData.ai_analysis || '');
    
    // Update comparison
    renderComparison(profileData.comparison || {});
}

function renderCurrentStatus(status) {
    var grid = document.getElementById('current-status-grid');
    if (!grid) return;
    
    grid.innerHTML = '';
    
    var items = [
        { key: 'ccyb', label: 'CCyB', value: status.ccyb?.rate, status: status.ccyb?.status },
        { key: 'syrb', label: 'SyRB', value: status.syrb?.rate, status: status.syrb?.status },
        { key: 'osii', label: 'O-SII', value: status.osii?.rate, status: status.osii?.status },
        { key: 'total', label: 'Total Capital', value: status.total_capital?.total, status: status.total_capital ? 'Active' : null },
    ];
    
    items.forEach(function(item) {
        if (item.value === undefined && item.value !== 0) return;
        
        var div = document.createElement('div');
        div.className = 'status-item';
        div.innerHTML = '<span class="status-label">' + item.label + '</span>' +
                       '<span class="status-value">' + (item.value !== null ? item.value.toFixed(2) + '%' : 'N/A') + '</span>' +
                       (item.status ? '<span class="status-badge ' + (item.status === 'Active' ? 'active' : 'inactive') + '">' + item.status + '</span>' : '');
        grid.appendChild(div);
    });
    
    // BBM
    if (status.bbm && status.bbm.length > 0) {
        var bbmDiv = document.createElement('div');
        bbmDiv.className = 'status-item';
        bbmDiv.innerHTML = '<span class="status-label">BBM</span>' +
                          '<span class="status-value">Yes</span>' +
                          '<span class="status-badge active">' + status.bbm.join(', ') + '</span>';
        grid.appendChild(bbmDiv);
    }
}

function renderHistoricalEvolution(country, evolution) {
    var chartDiv = document.getElementById('country-evolution-chart');
    if (!chartDiv || !window.Plotly) return;
    
    var traces = [];
    
    if (evolution.ccyb && evolution.ccyb.length > 0) {
        var ccybData = evolution.ccyb;
        traces.push({
            x: ccybData.map(function(d) { return d.date; }),
            y: ccybData.map(function(d) { return d.rate || 0; }),
            name: 'CCyB',
            type: 'scatter',
            mode: 'lines+markers',
            line: { color: '#3b82f6', width: 2 }
        });
    }
    
    if (evolution.syrb && evolution.syrb.length > 0) {
        var syrbData = evolution.syrb;
        traces.push({
            x: syrbData.map(function(d) { return d.date; }),
            y: syrbData.map(function(d) { return d.rate_numeric || 0; }),
            name: 'SyRB',
            type: 'scatter',
            mode: 'lines+markers',
            line: { color: '#10b981', width: 2 }
        });
    }
    
    if (traces.length === 0) {
        chartDiv.innerHTML = '<p style="color: #64748b; padding: 20px;">No historical data available.</p>';
        return;
    }
    
    var layout = {
        title: country + ' - Macroprudential Measures Evolution',
        xaxis: { title: 'Date' },
        yaxis: { title: 'Rate (%)' },
        hovermode: 'x unified',
        height: 400,
        margin: { t: 50, r: 20, b: 50, l: 60 }
    };
    
    Plotly.newPlot(chartDiv, traces, layout, { responsive: true, displayModeBar: false });
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

function renderActiveMeasures(measures) {
    var container = document.getElementById('active-measures-details');
    if (!container) return;
    
    container.innerHTML = '';
    
    if (measures.ccyb) {
        var div = document.createElement('div');
        div.className = 'measure-detail';
        div.innerHTML = '<h4>CCyB</h4>' +
                       '<div class="measure-detail-item"><strong>Rate:</strong> ' + (measures.ccyb.rate || 0).toFixed(2) + '%</div>' +
                       (measures.ccyb.date ? '<div class="measure-detail-item"><strong>Effective Date:</strong> ' + measures.ccyb.date + '</div>' : '') +
                       (measures.ccyb.credit_gap !== null && measures.ccyb.credit_gap !== undefined ? '<div class="measure-detail-item"><strong>Credit Gap:</strong> ' + measures.ccyb.credit_gap.toFixed(2) + '%</div>' : '');
        container.appendChild(div);
    }
    
    if (measures.syrb && measures.syrb.length > 0) {
        measures.syrb.forEach(function(syrb) {
            var div = document.createElement('div');
            div.className = 'measure-detail';
            div.innerHTML = '<h4>SyRB - ' + (syrb.type || 'General') + '</h4>' +
                           '<div class="measure-detail-item"><strong>Rate:</strong> ' + (syrb.rate || 0).toFixed(2) + '%</div>' +
                           (syrb.exposure ? '<div class="measure-detail-item"><strong>Exposure:</strong> ' + syrb.exposure + '</div>' : '') +
                           (syrb.date ? '<div class="measure-detail-item"><strong>Date:</strong> ' + syrb.date + '</div>' : '');
            container.appendChild(div);
        });
    }
    
    if (measures.bbm && measures.bbm.length > 0) {
        measures.bbm.forEach(function(bbm) {
            var div = document.createElement('div');
            div.className = 'measure-detail';
            div.innerHTML = '<h4>BBM - ' + (bbm.type || 'BBM') + '</h4>' +
                           (bbm.status ? '<div class="measure-detail-item"><strong>Status:</strong> ' + bbm.status + '</div>' : '') +
                           (bbm.date ? '<div class="measure-detail-item"><strong>Date:</strong> ' + bbm.date + '</div>' : '') +
                           (bbm.description ? '<div class="measure-detail-item"><strong>Description:</strong> ' + bbm.description + '</div>' : '');
            container.appendChild(div);
        });
    }
    
    if (container.innerHTML === '') {
        container.innerHTML = '<p style="color: #64748b; padding: 20px;">No active measures details available.</p>';
    }
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
    // Knowledge graph visualization removed - data is used for AI analysis only
});
