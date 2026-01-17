function toggleSidebar() {
    var sidebar = document.getElementById('sidebar');
    var overlay = document.querySelector('.overlay');
    if (sidebar) sidebar.classList.toggle('active');
    if (overlay) overlay.classList.toggle('active');
}

function activateTab(tabName) {
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
    navLinks.forEach(function(link) {
        link.classList.toggle('active', link.dataset.tab === tabName);
    });
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
            activateTab(link.dataset.tab);
        });
    });
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

document.addEventListener('DOMContentLoaded', function() {
    if (window.lucide) {
        window.lucide.createIcons();
    }
    initTabs();
    initPlotFilter();
    initResize();
    initNewsFilters();
});
