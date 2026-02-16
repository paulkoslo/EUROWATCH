// EP term number for Europarl URLs (matches parliament-fetch.js)
function getSessionNumber(date) {
  if (!date) return 10;
  if (date >= '2024-07-16') return 10;
  if (date >= '2019-07-02') return 9;
  if (date >= '2014-07-01') return 8;
  if (date >= '2009-07-14') return 7;
  if (date >= '2004-07-20') return 6;
  if (date >= '1999-07-20') return 5;
  if (date >= '1994-07-19') return 4;
  if (date >= '1989-07-25') return 3;
  if (date >= '1984-07-24') return 2;
  if (date >= '1979-07-17') return 1;
  return 1;
}

// Fetches a preview of a speech for a given date from the server
async function fetchPreview(date) {
  try {
    // Make a GET request to the preview API endpoint
    const res = await fetch(`/api/speech-preview?date=${date}`);
    if (!res.ok) return '—'; // Return dash if not successful
    const data = await res.json();
    return data.preview || '—'; // Return preview text or dash
  } catch (err) {
    return '—'; // Return dash on error
  }
}

// Main dashboard script: handles MEP and Speeches tabs
(async () => {
  // --- Tab switching logic ---
  // Get all tab buttons and tab content containers
  const tabs = document.querySelectorAll('.tab');
  const contents = document.querySelectorAll('.tab-content');
  // Add click event listeners to each tab
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      // Remove 'active' class from all tabs and contents
      tabs.forEach(t => t.classList.remove('active'));
      contents.forEach(c => c.classList.remove('active'));
      // Activate the clicked tab and its content
      tab.classList.add('active');
      document.getElementById(tab.dataset.tab).classList.add('active');
      // If switching to speeches tab, trigger sittings load (Parliament Sittings from newest.js)
      if (tab.dataset.tab === 'speeches') {
        if (typeof window.loadSittings === 'function') window.loadSittings(true);
      }
      // If switching to analytics tab: warm cache if needed, then load
      if (tab.dataset.tab === 'analytics') {
        checkCacheStatusOnce().then(status => {
          if (status && status.ready) {
            runAnalyticsLoad();
          } else {
            startAnalyticsWarmAndLoad();
          }
        });
      }
    });
  });

  // --- Fetch MEP data from server ---
  let mepsList = []; // All MEPs
  let filteredMeps = []; // Filtered MEPs for display
  let mepsPage = 1; // Current page for pagination
  const MEP_PAGE_SIZE = 50; // Number of MEPs per page
  const showMoreBtn = document.getElementById('showMoreMeps'); // 'Show more' button

  try {
    // Fetch MEPs from API
    const res = await fetch('/api/meps');
    const json = await res.json();
    mepsList = json.data || [];
  } catch (e) {
    console.error('Error fetching MEP data:', e);
    return;
  }

  // --- Populate filter dropdowns for country, group, and status ---
  const statusFilter = document.getElementById('statusFilter');
  const countryFilter = document.getElementById('countryFilter');
  const groupFilter = document.getElementById('groupFilter');
  const speechCountFilter = document.getElementById('speechCountFilter');
  
  // Get unique countries and groups from MEP data
  const countries = [...new Set(mepsList.map(m => m['api:country-of-representation'] || 'Unknown'))].sort();
  const groups = [...new Set(mepsList.map(m => m['api:political-group'] || 'Unknown'))].sort();
  
  // Populate country filter dropdown
  countries.forEach(country => {
    const option = document.createElement('option');
    option.value = country;
    option.textContent = country;
    countryFilter.appendChild(option);
  });
  
  // Populate group filter dropdown
  groups.forEach(group => {
    const option = document.createElement('option');
    option.value = group;
    option.textContent = group;
    groupFilter.appendChild(option);
  });

  // --- Filtering logic for MEPs table and charts ---
  function applyFilters() {
    const selectedStatus = statusFilter.value;
    const selectedCountry = countryFilter.value;
    const selectedGroup = groupFilter.value;
    const minSpeechCount = parseInt(speechCountFilter.value) || 0;
    mepsPage = 1; // Reset to first page
    
    // Filter MEPs by all criteria
    filteredMeps = mepsList.filter(mep => {
      const statusMatch = !selectedStatus || 
        (selectedStatus === 'current' && mep.isCurrent) ||
        (selectedStatus === 'historic' && !mep.isCurrent);
      const countryMatch = !selectedCountry || mep['api:country-of-representation'] === selectedCountry;
      const groupMatch = !selectedGroup || mep['api:political-group'] === selectedGroup;
      const speechCountMatch = mep.speechCount >= minSpeechCount;
      
      return statusMatch && countryMatch && groupMatch && speechCountMatch;
    });
    
    // Update total count display with breakdown
    const currentCount = filteredMeps.filter(m => m.isCurrent).length;
    const historicCount = filteredMeps.filter(m => !m.isCurrent).length;
    const totalSpeeches = filteredMeps.reduce((sum, m) => sum + m.speechCount, 0);
    
    document.getElementById('total-count').textContent = 
      `${filteredMeps.length} (${currentCount} current, ${historicCount} historic) - ${totalSpeeches} total speeches`;
    
    // Update charts with filtered data
    updateCharts(filteredMeps);
    // Update table with filtered data
    renderMepsTable();
  }

  // --- Render the MEPs table with pagination ---
  function renderMepsTable() {
    const mepsTbody = document.querySelector('#mepsTable tbody');
    // Get the MEPs to show for the current page
    const toShow = filteredMeps.slice(0, mepsPage * MEP_PAGE_SIZE);
    // Render table rows with View Speeches button
    mepsTbody.innerHTML = toShow.map(m => {
      const statusBadge = m.isCurrent 
        ? '<span class="status-badge current">✅ Current</span>' 
        : '<span class="status-badge historic">📜 Historic</span>';
      const speechCountBadge = m.speechCount > 0 
        ? `<span class="speech-count-badge">${m.speechCount} speeches</span>`
        : '<span class="speech-count-badge zero">No speeches</span>';
      return `
        <tr class="mep-row" data-mep-id="${m.identifier}">
          <td>${m.identifier}</td>
          <td>${m.label}</td>
          <td>${m['api:country-of-representation'] || ''}</td>
          <td>${m['api:political-group'] || ''}</td>
          <td>${statusBadge}</td>
          <td>${speechCountBadge}</td>
          <td>
            <button class="view-speeches-btn" onclick="viewMepDetails(${m.identifier})" title="View all speeches by this MEP">
              🎤 View Speeches
            </button>
          </td>
        </tr>
      `;
    }).join('');
    // Show or hide the 'Show more' button
    if (filteredMeps.length > toShow.length) {
      showMoreBtn.style.display = '';
    } else {
      showMoreBtn.style.display = 'none';
    }
  }

  // --- 'Show more' button for MEPs pagination ---
  showMoreBtn.addEventListener('click', () => {
    mepsPage++;
    renderMepsTable();
  });

  // --- Initial render of MEPs table and charts ---
  filteredMeps = mepsList;
  renderMepsTable();
  document.getElementById('total-count').textContent = filteredMeps.length;
  updateCharts(filteredMeps);

  // --- Add filter event listeners ---
  statusFilter.addEventListener('change', applyFilters);
  countryFilter.addEventListener('change', applyFilters);
  groupFilter.addEventListener('change', applyFilters);
  speechCountFilter.addEventListener('change', applyFilters);

  // --- Global function to view MEP details ---
  window.viewMepDetails = function(mepId) {
    console.log(`[FRONTEND] Opening MEP details for ID: ${mepId}`);
    window.open(`/mep-details.html?id=${mepId}`, '_blank');
  };


  // --- Update country and group charts ---
  function updateCharts(meps) {
    // --- Country bar chart (Top 10 countries) ---
    // Count MEPs per country
    const countryCounts = meps.reduce((acc, m) => {
      const c = m['api:country-of-representation'] || 'Unknown';
      acc[c] = (acc[c] || 0) + 1;
      return acc;
    }, {});
    // Get top 10 countries by count
    const countryEntries = Object.entries(countryCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    const countryLabels = countryEntries.map(e => e[0]);
    const countryData = countryEntries.map(e => e[1]);
    
    // Update or create the country chart
    const countryChart = Chart.getChart(document.getElementById('countryChart'));
    if (countryChart) {
      countryChart.data.labels = countryLabels;
      countryChart.data.datasets[0].data = countryData;
      countryChart.update();
    } else {
      new Chart(document.getElementById('countryChart').getContext('2d'), {
        type: 'bar',
        data: { labels: countryLabels, datasets: [{ label: 'MEPs per Country', data: countryData, backgroundColor: 'rgba(59, 130, 246, 0.6)' }] },
        options: { 
          responsive: true, 
          scales: { y: { beginAtZero: true } },
          plugins: {
            legend: { display: false }
          }
        }
      });
    }

    // --- Group pie chart ---
    // Count MEPs per political group
    const groupCounts = meps.reduce((acc, m) => {
      const g = m['api:political-group'] || 'Unknown';
      acc[g] = (acc[g] || 0) + 1;
      return acc;
    }, {});
    const groupEntries = Object.entries(groupCounts).sort((a, b) => b[1] - a[1]);
    const groupLabels = groupEntries.map(e => e[0]);
    const groupData = groupEntries.map(e => e[1]);
    
    // Update or create the group chart
    const groupChart = Chart.getChart(document.getElementById('groupChart'));
    if (groupChart) {
      groupChart.data.labels = groupLabels;
      groupChart.data.datasets[0].data = groupData;
      groupChart.update();
    } else {
      new Chart(document.getElementById('groupChart').getContext('2d'), {
        type: 'pie',
        data: { 
          labels: groupLabels, 
          datasets: [{ 
            data: groupData, 
            backgroundColor: groupLabels.map((_, i) => `hsl(${(i*360)/groupLabels.length},60%,60%)`) 
          }] 
        },
        options: { 
          responsive: true,
          plugins: {
            legend: {
              position: 'right',
              labels: {
                boxWidth: 12,
                padding: 15
              }
            }
          }
        }
      });
    }
  }

  // --- Speeches loading with pagination, sorting, and 'Load more' ---
  // State variables for speeches
  let speechesData = [];
  let speechesOffset = 0;
  const speechesLimit = 50;
  let speechesTotal = 0;
  let currentPersonId = '';
  let currentSort = { column: null, direction: 'asc' };

  // DOM elements for speeches
  const statsEl = document.getElementById('speech-count');
  const tbody = document.querySelector('#speechesTable tbody');
  const loadMoreBtn = document.getElementById('showMoreMeps');

  // --- Utility functions for speeches table ---
  // Shorten a URI to just the last part
  function shortId(id) {
    const parts = id.split('/');
    return parts[parts.length - 1] || id;
  }
  // Prettify the type string
  function prettyType(type) {
    if (!type) return '';
    const raw = type.split('/').pop();
    return raw.split('_').map(w => w.charAt(0) + w.slice(1).toLowerCase()).join(' ');
  }
  // Render the speeches table
  function renderSpeechesTable() {
    let displayData = speechesData.slice();
    statsEl.textContent = `${speechesOffset}/${speechesTotal}`;
  
    tbody.innerHTML = displayData.map(s => {
      const date = s.date || s.activity_date || '';
      const session = date ? getSessionNumber(date) : 10;
      const htmlUrl = date
        ? `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${date}_EN.html`
        : '#';
  
      return `
        <tr data-date="${date}" data-id="${s.id}">
          <td>${s.id}</td>
          <td>${s.type || ''}</td>
          <td>${s.label || ''}</td>
          <td>${date}</td>
          <td class="preview" style="cursor: pointer; color: blue;">(Klick für Vorschau)</td>
          <td><a href="${htmlUrl}" target="_blank">HTML</a></td>
          <td><button class="viewBtn" data-id="${encodeURIComponent(s.id)}">View</button></td>
        </tr>
      `;
    }).join('');
  
    // Add click event to preview cells to load preview text
    document.querySelectorAll('td.preview').forEach(previewCell => {
      previewCell.addEventListener('click', async () => {
        const row = previewCell.closest('tr');
        const date = row.getAttribute('data-date');
        previewCell.textContent = 'Lade...';
        try {
          // Try the main preview API first
          let previewText = await fetchPreview(date);
          if (previewText === '—') {
            const session = getSessionNumber(date);
            const fallbackUrl = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${date}_EN.html`;
            try {
              const resp = await fetch(fallbackUrl);
              if (resp.ok) {
                const html = await resp.text();
                // Try to extract the first item or a summary from the TOC HTML
                // This is a simple fallback: extract the first <a> in the main content
                const parser = new DOMParser();
                const doc = parser.parseFromString(html, 'text/html');
                // Try to find the first item in the TOC
                let firstItem = doc.querySelector('a[href*="ITM-"]');
                if (firstItem) {
                  previewText = `TOC: ${firstItem.textContent.trim()}`;
                } else {
                  // Fallback: just show a message
                  previewText = 'No preview available (TOC loaded)';
                }
              } else {
                previewText = 'Fehler beim Laden der TOC-Seite';
              }
            } catch (err2) {
              previewText = 'Fehler beim Laden der TOC-Seite';
            }
          }
          previewCell.textContent = previewText;
        } catch (err) {
          previewCell.textContent = 'Fehler';
        }
      });
    });
  
    // Add click event to view buttons to go to speech detail page
    document.querySelectorAll('.viewBtn').forEach(btn => {
      btn.addEventListener('click', () => {
        const enc = btn.getAttribute('data-id');
        const id = decodeURIComponent(enc);
        const rec = speechesData.find(x => x.id === id);
        if (rec) sessionStorage.setItem('speechRecord', JSON.stringify(rec));
        window.location.href = `speech.html?id=${encodeURIComponent(id)}`;
      });
    });
  }

  // --- Fetch a page of speeches for a given MEP ---
  async function loadSpeeches(personId, reset = true) {
    if (reset) {
      speechesData = [];
      speechesOffset = 0;
      speechesTotal = 0;
      currentPersonId = personId;
      currentSort.column = null;
      // Remove sort indicators from table headers
      document.querySelectorAll('#speechesTable thead th.sortable')
        .forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
    }
    // Show loading message
    tbody.innerHTML = '<tr><td colspan="5">Loading...</td></tr>';
    loadMoreBtn.style.display = 'none';
    try {
      // Build query params for API
      const params = new URLSearchParams();
      if (currentPersonId) params.set('personId', currentPersonId);
      params.set('limit', speechesLimit);
      params.set('offset', speechesOffset);
      // Fetch speeches from API
      const resp = await fetch(`/api/speeches?${params.toString()}`);
      const data = await resp.json();
      const newSpeeches = data.data || [];
      speechesTotal = data.meta && data.meta.total || speechesTotal;
      speechesData = speechesData.concat(newSpeeches);
      speechesOffset = speechesData.length;
      renderSpeechesTable();
    } catch (err) {
      console.error('Error loading speeches:', err);
      tbody.innerHTML = '<tr><td colspan="5">Error loading speeches</td></tr>';
    }
  }

  // --- 'Load more' button for speeches pagination ---
  loadMoreBtn.addEventListener('click', () => loadSpeeches(currentPersonId, false));

  // --- Sorting handlers for speeches table ---
  document.querySelectorAll('#speechesTable thead th.sortable').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (currentSort.column === col) {
        // Toggle sort direction
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
      } else {
        // Set new sort column
        currentSort.column = col;
        currentSort.direction = 'asc';
      }
      // Update sort indicators
      document.querySelectorAll('#speechesTable thead th.sortable')
        .forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
      th.classList.add(currentSort.direction === 'asc' ? 'sorted-asc' : 'sorted-desc');
      renderSpeechesTable();
    });
  });

  // --- Initial load: if speeches tab is active on page load, trigger sittings load ---
  tabs.forEach(tab => {
    if (tab.dataset.tab === 'speeches' && tab.classList.contains('active')) {
      if (typeof window.loadSittings === 'function') window.loadSittings(true);
    }
    if (tab.dataset.tab === 'analytics' && tab.classList.contains('active')) {
      checkCacheStatusOnce().then(status => {
        if (status && status.ready) {
          runAnalyticsLoad();
        } else {
          startAnalyticsWarmAndLoad();
        }
      });
    }
  });

  // Auto-start analytics warm when opening tab (no Calculate button)
  async function startAnalyticsWarmAndLoad() {
    const progressDiv = document.getElementById('cacheLoadingProgress');
    if (progressDiv) progressDiv.style.display = 'block';
    try {
      const warmRes = await fetch('/api/analytics/warm', { method: 'POST' });
      const warmData = await warmRes.json();
      if (warmData.ready) {
        runAnalyticsLoad();
      } else if (warmData.started || warmData.warming) {
        await checkCacheStatus();
        runAnalyticsLoad();
      }
    } catch (e) {
      console.error('Analytics warm failed:', e);
      if (progressDiv) progressDiv.style.display = 'none';
    }
  }
})();

// --- Descriptive Analytics ---
async function loadAnalytics() {
  try {
    console.time('[ANALYTICS] Total loadAnalytics');
    document.getElementById('trendLoading')?.classList.add('active');
    document.getElementById('groupLoading')?.classList.add('active');
    document.getElementById('countryLoading')?.classList.add('active');
    document.getElementById('langLoading')?.classList.add('active');
    
    console.time('[ANALYTICS] Fetch overview API');
    const res = await fetch('/api/analytics/overview');
    const data = await res.json();
    console.timeEnd('[ANALYTICS] Fetch overview API');
    
    if (!data || data.error) throw new Error(data?.error || 'Analytics API error');

    // Coverage
    const covEl = document.getElementById('coverageStats');
    if (covEl && data.coverage) {
      covEl.textContent = `Macro coverage: ${data.coverage.with_macro.toLocaleString()} of ${data.coverage.total.toLocaleString()} speeches (${data.coverage.pct_with_macro}%)`;
    }

    // Top Macro Topics bar
    const macroLabels = (data.macroTopicDistribution || []).map(r => r.topic);
    window.macroTopicsFromOverview = macroLabels;
    updateTopMepTopicDropdown();
    const macroCounts = (data.macroTopicDistribution || []).map(r => r.count);
    const macroCtx = document.getElementById('macroTopicChart');
    if (macroCtx) {
      const existing = Chart.getChart(macroCtx);
      if (existing) existing.destroy();
      new Chart(macroCtx.getContext('2d'), {
        type: 'bar',
        data: { labels: macroLabels, datasets: [{ label: 'Speeches', data: macroCounts, backgroundColor: 'rgba(0, 102, 204, 0.6)' }] },
        options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', scales: { x: { beginAtZero: true } }, plugins: { legend: { display: false } } }
      });
    }

    // Top Specific Focus horizontal bar
    const focusLabels = (data.topSpecificFocus || []).map(r => `${r.topic} — ${r.focus}`);
    const focusCounts = (data.topSpecificFocus || []).map(r => r.count);
    const focusCtx = document.getElementById('specificFocusChart');
    if (focusCtx) {
      const existing = Chart.getChart(focusCtx);
      if (existing) existing.destroy();
      new Chart(focusCtx.getContext('2d'), {
        type: 'bar',
        data: { labels: focusLabels, datasets: [{ label: 'Speeches', data: focusCounts, backgroundColor: 'rgba(34, 197, 94, 0.6)' }] },
        options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', scales: { x: { beginAtZero: true } }, plugins: { legend: { display: false } } }
      });
    }

    // Monthly Trends (line chart per topic)
    const trendCtx = document.getElementById('trendChart');
    if (trendCtx) {
      const rows = data.trendsMonthly || [];
      const months = Array.from(new Set(rows.map(r => r.ym))).sort();
      const topics = Array.from(new Set(rows.map(r => r.topic)));
      const colorFor = (i) => `hsl(${(i*360)/Math.max(1, topics.length)},70%,50%)`;
      const datasets = topics.map((t, i) => {
        const byMonth = new Map(rows.filter(r => r.topic === t).map(r => [r.ym, r.count]));
        const dataPoints = months.map(m => byMonth.get(m) || 0);
        return { label: t, data: dataPoints, borderColor: colorFor(i), backgroundColor: colorFor(i), tension: 0.2 };
      });
      const existing = Chart.getChart(trendCtx);
      if (existing) existing.destroy();
      new Chart(trendCtx.getContext('2d'), {
        type: 'line',
        data: { labels: months, datasets },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } }, scales: { y: { beginAtZero: true } } }
      });
    }
  } catch (e) {
    const covEl = document.getElementById('coverageStats');
    if (covEl) covEl.textContent = 'Failed to load analytics.';
    console.error('Analytics load failed:', e);
  } finally {
    document.getElementById('trendLoading')?.classList.remove('active');
    document.getElementById('groupLoading')?.classList.remove('active');
    document.getElementById('countryLoading')?.classList.remove('active');
    document.getElementById('langLoading')?.classList.remove('active');
    console.timeEnd('[ANALYTICS] Total loadAnalytics');
  }
}

// Global variables to store currently selected topics and chart data
window.selectedTopics = [];
window.allAvailableTopics = [];
window.trendChartData = null; // Store the full trend chart data for filtering

// Update selected topic count display
function updateSelectedCount() {
  const checked = document.querySelectorAll('.topicCheck:checked').length;
  const total = document.querySelectorAll('.topicCheck').length;
  const countEl = document.getElementById('selectedTopicCount');
  if (countEl) {
    countEl.textContent = `${checked} of ${total} topics selected`;
  }
}

// Calculate simple moving average
function calculateMovingAverage(data, windowSize) {
  if (!data || data.length === 0) return [];
  const result = [];
  for (let i = 0; i < data.length; i++) {
    if (i < windowSize - 1) {
      result.push(null); // Not enough data points yet
    } else {
      const window = data.slice(i - windowSize + 1, i + 1);
      const avg = window.reduce((sum, val) => sum + val, 0) / windowSize;
      result.push(Math.round(avg * 100) / 100); // Round to 2 decimals
    }
  }
  return result;
}

// One-shot cache status check (no polling)
async function checkCacheStatusOnce() {
  try {
    const res = await fetch('/api/analytics/cache-status');
    return await res.json();
  } catch (e) {
    return null;
  }
}

// Run full analytics load (overview, time series, charts)
function runAnalyticsLoad() {
  document.getElementById('cacheLoadingProgress')?.style.setProperty('display', 'none');
  loadAnalytics();
  loadTimeSeries().then(() => {
    Promise.all([
      loadGroupHeat(window.selectedTopics),
      loadLanguageHeat(window.selectedTopics)
    ]);
  });
}

// Check cache status and show loading progress (polls until ready when warming)
async function checkCacheStatus() {
  try {
    const res = await fetch('/api/analytics/cache-status');
    const status = await res.json();
    
    const progressDiv = document.getElementById('cacheLoadingProgress');
    const progressBar = document.getElementById('cacheProgressBar');
    const progressPercent = document.getElementById('cacheProgressPercent');
    const progressMessage = document.getElementById('cacheProgressMessage');
    
    if (!progressDiv) return status;
    
    if (status.ready) {
      // Cache is ready - hide progress bar
      progressDiv.style.display = 'none';
      console.log('[CACHE] Cache is ready!');
      return status;
    } else if (status.warming) {
      // Cache is warming - show progress
      progressDiv.style.display = 'block';
      const percent = status.progress?.percent || 0;
      progressBar.style.width = percent + '%';
      progressPercent.textContent = percent + '%';
      progressMessage.textContent = status.progress?.message || 'Loading...';
      console.log(`[CACHE] Warming... ${percent}% - ${status.progress?.message}`);
      
      // Poll again in 500ms and continue when ready
      return new Promise(resolve => {
        setTimeout(() => {
          checkCacheStatus().then(resolve);
        }, 500);
      });
    } else {
      // Cache not started - return so caller can show Calculate button (no auto-poll)
      progressDiv.style.display = 'none';
      return status;
    }
  } catch (error) {
    console.error('Error checking cache status:', error);
    return null;
  }
}

// Helpers for extended analytics
async function loadTimeSeries() {
  console.time('[TRENDS] Total loadTimeSeries');
  document.getElementById('trendLoading')?.classList.add('active');
  // Read granularity from radio buttons (month | quarter | year)
  const interval = document.getElementById('granYear')?.checked ? 'year' : document.getElementById('granQuarter')?.checked ? 'quarter' : 'month';
  const fromInput = document.getElementById('timeFilterFrom');
  const toInput = document.getElementById('timeFilterTo');
  const from = (fromInput && fromInput.value.trim()) || '';
  const to = (toInput && toInput.value.trim()) || '';
  
  const params = new URLSearchParams();
  params.set('interval', interval);
  params.set('all', 'true');
  if (from) params.set('from', from);
  if (to) params.set('to', to);
  
  // Update title based on granularity
  const titleEl = document.getElementById('trendChartTitle');
  const intervalLabels = { month: 'Monthly', quarter: 'Quarterly', year: 'Yearly' };
  if (titleEl) {
    titleEl.textContent = (intervalLabels[interval] || '') + ' Trends (Top Topics)' + (from || to ? ` ${from || '…'} – ${to || '…'}` : '');
  }
  
  console.time('[TRENDS] Fetch time-series API');
  const res = await fetch('/api/analytics/time-series?' + params.toString());
  const json = await res.json();
  console.timeEnd('[TRENDS] Fetch time-series API');

  // If no datasets, clear UI
  if (!json.datasets || !json.datasets.length) {
    const selector = document.getElementById('topicSelector');
    if (selector) selector.innerHTML = '<em>No topics found for this range.</em>';
    const ctx = document.getElementById('trendChart');
    const existing = Chart.getChart(ctx);
    if (existing) existing.destroy();
    document.getElementById('trendLoading')?.classList.remove('active');
    return;
  }

  // Build topic selector for ALL topics in this range
  const allLabels = (json.datasets || []).map(ds => ds.label).sort((a,b)=>a.localeCompare(b));
  // Preserve current topic selection: only keep topics that still exist in the new list
  const previousSelection = Array.isArray(window.selectedTopics) ? window.selectedTopics : [];
  window.selectedTopics = previousSelection.filter(t => allLabels.includes(t));
  window.allAvailableTopics = [...allLabels]; // Store all available topics
  updateTopMepTopicDropdown();
  
  const selector = document.getElementById('topicSelector');
  if (selector) {
    selector.innerHTML = allLabels.map(label => {
      const id = 'sel_' + label.replace(/[^a-z0-9]/gi,'_');
      const checked = window.selectedTopics.includes(label) ? ' checked' : '';
      return `<label style=\"display:flex; align-items:center; gap:6px; font-size:13px; color:#374151; padding:4px 6px; border-radius:4px; background:#fff; border:1px solid #e2e8f0;\">\
        <input type=\"checkbox\" class=\"topicCheck\" id=\"${id}\" data-label=\"${label}\"${checked}>\
        <span title=\"${label}\" style=\"overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;\">${label}</span>\
      </label>`;
    }).join('');
    
    // Update selected count
    updateSelectedCount();
  }

  // Store data globally for filtering
  window.trendChartData = {
    labels: json.labels || [],
    datasets: json.datasets || []
  };
  
  const ctx = document.getElementById('trendChart');
  const existing = Chart.getChart(ctx);
  if (existing) existing.destroy();
  
  // Better color palette with distinct, visually different colors
  const distinctColors = [
    '#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6',
    '#1abc9c', '#e67e22', '#34495e', '#16a085', '#c0392b',
    '#d35400', '#8e44ad', '#2980b9', '#27ae60', '#f1c40f',
    '#e84393', '#00b894', '#0984e3', '#fdcb6e', '#6c5ce7',
    '#fd79a8', '#00cec9', '#74b9ff', '#a29bfe', '#fd79a8',
    '#fab1a0', '#ff7675', '#ffeaa7', '#55efc4', '#81ecec',
    '#74b9ff', '#a29bfe', '#dfe6e9'
  ];
  window.getDistinctColor = (i) => distinctColors[i % distinctColors.length];
  
  const labels = json.labels || [];
  // Start with only selected topics (default: none)
  const initialDatasets = (json.datasets || []).filter(d => window.selectedTopics.includes(d.label));
  let datasets = initialDatasets.map((d, i) => ({
    ...d,
    borderColor: window.getDistinctColor(i),
    backgroundColor: window.getDistinctColor(i) + '40',
    tension: 0.2,
    borderWidth: 2,
    pointRadius: 3,
    pointHoverRadius: 5
  }));

  // Compute data max so y-axis scale shows actual counts (not 0–1 when all zeros)
  const dataMax = Math.max(1, ...datasets.flatMap(d => (d.data || []).filter(v => typeof v === 'number')));
  const yAxisMax = dataMax > 0 ? undefined : 10; // when all zeros, show 0–10 so "0" is clearly at bottom

  new Chart(ctx.getContext('2d'), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 12, padding: 8 } },
        tooltip: {
          callbacks: {
            label: function(context) {
              const v = context.parsed.y;
              const period = labels[context.dataIndex] || '';
              return context.dataset.label + ': ' + v + ' speech' + (v === 1 ? '' : 'es') + (period ? ' (' + period + ')' : '');
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grace: '8%',
          title: { display: true, text: 'Number of speeches' },
          ...(yAxisMax != null && { suggestedMax: yAxisMax })
        },
        x: { ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 24 } }
      }
    }
  });

  // Initialize filter controls after chart is created
  initializeFilterControls();

  document.getElementById('trendLoading')?.classList.remove('active');
  console.timeEnd('[TRENDS] Total loadTimeSeries');
}

// Apply topic filter function (accessible globally)
window.applyTopicFilter = async function() {
  const selected = Array.from(document.querySelectorAll('.topicCheck'))
    .filter(c=>c.checked)
    .map(c=>c.getAttribute('data-label'));
  
  console.log(`[FILTER] Applying filter with ${selected.length} topics`);
  
  // Update global selected topics
  window.selectedTopics = selected;
  
  // Update trend chart — ensure lines are clearly visible when topic filter is applied
  const ctx = document.getElementById('trendChart');
  const chart = Chart.getChart(ctx);
  if (chart && window.trendChartData) {
    const showSmooth = document.getElementById('showSmoothAvg')?.checked;
    const windowSize = parseInt(document.getElementById('smoothWindow')?.value || '6');
    const numSelected = selected.length;
    // Thicker, opaque lines when few topics selected so they stand out
    const lineWidth = numSelected <= 10 ? 3 : numSelected <= 20 ? 2.5 : 2;
    const pointRad = numSelected <= 10 ? 4 : numSelected <= 20 ? 3 : 2;
    
    let datasets = (window.trendChartData.datasets||[])
      .filter(ds => selected.includes(ds.label))
      .map((d, i) => {
        const color = window.getDistinctColor(i);
        return {
          ...d,
          label: d.label,
          borderColor: color,
          backgroundColor: color + '30',
          tension: 0.2,
          borderWidth: showSmooth ? 1 : lineWidth,
          borderDash: [],
          pointRadius: showSmooth ? 2 : pointRad,
          pointHoverRadius: pointRad + 2,
          pointBackgroundColor: color
        };
      });
    
    // Add smooth average datasets if enabled
    if (showSmooth) {
      const smoothDatasets = datasets.map((d, i) => {
        const color = window.getDistinctColor(i);
        return {
          label: (d.label || '').replace(/ \(trend\)$/, '') + ' (trend)',
          data: calculateMovingAverage(d.data, windowSize),
          borderColor: color,
          backgroundColor: color,
          tension: 0.4,
          borderWidth: 3,
          borderDash: [],
          pointRadius: 0,
          fill: false,
          order: -1
        };
      });
      datasets = [...datasets, ...smoothDatasets];
    }
    
    chart.data.datasets = datasets;
    const dataMax = Math.max(1, ...datasets.flatMap(d => (d.data || []).filter(v => typeof v === 'number')));
    if (chart.options.scales && chart.options.scales.y) {
      chart.options.scales.y.grace = '8%';
      chart.options.scales.y.title = chart.options.scales.y.title || { display: true, text: 'Number of speeches' };
      chart.options.scales.y.suggestedMax = dataMax > 0 ? undefined : 10;
    }
    chart.update();
  }
  
  // Update the other three charts with the selected topics
  await Promise.all([
    loadGroupHeat(selected),
    loadLanguageHeat(selected)
  ]);
  
  console.log('[FILTER] All charts updated');
};

// Initialize Apply button listener (called after loadTimeSeries)
function initializeFilterControls() {
  // Select/Clear All buttons only update checkboxes, not charts
  const selAllBtn = document.getElementById('selAllTopics');
  if (selAllBtn && !selAllBtn.hasAttribute('data-listener')) {
    selAllBtn.setAttribute('data-listener', 'true');
    selAllBtn.addEventListener('click', (e) => {
      e.preventDefault();
      document.querySelectorAll('.topicCheck').forEach(c => c.checked = true);
      updateSelectedCount();
    });
  }
  
  const clearBtn = document.getElementById('clearTopics');
  if (clearBtn && !clearBtn.hasAttribute('data-listener')) {
    clearBtn.setAttribute('data-listener', 'true');
    clearBtn.addEventListener('click', (e) => {
      e.preventDefault();
      document.querySelectorAll('.topicCheck').forEach(c => c.checked = false);
      updateSelectedCount();
    });
  }
  
  // Apply button triggers the actual update
  const applyBtn = document.getElementById('applyTopicFilter');
  if (applyBtn && !applyBtn.hasAttribute('data-listener')) {
    applyBtn.setAttribute('data-listener', 'true');
    applyBtn.addEventListener('click', (e) => {
      e.preventDefault();
      window.applyTopicFilter();
    });
  }
  
  // Update count on checkbox change (but don't reload charts)
  const selector = document.getElementById('topicSelector');
  if (selector && !selector.hasAttribute('data-listener')) {
    selector.setAttribute('data-listener', 'true');
    selector.addEventListener('change', (e) => {
      if (e.target && e.target.classList.contains('topicCheck')) {
        updateSelectedCount();
      }
    });
  }
  
  const topicFilter = document.getElementById('topicFilter');
  if (topicFilter && !topicFilter.hasAttribute('data-listener')) {
    topicFilter.setAttribute('data-listener', 'true');
    topicFilter.addEventListener('input', (e) => {
      const q = e.target.value.toLowerCase();
      const selectorEl = document.getElementById('topicSelector');
      if (selectorEl) {
        selectorEl.querySelectorAll('label').forEach(l => {
          const text = l.querySelector('span')?.textContent.toLowerCase() || '';
          l.style.display = text.includes(q) ? '' : 'none';
        });
      }
    });
  }
  
  // Smooth average checkbox - auto-apply when toggled
  const smoothCheckbox = document.getElementById('showSmoothAvg');
  if (smoothCheckbox && !smoothCheckbox.hasAttribute('data-listener')) {
    smoothCheckbox.setAttribute('data-listener', 'true');
    smoothCheckbox.addEventListener('change', () => {
      if (window.applyTopicFilter) {
        window.applyTopicFilter();
      }
    });
  }
  
  // Smooth window selector - auto-apply when changed
  const smoothWindow = document.getElementById('smoothWindow');
  if (smoothWindow && !smoothWindow.hasAttribute('data-listener')) {
    smoothWindow.setAttribute('data-listener', 'true');
    smoothWindow.addEventListener('change', () => {
      const smoothCheckbox = document.getElementById('showSmoothAvg');
      if (smoothCheckbox?.checked && window.applyTopicFilter) {
        window.applyTopicFilter();
      }
    });
  }
}

async function loadGroupHeat(selectedTopics = null) {
  console.time('[GROUPS] loadGroupHeat');
  document.getElementById('groupLoading')?.classList.add('active');
  const params = new URLSearchParams();
  if (selectedTopics && selectedTopics.length > 0) {
    params.set('topics', JSON.stringify(selectedTopics));
  }
  console.time('[GROUPS] Fetch by-group API');
  const res = await fetch('/api/analytics/by-group?' + params.toString());
  const json = await res.json();
  console.timeEnd('[GROUPS] Fetch by-group API');
  const topics = json.topics || [];
  const groups = json.groups || [];
  const matrix = topics.map(t => groups.map(g => {
    const r = (json.rows||[]).find(x => x.topic===t && x.grp===g);
    return r ? r.cnt : 0;
  }));
  const ctx = document.getElementById('groupHeat');
  const existing = Chart.getChart(ctx);
  if (existing) existing.destroy();
  // Render stacked bar: one dataset per topic, labels=groups
  const datasets = topics.map((t,i) => ({
    label: t,
    data: matrix[i],
    backgroundColor: `hsl(${(i*360)/Math.max(1,topics.length)},60%,60%)`,
    barPercentage: 0.75,
    categoryPercentage: 0.85
  }));
  new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: { labels: groups, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { top: 12, right: 16, bottom: 8, left: 8 } },
      plugins: { legend: { position: 'bottom', labels: { padding: 14, boxWidth: 14 } } },
      scales: {
        x: {
          stacked: true,
          ticks: { maxRotation: 55, minRotation: 45, padding: 8, maxTicksLimit: 20, font: { size: 11 } },
          grid: { display: false }
        },
        y: { stacked: true, beginAtZero: true, ticks: { padding: 8 } }
      }
    }
  });
  document.getElementById('groupLoading')?.classList.remove('active');
  console.timeEnd('[GROUPS] loadGroupHeat');
}

async function loadLanguageHeat(selectedTopics = null) {
  console.time('[LANGUAGES] loadLanguageHeat');
  document.getElementById('languageLoading')?.classList.add('active');
  const params = new URLSearchParams();
  if (selectedTopics && selectedTopics.length > 0) {
    params.set('topics', JSON.stringify(selectedTopics));
  }
  const res = await fetch('/api/analytics/by-language?' + params.toString());
  const json = await res.json();
  const topics = json.topics || [];
  const languages = json.languages || [];
  const matrix = topics.map(t => languages.map(lang => {
    const r = (json.rows || []).find(x => x.topic === t && x.language === lang);
    return r ? r.cnt : 0;
  }));
  const ctx = document.getElementById('languageHeat');
  const existing = Chart.getChart(ctx);
  if (existing) existing.destroy();
  const datasets = topics.map((t, i) => ({
    label: t,
    data: matrix[i],
    backgroundColor: `hsl(${(i * 360) / Math.max(1, topics.length)},60%,70%)`,
    barPercentage: 0.75,
    categoryPercentage: 0.85
  }));
  new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: { labels: languages, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { top: 12, right: 16, bottom: 8, left: 8 } },
      plugins: { legend: { position: 'bottom', labels: { padding: 14, boxWidth: 14 } } },
      scales: {
        x: {
          stacked: true,
          ticks: { maxRotation: 55, minRotation: 45, padding: 8, maxTicksLimit: 24, font: { size: 11 } },
          grid: { display: false }
        },
        y: { stacked: true, beginAtZero: true, ticks: { padding: 8 } }
      }
    }
  });
  document.getElementById('languageLoading')?.classList.remove('active');
  console.timeEnd('[LANGUAGES] loadLanguageHeat');
}

// Macro topics list for Top MEPs dropdown (set by loadAnalytics / loadTimeSeries)
window.macroTopicsFromOverview = [];

function updateTopMepTopicDropdown() {
  const topics = (window.allAvailableTopics && window.allAvailableTopics.length)
    ? window.allAvailableTopics
    : (window.macroTopicsFromOverview || []);
  window.topMepTopicList = [...new Set(topics)].sort((a, b) => a.localeCompare(b));
  // Re-render list if dropdown is open
  const listEl = document.getElementById('topMepTopicList');
  const inputEl = document.getElementById('topMepTopic');
  if (listEl && inputEl && listEl.style.display !== 'none') {
    renderTopMepTopicOptions(listEl, inputEl.value.trim());
  }
}

function renderTopMepTopicOptions(listEl, filter) {
  const topics = window.topMepTopicList || [];
  const q = (filter || '').toLowerCase();
  const filtered = q ? topics.filter(t => t.toLowerCase().includes(q)) : topics;
  listEl.innerHTML = [
    '<div class="top-mep-topic-option" data-value="">(All topics)</div>',
    ...filtered.map(t => `<div class="top-mep-topic-option" data-value="${escapeAttr(t)}">${escapeHtml(t)}</div>`)
  ].join('');
  listEl.style.display = 'block';
}
function escapeAttr(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function escapeHtml(s) {
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

function wireTopMepTopicDropdown() {
  const input = document.getElementById('topMepTopic');
  const list = document.getElementById('topMepTopicList');
  if (!input || !list) return;
  let blurTimer = null;
  input.addEventListener('focus', () => {
    clearTimeout(blurTimer);
    renderTopMepTopicOptions(list, input.value.trim());
    list.style.display = 'block';
  });
  input.addEventListener('input', () => {
    renderTopMepTopicOptions(list, input.value.trim());
    list.style.display = 'block';
  });
  input.addEventListener('blur', () => {
    blurTimer = setTimeout(() => { list.style.display = 'none'; }, 180);
  });
  list.addEventListener('mousedown', (e) => {
    e.preventDefault();
    const opt = e.target.closest('.top-mep-topic-option');
    if (!opt) return;
    const val = opt.getAttribute('data-value') || '';
    input.value = val;
    list.style.display = 'none';
    input.focus();
  });
}

async function loadTopMeps() {
  const topicInput = document.getElementById('topMepTopic');
  const topic = topicInput?.value.trim() || '';
  const params = new URLSearchParams();
  if (topic) params.set('topic', topic);
  params.set('top', '10');
  document.getElementById('mepLoading')?.classList.add('active');
  const res = await fetch('/api/analytics/top-meps?' + params.toString());
  const json = await res.json();
  const tbody = document.querySelector('#topMepTable tbody');
  const rows = json.rows || [];
  const topicForLink = topic ? encodeURIComponent(topic) : '';
  tbody.innerHTML = rows.length === 0
    ? '<tr><td colspan="4" style="color:#64748b;padding:1rem;">No data. Select a topic and click Load, or leave empty for top 10 across all topics.</td></tr>'
    : rows.map(r => {
        const displayName = r.label && r.label.trim() ? r.label.trim() : '(Unknown)';
        const href = r.id != null
          ? (topicForLink ? `/mep-details.html?id=${r.id}&macro_topic=${topicForLink}` : `/mep-details.html?id=${r.id}`)
          : '#';
        const mepCell = r.id != null
          ? `<a href="${href}" target="_blank" rel="noopener" class="mep-profile-link">${displayName}</a>`
          : displayName;
        return `<tr><td>${mepCell}</td><td>${r.country || ''}</td><td>${r.grp || ''}</td><td>${r.cnt}</td></tr>`;
      }).join('');
  document.getElementById('mepLoading')?.classList.remove('active');
}

// Wire analytics controls
document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'anaApply') {
    loadTimeSeries();
    loadGroupHeat();
    loadLanguageHeat();
  }
  if (e.target && e.target.id === 'loadTopMep') {
    loadTopMeps();
  }
});

// Wire analytics controls for granularity radios and time filter
document.addEventListener('change', (e) => {
  if (e.target && (e.target.id === 'granMonth' || e.target.id === 'granQuarter' || e.target.id === 'granYear')) {
    loadTimeSeries().then(() => {
      loadGroupHeat(window.selectedTopics);
      loadLanguageHeat(window.selectedTopics);
    });
  }
});
const applyTimeFilterBtn = document.getElementById('applyTimeFilter');
if (applyTimeFilterBtn) {
  applyTimeFilterBtn.addEventListener('click', () => {
    loadTimeSeries().then(() => {
      loadGroupHeat(window.selectedTopics);
      loadLanguageHeat(window.selectedTopics);
    });
  });
}

// --- Modal chart viewer ---
let modalChartInst = null;
function openChartModal(title, labels, datasets) {
  const modal = document.getElementById('chartModal');
  const titleEl = document.getElementById('chartModalTitle');
  const canvas = document.getElementById('modalChart');
  titleEl.textContent = title || 'Chart';
  modal.style.display = 'block';
  // Destroy previous instances
  if (modalChartInst) {
    try { modalChartInst.destroy(); } catch (_) {}
  }
  if (window.currentModalChart) {
    try { window.currentModalChart.destroy(); } catch (_) {}
    window.currentModalChart = null;
  }
  modalChartInst = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels, datasets },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } }, scales: { y: { beginAtZero: true } } }
  });
}

function closeChartModal() {
  const modal = document.getElementById('chartModal');
  modal.style.display = 'none';
  // Clean up modal chart instance
  if (window.currentModalChart) {
    try {
      window.currentModalChart.destroy();
    } catch (e) {
      // ignore
    }
    window.currentModalChart = null;
  }
  if (modalChartInst) {
    try {
      modalChartInst.destroy();
    } catch (e) {
      // ignore
    }
    modalChartInst = null;
  }
}

document.addEventListener('click', (e) => {
  if (e.target && (e.target.id === 'chartModalClose' || e.target.id === 'chartModalBackdrop')) {
    closeChartModal();
  }
});

// Attach click handlers to chart containers to open modal
function wireEnlargeableCharts() {
  // Top Macro Topics chart
  const macroCanvas = document.getElementById('macroTopicChart');
  if (macroCanvas) {
    macroCanvas.parentElement.style.cursor = 'zoom-in';
    macroCanvas.parentElement.addEventListener('click', () => {
      const chart = Chart.getChart(macroCanvas);
      if (!chart) return;
      const ds = chart.data.datasets.map(d => ({ ...d }));
      // Apply bar thickness settings to dataset
      ds.forEach(dataset => {
        dataset.maxBarThickness = 40;
        dataset.categoryPercentage = 0.7;
        dataset.barPercentage = 0.9;
      });
      const modalInst = new Chart(document.getElementById('modalChart').getContext('2d'), {
        type: 'bar',
        data: { labels: chart.data.labels.slice(), datasets: ds },
        options: { 
          responsive: true, 
          maintainAspectRatio: false, 
          indexAxis: 'y',
          layout: {
            padding: {
              top: 20,
              bottom: 20,
              left: 10,
              right: 10
            }
          },
          scales: { 
            x: { 
              beginAtZero: true,
              ticks: {
                font: {
                  size: 16
                }
              },
              title: {
                display: true,
                text: 'Number of Speeches',
                font: {
                  size: 18
                }
              }
            },
            y: {
              ticks: {
                font: {
                  size: 16
                }
              }
            }
          }, 
          plugins: { 
            legend: { display: false },
            tooltip: {
              titleFont: {
                size: 18
              },
              bodyFont: {
                size: 16
              },
              padding: 12
            }
          } 
        }
      });
      const modal = document.getElementById('chartModal');
      document.getElementById('chartModalTitle').textContent = 'Top Macro Topics';
      modal.style.display = 'block';
      if (window.currentModalChart) window.currentModalChart.destroy();
      window.currentModalChart = modalInst;
    });
  }
  
  // Top Specific Focus chart
  const focusCanvas = document.getElementById('specificFocusChart');
  if (focusCanvas) {
    focusCanvas.parentElement.style.cursor = 'zoom-in';
    focusCanvas.parentElement.addEventListener('click', () => {
      const chart = Chart.getChart(focusCanvas);
      if (!chart) return;
      const ds = chart.data.datasets.map(d => ({ ...d }));
      // Apply bar thickness settings to dataset
      ds.forEach(dataset => {
        dataset.maxBarThickness = 40;
        dataset.categoryPercentage = 0.7;
        dataset.barPercentage = 0.9;
      });
      const modalInst = new Chart(document.getElementById('modalChart').getContext('2d'), {
        type: 'bar',
        data: { labels: chart.data.labels.slice(), datasets: ds },
        options: { 
          responsive: true, 
          maintainAspectRatio: false, 
          indexAxis: 'y',
          layout: {
            padding: {
              top: 20,
              bottom: 20,
              left: 10,
              right: 10
            }
          },
          scales: { 
            x: { 
              beginAtZero: true,
              ticks: {
                font: {
                  size: 16
                }
              },
              title: {
                display: true,
                text: 'Number of Speeches',
                font: {
                  size: 18
                }
              }
            },
            y: {
              ticks: {
                font: {
                  size: 16
                }
              }
            }
          }, 
          plugins: { 
            legend: { display: false },
            tooltip: {
              titleFont: {
                size: 18
              },
              bodyFont: {
                size: 16
              },
              padding: 12
            }
          } 
        }
      });
      const modal = document.getElementById('chartModal');
      document.getElementById('chartModalTitle').textContent = 'Top Specific Focus';
      modal.style.display = 'block';
      if (window.currentModalChart) window.currentModalChart.destroy();
      window.currentModalChart = modalInst;
    });
  }
  
  // Monthly Trends
  const trendCard = document.querySelector('#analytics #trendChart')?.closest('.chart-container');
  if (trendCard) {
    trendCard.style.cursor = 'zoom-in';
    trendCard.addEventListener('click', () => {
      const chart = Chart.getChart(document.getElementById('trendChart'));
      if (!chart) return;
      // Clone datasets shallowly for modal
      const ds = chart.data.datasets.map(d => ({ ...d }));
      openChartModal('Monthly Trends', chart.data.labels.slice(), ds);
    });
  }
  // Group chart - horizontal stacked bar
  const groupCanvas = document.getElementById('groupHeat');
  if (groupCanvas) {
    groupCanvas.parentElement.style.cursor = 'zoom-in';
    groupCanvas.parentElement.addEventListener('click', () => {
      const chart = Chart.getChart(groupCanvas);
      if (!chart) return;
      const ds = chart.data.datasets.map(d => ({ ...d }));
      const modalInst = new Chart(document.getElementById('modalChart').getContext('2d'), {
        type: 'bar',
        data: { labels: chart.data.labels.slice(), datasets: ds },
        options: { 
          responsive: true, 
          maintainAspectRatio: false, 
          indexAxis: 'y',
          plugins: { legend: { position: 'bottom' } }, 
          scales: { 
            x: { stacked: true, beginAtZero: true },
            y: { stacked: true }
          }
        }
      });
      const modal = document.getElementById('chartModal');
      document.getElementById('chartModalTitle').textContent = 'Macro Topics × Political Groups';
      modal.style.display = 'block';
      if (window.currentModalChart) window.currentModalChart.destroy();
      window.currentModalChart = modalInst;
    });
  }
  
  // Macro Topics × Languages - stacked bar
  const languageCanvas = document.getElementById('languageHeat');
  if (languageCanvas) {
    languageCanvas.parentElement.style.cursor = 'zoom-in';
    languageCanvas.parentElement.addEventListener('click', () => {
      const chart = Chart.getChart(languageCanvas);
      if (!chart) return;
      const ds = chart.data.datasets.map(d => ({ ...d }));
      const modalInst = new Chart(document.getElementById('modalChart').getContext('2d'), {
        type: 'bar',
        data: { labels: chart.data.labels.slice(), datasets: ds },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          indexAxis: 'y',
          plugins: { legend: { position: 'bottom' } },
          scales: {
            x: { stacked: true, beginAtZero: true },
            y: { stacked: true }
          }
        }
      });
      document.getElementById('chartModalTitle').textContent = 'Macro Topics × Languages';
      document.getElementById('chartModal').style.display = 'block';
      if (window.currentModalChart) window.currentModalChart.destroy();
      window.currentModalChart = modalInst;
    });
  }
}

// Wire after analytics load
document.addEventListener('DOMContentLoaded', () => {
  wireTopMepTopicDropdown();
  setTimeout(wireEnlargeableCharts, 1000);
});


// Data Actions Dropdown (Refresh / Rebuild) — only shown when LOCALRUN env is set
(async function() {
  const container = document.getElementById('dataActionsContainer');
  const toggleBtn = document.getElementById('dataActionsToggle');
  const dropdown = document.getElementById('dataActionsDropdown');
  const jobConsoleEl = document.getElementById('dataJobConsole');
  const refreshBtn = document.getElementById('dataActionRefresh');
  const refreshMepDatasetBtn = document.getElementById('dataActionRefreshMepDataset');
  const rebuildBtn = document.getElementById('dataActionRebuild');
  const refreshLanguagesBtn = document.getElementById('dataActionRefreshLanguages');
  const normalizeTopicsBtn = document.getElementById('dataActionNormalizeTopics');
  const normalizePartiesBtn = document.getElementById('dataActionNormalizeParties');
  const iconEl = document.getElementById('dataActionsIcon');
  const textEl = document.getElementById('dataActionsText');
  const cacheStatus = document.getElementById('cacheStatus');
  const mepCountSpan = document.getElementById('mepCount');
  const speechCountSpan = document.getElementById('speechCount');
  const lastUpdatedSpan = document.getElementById('lastUpdated');

  if (!toggleBtn || !dropdown) return;

  try {
    const res = await fetch('/api/localrun');
    const data = await res.json();
    if (!data.localrun) {
      if (container) container.style.display = 'none';
      return;
    }
  } catch (_) {
    if (container) container.style.display = 'none';
    return;
  }

  let isWorking = false;
  let jobConsolePollTimer = null;

  function showJobConsole(line) {
    if (!jobConsoleEl) return;
    jobConsoleEl.textContent = line || '';
    jobConsoleEl.style.opacity = '1';
    jobConsoleEl.style.maxHeight = '48px';
  }

  function hideJobConsole() {
    if (!jobConsoleEl) return;
    jobConsoleEl.style.opacity = '0';
    jobConsoleEl.style.maxHeight = '0';
    setTimeout(() => { jobConsoleEl.textContent = ''; }, 300);
  }

  function startJobConsolePolling() {
    if (!jobConsoleEl) return;
    function poll() {
      if (!jobConsolePollTimer) return;
      fetch('/api/job-last-log')
        .then(r => r.json())
        .then(data => {
          if (data.line && jobConsoleEl) jobConsoleEl.textContent = data.line;
        })
        .catch(() => {});
    }
    poll();
    jobConsolePollTimer = setInterval(poll, 1500);
  }

  function stopJobConsolePolling() {
    if (jobConsolePollTimer) {
      clearInterval(jobConsolePollTimer);
      jobConsolePollTimer = null;
    }
  }

  function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.style.cssText = `
      position: fixed;
      top: 20px;
      right: 20px;
      background: ${type === 'success' ? '#4CAF50' : type === 'error' ? '#f44336' : '#2196F3'};
      color: white;
      padding: 12px 20px;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
      z-index: 1001;
      font-size: 14px;
      font-weight: 500;
      max-width: 300px;
      word-wrap: break-word;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);
    setTimeout(() => notification.remove(), 5000);
  }

  async function loadCacheStatus() {
    try {
      const response = await fetch('/api/cache-status');
      const status = await response.json();
      mepCountSpan.textContent = status.meps_last_updated ? 'Cached' : 'Not cached';
      speechCountSpan.textContent = status.total_speeches || 0;
      lastUpdatedSpan.textContent = status.speeches_last_updated ? new Date(status.speeches_last_updated).toLocaleString() : 'Never';
    } catch (e) {
      console.error('Error loading cache status:', e);
    }
  }

  toggleBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    dropdown.style.display = dropdown.style.display === 'flex' ? 'none' : 'flex';
  });

  document.addEventListener('click', () => {
    dropdown.style.display = 'none';
  });

  const analyzeBtn = document.getElementById('dataActionAnalyze');

  // Check New Sittings
  if (refreshBtn) {
    refreshBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      dropdown.style.display = 'none';
      if (isWorking) return;

      isWorking = true;
      iconEl.textContent = '...';
      textEl.textContent = 'Checking...';
      toggleBtn.disabled = true;
      showJobConsole('Checking new sittings...');
      startJobConsolePolling();

      try {
        const res = await fetch('/api/test-pipeline', { method: 'POST' });
        const data = await res.json();
        stopJobConsolePolling();
        if (data.success) {
          iconEl.textContent = '';
          textEl.textContent = 'Done';
          const msg = data.processed > 0
            ? `Refreshed: ${data.processed} sittings stored. Languages updated: ${data.language_detection_updated ?? 0}.`
            : `No new sittings. Languages updated: ${data.language_detection_updated ?? 0}.`;
          showJobConsole(msg);
          showNotification(msg, 'success');
          await loadCacheStatus();
          setTimeout(() => window.location.reload(), 1500);
        } else {
          throw new Error(data.message || data.error || 'Refresh failed');
        }
      } catch (err) {
        stopJobConsolePolling();
        showJobConsole('Error: ' + err.message);
        iconEl.textContent = '!';
        textEl.textContent = 'Failed';
        showNotification(err.message, 'error');
      } finally {
        setTimeout(() => {
          isWorking = false;
          iconEl.textContent = '';
          textEl.textContent = 'Data';
          toggleBtn.disabled = false;
          hideJobConsole();
        }, 3000);
      }
    });
  }

  // Build MEP Dataset — API upsert + link + historic (one per person) + group normalizer
  if (refreshMepDatasetBtn) {
    refreshMepDatasetBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      dropdown.style.display = 'none';

      const ok = confirm(
        'Build MEP Dataset?\n\n' +
        'This will clear all MEP data and rebuild from the API (term 5 to current), then link speeches and create historic MEPs for remaining speakers. Political groups will be normalized.\n\n' +
        'This may take a few minutes. Continue?'
      );
      if (!ok) return;

      if (isWorking) return;
      isWorking = true;
      iconEl.textContent = '...';
      textEl.textContent = 'Building...';
      toggleBtn.disabled = true;
      showJobConsole('Building MEP dataset...');
      startJobConsolePolling();
      showNotification('Building MEP dataset (API + historic + group normalization)...', 'info');

      try {
        const res = await fetch('/api/refresh-mep-dataset', { method: 'POST' });
        const data = await res.json();
        stopJobConsolePolling();
        if (data.success) {
          iconEl.textContent = '';
          textEl.textContent = 'Done';
          const msg = `Done: ${data.apiMeps} API, ${data.createdHistoric} historic, ${data.linkedSpeeches} speeches linked.`;
          showJobConsole(msg);
          showNotification(msg, 'success');
          await loadCacheStatus();
          setTimeout(() => window.location.reload(), 1500);
        } else {
          throw new Error(data.error || 'Build MEP dataset failed');
        }
      } catch (err) {
        stopJobConsolePolling();
        showJobConsole('Error: ' + err.message);
        iconEl.textContent = '!';
        textEl.textContent = 'Failed';
        showNotification(err.message, 'error');
      } finally {
        setTimeout(() => {
          isWorking = false;
          iconEl.textContent = '';
          textEl.textContent = 'Data';
          toggleBtn.disabled = false;
          hideJobConsole();
        }, 3000);
      }
    });
  }

  // Analyze / Generate Analytics Database (with warning)
  if (analyzeBtn) {
    analyzeBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      dropdown.style.display = 'none';

      const ok = confirm(
        '⚠️ Generate Analytics Database?\n\n' +
        'This will overwrite the existing analytics database with fresh calculations.\n\n' +
        'This process may take 1-5 minutes depending on your data size.\n\n' +
        'Continue?'
      );
      if (!ok) return;

      if (isWorking) return;
      isWorking = true;
      iconEl.textContent = '...';
      textEl.textContent = 'Analyzing...';
      toggleBtn.disabled = true;
      showJobConsole('Generating analytics database...');
      startJobConsolePolling();
      showNotification('Generating analytics database... This may take a few minutes.', 'info');

      try {
        const res = await fetch('/api/generate-analytics', { method: 'POST' });
        const data = await res.json();
        stopJobConsolePolling();
        if (data.success) {
          iconEl.textContent = '';
          textEl.textContent = 'Done';
          showJobConsole('Analytics database generated. Duration: ' + (data.duration || 'N/A'));
          showNotification(`Analytics database generated successfully! Duration: ${data.duration || 'N/A'}`, 'success');
          if (document.querySelector('.tab[data-tab="analytics"].active')) {
            setTimeout(() => window.location.reload(), 2000);
          }
        } else {
          throw new Error(data.error || 'Analytics generation failed');
        }
      } catch (err) {
        stopJobConsolePolling();
        showJobConsole('Error: ' + err.message);
        iconEl.textContent = '!';
        textEl.textContent = 'Failed';
        showNotification(err.message, 'error');
      } finally {
        setTimeout(() => {
          isWorking = false;
          iconEl.textContent = '';
          textEl.textContent = 'Data';
          toggleBtn.disabled = false;
          hideJobConsole();
        }, 3000);
      }
    });
  }

  // Refresh Languages — re-run language detection on all speeches
  if (refreshLanguagesBtn) {
    refreshLanguagesBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      dropdown.style.display = 'none';

      const ok = confirm(
        'Refresh Languages?\n\n' +
        'This will re-detect and overwrite the language for every speech in the database.\n\n' +
        'Use this if you updated the language detection script or want to fix incorrect languages.\n\n' +
        'Continue?'
      );
      if (!ok) return;

      if (isWorking) return;
      isWorking = true;
      iconEl.textContent = '...';
      textEl.textContent = 'Detecting...';
      toggleBtn.disabled = true;
      showJobConsole('Refreshing languages for all speeches...');
      startJobConsolePolling();
      showNotification('Refreshing languages for all speeches...', 'info');

      try {
        const res = await fetch('/api/refresh-languages', { method: 'POST' });
        const data = await res.json();
        stopJobConsolePolling();
        if (data.success) {
          iconEl.textContent = '';
          textEl.textContent = 'Done';
          showJobConsole(`Done: ${data.updated} updated (${data.total} total).`);
          showNotification(
            `Languages refreshed: ${data.updated} updated (${data.total} total)`,
            'success'
          );
          await loadCacheStatus();
          setTimeout(() => window.location.reload(), 1500);
        } else {
          throw new Error(data.error || 'Refresh languages failed');
        }
      } catch (err) {
        stopJobConsolePolling();
        showJobConsole('Error: ' + err.message);
        iconEl.textContent = '!';
        textEl.textContent = 'Failed';
        showNotification(err.message, 'error');
      } finally {
        setTimeout(() => {
          isWorking = false;
          iconEl.textContent = '';
          textEl.textContent = 'Data';
          toggleBtn.disabled = false;
          hideJobConsole();
        }, 3000);
      }
    });
  }

  // Normalize Macro Topics — AI suggests rules, then apply to DB
  if (normalizeTopicsBtn) {
    normalizeTopicsBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      dropdown.style.display = 'none';

      const ok = confirm(
        'Normalize Macro Topics?\n\n' +
        'This will use AI to group similar macro topics (e.g. "Foreign Policy Cuba" and "foreign policy central america") ' +
        'and update the database to use one label per group. Requires OPENAI_API_KEY.\n\n' +
        'Continue?'
      );
      if (!ok) return;

      if (isWorking) return;
      isWorking = true;
      iconEl.textContent = '...';
      textEl.textContent = 'Normalizing...';
      toggleBtn.disabled = true;
      showJobConsole('Normalizing macro topics...');
      startJobConsolePolling();
      showNotification('Normalizing macro topics (AI + applying rules)...', 'info');

      try {
        const res = await fetch('/api/normalize-macro-topics', { method: 'POST' });
        const data = await res.json();
        stopJobConsolePolling();
        if (data.success) {
          iconEl.textContent = '';
          textEl.textContent = 'Done';
          const msg = data.updated > 0 ? `${data.rules} rule(s), ${data.updated} speeches updated.` : (data.message || 'Done.');
          showJobConsole(msg);
          showNotification(
            data.updated > 0
              ? `Normalized: ${data.rules} rule(s), ${data.updated} speeches updated.`
              : `Normalized: ${data.rules} rule(s). ${data.message || ''}`,
            'success'
          );
          await loadCacheStatus();
          setTimeout(() => window.location.reload(), 1500);
        } else {
          throw new Error(data.error || 'Normalize macro topics failed');
        }
      } catch (err) {
        stopJobConsolePolling();
        showJobConsole('Error: ' + err.message);
        iconEl.textContent = '!';
        textEl.textContent = 'Failed';
        showNotification(err.message, 'error');
      } finally {
        setTimeout(() => {
          isWorking = false;
          iconEl.textContent = '';
          textEl.textContent = 'Data';
          toggleBtn.disabled = false;
          hideJobConsole();
        }, 3000);
      }
    });
  }

  // Normalize Parties — run political group normalizer on speeches (political_group_std)
  if (normalizePartiesBtn) {
    normalizePartiesBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      dropdown.style.display = 'none';

      const ok = confirm(
        'Normalize Parties?\n\n' +
        'This will run the political group normalizer on all speeches: raw party/affiliation text will be mapped to canonical groups (PPE, S&D, Renew, etc.). ' +
        'MEP Role/Affiliation on the All MEPs page will then show fewer "Unknown" entries where speeches have a recognizable group.\n\nContinue?'
      );
      if (!ok) return;

      if (isWorking) return;
      isWorking = true;
      iconEl.textContent = '...';
      textEl.textContent = 'Normalizing...';
      toggleBtn.disabled = true;
      showJobConsole('Normalizing parties (political groups)...');
      startJobConsolePolling();
      showNotification('Normalizing political groups...', 'info');

      try {
        const res = await fetch('/api/normalize-parties', { method: 'POST' });
        const data = await res.json();
        stopJobConsolePolling();
        if (data.success) {
          iconEl.textContent = '';
          textEl.textContent = 'Done';
          showJobConsole(data.message || 'Parties normalized.');
          showNotification('Political groups normalized.', 'success');
          await loadCacheStatus();
          setTimeout(() => window.location.reload(), 1500);
        } else {
          throw new Error(data.error || 'Normalize parties failed');
        }
      } catch (err) {
        stopJobConsolePolling();
        showJobConsole('Error: ' + err.message);
        iconEl.textContent = '!';
        textEl.textContent = 'Failed';
        showNotification(err.message, 'error');
      } finally {
        setTimeout(() => {
          isWorking = false;
          iconEl.textContent = '';
          textEl.textContent = 'Data';
          toggleBtn.disabled = false;
          hideJobConsole();
        }, 3000);
      }
    });
  }

  // Rebuild Database (with confirmation)
  if (rebuildBtn) {
    rebuildBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      dropdown.style.display = 'none';

      const ok = confirm(
        'Rebuild the entire database?\n\n' +
        'This will clear all sittings and rebuild from 1999. It can take many hours. Are you sure?'
      );
      if (!ok) return;

      if (isWorking) return;
      isWorking = true;
      iconEl.textContent = '...';
      textEl.textContent = 'Rebuilding...';
      toggleBtn.disabled = true;
      showJobConsole('Rebuilding database (sittings + speeches from 1999)...');
      startJobConsolePolling();

      try {
        const res = await fetch('/api/rebuild-database', { method: 'POST' });
        const data = await res.json();
        stopJobConsolePolling();
        if (data.success) {
          iconEl.textContent = '';
          textEl.textContent = 'Done';
          showJobConsole('Done: ' + (data.processed || 0) + ' sittings stored.');
          showNotification(`Rebuilt: ${data.processed || 0} sittings stored`, 'success');
          await loadCacheStatus();
          setTimeout(() => window.location.reload(), 2000);
        } else {
          throw new Error(data.error || 'Rebuild failed');
        }
      } catch (err) {
        stopJobConsolePolling();
        showJobConsole('Error: ' + err.message);
        iconEl.textContent = '!';
        textEl.textContent = 'Failed';
        showNotification(err.message, 'error');
      } finally {
        setTimeout(() => {
          isWorking = false;
          iconEl.textContent = '';
          textEl.textContent = 'Data';
          toggleBtn.disabled = false;
          hideJobConsole();
        }, 3000);
      }
    });
  }

  loadCacheStatus();
})();

// --- Export Functionality ---
(function() {
  // Get DOM elements
  const selectAllFieldsBtn = document.getElementById('selectAllFields');
  const deselectAllFieldsBtn = document.getElementById('deselectAllFields');
  const previewExportBtn = document.getElementById('previewExport');
  const exportCSVBtn = document.getElementById('exportCSV');
  const exportStatus = document.getElementById('exportStatus');
  const exportStats = document.getElementById('exportStats');
  const exportCount = document.getElementById('exportCount');
  const customDateRange = document.getElementById('customDateRange');
  const timeFrameRadios = document.querySelectorAll('input[name="timeFrame"]');
  const exportProgress = document.getElementById('exportProgress');
  const exportProgressBar = document.getElementById('exportProgressBar');
  const exportProgressPercent = document.getElementById('exportProgressPercent');
  const exportProgressMessage = document.getElementById('exportProgressMessage');

  // Field selection handlers
  if (selectAllFieldsBtn) {
    selectAllFieldsBtn.addEventListener('click', () => {
      document.querySelectorAll('.export-field').forEach(cb => cb.checked = true);
    });
  }

  if (deselectAllFieldsBtn) {
    deselectAllFieldsBtn.addEventListener('click', () => {
      document.querySelectorAll('.export-field').forEach(cb => cb.checked = false);
    });
  }

  // Show/hide custom date range
  timeFrameRadios.forEach(radio => {
    radio.addEventListener('change', (e) => {
      if (e.target.value === 'custom') {
        customDateRange.style.display = 'block';
      } else {
        customDateRange.style.display = 'none';
      }
    });
  });

  // Get selected fields
  function getSelectedFields() {
    const fields = [];
    document.querySelectorAll('.export-field:checked').forEach(cb => {
      fields.push(cb.getAttribute('data-field'));
    });
    return fields;
  }

  // Get time frame parameters
  function getTimeFrameParams() {
    const selectedTimeFrame = document.querySelector('input[name="timeFrame"]:checked').value;
    const params = {};
    
    const today = new Date();
    let startDate = null;
    
    switch(selectedTimeFrame) {
      case 'year':
        startDate = new Date(today);
        startDate.setFullYear(today.getFullYear() - 1);
        params.startDate = startDate.toISOString().split('T')[0];
        break;
      case '6months':
        startDate = new Date(today);
        startDate.setMonth(today.getMonth() - 6);
        params.startDate = startDate.toISOString().split('T')[0];
        break;
      case '3months':
        startDate = new Date(today);
        startDate.setMonth(today.getMonth() - 3);
        params.startDate = startDate.toISOString().split('T')[0];
        break;
      case 'custom':
        const customStart = document.getElementById('exportStartDate').value;
        const customEnd = document.getElementById('exportEndDate').value;
        if (customStart) params.startDate = customStart;
        if (customEnd) params.endDate = customEnd;
        break;
      case 'all':
      default:
        // No date restrictions
        break;
    }
    
    return params;
  }

  // Debug log area
  const debugLog = document.createElement('div');
  debugLog.id = 'exportDebugLog';
  debugLog.style.cssText = `
    margin-top: 1rem;
    padding: 1rem;
    background: #1e293b;
    color: #e2e8f0;
    border-radius: 6px;
    font-family: 'Courier New', monospace;
    font-size: 12px;
    max-height: 300px;
    overflow-y: auto;
    display: none;
  `;
  if (exportProgress) {
    exportProgress.parentNode.insertBefore(debugLog, exportProgress.nextSibling);
  }

  function addDebugLog(message, type = 'info') {
    const timestamp = new Date().toLocaleTimeString();
    const color = type === 'error' ? '#ef4444' : type === 'success' ? '#10b981' : '#60a5fa';
    debugLog.innerHTML += `<div style="color:${color};margin-bottom:4px;">[${timestamp}] ${message}</div>`;
    debugLog.scrollTop = debugLog.scrollHeight;
    debugLog.style.display = 'block';
    console.log(`[EXPORT] ${message}`);
  }

  function clearDebugLog() {
    debugLog.innerHTML = '';
    debugLog.style.display = 'none';
  }

  // Preview export count
  if (previewExportBtn) {
    previewExportBtn.addEventListener('click', async () => {
      try {
        clearDebugLog();
        const startTime = Date.now();
        addDebugLog('Preview request started', 'info');
        
        exportStatus.textContent = 'Loading count...';
        exportStatus.style.color = '#3b82f6';
        
        const timeParams = getTimeFrameParams();
        addDebugLog(`Time frame: ${JSON.stringify(timeParams)}`, 'info');
        
        const queryParams = new URLSearchParams({
          ...timeParams,
          countOnly: 'true'
        });
        
        addDebugLog('Sending request to /api/export/speeches', 'info');
        const fetchStart = Date.now();
        
        const response = await fetch(`/api/export/speeches?${queryParams.toString()}`);
        
        const fetchTime = Date.now() - fetchStart;
        addDebugLog(`Response received in ${fetchTime}ms`, 'success');
        
        const data = await response.json();
        
        if (data.count !== undefined) {
          const totalTime = Date.now() - startTime;
          addDebugLog(`Found ${data.count.toLocaleString()} speeches`, 'success');
          addDebugLog(`Total preview time: ${totalTime}ms`, 'success');
          
          exportCount.textContent = data.count.toLocaleString();
          exportStats.style.display = 'block';
          exportStatus.textContent = '✅ Preview loaded';
          exportStatus.style.color = '#10b981';
        } else {
          throw new Error('Invalid response from server');
        }
      } catch (error) {
        console.error('Error previewing export:', error);
        addDebugLog(`Error: ${error.message}`, 'error');
        exportStatus.textContent = '❌ Error loading preview';
        exportStatus.style.color = '#ef4444';
      }
    });
  }

  // Export to CSV
  if (exportCSVBtn) {
    exportCSVBtn.addEventListener('click', async () => {
      try {
        clearDebugLog();
        const totalStartTime = Date.now();
        addDebugLog('Export request started', 'info');
        
        const selectedFields = getSelectedFields();
        addDebugLog(`Selected ${selectedFields.length} fields: ${selectedFields.join(', ')}`, 'info');
        
        if (selectedFields.length === 0) {
          exportStatus.textContent = '⚠️ Please select at least one field';
          exportStatus.style.color = '#f59e0b';
          addDebugLog('No fields selected', 'error');
          return;
        }
        
        exportStatus.textContent = 'Preparing export...';
        exportStatus.style.color = '#3b82f6';
        exportProgress.style.display = 'block';
        exportProgressBar.style.width = '10%';
        exportProgressPercent.textContent = '10%';
        exportProgressMessage.textContent = 'Fetching data from server...';
        
        const timeParams = getTimeFrameParams();
        addDebugLog(`Time frame params: ${JSON.stringify(timeParams)}`, 'info');
        
        const queryParams = new URLSearchParams({
          ...timeParams,
          fields: selectedFields.join(',')
        });
        
        addDebugLog('Building request URL...', 'info');
        const requestUrl = `/api/export/speeches?${queryParams.toString()}`;
        addDebugLog(`Request URL: ${requestUrl}`, 'info');
        
        exportProgressBar.style.width = '30%';
        exportProgressPercent.textContent = '30%';
        exportProgressMessage.textContent = 'Downloading data...';
        
        addDebugLog('Sending fetch request to server...', 'info');
        const fetchStart = Date.now();
        
        const response = await fetch(requestUrl);
        
        const fetchTime = Date.now() - fetchStart;
        addDebugLog(`Server response received in ${fetchTime}ms (${(fetchTime/1000).toFixed(2)}s)`, 'success');
        
        if (!response.ok) {
          addDebugLog(`Server returned error: ${response.status} ${response.statusText}`, 'error');
          throw new Error('Export failed: ' + response.statusText);
        }
        
        const contentLength = response.headers.get('content-length');
        if (contentLength) {
          const sizeMB = (parseInt(contentLength) / 1024 / 1024).toFixed(2);
          addDebugLog(`Response size: ${sizeMB} MB`, 'info');
        }
        
        exportProgressBar.style.width = '60%';
        exportProgressPercent.textContent = '60%';
        exportProgressMessage.textContent = 'Processing CSV...';
        
        addDebugLog('Converting response to blob...', 'info');
        const blobStart = Date.now();
        
        const blob = await response.blob();
        
        const blobTime = Date.now() - blobStart;
        const blobSizeMB = (blob.size / 1024 / 1024).toFixed(2);
        addDebugLog(`Blob created in ${blobTime}ms - Size: ${blobSizeMB} MB`, 'success');
        
        exportProgressBar.style.width = '90%';
        exportProgressPercent.textContent = '90%';
        exportProgressMessage.textContent = 'Preparing download...';
        
        addDebugLog('Creating download link...', 'info');
        // Create download link
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        
        // Generate filename with timestamp
        const timestamp = new Date().toISOString().split('T')[0];
        const timeFrame = document.querySelector('input[name="timeFrame"]:checked').value;
        const filename = `eu_speeches_${timeFrame}_${timestamp}.csv`;
        a.download = filename;
        
        addDebugLog(`Filename: ${filename}`, 'info');
        
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        exportProgressBar.style.width = '100%';
        exportProgressPercent.textContent = '100%';
        exportProgressMessage.textContent = 'Export complete!';
        
        const totalTime = Date.now() - totalStartTime;
        addDebugLog('Export completed successfully!', 'success');
        addDebugLog(`Total export time: ${totalTime}ms (${(totalTime/1000).toFixed(2)}s)`, 'success');
        
        exportStatus.textContent = '✅ Export completed';
        exportStatus.style.color = '#10b981';
        
        // Hide progress bar after 3 seconds
        setTimeout(() => {
          exportProgress.style.display = 'none';
          exportProgressBar.style.width = '0%';
        }, 3000);
        
      } catch (error) {
        console.error('Error exporting to CSV:', error);
        addDebugLog(`Export failed: ${error.message}`, 'error');
        addDebugLog(`Stack trace: ${error.stack}`, 'error');
        exportStatus.textContent = '❌ Export failed: ' + error.message;
        exportStatus.style.color = '#ef4444';
        exportProgress.style.display = 'none';
      }
    });
  }
})();

