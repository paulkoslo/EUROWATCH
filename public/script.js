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
      // If switching to speeches tab, load speeches for selected MEP
      if (tab.dataset.tab === 'speeches') {
        loadSpeeches(mepSelect.value, true);
      }
      // If switching to analytics tab, load analytics
      if (tab.dataset.tab === 'analytics') {
        console.log('🔄 [ANALYTICS] Tab switched - checking cache status');
        
        // Check cache status first
        checkCacheStatus().then(status => {
          if (status && status.ready) {
            console.log('✅ [CACHE] Cache ready, loading analytics');
            console.time('⏱️ [ANALYTICS] Total tab load time');
            loadAnalytics();
            loadTimeSeries().then(() => {
              console.log('✅ [ANALYTICS] Time series loaded, loading other charts...');
              // After time series loads and populates selectedTopics, load other charts
              Promise.all([
                loadGroupHeat(window.selectedTopics),
                loadCountryHeat(window.selectedTopics),
                loadLanguages(window.selectedTopics)
              ]).then(() => {
                console.timeEnd('⏱️ [ANALYTICS] Total tab load time');
                console.log('✅ [ANALYTICS] All charts loaded successfully');
              });
            });
          } else {
            console.log('⏳ [CACHE] Cache not ready yet, waiting...');
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
  
  // --- Historic MEP Linking functionality ---
  const linkHistoricMepsBtn = document.getElementById('linkHistoricMepsBtn');
  const linkingStatus = document.getElementById('linkingStatus');
  
  linkHistoricMepsBtn.addEventListener('click', async () => {
    try {
      linkHistoricMepsBtn.disabled = true;
      linkHistoricMepsBtn.textContent = '🔄 Processing...';
      linkingStatus.textContent = 'Creating historic MEPs and linking speeches...';
      linkingStatus.style.color = 'var(--eu-blue)';
      
      const response = await fetch('/api/link-historic-meps', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      const result = await response.json();
      
      if (result.success) {
        linkingStatus.textContent = `✅ Success! Created ${result.createdHistoricMeps} historic MEPs and linked ${result.linkedSpeeches} speeches.`;
        linkingStatus.style.color = 'var(--eu-green)';
        
        // Refresh MEP data to show new historic MEPs
        setTimeout(async () => {
          const res = await fetch('/api/meps');
          const json = await res.json();
          mepsList = json.data || [];
          filteredMeps = [...mepsList];
          updateCharts(filteredMeps);
          renderMepsTable();
          document.getElementById('total-count').textContent = mepsList.length;
        }, 2000);
        
      } else {
        linkingStatus.textContent = `❌ Error: ${result.error}`;
        linkingStatus.style.color = 'var(--eu-red)';
      }
      
    } catch (error) {
      console.error('Error linking historic MEPs:', error);
      linkingStatus.textContent = `❌ Error: ${error.message}`;
      linkingStatus.style.color = 'var(--eu-red)';
    } finally {
      linkHistoricMepsBtn.disabled = false;
      linkHistoricMepsBtn.textContent = '🔗 Create Historic MEPs & Link Speeches';
    }
  });

  // --- Add filter event listeners ---
  statusFilter.addEventListener('change', applyFilters);
  countryFilter.addEventListener('change', applyFilters);
  groupFilter.addEventListener('change', applyFilters);
  speechCountFilter.addEventListener('change', applyFilters);

  // --- Global function to view MEP details ---
  window.viewMepDetails = function(mepId) {
    console.log(`🔍 [FRONTEND] Opening MEP details for ID: ${mepId}`);
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
      const date = s.date || '';
      const htmlUrl = date
        ? `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`
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
            // If the preview API fails, try to fetch the TOC page and extract some content
            const tocUrl = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}-TOC_EN.html`;
            try {
              const resp = await fetch(tocUrl);
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

  // --- Initial load of speeches if speeches tab is active on page load ---
  tabs.forEach(tab => {
    if (tab.dataset.tab === 'speeches' && tab.classList.contains('active')) {
      loadSpeeches(mepSelect.value, true);
    }
    if (tab.dataset.tab === 'analytics' && tab.classList.contains('active')) {
      console.log('🔄 [ANALYTICS] Initial load - checking cache status');
      
      // Check cache status first
      checkCacheStatus().then(status => {
        if (status && status.ready) {
          console.log('✅ [CACHE] Cache ready, loading analytics');
          console.time('⏱️ [ANALYTICS] Initial total load time');
          loadAnalytics();
          loadTimeSeries().then(() => {
            console.log('✅ [ANALYTICS] Time series loaded, loading other charts...');
            // After time series loads and populates selectedTopics, load other charts
            Promise.all([
              loadGroupHeat(window.selectedTopics),
              loadCountryHeat(window.selectedTopics),
              loadLanguages(window.selectedTopics)
            ]).then(() => {
              console.timeEnd('⏱️ [ANALYTICS] Initial total load time');
              console.log('✅ [ANALYTICS] All charts loaded successfully');
            });
          });
        } else {
          console.log('⏳ [CACHE] Cache not ready yet, waiting...');
        }
      });
    }
  });
})();

// --- Descriptive Analytics ---
async function loadAnalytics() {
  try {
    console.time('⏱️ [ANALYTICS] Total loadAnalytics');
    document.getElementById('trendLoading')?.classList.add('active');
    document.getElementById('groupLoading')?.classList.add('active');
    document.getElementById('countryLoading')?.classList.add('active');
    document.getElementById('langLoading')?.classList.add('active');
    
    console.time('⏱️ [ANALYTICS] Fetch overview API');
    const res = await fetch('/api/analytics/overview');
    const data = await res.json();
    console.timeEnd('⏱️ [ANALYTICS] Fetch overview API');
    
    if (!data || data.error) throw new Error(data?.error || 'Analytics API error');

    // Coverage
    const covEl = document.getElementById('coverageStats');
    if (covEl && data.coverage) {
      covEl.textContent = `Macro coverage: ${data.coverage.with_macro.toLocaleString()} of ${data.coverage.total.toLocaleString()} speeches (${data.coverage.pct_with_macro}%)`;
    }

    // Top Macro Topics bar
    const macroLabels = (data.macroTopicDistribution || []).map(r => r.topic);
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
    console.timeEnd('⏱️ [ANALYTICS] Total loadAnalytics');
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

// Check cache status and show loading progress
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
      console.log('✅ [CACHE] Cache is ready!');
      return status;
    } else if (status.warming) {
      // Cache is warming - show progress
      progressDiv.style.display = 'block';
      const percent = status.progress?.percent || 0;
      progressBar.style.width = percent + '%';
      progressPercent.textContent = percent + '%';
      progressMessage.textContent = status.progress?.message || 'Loading...';
      console.log(`⏳ [CACHE] Warming... ${percent}% - ${status.progress?.message}`);
      
      // Poll again in 500ms and continue when ready
      return new Promise(resolve => {
        setTimeout(() => {
          checkCacheStatus().then(resolve);
        }, 500);
      });
    } else {
      // Cache not started yet - show waiting message
      progressDiv.style.display = 'block';
      progressBar.style.width = '0%';
      progressPercent.textContent = '';
      progressMessage.textContent = 'Initializing analytics cache...';
      console.log('⏳ [CACHE] Cache not started yet, waiting...');
      
      // Poll again in 1s
      return new Promise(resolve => {
        setTimeout(() => {
          checkCacheStatus().then(resolve);
        }, 1000);
      });
    }
  } catch (error) {
    console.error('Error checking cache status:', error);
    return null;
  }
}

// Helpers for extended analytics
async function loadTimeSeries() {
  console.time('⏱️ [TRENDS] Total loadTimeSeries');
  document.getElementById('trendLoading')?.classList.add('active');
  // Read granularity from radio buttons
  const interval = document.getElementById('granQuarter')?.checked ? 'quarter' : 'month';
  const params = new URLSearchParams();
  params.set('interval', interval);
  params.set('all', 'true'); // always fetch all topics over full range
  
  // Update title based on granularity
  const titleEl = document.getElementById('trendChartTitle');
  if (titleEl) {
    titleEl.textContent = interval === 'quarter' ? 'Quarterly Trends (Top Topics)' : 'Monthly Trends (Top Topics)';
  }
  
  console.time('⏱️ [TRENDS] Fetch time-series API');
  const res = await fetch('/api/analytics/time-series?' + params.toString());
  const json = await res.json();
  console.timeEnd('⏱️ [TRENDS] Fetch time-series API');

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
  window.selectedTopics = [...allLabels]; // Initialize with all topics selected
  window.allAvailableTopics = [...allLabels]; // Store all available topics
  
  const selector = document.getElementById('topicSelector');
  if (selector) {
    selector.innerHTML = allLabels.map(label => {
      const id = 'sel_' + label.replace(/[^a-z0-9]/gi,'_');
      return `<label style=\"display:flex; align-items:center; gap:6px; font-size:13px; color:#374151; padding:4px 6px; border-radius:4px; background:#fff; border:1px solid #e2e8f0;\">\
        <input type=\"checkbox\" class=\"topicCheck\" id=\"${id}\" data-label=\"${label}\" checked>\
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
  
  let datasets = (json.datasets||[]).map((d, i) => ({
    ...d,
    borderColor: window.getDistinctColor(i),
    backgroundColor: window.getDistinctColor(i),
    tension: 0.2,
    borderWidth: 2
  }));
  new Chart(ctx.getContext('2d'), {
    type: 'line',
    data: { labels: json.labels || [], datasets },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } }, scales: { y: { beginAtZero: true } } }
  });

  // Initialize filter controls after chart is created
  initializeFilterControls();

  document.getElementById('trendLoading')?.classList.remove('active');
  console.timeEnd('⏱️ [TRENDS] Total loadTimeSeries');
}

// Apply topic filter function (accessible globally)
window.applyTopicFilter = async function() {
  const selected = Array.from(document.querySelectorAll('.topicCheck'))
    .filter(c=>c.checked)
    .map(c=>c.getAttribute('data-label'));
  
  console.log(`🔄 [FILTER] Applying filter with ${selected.length} topics`);
  
  // Update global selected topics
  window.selectedTopics = selected;
  
  // Update trend chart
  const ctx = document.getElementById('trendChart');
  const chart = Chart.getChart(ctx);
  if (chart && window.trendChartData) {
    const showSmooth = document.getElementById('showSmoothAvg')?.checked;
    const windowSize = parseInt(document.getElementById('smoothWindow')?.value || '6');
    
    let datasets = (window.trendChartData.datasets||[])
      .filter(ds => selected.includes(ds.label))
      .map((d, i) => {
        const color = window.getDistinctColor(i);
        return {
          ...d,
          label: d.label,
          borderColor: showSmooth ? color + '40' : color, // Light/transparent when smooth is on
          backgroundColor: showSmooth ? color + '20' : color + '40',
          tension: 0.2,
          borderWidth: showSmooth ? 1 : 2, // Thinner when smooth is on
          borderDash: [], // Solid line for actual data
          pointRadius: showSmooth ? 2 : 3, // Smaller points when smooth is on
          pointBackgroundColor: showSmooth ? color + '60' : color
        };
      });
    
    // Add smooth average datasets if enabled
    if (showSmooth) {
      const smoothDatasets = datasets.map((d, i) => {
        const color = window.getDistinctColor(i);
        return {
          label: d.label.replace(/ \(avg\)$/, '') + ' (trend)',
          data: calculateMovingAverage(d.data, windowSize),
          borderColor: color, // Full opacity for trend line
          backgroundColor: color,
          tension: 0.4,
          borderWidth: 3, // Thick line for visibility
          borderDash: [], // Solid line for trend
          pointRadius: 0,
          fill: false,
          order: -1 // Draw on top
        };
      });
      datasets = [...datasets, ...smoothDatasets];
    }
    
    chart.data.datasets = datasets;
    chart.update();
  }
  
  // Update the other three charts with the selected topics
  await Promise.all([
    loadGroupHeat(selected),
    loadCountryHeat(selected),
    loadLanguages(selected)
  ]);
  
  console.log('✅ [FILTER] All charts updated');
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
  console.time('⏱️ [GROUPS] loadGroupHeat');
  document.getElementById('groupLoading')?.classList.add('active');
  const params = new URLSearchParams();
  if (selectedTopics && selectedTopics.length > 0) {
    params.set('topics', JSON.stringify(selectedTopics));
  }
  console.time('⏱️ [GROUPS] Fetch by-group API');
  const res = await fetch('/api/analytics/by-group?' + params.toString());
  const json = await res.json();
  console.timeEnd('⏱️ [GROUPS] Fetch by-group API');
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
  const datasets = topics.map((t,i) => ({ label: t, data: matrix[i], backgroundColor: `hsl(${(i*360)/Math.max(1,topics.length)},60%,60%)` }));
  new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: { labels: groups, datasets },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } }, scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } } }
  });
  document.getElementById('groupLoading')?.classList.remove('active');
  console.timeEnd('⏱️ [GROUPS] loadGroupHeat');
}

async function loadCountryHeat(selectedTopics = null) {
  console.time('⏱️ [COUNTRIES] loadCountryHeat');
  document.getElementById('countryLoading')?.classList.add('active');
  const params = new URLSearchParams();
  if (selectedTopics && selectedTopics.length > 0) {
    params.set('topics', JSON.stringify(selectedTopics));
  }
  console.time('⏱️ [COUNTRIES] Fetch by-country API');
  const res = await fetch('/api/analytics/by-country?' + params.toString());
  const json = await res.json();
  console.timeEnd('⏱️ [COUNTRIES] Fetch by-country API');
  const topics = json.topics || [];
  const countries = json.countries || [];
  const matrix = topics.map(t => countries.map(c => {
    const r = (json.rows||[]).find(x => x.topic===t && x.country===c);
    return r ? r.cnt : 0;
  }));
  const ctx = document.getElementById('countryHeat');
  const existing = Chart.getChart(ctx);
  if (existing) existing.destroy();
  const datasets = topics.map((t,i) => ({ label: t, data: matrix[i], backgroundColor: `hsl(${(i*360)/Math.max(1,topics.length)},60%,70%)` }));
  new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: { labels: countries, datasets },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } }, scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } } }
  });
  document.getElementById('countryLoading')?.classList.remove('active');
  console.timeEnd('⏱️ [COUNTRIES] loadCountryHeat');
}

async function loadLanguages(selectedTopics = null) {
  console.time('⏱️ [LANGUAGES] loadLanguages');
  document.getElementById('langLoading')?.classList.add('active');
  const params = new URLSearchParams();
  if (selectedTopics && selectedTopics.length > 0) {
    params.set('topics', JSON.stringify(selectedTopics));
  }
  console.time('⏱️ [LANGUAGES] Fetch languages API');
  const res = await fetch('/api/analytics/languages?' + params.toString());
  const json = await res.json();
  console.timeEnd('⏱️ [LANGUAGES] Fetch languages API');
  const labels = (json.rows||[]).map(r => r.language);
  const data = (json.rows||[]).map(r => r.cnt);
  const ctx = document.getElementById('langChart');
  const existing = Chart.getChart(ctx);
  if (existing) existing.destroy();
  new Chart(ctx.getContext('2d'), {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: labels.map((_,i)=>`hsl(${(i*360)/Math.max(1,labels.length)},70%,60%)`) }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
  });
  document.getElementById('langLoading')?.classList.remove('active');
  console.timeEnd('⏱️ [LANGUAGES] loadLanguages');
}

async function loadTopMeps() {
  document.getElementById('mepLoading')?.classList.add('active');
  const topic = document.getElementById('topMepTopic').value.trim();
  const params = new URLSearchParams();
  if (topic) params.set('topic', topic);
  const res = await fetch('/api/analytics/top-meps?' + params.toString());
  const json = await res.json();
  const tbody = document.querySelector('#topMepTable tbody');
  tbody.innerHTML = (json.rows||[]).map(r => `<tr><td>${r.label||'(Unknown)'}</td><td>${r.country||''}</td><td>${r.grp||''}</td><td>${r.cnt}</td></tr>`).join('');
  document.getElementById('mepLoading')?.classList.remove('active');
}

// Wire analytics controls
document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'anaApply') {
    loadTimeSeries();
    loadGroupHeat();
    loadCountryHeat();
    loadLanguages();
  }
  if (e.target && e.target.id === 'loadTopMep') {
    loadTopMeps();
  }
});

// Wire analytics controls for granularity radios
document.addEventListener('change', (e) => {
  if (e.target && (e.target.id === 'granMonth' || e.target.id === 'granQuarter')) {
    loadTimeSeries().then(() => {
      // After time series reloads with new granularity, reload other charts
      loadGroupHeat(window.selectedTopics);
      loadCountryHeat(window.selectedTopics);
      loadLanguages(window.selectedTopics);
    });
  }
});

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
      const modalInst = new Chart(document.getElementById('modalChart').getContext('2d'), {
        type: 'bar',
        data: { labels: chart.data.labels.slice(), datasets: ds },
        options: { 
          responsive: true, 
          maintainAspectRatio: false, 
          indexAxis: 'y', 
          scales: { x: { beginAtZero: true } }, 
          plugins: { legend: { display: false } } 
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
      const modalInst = new Chart(document.getElementById('modalChart').getContext('2d'), {
        type: 'bar',
        data: { labels: chart.data.labels.slice(), datasets: ds },
        options: { 
          responsive: true, 
          maintainAspectRatio: false, 
          indexAxis: 'y', 
          scales: { x: { beginAtZero: true } }, 
          plugins: { legend: { display: false } } 
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
  
  // Country chart - horizontal stacked bar
  const countryCanvas = document.getElementById('countryHeat');
  if (countryCanvas) {
    countryCanvas.parentElement.style.cursor = 'zoom-in';
    countryCanvas.parentElement.addEventListener('click', () => {
      const chart = Chart.getChart(countryCanvas);
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
      document.getElementById('chartModalTitle').textContent = 'Macro Topics × Countries';
      modal.style.display = 'block';
      if (window.currentModalChart) window.currentModalChart.destroy();
      window.currentModalChart = modalInst;
    });
  }
  
  // Languages chart - doughnut
  const langCanvas = document.getElementById('langChart');
  if (langCanvas) {
    langCanvas.parentElement.style.cursor = 'zoom-in';
    langCanvas.parentElement.addEventListener('click', () => {
      const chart = Chart.getChart(langCanvas);
      if (!chart) return;
      const ds = chart.data.datasets.map(d => ({ ...d }));
      const modalInst = new Chart(document.getElementById('modalChart').getContext('2d'), {
        type: 'doughnut',
        data: { labels: chart.data.labels.slice(), datasets: ds },
        options: { 
          responsive: true, 
          maintainAspectRatio: false,
          plugins: { legend: { position: 'right' } }
        }
      });
      const modal = document.getElementById('chartModal');
      document.getElementById('chartModalTitle').textContent = 'Languages';
      modal.style.display = 'block';
      if (window.currentModalChart) window.currentModalChart.destroy();
      window.currentModalChart = modalInst;
    });
  }
}

// Wire after analytics load
document.addEventListener('DOMContentLoaded', () => {
  setTimeout(wireEnlargeableCharts, 1000);
});


// Refresh Button Functionality
(function() {
  const refreshBtn = document.getElementById('refreshDataBtn');
  const refreshIcon = document.getElementById('refreshIcon');
  const refreshText = document.getElementById('refreshText');
  const cacheStatus = document.getElementById('cacheStatus');
  const mepCountSpan = document.getElementById('mepCount');
  const speechCountSpan = document.getElementById('speechCount');
  const lastUpdatedSpan = document.getElementById('lastUpdated');
  
  let isRefreshing = false;
  
  // Load cache status on page load
  async function loadCacheStatus() {
    try {
      console.log('🔄 [FRONTEND] Loading cache status...');
      const response = await fetch('/api/cache-status');
      const status = await response.json();
      
      console.log('📊 [FRONTEND] Cache status received:', status);
      
      mepCountSpan.textContent = status.meps_last_updated ? 'Cached' : 'Not cached';
      speechCountSpan.textContent = status.total_speeches || 0;
      
      if (status.speeches_last_updated) {
        const date = new Date(status.speeches_last_updated);
        lastUpdatedSpan.textContent = date.toLocaleString();
        console.log(`✅ [FRONTEND] Cache status loaded - MEPs: ${status.meps_last_updated ? 'Cached' : 'Not cached'}, Speeches: ${status.total_speeches}, Last updated: ${date.toLocaleString()}`);
      } else {
        lastUpdatedSpan.textContent = 'Never';
        console.log('⚠️ [FRONTEND] No cache data found');
      }
    } catch (error) {
      console.error('❌ [FRONTEND] Error loading cache status:', error);
    }
  }
  
  // Show/hide cache status
  refreshBtn.addEventListener('mouseenter', () => {
    cacheStatus.style.display = 'block';
  });
  
  refreshBtn.addEventListener('mouseleave', () => {
    cacheStatus.style.display = 'none';
  });
  
  // Refresh data function
  async function refreshAllData() {
    if (isRefreshing) return;
    
    console.log('🔄 [FRONTEND] Starting data refresh...');
    isRefreshing = true;
    refreshIcon.textContent = '⏳';
    refreshText.textContent = 'Refreshing...';
    refreshBtn.disabled = true;
    refreshBtn.style.opacity = '0.7';
    
    try {
      console.log('📡 [FRONTEND] Sending perfect refresh request to server...');
      const response = await fetch('/api/refresh-perfect', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          startDate: '2023-01-01' // Default to 2023, can be made configurable
        })
      });
      
      const result = await response.json();
      console.log('📊 [FRONTEND] Refresh response received:', result);
      
      if (result.success) {
        refreshIcon.textContent = '✅';
        refreshText.textContent = 'Refreshed!';
        
        console.log(`✅ [FRONTEND] Refresh successful - MEPs: ${result.meps_count}, Speeches: ${result.speeches_count}`);
        
        // Update cache status
        await loadCacheStatus();
        
        // Show success message
        showNotification(`Perfect refresh completed. Reloading data...`, 'success');
        
        // Reload the page to show updated data
        setTimeout(() => {
          console.log('🔄 [FRONTEND] Reloading page to show updated data...');
          window.location.reload();
        }, 2000);
      } else {
        throw new Error(result.error || 'Refresh failed');
      }
    } catch (error) {
      console.error('❌ [FRONTEND] Error refreshing data:', error);
      refreshIcon.textContent = '❌';
      refreshText.textContent = 'Failed';
      showNotification('Failed to refresh data: ' + error.message, 'error');
    } finally {
      setTimeout(() => {
        isRefreshing = false;
        refreshIcon.textContent = '🔄';
        refreshText.textContent = 'Refresh Data';
        refreshBtn.disabled = false;
        refreshBtn.style.opacity = '1';
      }, 3000);
    }
  }
  
  // Add click event listener
  refreshBtn.addEventListener('click', refreshAllData);
  
  // Load initial cache status
  loadCacheStatus();
  
  // Show notification function
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
    
    setTimeout(() => {
      notification.remove();
    }, 5000);
  }
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
        addDebugLog('🔍 Preview request started', 'info');
        
        exportStatus.textContent = 'Loading count...';
        exportStatus.style.color = '#3b82f6';
        
        const timeParams = getTimeFrameParams();
        addDebugLog(`⏰ Time frame: ${JSON.stringify(timeParams)}`, 'info');
        
        const queryParams = new URLSearchParams({
          ...timeParams,
          countOnly: 'true'
        });
        
        addDebugLog(`📡 Sending request to /api/export/speeches`, 'info');
        const fetchStart = Date.now();
        
        const response = await fetch(`/api/export/speeches?${queryParams.toString()}`);
        
        const fetchTime = Date.now() - fetchStart;
        addDebugLog(`✅ Response received in ${fetchTime}ms`, 'success');
        
        const data = await response.json();
        
        if (data.count !== undefined) {
          const totalTime = Date.now() - startTime;
          addDebugLog(`📊 Found ${data.count.toLocaleString()} speeches`, 'success');
          addDebugLog(`⏱️ Total preview time: ${totalTime}ms`, 'success');
          
          exportCount.textContent = data.count.toLocaleString();
          exportStats.style.display = 'block';
          exportStatus.textContent = '✅ Preview loaded';
          exportStatus.style.color = '#10b981';
        } else {
          throw new Error('Invalid response from server');
        }
      } catch (error) {
        console.error('Error previewing export:', error);
        addDebugLog(`❌ Error: ${error.message}`, 'error');
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
        addDebugLog('📥 Export request started', 'info');
        
        const selectedFields = getSelectedFields();
        addDebugLog(`📋 Selected ${selectedFields.length} fields: ${selectedFields.join(', ')}`, 'info');
        
        if (selectedFields.length === 0) {
          exportStatus.textContent = '⚠️ Please select at least one field';
          exportStatus.style.color = '#f59e0b';
          addDebugLog('⚠️ No fields selected', 'error');
          return;
        }
        
        exportStatus.textContent = 'Preparing export...';
        exportStatus.style.color = '#3b82f6';
        exportProgress.style.display = 'block';
        exportProgressBar.style.width = '10%';
        exportProgressPercent.textContent = '10%';
        exportProgressMessage.textContent = 'Fetching data from server...';
        
        const timeParams = getTimeFrameParams();
        addDebugLog(`⏰ Time frame params: ${JSON.stringify(timeParams)}`, 'info');
        
        const queryParams = new URLSearchParams({
          ...timeParams,
          fields: selectedFields.join(',')
        });
        
        addDebugLog(`📡 Building request URL...`, 'info');
        const requestUrl = `/api/export/speeches?${queryParams.toString()}`;
        addDebugLog(`🔗 Request URL: ${requestUrl}`, 'info');
        
        exportProgressBar.style.width = '30%';
        exportProgressPercent.textContent = '30%';
        exportProgressMessage.textContent = 'Downloading data...';
        
        addDebugLog(`🌐 Sending fetch request to server...`, 'info');
        const fetchStart = Date.now();
        
        const response = await fetch(requestUrl);
        
        const fetchTime = Date.now() - fetchStart;
        addDebugLog(`✅ Server response received in ${fetchTime}ms (${(fetchTime/1000).toFixed(2)}s)`, 'success');
        
        if (!response.ok) {
          addDebugLog(`❌ Server returned error: ${response.status} ${response.statusText}`, 'error');
          throw new Error('Export failed: ' + response.statusText);
        }
        
        const contentLength = response.headers.get('content-length');
        if (contentLength) {
          const sizeMB = (parseInt(contentLength) / 1024 / 1024).toFixed(2);
          addDebugLog(`📦 Response size: ${sizeMB} MB`, 'info');
        }
        
        exportProgressBar.style.width = '60%';
        exportProgressPercent.textContent = '60%';
        exportProgressMessage.textContent = 'Processing CSV...';
        
        addDebugLog(`💾 Converting response to blob...`, 'info');
        const blobStart = Date.now();
        
        const blob = await response.blob();
        
        const blobTime = Date.now() - blobStart;
        const blobSizeMB = (blob.size / 1024 / 1024).toFixed(2);
        addDebugLog(`✅ Blob created in ${blobTime}ms - Size: ${blobSizeMB} MB`, 'success');
        
        exportProgressBar.style.width = '90%';
        exportProgressPercent.textContent = '90%';
        exportProgressMessage.textContent = 'Preparing download...';
        
        addDebugLog(`🔗 Creating download link...`, 'info');
        // Create download link
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        
        // Generate filename with timestamp
        const timestamp = new Date().toISOString().split('T')[0];
        const timeFrame = document.querySelector('input[name="timeFrame"]:checked').value;
        const filename = `eu_speeches_${timeFrame}_${timestamp}.csv`;
        a.download = filename;
        
        addDebugLog(`📁 Filename: ${filename}`, 'info');
        
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        exportProgressBar.style.width = '100%';
        exportProgressPercent.textContent = '100%';
        exportProgressMessage.textContent = 'Export complete!';
        
        const totalTime = Date.now() - totalStartTime;
        addDebugLog(`✅ Export completed successfully!`, 'success');
        addDebugLog(`⏱️ Total export time: ${totalTime}ms (${(totalTime/1000).toFixed(2)}s)`, 'success');
        
        exportStatus.textContent = '✅ Export completed';
        exportStatus.style.color = '#10b981';
        
        // Hide progress bar after 3 seconds
        setTimeout(() => {
          exportProgress.style.display = 'none';
          exportProgressBar.style.width = '0%';
        }, 3000);
        
      } catch (error) {
        console.error('Error exporting to CSV:', error);
        addDebugLog(`❌ Export failed: ${error.message}`, 'error');
        addDebugLog(`📚 Stack trace: ${error.stack}`, 'error');
        exportStatus.textContent = '❌ Export failed: ' + error.message;
        exportStatus.style.color = '#ef4444';
        exportProgress.style.display = 'none';
      }
    });
  }
})();

// --- Memory Monitor ---
(function() {
  const memoryMonitor = document.getElementById('memoryMonitor');
  const memoryToggle = document.getElementById('memoryToggle');
  const closeMemoryMonitor = document.getElementById('closeMemoryMonitor');
  
  let memoryInterval = null;
  let isMonitoring = false;
  
  // Format bytes to human readable
  function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
  
  // Format uptime
  function formatUptime(seconds) {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (days > 0) return `${days}d ${hours}h ${minutes}m`;
    if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
    if (minutes > 0) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  }
  
  // Update memory display
  function updateMemoryDisplay() {
    if (!isMonitoring) return;
    
    // Get memory usage from performance API
    if (performance.memory) {
      const mem = performance.memory;
      const heapUsed = mem.usedJSHeapSize;
      const heapTotal = mem.totalJSHeapSize;
      const heapLimit = mem.jsHeapSizeLimit;
      
      document.getElementById('heapUsed').textContent = formatBytes(heapUsed);
      document.getElementById('heapTotal').textContent = formatBytes(heapTotal);
      document.getElementById('heapLimit').textContent = formatBytes(heapLimit);
      document.getElementById('heapPercent').textContent = ((heapUsed / heapLimit) * 100).toFixed(1) + '%';
      
      // Color code heap percentage
      const heapPercentEl = document.getElementById('heapPercent');
      const percent = (heapUsed / heapLimit) * 100;
      if (percent > 80) {
        heapPercentEl.style.color = '#ff4444'; // Red
      } else if (percent > 60) {
        heapPercentEl.style.color = '#ffaa00'; // Orange
      } else {
        heapPercentEl.style.color = '#00ff00'; // Green
      }
    } else {
      document.getElementById('heapUsed').textContent = 'N/A';
      document.getElementById('heapTotal').textContent = 'N/A';
      document.getElementById('heapLimit').textContent = 'N/A';
      document.getElementById('heapPercent').textContent = 'N/A';
    }
    
    // Get additional memory info from navigator
    if (navigator.deviceMemory) {
      document.getElementById('external').textContent = navigator.deviceMemory + ' GB';
    } else {
      document.getElementById('external').textContent = 'N/A';
    }
    
    // Get process memory info (if available)
    if (window.performance && window.performance.memory) {
      const mem = window.performance.memory;
      document.getElementById('arrayBuffers').textContent = formatBytes(mem.usedJSHeapSize - mem.totalJSHeapSize);
    } else {
      document.getElementById('arrayBuffers').textContent = 'N/A';
    }
    
    // RSS (Resident Set Size) - approximate
    if (performance.memory) {
      const mem = performance.memory;
      document.getElementById('rss').textContent = formatBytes(mem.jsHeapSizeLimit);
    } else {
      document.getElementById('rss').textContent = 'N/A';
    }
    
    // Uptime
    const uptime = performance.now() / 1000;
    document.getElementById('uptime').textContent = formatUptime(uptime);
    
    // GC Count (approximate)
    if (window.gc) {
      document.getElementById('gcCount').textContent = 'Available';
    } else {
      document.getElementById('gcCount').textContent = 'N/A';
    }
  }
  
  // Start monitoring
  function startMonitoring() {
    if (isMonitoring) return;
    
    isMonitoring = true;
    memoryMonitor.style.display = 'block';
    memoryToggle.style.display = 'none';
    
    // Update immediately
    updateMemoryDisplay();
    
    // Update every 2 seconds
    memoryInterval = setInterval(updateMemoryDisplay, 2000);
    
    console.log('🧠 Memory monitoring started');
  }
  
  // Stop monitoring
  function stopMonitoring() {
    if (!isMonitoring) return;
    
    isMonitoring = false;
    memoryMonitor.style.display = 'none';
    memoryToggle.style.display = 'flex';
    
    if (memoryInterval) {
      clearInterval(memoryInterval);
      memoryInterval = null;
    }
    
    console.log('🧠 Memory monitoring stopped');
  }
  
  // Event listeners
  if (memoryToggle) {
    memoryToggle.addEventListener('click', startMonitoring);
  }
  
  if (closeMemoryMonitor) {
    closeMemoryMonitor.addEventListener('click', stopMonitoring);
  }
  
  // Auto-start monitoring if memory usage is high
  if (performance.memory) {
    const mem = performance.memory;
    const heapPercent = (mem.usedJSHeapSize / mem.jsHeapSizeLimit) * 100;
    
    if (heapPercent > 50) {
      console.log('🧠 High memory usage detected, auto-starting monitor');
      setTimeout(startMonitoring, 1000);
    }
  }
  
  // Expose functions globally for debugging
  window.startMemoryMonitor = startMonitoring;
  window.stopMemoryMonitor = stopMonitoring;
})();
