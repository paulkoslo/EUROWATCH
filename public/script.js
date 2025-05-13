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

  // --- Populate filter dropdowns for country and group ---
  const countryFilter = document.getElementById('countryFilter');
  const groupFilter = document.getElementById('groupFilter');
  
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
    const selectedCountry = countryFilter.value;
    const selectedGroup = groupFilter.value;
    mepsPage = 1; // Reset to first page
    // Filter MEPs by selected country and group
    filteredMeps = mepsList.filter(mep => {
      const countryMatch = !selectedCountry || mep['api:country-of-representation'] === selectedCountry;
      const groupMatch = !selectedGroup || mep['api:political-group'] === selectedGroup;
      return countryMatch && groupMatch;
    });
    // Update total count display
    document.getElementById('total-count').textContent = filteredMeps.length;
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
    // Render table rows
    mepsTbody.innerHTML = toShow.map(m => `
      <tr>
        <td>${m.identifier}</td>
        <td>${m.label}</td>
        <td>${m['api:country-of-representation'] || ''}</td>
        <td>${m['api:political-group'] || ''}</td>
      </tr>
    `).join('');
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
  countryFilter.addEventListener('change', applyFilters);
  groupFilter.addEventListener('change', applyFilters);

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
  const loadMoreBtn = document.getElementById('loadMoreSpeeches');

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
  });
})();