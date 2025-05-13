const LOCAL_STORAGE_KEY = 'allSittings';
const LOCAL_STORAGE_DATE_KEY = 'sittingsLastFetched';

async function fetchPreview(date) {
  try {
    const res = await fetch(`/api/speech-preview?date=${date}`);
    if (!res.ok) return '—';
    const data = await res.json();
    return data.preview || '—';
  } catch (err) {
    return '—';
  }
}

// State for all speeches and current sort order
let allSpeeches = [];
let currentSort = { column: 'date', direction: 'desc' };
let dateFilter = { start: null, end: null };

// Load from localStorage if available
function loadSittingsFromStorage() {
  const cached = localStorage.getItem(LOCAL_STORAGE_KEY);
  if (cached) {
    try {
      allSpeeches = JSON.parse(cached);
      renderTable();
      // Hide progress bar if present
      const barContainer = document.getElementById('speechesLoadingBarContainer');
      if (barContainer) barContainer.style.display = 'none';
      return true;
    } catch (e) {
      // If parsing fails, clear storage and fall back to fetch
      localStorage.removeItem(LOCAL_STORAGE_KEY);
    }
  }
  return false;
}

// Save to localStorage
function saveSittingsToStorage() {
  localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(allSpeeches));
  localStorage.setItem(LOCAL_STORAGE_DATE_KEY, Date.now());
}

// Group speeches by date
function groupSpeechesByDate(speeches) {
  const grouped = {};
  speeches.forEach(speech => {
    if (!speech.date) return;
    // Apply date filter
    if (dateFilter.start && speech.date < dateFilter.start) return;
    if (dateFilter.end && speech.date > dateFilter.end) return;
    
    if (!grouped[speech.date]) {
      grouped[speech.date] = {
        date: speech.date,
        count: 0,
        id: speech.id // Keep the first speech ID for reference
      };
    }
    grouped[speech.date].count++;
  });
  return Object.values(grouped);
}

// Render table based on allSpeeches and currentSort
function renderTable() {
  const tbody = document.querySelector('#newestSpeechesTable tbody');
  const groupedSpeeches = groupSpeechesByDate(allSpeeches);
  const data = groupedSpeeches.sort((a, b) => {
    const da = new Date(a.date);
    const db = new Date(b.date);
    return currentSort.direction === 'asc' ? da - db : db - da;
  });

  tbody.innerHTML = data.map(s => {
    return `
      <tr data-date="${s.date}" data-id="${encodeURIComponent(s.id)}">
        <td>${s.date}</td>
        <td>Sitting of ${s.date}</td>
        <td>${s.count}</td>
        <td><button class="viewBtn">View</button></td>
      </tr>
    `;
  }).join('');

  // Attach view button listeners
  document.querySelectorAll('#newestSpeechesTable .viewBtn').forEach(btn => {
    btn.addEventListener('click', () => {
      const row = btn.closest('tr');
      const encId = row.getAttribute('data-id');
      const id = decodeURIComponent(encId);
      const rec = allSpeeches.find(x => x.id === id);
      if (rec) sessionStorage.setItem('speechRecord', JSON.stringify(rec));
      window.location.href = `speech.html?id=${encodeURIComponent(id)}`;
    });
  });
}

// Setup date filter form
function setupDateFilter() {
  const form = document.getElementById('dateFilterForm');
  const clearBtn = document.getElementById('clearFilter');
  
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    
    dateFilter = {
      start: startDate || null,
      end: endDate || null
    };
    
    renderTable();
  });
  
  clearBtn.addEventListener('click', () => {
    document.getElementById('startDate').value = '';
    document.getElementById('endDate').value = '';
    dateFilter = { start: null, end: null };
    renderTable();
  });
}

// Fetch all speeches in pages of 500 until we get them all
async function fetchAllSpeechesAndStore() {
  try {
    const barContainer = document.getElementById('speechesLoadingBarContainer');
    const bar = document.getElementById('speechesLoadingBar');
    const percentSpan = document.getElementById('speechesLoadingPercent');
    if (barContainer) barContainer.style.display = '';
    if (bar) bar.style.width = '0%';
    if (percentSpan) percentSpan.textContent = '0%';
    const limit = 500;
    // Fetch total count
    const metaResp = await fetch('/api/speeches?limit=1');
    const metaJson = await metaResp.json();
    const total = (metaJson.meta && metaJson.meta.total) || 0;
    allSpeeches = [];
    for (let offset = 0; offset < total; offset += limit) {
      const resp = await fetch(`/api/speeches?limit=${limit}&offset=${offset}`);
      const json = await resp.json();
      allSpeeches = allSpeeches.concat(json.data || []);
      // Update bar and percent
      if (bar && percentSpan) {
        const percent = Math.min(100, Math.round((allSpeeches.length / total) * 100));
        bar.style.width = percent + '%';
        percentSpan.textContent = percent + '%';
      }
    }
    if (barContainer) barContainer.style.display = 'none';
    saveSittingsToStorage();
    renderTable();
  } catch (err) {
    console.error('Error loading all speeches:', err);
    const barContainer = document.getElementById('speechesLoadingBarContainer');
    if (barContainer) barContainer.innerHTML = '<span style="color:red">Failed to load speeches.</span>';
  }
}

// Manual refresh handler
function refreshSittings() {
  localStorage.removeItem(LOCAL_STORAGE_KEY);
  fetchAllSpeechesAndStore();
}

document.addEventListener('DOMContentLoaded', () => {
  const refreshBtn = document.getElementById('refreshSittingsBtn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', refreshSittings);
  }

  // Only fetch if not in localStorage
  if (!localStorage.getItem(LOCAL_STORAGE_KEY)) {
    fetchAllSpeechesAndStore();
  } else {
    allSpeeches = JSON.parse(localStorage.getItem(LOCAL_STORAGE_KEY));
    renderTable();
  }

  setupDateFilter();
  
  const th = document.querySelector('#newestSpeechesTable th.sortable[data-col="date"]');
  if (th) {
    // Initial sort arrow state
    th.classList.add('sorted-desc');
    th.addEventListener('click', () => {
      if (currentSort.column === 'date') {
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
      } else {
        currentSort.column = 'date';
        currentSort.direction = 'asc';
      }
      // Update arrow classes
      document.querySelectorAll('#newestSpeechesTable th.sortable').forEach(h => {
        h.classList.remove('sorted-asc', 'sorted-desc');
      });
      th.classList.add(currentSort.direction === 'asc' ? 'sorted-asc' : 'sorted-desc');
      renderTable();
    });
  }
});