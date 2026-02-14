const SITTINGS_LIMIT = 100000;

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
let totalSittings = 0;
let currentSort = { column: 'date', direction: 'desc' };
let dateFilter = { start: null, end: null };

// Fetch all sitting dates (metadata only — content loaded when opening a sitting)
async function loadSittings() {
  try {
    const barContainer = document.getElementById('speechesLoadingBarContainer');
    const bar = document.getElementById('speechesLoadingBar');
    const percentSpan = document.getElementById('speechesLoadingPercent');
    if (barContainer) barContainer.style.display = '';
    if (bar) bar.style.width = '50%';
    if (percentSpan) percentSpan.textContent = '50%';

    const params = new URLSearchParams();
    params.set('limit', String(SITTINGS_LIMIT));
    if (dateFilter.start) params.set('startDate', dateFilter.start);
    if (dateFilter.end) params.set('endDate', dateFilter.end);

    const response = await fetch(`/api/speeches?${params.toString()}`);
    const json = await response.json();

    allSpeeches = json.data || [];
    totalSittings = (json.meta && json.meta.total) || allSpeeches.length;

    if (bar) bar.style.width = '100%';
    if (percentSpan) percentSpan.textContent = '100%';
    if (barContainer) barContainer.style.display = 'none';

    console.log(`✅ [FRONTEND] Loaded ${allSpeeches.length} sitting dates (content loads on open)`);
    renderTable();
    updateSittingsStatus();
    return true;
  } catch (err) {
    console.error('❌ [FRONTEND] Error loading sittings:', err);
    return false;
  }
}

// Group speeches by date (no client-side date filter; API does that when filtered)
function groupSpeechesByDate(speeches) {
  const grouped = {};
  speeches.forEach((speech) => {
    const speechDate = speech.activity_date || speech.date;
    if (!speechDate) return;
    if (!grouped[speechDate]) {
      grouped[speechDate] = {
        date: speechDate,
        count: speech.individual_speech_count || 0,
        id: speech.id
      };
    } else {
      grouped[speechDate].count = Math.max(grouped[speechDate].count, speech.individual_speech_count || 0);
    }
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

  // Attach view button listeners — navigate by id only; content loads when speech page opens
  document.querySelectorAll('#newestSpeechesTable .viewBtn').forEach(btn => {
    btn.addEventListener('click', () => {
      const row = btn.closest('tr');
      const encId = row.getAttribute('data-id');
      const id = decodeURIComponent(encId);
      window.location.href = `speech.html?id=${encodeURIComponent(id)}`;
    });
  });
}

function updateSittingsStatus() {
  const status = document.getElementById('sittingsCountStatus');
  const btn = document.getElementById('loadMoreSittingsBtn');
  if (status) status.textContent = `${allSpeeches.length} sittings (content loads when you open one)`;
  if (btn) btn.style.display = 'none';
}

// Setup date filter form — fetches from API with server-side date filter
function setupDateFilter() {
  const form = document.getElementById('dateFilterForm');
  const clearBtn = document.getElementById('clearFilter');
  if (!form || !clearBtn) return;
  
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    dateFilter.start = document.getElementById('startDate').value || null;
    dateFilter.end = document.getElementById('endDate').value || null;
    loadSittings();
  });
  
  clearBtn.addEventListener('click', () => {
    document.getElementById('startDate').value = '';
    document.getElementById('endDate').value = '';
    dateFilter = { start: null, end: null };
    loadSittings();
  });
}

document.addEventListener('DOMContentLoaded', () => {
  loadSittings();
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