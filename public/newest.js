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

// Load from database cache if available
async function loadSittingsFromCache() {
  try {
    console.log('🎤 [FRONTEND] Loading speeches from database cache...');
    const response = await fetch('/api/speeches?limit=10000'); // Get a large number to load all
    const json = await response.json();
    
    console.log(`🎤 [FRONTEND] Received ${json.data ? json.data.length : 0} speeches from cache`);
    
    if (json.data && json.data.length > 0) {
      allSpeeches = json.data;
      console.log(`✅ [FRONTEND] Loaded ${allSpeeches.length} speeches from cache`);
      console.log('🎤 [FRONTEND] Sample speech data:', allSpeeches[0]);
      renderTable();
      // Hide progress bar if present
      const barContainer = document.getElementById('speechesLoadingBarContainer');
      if (barContainer) barContainer.style.display = 'none';
      return true;
    } else {
      console.log('⚠️ [FRONTEND] No speeches found in cache');
    }
  } catch (error) {
    console.error('❌ [FRONTEND] Error loading speeches from cache:', error);
  }
  return false;
}

// Legacy localStorage functions for backward compatibility
function loadSittingsFromStorage() {
  return loadSittingsFromCache();
}

function saveSittingsToStorage() {
  // No longer needed since data is cached in database
  console.log('Data is now cached in database, localStorage save not needed');
}

// Group speeches by date
function groupSpeechesByDate(speeches) {
  console.log('🎤 [FRONTEND] Grouping speeches:', speeches.length, 'speeches');
  const grouped = {};
  speeches.forEach((speech, index) => {
    // Use activity_date from API instead of date
    const speechDate = speech.activity_date || speech.date;
    if (!speechDate) {
      console.log('⚠️ [FRONTEND] Speech without date:', speech);
      return;
    }
    // Apply date filter
    if (dateFilter.start && speechDate < dateFilter.start) return;
    if (dateFilter.end && speechDate > dateFilter.end) return;
    
    if (!grouped[speechDate]) {
      grouped[speechDate] = {
        date: speechDate,
        count: speech.individual_speech_count || 0, // Use individual speech count from API
        id: speech.id // Keep the first speech ID for reference
      };
      console.log(`🎤 [FRONTEND] Created group for ${speechDate} with count ${speech.individual_speech_count}`);
    } else {
      // If we already have this date, use the higher count (in case of duplicates)
      grouped[speechDate].count = Math.max(grouped[speechDate].count, speech.individual_speech_count || 0);
      console.log(`🎤 [FRONTEND] Updated group for ${speechDate} with count ${speech.individual_speech_count}`);
    }
  });
  console.log('🎤 [FRONTEND] Final grouped result:', Object.values(grouped));
  return Object.values(grouped);
}

// Render table based on allSpeeches and currentSort
function renderTable() {
  const tbody = document.querySelector('#newestSpeechesTable tbody');
  const groupedSpeeches = groupSpeechesByDate(allSpeeches);
  console.log('🎤 [FRONTEND] Grouped speeches:', groupedSpeeches);
  
  const data = groupedSpeeches.sort((a, b) => {
    const da = new Date(a.date);
    const db = new Date(b.date);
    return currentSort.direction === 'asc' ? da - db : db - da;
  });

  console.log('🎤 [FRONTEND] Sorted data:', data);

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

// Load all speeches from database cache
async function fetchAllSpeechesAndStore() {
  try {
    console.log('🎤 [FRONTEND] Starting to load all speeches from cache...');
    const barContainer = document.getElementById('speechesLoadingBarContainer');
    const bar = document.getElementById('speechesLoadingBar');
    const percentSpan = document.getElementById('speechesLoadingPercent');
    
    if (barContainer) barContainer.style.display = '';
    if (bar) bar.style.width = '0%';
    if (percentSpan) percentSpan.textContent = '0%';
    
    // Show loading progress
    if (bar && percentSpan) {
      bar.style.width = '50%';
      percentSpan.textContent = '50%';
    }
    
    console.log('📡 [FRONTEND] Fetching speeches from cached database...');
    // Fetch all speeches from cached database
    const response = await fetch('/api/speeches?limit=50000'); // Large limit to get all
    const json = await response.json();
    
    console.log(`📊 [FRONTEND] Received response with ${json.data ? json.data.length : 0} speeches`);
    
    if (json.data) {
      allSpeeches = json.data;
      console.log(`✅ [FRONTEND] Successfully loaded ${allSpeeches.length} speeches from cache`);
      
      // Complete progress bar
      if (bar && percentSpan) {
        bar.style.width = '100%';
        percentSpan.textContent = '100%';
      }
      
      renderTable();
    } else {
      throw new Error('No data received from API');
    }
    
    if (barContainer) barContainer.style.display = 'none';
  } catch (err) {
    console.error('❌ [FRONTEND] Error loading speeches from cache:', err);
    const barContainer = document.getElementById('speechesLoadingBarContainer');
    if (barContainer) {
      barContainer.innerHTML = '<span style="color:red">Failed to load speeches from cache. Try refreshing the data.</span>';
    }
  }
}

// Manual refresh handler
async function refreshSittings() {
  try {
    // Clear any legacy cache
    localStorage.removeItem(LOCAL_STORAGE_KEY);
    localStorage.removeItem(LOCAL_STORAGE_DATE_KEY);

    // Trigger perfect refresh on server
    const resp = await fetch('/api/refresh-perfect', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        startDate: '2023-01-01' // Default to 2023, can be made configurable
      })
    });
    if (!resp.ok) throw new Error('Refresh failed');

    // After backend completes, load from DB cache endpoint
    await loadSittingsFromCache();
  } catch (e) {
    console.error('❌ [FRONTEND] Perfect refresh failed:', e);
  }
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