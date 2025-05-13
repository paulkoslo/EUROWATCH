const express = require('express');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const API_BASE = 'https://data.europarl.europa.eu/api/v2';

// Initialize SQLite database
const dbFile = path.join(__dirname, 'ep_data.db');
const db = new sqlite3.Database(dbFile);

// Fetch all current MEPs from remote API
async function fetchAllMeps(lang = 'EN') {
  const limit = 500;
  let offset = 0;
  let allMeps = [];
  while (true) {
    const response = await axios.get(`${API_BASE}/meps/show-current`, {
      params: { language: lang, format: 'application/ld+json', limit, offset },
      headers: { Accept: 'application/ld+json' }
    });
    const meps = (response.data && response.data.data) || [];
    allMeps = allMeps.concat(meps);
    if (meps.length < limit) break;
    offset += limit;
  }
  return allMeps;
}

// Initialize database tables and seed MEPs
async function initDatabase() {
  return new Promise((resolve, reject) => {
    db.serialize(async () => {
      db.run(`CREATE TABLE IF NOT EXISTS meps (
        id INTEGER PRIMARY KEY,
        label TEXT,
        givenName TEXT,
        familyName TEXT,
        sortLabel TEXT,
        country TEXT,
        politicalGroup TEXT
      )`);
      db.run(`CREATE TABLE IF NOT EXISTS speeches (
        id TEXT PRIMARY KEY,
        type TEXT,
        label TEXT,
        personId INTEGER,
        date TEXT,
        content TEXT
      )`);
      db.run(`CREATE TABLE IF NOT EXISTS sittings_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT,
        last_updated INTEGER
      )`);
      try {
        const meps = await fetchAllMeps();
        db.run('DELETE FROM meps');
        const stmt = db.prepare(`INSERT OR REPLACE INTO meps 
          (id, label, givenName, familyName, sortLabel, country, politicalGroup)
          VALUES (?, ?, ?, ?, ?, ?, ?)`);
        for (const m of meps) {
          const pid = parseInt(m.identifier, 10);
          stmt.run(pid, m.label, m.givenName, m.familyName, m.sortLabel,
            m['api:country-of-representation'], m['api:political-group']);
        }
        stmt.finalize();
        console.log(`Seeded ${meps.length} MEP records into database`);
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  });
}

// Start server after database init
(async () => {
  try {
    await initDatabase();

    // Serve static assets (static site files located in public directory)
    app.use(express.static(path.join(__dirname, 'public')));

    // GET /api/meps: return all MEPs from DB
    app.get('/api/meps', (req, res) => {
      db.all('SELECT * FROM meps', (err, rows) => {
        if (err) {
          console.error('DB error fetching MEPs:', err);
          return res.status(500).json({ error: err.toString() });
        }
        const data = rows.map(r => ({
          id: `person/${r.id}`,
          type: 'Person',
          identifier: r.id.toString(),
          label: r.label,
          familyName: r.familyName,
          givenName: r.givenName,
          sortLabel: r.sortLabel,
          'api:country-of-representation': r.country,
          'api:political-group': r.politicalGroup
        }));
        res.json({ data });
      });
    });

    // GET /api/meps/:id: return single MEP by ID
    app.get('/api/meps/:id', (req, res) => {
      const id = parseInt(req.params.id, 10);
      db.get('SELECT * FROM meps WHERE id = ?', [id], (err, row) => {
        if (err) {
          console.error('DB error fetching MEP:', err);
          return res.status(500).json({ error: err.toString() });
        }
        if (!row) {
          return res.status(404).json({ error: 'MEP not found' });
        }
        const mep = {
          id: `person/${row.id}`,
          type: 'Person',
          identifier: row.id.toString(),
          label: row.label,
          familyName: row.familyName,
          givenName: row.givenName,
          sortLabel: row.sortLabel,
          'api:country-of-representation': row.country,
          'api:political-group': row.politicalGroup
        };
        res.json({ data: mep });
      });
    });

    // GET /api/speeches: proxy to remote API (optionally filter by MEP)
    app.get('/api/speeches', async (req, res) => {
      try {
        const lang = req.query.lang || 'EN';
        const mepId = req.query.personId;
        const limit = parseInt(req.query.limit, 10) || 50;
        const offset = parseInt(req.query.offset, 10) || 0;
        // Use search-language param to request text labels in desired language
        const params = { 'search-language': lang, format: 'application/ld+json', limit, offset };
        if (mepId) params['person-id'] = mepId;
        const response = await axios.get(`${API_BASE}/speeches`, {
          params,
          headers: { Accept: 'application/ld+json' }
        });
        let speeches = [];
        if (response.data && response.data.data) {
          speeches = response.data.data;
        } else if (response.data.searchResults && response.data.searchResults.hits) {
          speeches = response.data.searchResults.hits.map(h => ({ id: h.id }));
        }
        // Total number of speeches matching query
        const total = (response.data.meta && response.data.meta.total) || 0;
        // Map remote speeches to simplified shape: id, type, label (title), date, content
        const data = (speeches || []).map(m => {
          // Determine title/label
          let label = '';
          if (m.activity_label) {
            if (typeof m.activity_label === 'object') {
              label = m.activity_label[lang] || m.activity_label['en'] || Object.values(m.activity_label)[0] || '';
            } else {
              label = m.activity_label;
            }
          } else if (m.label) {
            label = m.label;
          }
          // Determine date of activity
          const date = m.activity_date || m.activity_start_date || '';
          // Determine content (if available)
          let content = '';
          if (m.comment) {
            content = typeof m.comment === 'object'
              ? (m.comment[lang] || m.comment['en'] || Object.values(m.comment)[0] || '')
              : m.comment;
          } else if (m.structuredContent) {
            content = typeof m.structuredContent === 'object'
              ? (m.structuredContent[lang] || m.structuredContent['en'] || Object.values(m.structuredContent)[0] || '')
              : m.structuredContent;
          } else if (m.speakingTimeContent) {
            content = typeof m.speakingTimeContent === 'object'
              ? (m.speakingTimeContent[lang] || m.speakingTimeContent['en'] || Object.values(m.speakingTimeContent)[0] || '')
              : m.speakingTimeContent;
          }
          // Extract document identifiers for external transcript link
          const docRec = Array.isArray(m.recorded_in_a_realization_of) && m.recorded_in_a_realization_of[0];
          const docIdentifier = docRec && docRec.identifier;
          const notationId = docRec && docRec.notation_speechId;
          return {
            id: m.id,
            type: m.had_activity_type || m.type || '',
            label,
            date,
            content,
            docIdentifier: docIdentifier || '',
            notationId: notationId || ''
          };
        });
        res.json({ data, meta: { total } });
      } catch (error) {
        console.error('Error fetching speeches:', error.toString());
        res.status(500).json({ error: error.toString() });
      }
    });

    // GET /api/speeches/:id: return detailed speech info
    app.get('/api/speeches/:id', async (req, res) => {
      try {
        const rawId = req.params.id;
        const speechId = decodeURIComponent(rawId);
        const lang = req.query.lang || req.query['search-language'] || 'EN';
        const params = { 'search-language': lang, format: 'application/ld+json' };
        if (req.query.text) params.text = req.query.text;
        if (req.query['include-output']) params['include-output'] = req.query['include-output'];
        // Call remote detail endpoint; speechId contains the full path segments
        const response = await axios.get(`${API_BASE}/speeches/${speechId}`, {
          params,
          headers: { Accept: 'application/ld+json' }
        });
        res.json(response.data);
      } catch (error) {
        console.error('Error fetching speech detail:', error.toString());
        res.status(500).json({ error: error.toString() });
      }
    });

    // Endpoint: fetch and parse table of contents for a given date
    app.get('/api/speech-toc', async (req, res) => {
      const { date } = req.query;
      if (!date) return res.status(400).json({ error: 'Missing date' });
      try {
        const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
        const response = await axios.get(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
          }
        });
        const html = response.data;
        const $ = require('cheerio').load(html);
        // Find the table of contents (usually a <ul> or <ol> with links to #creitemX)
        let toc = [];
        let found = false;
        $('a[href^="#creitem"]').each((i, el) => {
          const anchor = $(el).attr('href');
          const title = $(el).text().trim();
          if (anchor && title) {
            toc.push({ anchor, title, index: i });
            found = true;
          }
        });
        if (!found || toc.length === 0) {
          console.error('No TOC items found for date', date);
          return res.status(404).json({ error: 'No table of contents found for this date.' });
        }
        res.json({ toc });
      } catch (err) {
        console.error('TOC fetch failed:', err.toString());
        res.status(500).json({ error: 'Failed to fetch or parse TOC', details: err.toString() });
      }
    });

    // Endpoint: fetch and extract content for a specific anchor (speech) on a given date
    app.get('/api/speech-content-by-anchor', async (req, res) => {
      const { date, anchor } = req.query;
      if (!date || !anchor) return res.status(400).json({ error: 'Missing date or anchor' });
      try {
        const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
        const response = await axios.get(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
          }
        });
        const html = response.data;
        const $ = require('cheerio').load(html);
        // Find the anchor element
        const anchorElem = $(anchor);
        if (!anchorElem.length) {
          console.error('Anchor not found:', anchor, 'for date', date);
          return res.status(404).json({ error: 'Anchor not found in document.' });
        }
        // Collect all elements from anchor to next anchor or end of document
        let contentHtml = '';
        let contentText = '';
        let found = false;
        let next = anchorElem[0];
        while (next) {
          // Stop if we hit the next anchor
          if (found && next.attribs && next.attribs.id && next.attribs.id.startsWith('creitem')) break;
          // Skip the anchor itself if it's not a content node
          if (next !== anchorElem[0] || anchorElem[0].type !== 'tag' || anchorElem[0].name !== 'a') {
            contentHtml += $.html(next);
            contentText += $(next).text() + '\n';
          }
          found = true;
          next = next.nextSibling;
        }
        if (!contentHtml) {
          console.error('No content found for anchor:', anchor, 'on date', date);
          // Fallback: extract all <p> text from the document (like /api/speech-html-content)
          let paragraphs = $('p').toArray().map(p => $(p).text().trim()).filter(Boolean);
          let fallbackText = paragraphs.join('\n\n');
          if (!fallbackText || fallbackText.length < 100) {
            // Fallback: extract all text from <body>
            fallbackText = $('body').text().replace(/\s+/g, ' ').trim();
            fallbackText = fallbackText.slice(0, 2000);
            console.log('Fallback to <body> text:', fallbackText.slice(0, 200));
          } else {
            fallbackText = fallbackText.slice(0, 2000);
            console.log('Fallback to <p> text:', fallbackText.slice(0, 200));
          }
          return res.json({ html: '', text: fallbackText || null, fallback: true });
        }
        res.json({ html: contentHtml, text: contentText });
      } catch (err) {
        console.error('Speech content by anchor fetch failed:', err.toString());
        res.status(500).json({ error: 'Failed to fetch or parse speech content by anchor', details: err.toString() });
      }
    });

    // Start listening
    app.listen(PORT, () => {
      console.log(`Server is running on http://localhost:${PORT}`);
    });
  } catch (e) {
    console.error('Failed to initialize application:', e);
  }
})();

const cheerio = require('cheerio');

app.get('/api/speech-preview', async (req, res) => {
  const { date } = req.query;
  if (!date) return res.status(400).json({ error: 'Missing date' });

  try {
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
    const response = await axios.get(url);
    const html = response.data;
    const $ = cheerio.load(html);

    const paragraphs = $('p').toArray().map(p => $(p).text().trim());
    const preview = paragraphs.slice(0, 3).join(' ').substring(0, 300);

    res.json({ preview });
  } catch (err) {
    console.error('Preview fetch failed:', err.toString());
    res.status(500).json({ error: 'Could not fetch preview' });
  }
});

// New endpoint: fetch and extract main content from Europarl HTML
app.get('/api/speech-html-content', async (req, res) => {
  const { date } = req.query;
  if (!date) return res.status(400).json({ error: 'Missing date' });
  try {
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
      }
    });
    const html = response.data;
    const $ = require('cheerio').load(html);
    let paragraphs = $('p').toArray().map(p => $(p).text().trim()).filter(Boolean);
    let fullText = paragraphs.join('\n\n');
    if (!fullText || fullText.length < 100) {
      // Fallback: extract all text from <body>
      fullText = $('body').text().replace(/\s+/g, ' ').trim();
      console.log('Fallback to <body> text:', fullText.slice(0, 200));
    } else {
      console.log('Extracted paragraphs:', fullText.slice(0, 200));
    }
    if (!fullText) {
      return res.status(404).json({ error: 'No content found in HTML.' });
    }
    res.json({ content: fullText });
  } catch (err) {
    console.error('HTML content fetch failed:', err.toString());
    res.status(500).json({ error: 'Failed to fetch or parse HTML content' });
  }
});

app.get('/api/sittings', (req, res) => {
  db.get('SELECT data, last_updated FROM sittings_cache ORDER BY id DESC LIMIT 1', (err, row) => {
    if (err || !row) {
      return res.status(404).json({ error: 'No cached sittings found.' });
    }
    res.json({ data: JSON.parse(row.data), last_updated: row.last_updated });
  });
});

async function fetchAllSittingsFromRemote() {
  const limit = 500;
  let offset = 0;
  let all = [];
  while (true) {
    const response = await axios.get(`${API_BASE}/speeches`, {
      params: { format: 'application/ld+json', limit, offset },
      headers: { Accept: 'application/ld+json' }
    });
    const speeches = (response.data && response.data.data) || [];
    all = all.concat(speeches);
    if (speeches.length < limit) break;
    offset += limit;
  }
  return all;
}

app.post('/api/refresh-sittings', async (req, res) => {
  try {
    const all = await fetchAllSittingsFromRemote();
    db.run('INSERT INTO sittings_cache (data, last_updated) VALUES (?, ?)', JSON.stringify(all), Date.now());
    res.json({ success: true, count: all.length });
  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});