/**
 * Fetch and cache speeches from Europarl API and HTML. Used by init, refresh, and speech routes.
 */
const axios = require('axios');
const cheerio = require('cheerio');
const { API_BASE } = require('./config');
const { createProgressBar } = require('./progress');
const { fetchSpeechContentFromHTML } = require('./fetch-speech-html');
const { parseIndividualSpeeches, storeIndividualSpeeches, parseAllSpeechesWithContent } = require('./parse-speeches');

function insertOneSitting(db, speech) {
  return new Promise((resolve, reject) => {
    const docRec = Array.isArray(speech.recorded_in_a_realization_of) && speech.recorded_in_a_realization_of[0];
    const docId = docRec && docRec.identifier;
    const notationId = docRec && docRec.notation_speechId;
    let personId = null;
    if (speech.person && speech.person.identifier) personId = parseInt(speech.person.identifier, 10);
    let label = '';
    if (speech.activity_label) label = typeof speech.activity_label === 'object' ? (speech.activity_label['EN'] || Object.values(speech.activity_label)[0] || '') : speech.activity_label;
    else if (speech.label) label = speech.label;
    const date = speech.activity_date || speech.activity_start_date || '';
    let content = '';
    if (speech.comment) content = typeof speech.comment === 'object' ? (speech.comment['EN'] || Object.values(speech.comment)[0] || '') : speech.comment;
    else if (speech.structuredContent) content = typeof speech.structuredContent === 'object' ? (speech.structuredContent['EN'] || Object.values(speech.structuredContent)[0] || '') : speech.structuredContent;
    else if (speech.speakingTimeContent) content = typeof speech.speakingTimeContent === 'object' ? (speech.speakingTimeContent['EN'] || Object.values(speech.speakingTimeContent)[0] || '') : speech.speakingTimeContent;
    const sql = 'INSERT OR REPLACE INTO sittings (id, type, label, personId, date, content, docIdentifier, notationId, activity_type, activity_date, activity_start_date, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
    const params = [speech.id, speech.had_activity_type || speech.type || '', label, personId, date, content, docId || '', notationId || '', speech.had_activity_type || '', speech.activity_date || '', speech.activity_start_date || '', Date.now()];
    db.run(sql, params, (err) => err ? reject(err) : resolve());
  });
}


// Helper function to fetch from HTML (fallback) - improved with multiple fallback methods
async function fetchFromHTML(date, res, speechId, db) {
  try {
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
      },
      timeout: 8000
    });
    const html = response.data;
    const $ = cheerio.load(html);
    
    let content = '';
    
    // Method 1: Extract notation ID from speech ID for better matching
    if (speechId) {
      const notationMatch = speechId.match(/(\d+)$/);
      const notationId = notationMatch ? notationMatch[1] : null;
      
      // Look for anchor with notation ID
      if (notationId) {
        const anchor = $(`a[name="creitem${notationId}"]`);
        if (anchor.length > 0) {
          // Get content from anchor to next anchor
          let next = anchor[0].nextSibling;
          while (next) {
            if (next.attribs && next.attribs.id && next.attribs.id.startsWith('creitem')) break;
            if (next.type === 'text' || next.name === 'p' || next.name === 'div') {
              content += $(next).text() + '\n';
            }
            next = next.nextSibling;
          }
        }
        
        // Look for any anchor containing the notation ID
        if (!content) {
          const anchors = $(`a[name*="${notationId}"]`);
          if (anchors.length > 0) {
            const anchor = anchors.first();
            let next = anchor[0].nextSibling;
            while (next) {
              if (next.attribs && next.attribs.id && next.attribs.id.startsWith('creitem')) break;
              if (next.type === 'text' || next.name === 'p' || next.name === 'div') {
                content += $(next).text() + '\n';
              }
              next = next.nextSibling;
            }
          }
        }
      }
    }
    
    // Method 2: Extract all paragraphs and look for speech content
    if (!content || content.length < 100) {
      const paragraphs = $('p').toArray().map(p => $(p).text().trim()).filter(Boolean);
      content = paragraphs.join('\n\n');
    }
    
    // Method 3: Fallback to body text
    if (!content || content.length < 100) {
      content = $('body').text().replace(/\s+/g, ' ').trim();
    }
    
    // Method 4: Try TOC page as last resort
    if (!content || content.length < 100) {
      try {
        console.log(`[SPEECH] Trying TOC page for ${date}...`);
        const tocUrl = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}-TOC_EN.html`;
        const tocResponse = await axios.get(tocUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
          },
          timeout: 8000
        });
        const tocHtml = tocResponse.data;
        const $toc = cheerio.load(tocHtml);
        const items = $toc('a[href*="ITM-"]').toArray();
        if (items.length > 0) {
          content = `TOC Agenda Items:\n` + items.map(a => $toc(a).text().trim()).join('\n');
          console.log(`📄 [SPEECH] Found ${items.length} TOC items for ${date}`);
        }
      } catch (tocErr) {
        console.log(`[SPEECH] TOC fetch also failed for ${date}: ${tocErr.message}`);
      }
    }
    
    // Clean up the content
    content = content.replace(/\n\s*\n/g, '\n\n').trim();
    
    if (!content || content.length < 50) {
      return res.status(404).json({ error: 'No content found in HTML or TOC.' });
    }
    
    console.log(`📄 [SPEECH] Extracted content (${content.length} chars):`, content.slice(0, 200));
    
    // If we have a speechId, try to parse individual speeches
    if (speechId && content.length > 100) {
      try {
        const individualSpeeches = parseIndividualSpeeches(content, speechId);
        if (individualSpeeches.length > 0) {
          console.log(`[SPEECH] Parsed ${individualSpeeches.length} individual speeches from HTML content`);
          await storeIndividualSpeeches(db, individualSpeeches);
          console.log(`[SPEECH] Stored ${individualSpeeches.length} individual speeches in database`);
        }
      } catch (parseError) {
        console.error(`[SPEECH] Error parsing individual speeches from HTML:`, parseError.message);
      }
    }
    
    res.json({ content });
  } catch (err) {
    console.error('[SPEECH] HTML fetch failed:', err.toString());
    res.status(500).json({ error: 'Failed to fetch or parse HTML content' });
  }
}

// Fetch all speeches from remote API with comprehensive data
async function fetchAllSpeechesFromRemote() {
  const limit = 500;
  let offset = 0;
  let all = [];
  let totalFetched = 0;
  let estimatedTotal = 0;
  let firstBatch = true;
  let retryCount = 0;
  const maxRetries = 3;
  const startTime = Date.now();
  
  console.log('Starting comprehensive speech fetch from 2023-01-01...');
  console.log('📡 Fetching first batch to estimate total...');
  
  while (true) {
    let success = false;
    let batchError = null;
    
    // Retry logic for each batch
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        if (attempt === 1) {
          console.log(`📡 Fetching batch: offset=${offset}, limit=${limit}`);
        } else {
          console.log(`Retry ${attempt}/${maxRetries} for offset=${offset}`);
        }
        
    const response = await axios.get(`${API_BASE}/speeches`, {
          params: { 
            format: 'application/ld+json', 
            limit, 
            offset,
            'search-language': 'EN',
            'activity-date-from': '2023-01-01'
          },
          headers: { Accept: 'application/ld+json' },
          timeout: 45000 // Increased timeout
        });
      
    const speeches = (response.data && response.data.data) || [];
    all = all.concat(speeches);
        totalFetched += speeches.length;
        
        // Estimate total on first batch
        if (firstBatch && speeches.length > 0) {
          const meta = response.data.meta;
          if (meta && meta.total) {
            estimatedTotal = meta.total;
            console.log(`Estimated total speeches: ${estimatedTotal}`);
            
            // Show date range from first batch
            const dates = speeches.map(s => s.activity_date).filter(Boolean).sort();
            if (dates.length > 0) {
              console.log(`📅 Date range: ${dates[0]} to ${dates[dates.length - 1]}`);
            }
            console.log('Starting progress tracking...\n');
          }
          firstBatch = false;
        }
        
        // Show progress bar if we have an estimate
        if (estimatedTotal > 0) {
          const progressBar = createProgressBar(totalFetched, estimatedTotal);
          const rate = totalFetched / ((Date.now() - startTime) / 1000);
          process.stdout.write(`\r${progressBar} | Rate: ${rate.toFixed(1)}/sec`);
        } else {
          console.log(`📦 Fetched ${speeches.length} speeches (total: ${totalFetched})`);
        }
        
        if (speeches.length < limit) {
          if (estimatedTotal > 0) {
            console.log('\nReached end of speeches data');
          } else {
            console.log('Reached end of speeches data');
          }
          return all;
        }
        
    offset += limit;
        success = true;
        retryCount = 0; // Reset retry count on success
        
        // Add a small delay to be respectful to the API
        await new Promise(resolve => setTimeout(resolve, 200));
        break;
        
      } catch (error) {
        batchError = error;
        console.log(`\nAttempt ${attempt}/${maxRetries} failed for offset ${offset}: ${error.message}`);
        
        if (error.response?.status === 404) {
          console.log('Reached end of speeches (404)');
          return all;
        }
        
        if (attempt < maxRetries) {
          const delay = Math.pow(2, attempt) * 1000; // Exponential backoff
          console.log(`⏳ Waiting ${delay/1000}s before retry...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    
    if (!success) {
      console.error(`\nFailed to fetch batch at offset ${offset} after ${maxRetries} attempts:`, batchError.message);
      retryCount++;
      
      if (retryCount >= 5) {
        console.error('Too many consecutive failures, stopping fetch');
        throw new Error(`Failed to fetch speeches after ${retryCount} consecutive batch failures`);
      }
      
      // Skip this batch and continue
      console.log(`Skipping batch at offset ${offset} and continuing...`);
      offset += limit;
      // Removed delay for faster fetching
    }
  }
  
  const finalTime = (Date.now() - startTime) / 1000;
  const finalRate = totalFetched / finalTime;
  console.log(`\nTotal speeches fetched: ${all.length} in ${finalTime.toFixed(1)}s (${finalRate.toFixed(1)}/sec)`);
  return all;
}

// Cache all speeches to database
async function cacheAllSpeeches(db) {
  return new Promise((resolve, reject) => {
    db.serialize(async () => {
      try {
        console.log('[CACHE] Starting comprehensive speech caching...');
        const allSpeeches = await fetchAllSpeechesFromRemote();
        console.log(`[CACHE] Fetched ${allSpeeches.length} speeches from API`);
        
        // Clear existing speeches
        db.run('DELETE FROM sittings');
        console.log('[CACHE] Cleared existing sittings from database');
        
        // Prepare statement for batch insert
        const stmt = db.prepare(`INSERT OR REPLACE INTO sittings 
          (id, type, label, personId, date, content, docIdentifier, notationId, 
           activity_type, activity_date, activity_start_date, last_updated)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
        
        let processed = 0;
        const cacheStartTime = Date.now();
        const totalSpeeches = allSpeeches.length;
        
        console.log(`Starting to cache ${totalSpeeches} sittings to database...`);
        console.log(`Will fetch HTML content for speeches with dates...`);
        
        for (const speech of allSpeeches) {
          // Extract person ID if available
          let personId = null;
          if (speech.person && speech.person.identifier) {
            personId = parseInt(speech.person.identifier, 10);
          }
          
          // Determine title/label
          let label = '';
          if (speech.activity_label) {
            if (typeof speech.activity_label === 'object') {
              label = speech.activity_label['EN'] || speech.activity_label['en'] || Object.values(speech.activity_label)[0] || '';
            } else {
              label = speech.activity_label;
            }
          } else if (speech.label) {
            label = speech.label;
          }
          
          // Determine date of activity
          const date = speech.activity_date || speech.activity_start_date || '';
          
          // Determine content (if available)
          let content = '';
          if (speech.comment) {
            content = typeof speech.comment === 'object'
              ? (speech.comment['EN'] || speech.comment['en'] || Object.values(speech.comment)[0] || '')
              : speech.comment;
          } else if (speech.structuredContent) {
            content = typeof speech.structuredContent === 'object'
              ? (speech.structuredContent['EN'] || speech.structuredContent['en'] || Object.values(speech.structuredContent)[0] || '')
              : speech.structuredContent;
          } else if (speech.speakingTimeContent) {
            content = typeof speech.speakingTimeContent === 'object'
              ? (speech.speakingTimeContent['EN'] || speech.speakingTimeContent['en'] || Object.values(speech.speakingTimeContent)[0] || '')
              : speech.speakingTimeContent;
          }
          
          // Always try to fetch content from Europarl HTML for complete data
          if (date) {
            console.log(`Fetching: ${date}`);
            try {
              const htmlContent = await fetchSpeechContentFromHTML(date, speech.id);
              if (htmlContent && htmlContent.length > content.length) {
                content = htmlContent;
                console.log(`${date}: ${htmlContent.length} chars`);
              } else {
                console.log(`${date}: No content`);
              }
              // Removed delay for faster fetching
            } catch (htmlErr) {
              console.log(`${date}: ${htmlErr.message}`);
            }
          }
          
          // Extract document identifiers
          const docRec = Array.isArray(speech.recorded_in_a_realization_of) && speech.recorded_in_a_realization_of[0];
          const docIdentifier = docRec && docRec.identifier;
          const notationId = docRec && docRec.notation_speechId;
          
          try {
            stmt.run(
              speech.id,
              speech.had_activity_type || speech.type || '',
              label,
              personId,
              date,
              content,
              docIdentifier || '',
              notationId || '',
              speech.had_activity_type || '',
              speech.activity_date || '',
              speech.activity_start_date || '',
              Date.now()
            );
          } catch (stmtErr) {
            console.error(`[CACHE] Error inserting speech ${speech.id}:`, stmtErr.message);
            // Continue with next speech instead of crashing
          }
          
          processed++;
          if (processed % 50 === 0 || processed === totalSpeeches) {
            const elapsed = (Date.now() - cacheStartTime) / 1000;
            const rate = processed / elapsed;
            const progressBar = createProgressBar(processed, totalSpeeches, 40);
            const estimatedTime = (totalSpeeches - processed) / rate;
            process.stdout.write(`\r${progressBar} | Rate: ${rate.toFixed(1)}/sec | ETA: ${estimatedTime.toFixed(0)}s`);
          }
        }
        
        stmt.finalize();
        const totalTime = (Date.now() - cacheStartTime) / 1000;
        console.log(`\n[CACHE] Processed all ${allSpeeches.length} speeches in ${totalTime.toFixed(1)} seconds`);
        
        // Update cache status
        const cacheTime = Date.now();
        db.run(`INSERT OR REPLACE INTO cache_status 
          (id, speeches_last_updated, total_speeches) 
          VALUES (1, ?, ?)`, [cacheTime, allSpeeches.length]);
        
        console.log(`[CACHE] Successfully cached ${allSpeeches.length} sittings to database at ${new Date(cacheTime).toLocaleString()}`);
        resolve(allSpeeches.length);
      } catch (err) {
        console.error('[CACHE] Error caching speeches:', err);
        reject(err);
      }
    });
  });
}

// Efficient incremental refresh: only fetch truly new data
async function fetchNewSpeechesIncremental(db) {
  return new Promise((resolve, reject) => {
    db.serialize(async () => {
      try {
        console.log('[EFFICIENT FETCH] Starting efficient incremental fetch...');
        
        // Get the most recent sitting to determine what's new
        db.get(`
          SELECT activity_date, id, docIdentifier, notationId 
          FROM sittings 
          ORDER BY activity_date DESC, id DESC 
          LIMIT 1
        `, async (err, latestSitting) => {
          if (err) {
            console.error('[EFFICIENT FETCH] Error getting latest sitting:', err);
            reject(err);
            return;
          }
          
          if (!latestSitting) {
            console.log('[EFFICIENT FETCH] No existing sittings found, will fetch all data');
            // If no data exists, fetch everything
            const allSpeeches = await fetchAllSpeechesFromRemote();
            console.log(`[EFFICIENT FETCH] Fetched ${allSpeeches.length} speeches from API (full fetch)`);
            
            // Store all speeches (no duplicates to check)
            let stored = 0;
            for (const speech of allSpeeches) {
              try {
                await insertOneSitting(db, speech);
                stored++;
              } catch (storeErr) {
                console.error(`[EFFICIENT FETCH] Error storing speech ${speech.id}:`, storeErr);
              }
            }
            
            console.log(`[EFFICIENT FETCH] Stored ${stored} new speeches (full fetch)`);
            resolve(stored);
            return;
          }
          
          console.log(`[EFFICIENT FETCH] Latest sitting: ${latestSitting.activity_date} (ID: ${latestSitting.id})`);
          
          // Fetch all speeches from API (since API doesn't support date filtering)
          const allSpeeches = await fetchAllSpeechesFromRemote();
          console.log(`[EFFICIENT FETCH] Fetched ${allSpeeches.length} speeches from API`);
          
          // Get existing speeches with all identifiers to avoid duplicates
          db.all('SELECT id, docIdentifier, notationId FROM sittings', async (err, existingRows) => {
            if (err) {
              console.error('[EFFICIENT FETCH] Error getting existing speeches:', err);
              reject(err);
              return;
            }
            
            // Create multiple lookup sets for robust duplicate detection
            const existingIds = new Set(existingRows.map(row => row.id));
            const existingDocIds = new Set(existingRows.map(row => row.docIdentifier).filter(Boolean));
            const existingNotationIds = new Set(existingRows.map(row => row.notationId).filter(Boolean));
            
            console.log(`[EFFICIENT FETCH] Existing: ${existingIds.size} IDs, ${existingDocIds.size} docIds, ${existingNotationIds.size} notationIds`);
            
            // Filter out speeches that already exist (check multiple identifiers)
            const newSpeeches = allSpeeches.filter(speech => {
              // Extract identifiers from speech
              const docRec = Array.isArray(speech.recorded_in_a_realization_of) && speech.recorded_in_a_realization_of[0];
              const docIdentifier = docRec && docRec.identifier;
              const notationId = docRec && docRec.notation_speechId;
              
              // Check if any identifier already exists
              const isDuplicate = existingIds.has(speech.id) || 
                                (docIdentifier && existingDocIds.has(docIdentifier)) ||
                                (notationId && existingNotationIds.has(notationId));
              
              if (isDuplicate) {
                console.log(`🚫 [EFFICIENT FETCH] Duplicate found: ${speech.id} (${speech.activity_date || 'no date'})`);
              }
              
              return !isDuplicate;
            });
            
            console.log(`🆕 [EFFICIENT FETCH] Found ${newSpeeches.length} truly new speeches to add`);
            
            if (newSpeeches.length === 0) {
              console.log('[EFFICIENT FETCH] No new speeches found, database is up to date');
              resolve(0);
              return;
            }
            
            // Insert only new speeches
            const stmt = db.prepare(`INSERT OR REPLACE INTO sittings 
              (id, type, label, personId, date, content, docIdentifier, notationId, 
               activity_type, activity_date, activity_start_date, last_updated)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
            
            let processed = 0;
            const incrementalStartTime = Date.now();
            console.log(`Starting to cache ${newSpeeches.length} new speeches...`);
            
            for (const speech of newSpeeches) {
              // Extract person ID if available
              let personId = null;
              if (speech.person && speech.person.identifier) {
                personId = parseInt(speech.person.identifier, 10);
              }
              
              // Determine title/label
              let label = '';
              if (speech.activity_label) {
                if (typeof speech.activity_label === 'object') {
                  label = speech.activity_label['EN'] || speech.activity_label['en'] || Object.values(speech.activity_label)[0] || '';
                } else {
                  label = speech.activity_label;
                }
              } else if (speech.label) {
                label = speech.label;
              }
              
              // Determine date of activity
              const date = speech.activity_date || speech.activity_start_date || '';
              
              // Determine content (if available)
              let content = '';
              if (speech.comment) {
                content = typeof speech.comment === 'object'
                  ? (speech.comment['EN'] || speech.comment['en'] || Object.values(speech.comment)[0] || '')
                  : speech.comment;
              } else if (speech.structuredContent) {
                content = typeof speech.structuredContent === 'object'
                  ? (speech.structuredContent['EN'] || speech.structuredContent['en'] || Object.values(speech.structuredContent)[0] || '')
                  : speech.structuredContent;
              } else if (speech.speakingTimeContent) {
                content = typeof speech.speakingTimeContent === 'object'
                  ? (speech.speakingTimeContent['EN'] || speech.speakingTimeContent['en'] || Object.values(speech.speakingTimeContent)[0] || '')
                  : speech.speakingTimeContent;
              }
              
              // Always try to fetch content from Europarl HTML for complete data
              if (date) {
                console.log(`Fetching: ${date}`);
                try {
                  const htmlContent = await fetchSpeechContentFromHTML(date, speech.id);
                  if (htmlContent && htmlContent.length > content.length) {
                    content = htmlContent;
                    console.log(`${date}: ${htmlContent.length} chars`);
                  } else {
                    console.log(`${date}: No content`);
                  }
                } catch (htmlErr) {
                  console.log(`${date}: ${htmlErr.message}`);
                }
              }
              
              // Extract document identifiers
              const docRec = Array.isArray(speech.recorded_in_a_realization_of) && speech.recorded_in_a_realization_of[0];
              const docIdentifier = docRec && docRec.identifier;
              const notationId = docRec && docRec.notation_speechId;
              
              try {
                stmt.run(
                  speech.id,
                  speech.had_activity_type || speech.type || '',
                  label,
                  personId,
                  date,
                  content,
                  docIdentifier || '',
                  notationId || '',
                  speech.had_activity_type || '',
                  speech.activity_date || '',
                  speech.activity_start_date || '',
                  Date.now()
                );
              } catch (stmtErr) {
                console.error(`[INCREMENTAL] Error inserting speech ${speech.id}:`, stmtErr.message);
                // Continue with next speech instead of crashing
              }
              
              processed++;
              if (processed % 50 === 0 || processed === newSpeeches.length) {
                const elapsed = (Date.now() - incrementalStartTime) / 1000;
                const rate = processed / elapsed;
                const progressBar = createProgressBar(processed, newSpeeches.length, 30);
                process.stdout.write(`\r${progressBar} | Rate: ${rate.toFixed(1)}/sec`);
              }
            }
            
            stmt.finalize();
            const incrementalTime = (Date.now() - incrementalStartTime) / 1000;
            console.log(`\n[INCREMENTAL] Cached ${newSpeeches.length} new speeches in ${incrementalTime.toFixed(1)} seconds`);
            
            // Update cache status with new total count
            db.get('SELECT COUNT(*) as total FROM sittings', (err, countRow) => {
              if (err) {
                console.error('[INCREMENTAL] Error getting total count:', err);
                reject(err);
                return;
              }
              
              const totalCount = countRow.total;
              const now = Date.now();
              
              db.run(`INSERT OR REPLACE INTO cache_status 
                (id, speeches_last_updated, total_speeches) 
                VALUES (1, ?, ?)`, [now, totalCount]);
              
              console.log(`[INCREMENTAL] Successfully added ${newSpeeches.length} new speeches. Total: ${totalCount}`);
              resolve(newSpeeches.length);
            });
          });
        });
      } catch (err) {
        console.error('[INCREMENTAL] Error in incremental refresh:', err);
        reject(err);
      }
    });
  });
}

// Add content to existing speeches that don't have content
async function addContentToExistingSpeeches(db) {
  return new Promise((resolve, reject) => {
    try {
      // Get speeches without content, but only for dates up to today (avoid future dates)
      const today = new Date().toISOString().split('T')[0];
      db.all('SELECT id, date FROM sittings WHERE (content = "" OR content IS NULL OR LENGTH(content) < 100) AND date != "" AND date <= ?', [today], async (err, speeches) => {
        if (err) {
          console.error('[CONTENT] Error getting speeches without content:', err);
          reject(err);
          return;
        }
        
        if (speeches.length === 0) {
          console.log('[CONTENT] All speeches already have content');
          resolve(0);
          return;
        }
        
        console.log(`[CONTENT] Found ${speeches.length} speeches without content, fetching content...`);
        
        let processed = 0;
        let successCount = 0;
        const startTime = Date.now();
        
        for (const speech of speeches) {
          try {
            const content = await fetchSpeechContentFromHTML(speech.date, speech.id);
            if (content && content.length > 50) {
              // Update the speech with content synchronously
              await new Promise((updateResolve, updateReject) => {
                db.run('UPDATE sittings SET content = ? WHERE id = ?', [content, speech.id], function(err) {
                  if (err) {
                    console.error(`[CONTENT] Error updating speech ${speech.id}:`, err);
                    updateReject(err);
                  } else {
                    successCount++;
                    updateResolve();
                  }
                });
              });
            }
            
            processed++;
            
            // Show progress every 10 speeches
            if (processed % 10 === 0 || processed === speeches.length) {
              const elapsed = (Date.now() - startTime) / 1000;
              const rate = processed / elapsed;
              const progressBar = createProgressBar(processed, speeches.length, 40);
              process.stdout.write(`\r[CONTENT] ${progressBar} | Rate: ${rate.toFixed(1)}/sec | Success: ${successCount}`);
            }
            
            // Add small delay to be respectful to the server
            await new Promise(resolve => setTimeout(resolve, 500));
            
          } catch (error) {
            console.error(`[CONTENT] Error processing speech ${speech.id}:`, error.message);
            processed++;
          }
        }
        
        const totalTime = (Date.now() - startTime) / 1000;
        console.log(`\n[CONTENT] Completed content fetching: ${successCount}/${speeches.length} speeches updated in ${totalTime.toFixed(1)}s`);
        console.log(`[CONTENT] Success rate: ${((successCount/speeches.length)*100).toFixed(1)}%`);
        
        // If we successfully fetched content for any speeches, parse them automatically
        if (successCount > 0) {
          console.log(`\n[AUTO-PARSE] Automatically parsing ${successCount} newly fetched sittings...`);
          try {
            await parseAllSpeechesWithContent(db);
            console.log('[AUTO-PARSE] Successfully parsed all newly fetched sittings');
          } catch (parseErr) {
            console.error('[AUTO-PARSE] Error parsing newly fetched sittings:', parseErr);
            // Don't fail the whole operation if parsing fails
          }
        }
        
        resolve(successCount);
      });
    } catch (err) {
      console.error('[CONTENT] Error in addContentToExistingSpeeches:', err);
      reject(err);
    }
  });
}

function fetchAllSittingsFromRemote() { return fetchAllSpeechesFromRemote(); }

module.exports = { fetchFromHTML, fetchAllSpeechesFromRemote, cacheAllSpeeches, fetchNewSpeechesIncremental, addContentToExistingSpeeches, fetchAllSittingsFromRemote };
