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

// Analytics cache
const analyticsCache = {
  data: null,
  lastUpdated: null,
  isWarming: false,
  progress: {
    stage: '',
    percent: 0,
    message: ''
  }
};

// Progress bar utility
function createProgressBar(current, total, width = 50) {
  const percentage = Math.round((current / total) * 100);
  const filled = Math.round((current / total) * width);
  const empty = width - filled;
  
  const bar = '█'.repeat(filled) + '░'.repeat(empty);
  return `[${bar}] ${percentage}% (${current}/${total})`;
}

// Fetch speech content from Europarl HTML pages
async function fetchSpeechContentFromHTML(date, speechId) {
  const maxRetries = 3;
  let retryCount = 0;
  
  while (retryCount < maxRetries) {
    try {
      const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
      const response = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
        },
        timeout: 15000 // Increased timeout
      });
    
    const html = response.data;
    const $ = require('cheerio').load(html);
    
    let content = '';
    
    // Extract notation ID from speech ID for better matching
    const notationMatch = speechId.match(/(\d+)$/);
    const notationId = notationMatch ? notationMatch[1] : null;
    
    // Method 1: Look for anchor with notation ID
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
    }
    
    // Method 2: Look for any anchor containing the notation ID
    if (!content && notationId) {
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
    
    // Method 3: Extract all paragraphs and look for speech content
    if (!content || content.length < 100) {
      const paragraphs = $('p').toArray().map(p => $(p).text().trim()).filter(Boolean);
      content = paragraphs.join('\n\n');
    }
    
    // Method 4: Fallback to body text
    if (!content || content.length < 100) {
      content = $('body').text().replace(/\s+/g, ' ').trim();
    }
    
    // Method 5: Try TOC page as last resort
    if (!content || content.length < 100) {
      try {
        const tocUrl = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}-TOC_EN.html`;
        const tocResponse = await axios.get(tocUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
          },
          timeout: 8000
        });
        const tocHtml = tocResponse.data;
        const $toc = require('cheerio').load(tocHtml);
        const items = $toc('a[href*="ITM-"]').toArray();
        if (items.length > 0) {
          content = `TOC Agenda Items:\n` + items.map(a => $toc(a).text().trim()).join('\n');
        }
      } catch (tocErr) {
        // TOC fetch failed, continue with empty content
      }
    }
    
      // Clean up the content
      content = content.replace(/\n\s*\n/g, '\n\n').trim();
      
      return content; // Return full content without character limit
    } catch (error) {
      retryCount++;
      if (retryCount < maxRetries) {
        // Wait before retrying (exponential backoff)
        const delay = Math.pow(2, retryCount) * 1000; // 2s, 4s, 8s
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      // All retries failed
      throw error;
    }
  }
}

// Demo function to show progress bars (for testing)
function demoProgressBars() {
  console.log('🎬 Demo Progress Bars:');
  console.log('👥 MEP Fetch Progress:');
  for (let i = 0; i <= 100; i += 10) {
    const progressBar = createProgressBar(i, 100, 30);
    process.stdout.write(`\r👥 ${progressBar} | Rate: ${(i * 2.5).toFixed(1)}/sec`);
    // Simulate delay using setTimeout
    const start = Date.now();
    while (Date.now() - start < 100) { /* wait */ }
  }
  console.log('\n✅ MEP fetch completed!');
  
  console.log('\n🎤 Speech Fetch Progress:');
  for (let i = 0; i <= 100; i += 5) {
    const progressBar = createProgressBar(i, 100, 50);
    process.stdout.write(`\r🎤 ${progressBar} | Rate: ${(i * 15.2).toFixed(1)}/sec`);
    const start = Date.now();
    while (Date.now() - start < 50) { /* wait */ }
  }
  console.log('\n✅ Speech fetch completed!');
  
  console.log('\n💾 Database Caching Progress:');
  for (let i = 0; i <= 100; i += 2) {
    const progressBar = createProgressBar(i, 100, 30);
    process.stdout.write(`\r💾 ${progressBar} | Rate: ${(i * 125.8).toFixed(1)}/sec`);
    const start = Date.now();
    while (Date.now() - start < 20) { /* wait */ }
  }
  console.log('\n✅ Database caching completed!');
}

// Fetch all current MEPs from remote API
async function fetchAllMeps(lang = 'EN') {
  const limit = 500;
  let offset = 0;
  let allMeps = [];
  let estimatedTotal = 0;
  let firstBatch = true;
  const mepStartTime = Date.now();
  
  console.log('👥 Starting MEP fetch...');
  
  while (true) {
    const response = await axios.get(`${API_BASE}/meps/show-current`, {
      params: { language: lang, format: 'application/ld+json', limit, offset },
      headers: { Accept: 'application/ld+json' }
    });
    const meps = (response.data && response.data.data) || [];
    allMeps = allMeps.concat(meps);
    
    // Estimate total on first batch
    if (firstBatch && meps.length > 0) {
      const meta = response.data.meta;
      if (meta && meta.total) {
        estimatedTotal = meta.total;
        console.log(`📊 Estimated total MEPs: ${estimatedTotal}`);
      }
      firstBatch = false;
    }
    
    // Show progress
    if (estimatedTotal > 0) {
      const progressBar = createProgressBar(allMeps.length, estimatedTotal, 30);
      const rate = allMeps.length / ((Date.now() - mepStartTime) / 1000);
      process.stdout.write(`\r👥 ${progressBar} | Rate: ${rate.toFixed(1)}/sec`);
    } else {
      console.log(`👥 Fetched ${meps.length} MEPs (total: ${allMeps.length})`);
    }
    
    if (meps.length < limit) {
      if (estimatedTotal > 0) {
        console.log('\n✅ Reached end of MEP data');
      } else {
        console.log('✅ Reached end of MEP data');
      }
      break;
    }
    offset += limit;
  }
  
  const mepTime = (Date.now() - mepStartTime) / 1000;
  const mepRate = allMeps.length / mepTime;
  console.log(`\n🎉 Total MEPs fetched: ${allMeps.length} in ${mepTime.toFixed(1)}s (${mepRate.toFixed(1)}/sec)`);
  return allMeps;
}

// Initialize database tables and seed MEPs
async function initDatabase() {
  return new Promise((resolve, reject) => {
    db.serialize(async () => {
      // Create tables
      db.run(`CREATE TABLE IF NOT EXISTS meps (
        id INTEGER PRIMARY KEY,
        label TEXT,
        givenName TEXT,
        familyName TEXT,
        sortLabel TEXT,
        country TEXT,
        politicalGroup TEXT,
        is_current BOOLEAN DEFAULT 0,
        source TEXT DEFAULT 'api',
        last_updated INTEGER DEFAULT 0
      )`);
      
      db.run(`CREATE TABLE IF NOT EXISTS sittings (
        id TEXT PRIMARY KEY,
        type TEXT,
        label TEXT,
        personId INTEGER,
        date TEXT,
        content TEXT UNIQUE,
        docIdentifier TEXT,
        notationId TEXT,
        activity_type TEXT,
        activity_date TEXT,
        activity_start_date TEXT,
        last_updated INTEGER DEFAULT 0
      )`);
      
      // Create individual_speeches table for parsed speeches
      db.run(`CREATE TABLE IF NOT EXISTS individual_speeches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sitting_id TEXT,
        speaker_name TEXT,
        political_group TEXT,
        title TEXT,
        speech_content TEXT,
        speech_order INTEGER,
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        FOREIGN KEY (sitting_id) REFERENCES sittings (id)
      )`);
      
      db.run(`CREATE TABLE IF NOT EXISTS sittings_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT,
        last_updated INTEGER
      )`);
      
      db.run(`CREATE TABLE IF NOT EXISTS cache_status (
        id INTEGER PRIMARY KEY,
        meps_last_updated INTEGER DEFAULT 0,
        speeches_last_updated INTEGER DEFAULT 0,
        total_speeches INTEGER DEFAULT 0
      )`);
      
      // Check if sittings table needs to be recreated with new schema
      db.get("PRAGMA table_info(sittings)", (err, row) => {
        if (err) {
          console.log('📊 [MIGRATION] Sittings table does not exist, will be created with new schema');
          return;
        }
        
        // Check if the table has the new columns
        db.all("PRAGMA table_info(sittings)", (err, columns) => {
          if (err) {
            console.error('❌ [MIGRATION] Error getting table info:', err);
            return;
          }
          
          const columnNames = columns.map(col => col.name);
          const requiredColumns = ['docIdentifier', 'notationId', 'activity_type', 'activity_date', 'activity_start_date', 'last_updated'];
          const missingColumns = requiredColumns.filter(col => !columnNames.includes(col));
          
          if (missingColumns.length > 0) {
            console.log(`🔄 [MIGRATION] Sittings table missing columns: ${missingColumns.join(', ')}`);
            console.log('🗑️ [MIGRATION] Dropping and recreating sittings table with new schema...');
            
            // Drop and recreate the table with the correct schema
            db.run('DROP TABLE IF EXISTS sittings', (err) => {
              if (err) {
                console.error('❌ [MIGRATION] Error dropping sittings table:', err);
                return;
              }
              
              db.run(`CREATE TABLE sittings (
                id TEXT PRIMARY KEY,
                type TEXT,
                label TEXT,
                personId INTEGER,
                date TEXT,
                content TEXT UNIQUE,
                docIdentifier TEXT,
                notationId TEXT,
                activity_type TEXT,
                activity_date TEXT,
                activity_start_date TEXT,
                last_updated INTEGER DEFAULT 0
              )`, (err) => {
                if (err) {
                  console.error('❌ [MIGRATION] Error recreating sittings table:', err);
                } else {
                  console.log('✅ [MIGRATION] Successfully recreated sittings table with new schema');
                }
              });
            });
          } else {
            console.log('✅ [MIGRATION] Sittings table is up to date');
          }
        });
      });
      
      // Check if MEPs table needs migration for new columns
      db.all("PRAGMA table_info(meps)", (err, columns) => {
        if (err) {
          console.error('❌ [MIGRATION] Error checking meps table:', err);
          return;
        }
        
        const columnNames = columns.map(col => col.name);
        const hasIsCurrentColumn = columnNames.includes('is_current');
        const hasSourceColumn = columnNames.includes('source');
        
        if (!hasIsCurrentColumn) {
          console.log('⚙️ [MIGRATION] Adding is_current column to meps table...');
          db.run('ALTER TABLE meps ADD COLUMN is_current BOOLEAN DEFAULT 0', (err) => {
            if (err) {
              console.error('❌ [MIGRATION] Error adding is_current column:', err);
            } else {
              console.log('✅ [MIGRATION] Added is_current column to meps table');
              // Mark existing MEPs as current since they came from API
              db.run('UPDATE meps SET is_current = 1 WHERE source = "api" OR source IS NULL', (err) => {
                if (err) {
                  console.error('❌ [MIGRATION] Error updating existing MEPs as current:', err);
                } else {
                  console.log('✅ [MIGRATION] Marked existing MEPs as current');
                }
              });
            }
          });
        }
        
        if (!hasSourceColumn) {
          console.log('⚙️ [MIGRATION] Adding source column to meps table...');
          db.run('ALTER TABLE meps ADD COLUMN source TEXT DEFAULT "api"', (err) => {
            if (err) {
              console.error('❌ [MIGRATION] Error adding source column:', err);
            } else {
              console.log('✅ [MIGRATION] Added source column to meps table');
            }
          });
        }
      });
      
      // Check if we need to seed data
      db.get('SELECT COUNT(*) as count FROM meps', async (err, mepRow) => {
        if (err) {
          reject(err);
          return;
        }
        
        const mepCount = mepRow.count;
        console.log(`📊 [INIT] Found ${mepCount} MEPs in database`);
        
        // Check speeches count
        db.get('SELECT COUNT(*) as count FROM sittings', async (err, speechRow) => {
          if (err) {
            reject(err);
            return;
          }
          
          const speechCount = speechRow.count;
          console.log(`📊 [INIT] Found ${speechCount} sittings in database`);
          
          // Fetch MEPs if database is empty
          if (mepCount === 0) {
            try {
              console.log('👥 [INIT] Database empty, fetching MEPs from API...');
        const meps = await fetchAllMeps();
              console.log(`👥 [INIT] Fetched ${meps.length} MEPs from API`);
              
        const stmt = db.prepare(`INSERT OR REPLACE INTO meps 
                (id, label, givenName, familyName, sortLabel, country, politicalGroup, is_current, source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
        for (const m of meps) {
          const pid = parseInt(m.identifier, 10);
          stmt.run(pid, m.label, m.givenName, m.familyName, m.sortLabel,
                  m['api:country-of-representation'], m['api:political-group'], 1, 'api', Date.now());
        }
        stmt.finalize();
              console.log(`✅ [INIT] Seeded ${meps.length} MEP records into database`);
      } catch (err) {
              console.error('❌ [INIT] Error fetching MEPs:', err);
        reject(err);
              return;
            }
          } else {
            console.log('✅ [INIT] MEPs already cached, skipping fetch');
          }
          
          // Check if speeches have content, if not, fetch and add content
          if (process.env.ENABLE_AUTO_INIT === 'true' && speechCount === 0) {
            try {
              console.log('🎤 [INIT] No sittings in database, fetching all sittings from API...');
              const speechCount = await cacheAllSpeeches();
              console.log(`✅ [INIT] Successfully cached ${speechCount} sittings to database`);
            } catch (err) {
              console.error('❌ [INIT] Error fetching speeches:', err);
              console.log('⚠️ [INIT] Continuing without speeches cache...');
            }
          } else if (process.env.ENABLE_AUTO_INIT === 'true') {
            // Check if speeches have content (only for recent speeches)
            const twoYearsAgo = new Date();
            twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
            const cutoffDate = twoYearsAgo.toISOString().split('T')[0];
            
            db.get('SELECT COUNT(*) as count FROM sittings WHERE content != "" AND LENGTH(content) > 100 AND date >= ?', [cutoffDate], async (err, contentRow) => {
              if (err) {
                console.error('❌ [INIT] Error checking speech content:', err);
                return;
              }
              
              const speechesWithContent = contentRow.count;
              console.log(`📊 [INIT] Found ${speechesWithContent} recent speeches with content out of ${speechCount} total`);
              
              if (speechesWithContent < 50) { // If less than 50 recent speeches have content
                console.log('🔍 [INIT] Most recent speeches missing content, fetching content for existing speeches...');
                try {
                  await addContentToExistingSpeeches();
                  console.log('✅ [INIT] Successfully added content to existing speeches');
                } catch (err) {
                  console.error('❌ [INIT] Error adding content to speeches:', err);
                }
              } else {
                console.log('✅ [INIT] Recent speeches already have content, skipping content fetch');
              }
              
              // Check if we need to parse individual speeches for recent speeches
              db.get(`
                SELECT COUNT(*) as count 
                FROM sittings s 
                LEFT JOIN individual_speeches i ON s.id = i.sitting_id
                WHERE s.activity_date >= date('now', '-1 year') 
                AND s.content IS NOT NULL AND s.content != '' AND s.content != 'No content available'
                AND i.sitting_id IS NULL
              `, async (err, row) => {
                if (err) {
                  console.error('❌ [INIT] Error checking individual speeches:', err);
                  return;
                }
                
                const recentSpeechesNeedingParsing = row.count;
                if (recentSpeechesNeedingParsing > 0) {
                  console.log(`🔄 [INIT] Found ${recentSpeechesNeedingParsing} recent speeches needing individual speech parsing...`);
                  try {
                    await parseRecentSpeeches();
                    console.log('✅ [INIT] Successfully parsed recent individual speeches');
                  } catch (err) {
                    console.error('❌ [INIT] Error parsing recent individual speeches:', err);
                  }
                } else {
                  console.log('✅ [INIT] All recent speeches already have individual speeches parsed');
      }
    });
  });
          } else {
            console.log('⏸️ [INIT] Auto fetch/parse on startup is disabled');
          }
          
          resolve();
        });
      });
    });
  });
}

// Function to parse recent speeches that have content but no individual speeches
async function parseRecentSpeeches() {
  console.log('🔄 [PARSE RECENT] Starting parsing of recent speeches...');
  
  return new Promise((resolve, reject) => {
    db.all(`
      SELECT s.id, s.docIdentifier, s.activity_date, s.content 
      FROM sittings s 
      LEFT JOIN individual_speeches i ON s.id = i.sitting_id
      WHERE s.activity_date >= date('now', '-1 year') 
      AND s.content IS NOT NULL AND s.content != '' AND s.content != 'No content available'
      AND i.sitting_id IS NULL
      ORDER BY s.activity_date DESC
    `, async (err, speeches) => {
      if (err) {
        console.error('❌ [PARSE RECENT] Error fetching recent speeches:', err);
        reject(err);
        return;
      }
      
      console.log(`📊 [PARSE RECENT] Found ${speeches.length} recent speeches needing parsing`);
      
      if (speeches.length === 0) {
        console.log('✅ [PARSE RECENT] No recent speeches need parsing');
        resolve();
        return;
      }
      
      let processed = 0;
      let successCount = 0;
      let errorCount = 0;
      
      for (const speech of speeches) {
        try {
          // Parse the speech
          const individualSpeeches = parseIndividualSpeeches(speech.content, speech.id);
          
          if (individualSpeeches.length > 0) {
            await storeIndividualSpeeches(individualSpeeches);
            console.log(`✅ [PARSE RECENT] Parsed ${individualSpeeches.length} speeches from ${speech.docIdentifier}`);
            successCount++;
          } else {
            console.log(`⚠️ [PARSE RECENT] No individual speeches found in ${speech.docIdentifier}`);
          }
          
        } catch (error) {
          console.error(`❌ [PARSE RECENT] Error parsing ${speech.docIdentifier}:`, error.message);
          errorCount++;
        }
        
        processed++;
        
        // Small delay to prevent overwhelming the system
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      
      console.log(`🎉 [PARSE RECENT] Completed! Processed ${processed} recent speeches:`);
      console.log(`   ✅ Successfully parsed: ${successCount}`);
      console.log(`   ❌ Errors: ${errorCount}`);
      
      resolve();
    });
  });
}

// Function to parse all speeches with content (only those without individual speeches)
async function parseAllSpeechesWithContent() {
  console.log('🔄 [BULK PARSE] Starting bulk parsing of speeches with content...');
  
  return new Promise((resolve, reject) => {
    db.all(`
      SELECT s.id, s.docIdentifier, s.activity_date, s.content, s.label
      FROM sittings s 
      LEFT JOIN individual_speeches i ON s.id = i.sitting_id
      WHERE s.content IS NOT NULL AND s.content != '' AND s.content != 'No content available'
      AND i.sitting_id IS NULL
      ORDER BY s.activity_date DESC
    `, async (err, speeches) => {
      if (err) {
        console.error('❌ [BULK PARSE] Error fetching speeches:', err);
        reject(err);
        return;
      }
      
      console.log(`📊 [BULK PARSE] Found ${speeches.length} speeches with content to parse`);
      
      if (speeches.length === 0) {
        console.log('✅ [BULK PARSE] No speeches with content found');
        resolve();
        return;
      }
      
      const progressBar = createProgressBar('Parsing Individual Speeches', speeches.length);
      let processed = 0;
      let successCount = 0;
      let errorCount = 0;
      const startTime = Date.now();
      
      for (const speech of speeches) {
        try {
          // Check if already parsed
          const existingCount = await new Promise((resolve) => {
            db.get('SELECT COUNT(*) as count FROM individual_speeches WHERE sitting_id = ?', [speech.id], (err, row) => {
              resolve(err ? 0 : row.count);
            });
          });
          
          if (existingCount > 0) {
            console.log(`⏭️ [BULK PARSE] Skipping ${speech.docIdentifier} - already has ${existingCount} individual speeches`);
            processed++;
            if (progressBar && progressBar.update) {
              progressBar.update(processed);
            }
            continue;
          }
          
          // Parse the speech
          const individualSpeeches = parseIndividualSpeeches(speech.content, speech.id);
          
          if (individualSpeeches.length > 0) {
            await storeIndividualSpeeches(individualSpeeches);
            console.log(`✅ [BULK PARSE] Parsed ${individualSpeeches.length} speeches from ${speech.docIdentifier}`);
            successCount++;
          } else {
            console.log(`⚠️ [BULK PARSE] No individual speeches found in ${speech.docIdentifier}`);
          }
          
        } catch (error) {
          console.error(`❌ [BULK PARSE] Error parsing ${speech.docIdentifier}:`, error.message);
          errorCount++;
        }
        
        processed++;
        if (progressBar && progressBar.update) {
          progressBar.update(processed);
        }
        
        // Small delay to prevent overwhelming the system
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      
      const totalTime = (Date.now() - startTime) / 1000;
      console.log(`\n🎉 [BULK PARSE] Completed! Processed ${processed} speeches:`);
      console.log(`   ✅ Successfully parsed: ${successCount}`);
      console.log(`   ❌ Errors: ${errorCount}`);
      console.log(`   ⏱️ Total time: ${totalTime.toFixed(1)}s`);
      
      resolve();
    });
  });
}

// Check for demo mode
if (process.argv.includes('--demo-progress')) {
  console.log('🎬 Running progress bar demo...\n');
  demoProgressBars();
  console.log('\n🎉 Demo completed! Run without --demo-progress to start the server.');
  process.exit(0);
}

// Check for content test mode
if (process.argv.includes('--test-content')) {
  console.log('🧪 Testing content fetching...\n');
  (async () => {
    try {
      const testDate = '2025-07-10';
      const testSpeechId = 'eli/dl/event/MTG-PL-2025-07-10-OTH-2017033239347';
      console.log(`Testing content fetch for date: ${testDate}, speech: ${testSpeechId}`);
      const content = await fetchSpeechContentFromHTML(testDate, testSpeechId);
      console.log(`Content length: ${content.length} characters`);
      console.log(`Content preview: ${content.slice(0, 200)}...`);
    } catch (error) {
      console.error('Test failed:', error);
    }
    process.exit(0);
  })();
}

// Check for force content fetch mode
if (process.argv.includes('--fetch-content')) {
  console.log('🔍 Force fetching content for all speeches...\n');
  (async () => {
    try {
      const successCount = await addContentToExistingSpeeches();
      console.log(`\n🎉 Content fetching completed! Updated ${successCount} speeches.`);
    } catch (error) {
      console.error('Content fetching failed:', error);
    }
    process.exit(0);
  })();
}

// Check for complete speeches refetch mode
if (process.argv.includes('--refetch-all-speeches')) {
  console.log('🔄 COMPLETE SITTINGS REFETCH - This will clear and rebuild the entire sittings database...\n');
  (async () => {
    try {
      console.log('🗑️ Clearing existing speeches...');
      db.run('DELETE FROM sittings', (err) => {
        if (err) {
          console.error('❌ Error clearing speeches:', err);
          process.exit(1);
        }
        console.log('✅ Cleared existing speeches');
        
        console.log('🔄 Starting complete refetch of all speeches...');
        cacheAllSpeeches().then((count) => {
          console.log(`\n🎉 COMPLETE REFETCH COMPLETED! Cached ${count} speeches.`);
          process.exit(0);
        }).catch((error) => {
          console.error('❌ Complete refetch failed:', error);
          process.exit(1);
        });
      });
    } catch (error) {
      console.error('❌ Refetch failed:', error);
      process.exit(1);
    }
  })();
}

// Check for parse speeches mode
if (process.argv.includes('--parse-speeches')) {
  const sittingId = process.argv[process.argv.indexOf('--parse-speeches') + 1];
  if (!sittingId) {
    console.log('❌ Please provide a sitting ID: node server.js --parse-speeches <sitting_id>');
    process.exit(1);
  }
  
  console.log(`🔍 Parsing individual speeches for sitting: ${sittingId}\n`);
  (async () => {
    try {
      // Get the raw content for this sitting
      db.get('SELECT content FROM sittings WHERE id = ?', [sittingId], async (err, row) => {
        if (err) {
          console.error('❌ Database error:', err);
          process.exit(1);
        }
        
        if (!row || !row.content) {
          console.error('❌ No content found for sitting:', sittingId);
          process.exit(1);
        }
        
        console.log(`📄 Found content (${row.content.length} characters)`);
        console.log('🔍 Parsing individual speeches...');
        
        const individualSpeeches = parseIndividualSpeeches(row.content, sittingId);
        console.log(`✅ Parsed ${individualSpeeches.length} individual speeches`);
        
        // Show first few speeches as preview
        console.log('\n📋 Preview of parsed speeches:');
        individualSpeeches.slice(0, 5).forEach((speech, index) => {
          console.log(`${index + 1}. ${speech.speaker_name || speech.title} ${speech.political_group ? `(${speech.political_group})` : ''}`);
          console.log(`   Content: ${speech.speech_content.substring(0, 100)}...`);
        });
        
        // Store in database
        const stored = await storeIndividualSpeeches(individualSpeeches);
        console.log(`\n🎉 Successfully stored ${stored} individual speeches!`);
        process.exit(0);
      });
    } catch (error) {
      console.error('❌ Parse failed:', error);
      process.exit(1);
    }
  })();
}

// Check for bulk parse all speeches mode
if (process.argv.includes('--parse-all-speeches')) {
  console.log('🚀 [BULK PARSE] Starting bulk parsing of all speeches...');
  (async () => {
    try {
      await parseAllSpeechesWithContent();
      console.log('🎉 [BULK PARSE] All speeches parsed successfully!');
      process.exit(0);
    } catch (error) {
      console.error('❌ [BULK PARSE] Error:', error);
      process.exit(1);
    }
  })();
}

// Check for parse recent speeches mode
if (process.argv.includes('--parse-recent-speeches')) {
  console.log('🚀 [PARSE RECENT] Starting parsing of recent speeches...');
  (async () => {
    try {
      await parseRecentSpeeches();
      console.log('🎉 [PARSE RECENT] Recent speeches parsed successfully!');
      process.exit(0);
    } catch (error) {
      console.error('❌ [PARSE RECENT] Error:', error);
      process.exit(1);
    }
  })();
}

// Start server after database init
(async () => {
  try {
    await initDatabase();

    // Serve static assets (static site files located in public directory)
    app.use(express.static(path.join(__dirname, 'public')));

    // GET /api/meps: return all MEPs from DB with speech counts
    app.get('/api/meps', (req, res) => {
      console.log('📊 [CACHE] Fetching MEPs from database with speech counts...');
      db.all(`
        SELECT 
          m.*,
          COALESCE(COUNT(s.id), 0) as speech_count
        FROM meps m
        LEFT JOIN individual_speeches s ON m.id = s.mep_id
        GROUP BY m.id
        ORDER BY speech_count DESC, m.label ASC
      `, (err, rows) => {
        if (err) {
          console.error('❌ [CACHE] DB error fetching MEPs:', err);
          return res.status(500).json({ error: err.toString() });
        }
        console.log(`✅ [CACHE] Retrieved ${rows.length} MEPs from database with speech counts`);
        
        // Get standardized political groups and role information for all MEPs in a batch
        const mepIds = rows.map(r => r.id);
        db.all(`
          SELECT DISTINCT mep_id, political_group_std, political_group_kind, political_group_raw
          FROM individual_speeches 
          WHERE mep_id IN (${mepIds.map(() => '?').join(',')}) 
          AND political_group_std IS NOT NULL
        `, mepIds, (err2, groupRows) => {
          if (err2) {
            console.error('❌ [CACHE] Error fetching standardized groups:', err2);
            // Continue without standardized groups if there's an error
          }
          
          // Create a map of mep_id -> role/affiliation info
          const roleMap = {};
          if (groupRows) {
            groupRows.forEach(g => {
              if (!roleMap[g.mep_id]) {
                // Determine the best display value based on kind
                let displayValue = g.political_group_std;
                
                if (g.political_group_kind === 'institution') {
                  // For institutions, show a cleaned up version
                  if (g.political_group_raw.includes('Commission')) {
                    displayValue = 'European Commission';
                  } else if (g.political_group_raw.includes('Council')) {
                    displayValue = 'Council of the EU';
                  } else if (g.political_group_raw.includes('High Representative')) {
                    displayValue = 'High Representative';
                  } else {
                    displayValue = 'EU Institution';
                  }
                } else if (g.political_group_kind === 'role') {
                  // For parliamentary roles, show a cleaned up version
                  if (g.political_group_raw.includes('rapporteur')) {
                    displayValue = 'Committee Rapporteur';
                  } else if (g.political_group_raw.includes('Chair') || g.political_group_raw.includes('chair')) {
                    displayValue = 'Committee Chair';
                  } else if (g.political_group_raw.includes('delegat')) {
                    displayValue = 'Delegation Member';
                  } else {
                    displayValue = 'Parliamentary Role';
                  }
                } else if (g.political_group_std === 'NI' && g.political_group_kind === 'group') {
                  displayValue = 'Non-Attached';
                }
                
                roleMap[g.mep_id] = displayValue;
              }
            });
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
            'api:political-group': roleMap[r.id] || r.politicalGroup || 'Unknown',
            isCurrent: Boolean(r.is_current),
            source: r.source || 'api',
            speechCount: r.speech_count
          }));
          res.json({ data });
        });
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
        
        // Get standardized political group and role info for this MEP
        db.get(`
          SELECT political_group_std, political_group_kind, political_group_raw
          FROM individual_speeches 
          WHERE mep_id = ? 
          AND political_group_std IS NOT NULL 
          LIMIT 1
        `, [id], (err2, groupRow) => {
          let displayValue = row.politicalGroup || 'Unknown';
          
          if (groupRow) {
            displayValue = groupRow.political_group_std;
            
            if (groupRow.political_group_kind === 'institution') {
              if (groupRow.political_group_raw.includes('Commission')) {
                displayValue = 'European Commission';
              } else if (groupRow.political_group_raw.includes('Council')) {
                displayValue = 'Council of the EU';
              } else if (groupRow.political_group_raw.includes('High Representative')) {
                displayValue = 'High Representative';
              } else {
                displayValue = 'EU Institution';
              }
            } else if (groupRow.political_group_kind === 'role') {
              if (groupRow.political_group_raw.includes('rapporteur')) {
                displayValue = 'Committee Rapporteur';
              } else if (groupRow.political_group_raw.includes('Chair') || groupRow.political_group_raw.includes('chair')) {
                displayValue = 'Committee Chair';
              } else if (groupRow.political_group_raw.includes('delegat')) {
                displayValue = 'Delegation Member';
              } else {
                displayValue = 'Parliamentary Role';
              }
            } else if (groupRow.political_group_std === 'NI' && groupRow.political_group_kind === 'group') {
              displayValue = 'Non-Attached';
            }
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
            'api:political-group': displayValue
          };
          res.json({ data: mep });
        });
      });
    });

    // GET /api/meps/:id/speeches: get all speeches by a specific MEP
    app.get('/api/meps/:id/speeches', (req, res) => {
      const mepId = parseInt(req.params.id, 10);
      const limit = parseInt(req.query.limit, 10) || 100;
      const offset = parseInt(req.query.offset, 10) || 0;
      
      console.log(`🎤 [MEP-SPEECHES] Fetching speeches for MEP ID: ${mepId} (limit: ${limit}, offset: ${offset})`);
      
      // Get MEP info first
      db.get('SELECT * FROM meps WHERE id = ?', [mepId], (err, mep) => {
        if (err) {
          console.error('❌ [MEP-SPEECHES] Error fetching MEP:', err);
          res.status(500).json({ error: err.toString() });
          return;
        }
        if (!mep) {
          res.status(404).json({ error: 'MEP not found' });
          return;
        }
        
        // Get speeches for this MEP
        db.all(`
          SELECT 
            i.id,
            i.speaker_name,
            i.political_group,
            i.title,
            i.speech_content,
            i.speech_order,
            i.language,
            s.date,
            s.label as sitting_title,
            s.docIdentifier,
            s.notationId
          FROM individual_speeches i
          JOIN sittings s ON i.sitting_id = s.id
          WHERE i.mep_id = ?
          ORDER BY s.date DESC, i.speech_order ASC
          LIMIT ? OFFSET ?
        `, [mepId, limit, offset], (err, speeches) => {
          if (err) {
            console.error('❌ [MEP-SPEECHES] Error fetching speeches:', err);
            res.status(500).json({ error: err.toString() });
            return;
          }
          
          // Get total count
          db.get('SELECT COUNT(*) as total FROM individual_speeches WHERE mep_id = ?', [mepId], (err, countRow) => {
            if (err) {
              console.error('❌ [MEP-SPEECHES] Error fetching count:', err);
              res.status(500).json({ error: err.toString() });
              return;
            }
            
            console.log(`✅ [MEP-SPEECHES] Found ${speeches.length} speeches for ${mep.givenName} ${mep.familyName} (total: ${countRow.total})`);
            
            res.json({
              mep: mep,
              speeches: speeches,
              pagination: {
                total: countRow.total,
                limit: limit,
                offset: offset,
                hasMore: (offset + speeches.length) < countRow.total
              }
            });
          });
        });
      });
    });

    // GET /api/speeches: return cached speeches (optionally filter by MEP)
    app.get('/api/speeches', (req, res) => {
        const mepId = req.query.personId;
        const limit = parseInt(req.query.limit, 10) || 50;
        const offset = parseInt(req.query.offset, 10) || 0;
      
      console.log(`🎤 [CACHE] Fetching speeches - MEP ID: ${mepId || 'all'}, limit: ${limit}, offset: ${offset}`);
      
      // Only return sittings with content and include individual speech counts
      let query = `
        SELECT s.*, 
               COUNT(i.id) as individual_speech_count
        FROM sittings s
        LEFT JOIN individual_speeches i ON s.id = i.sitting_id
        WHERE LENGTH(s.content) > 100
      `;
      let params = [];
      
      if (mepId) {
        query += ' AND s.personId = ?';
        params.push(parseInt(mepId, 10));
        console.log(`🎤 [CACHE] Filtering speeches for MEP ID: ${mepId}`);
      }
      
      query += ' GROUP BY s.id ORDER BY s.activity_date DESC LIMIT ? OFFSET ?';
      params.push(limit, offset);
      
      // Get total count for pagination (only speeches with content)
      let countQuery = 'SELECT COUNT(*) as total FROM sittings WHERE LENGTH(content) > 100';
      let countParams = [];
      if (mepId) {
        countQuery += ' AND personId = ?';
        countParams.push(parseInt(mepId, 10));
      }
      
      db.get(countQuery, countParams, (err, countRow) => {
        if (err) {
          console.error('❌ [CACHE] DB error getting speech count:', err);
          return res.status(500).json({ error: err.toString() });
        }
        
        const total = countRow.total;
        console.log(`📊 [CACHE] Total speeches with content in database: ${total}`);
        
        db.all(query, params, (err, rows) => {
          if (err) {
            console.error('❌ [CACHE] DB error fetching speeches:', err);
            return res.status(500).json({ error: err.toString() });
          }
          
          console.log(`✅ [CACHE] Retrieved ${rows.length} speeches with content from database`);
          
          const data = rows.map(row => ({
            id: row.id,
            type: row.type,
            label: row.label,
            date: row.activity_date, // Fix: use activity_date instead of date
            activity_date: row.activity_date, // Also include activity_date for frontend compatibility
            content: row.content,
            individual_speech_count: row.individual_speech_count,
            docIdentifier: row.docIdentifier,
            notationId: row.notationId
          }));
          
        res.json({ data, meta: { total } });
        });
      });
    });

    // GET /api/speeches/:id/individual: return individual speeches for a sitting
    app.get('/api/speeches/:id/individual', (req, res) => {
      const sittingId = req.params.id;
      console.log(`🔍 [INDIVIDUAL] Fetching individual speeches for sitting: ${sittingId}`);
      
      db.all('SELECT *, language FROM individual_speeches WHERE sitting_id = ? ORDER BY speech_order', [sittingId], (err, rows) => {
        if (err) {
          console.error('❌ [INDIVIDUAL] Database error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        console.log(`✅ [INDIVIDUAL] Found ${rows.length} individual speeches for sitting ${sittingId}`);
        res.json({ 
          sitting_id: sittingId,
          individual_speeches: rows,
          count: rows.length 
        });
      });
    });

    // GET /api/speeches/:id: return detailed speech info from database
    app.get('/api/speeches/:id', async (req, res) => {
      try {
        const rawId = req.params.id;
        const speechId = decodeURIComponent(rawId);
        
        console.log(`🔍 [SPEECH] Fetching speech details for ID: ${speechId}`);
        
        // First try to get from database
        db.get('SELECT * FROM sittings WHERE id = ?', [speechId], (err, row) => {
          if (err) {
            console.error('❌ [SPEECH] DB error:', err);
            return res.status(500).json({ error: 'Database error' });
          }
          
          if (row) {
            console.log(`✅ [SPEECH] Found speech in database: ${speechId}`);
            
            // Convert database row to API format
            const speechData = {
              id: row.id,
              type: row.type,
              label: row.label,
              date: row.date,
              content: row.content,
              docIdentifier: row.docIdentifier,
              notationId: row.notationId,
              activity_type: row.activity_type,
              activity_date: row.activity_date,
              activity_start_date: row.activity_start_date,
              last_updated: row.last_updated,
              // Add some mock fields for compatibility
              had_activity_type: row.activity_type,
              recorded_in_a_realization_of: row.docIdentifier ? [{
                identifier: row.docIdentifier,
                notation_speechId: row.notationId
              }] : []
            };
            
            return res.json(speechData);
          }
          
          // Fallback to remote API if not in database
          console.log(`⚠️ [SPEECH] Speech not in database, falling back to remote API: ${speechId}`);
        const lang = req.query.lang || req.query['search-language'] || 'EN';
        const params = { 'search-language': lang, format: 'application/ld+json' };
        if (req.query.text) params.text = req.query.text;
        if (req.query['include-output']) params['include-output'] = req.query['include-output'];
          
          axios.get(`${API_BASE}/speeches/${speechId}`, {
          params,
          headers: { Accept: 'application/ld+json' }
          }).then(response => {
        res.json(response.data);
          }).catch(error => {
            console.error('❌ [SPEECH] Remote API error:', error.toString());
            res.status(500).json({ error: error.toString() });
          });
        });
      } catch (error) {
        console.error('❌ [SPEECH] Error:', error.toString());
        res.status(500).json({ error: error.toString() });
      }
    });

    // =============================================
    // Analytics Endpoints
    // =============================================
    // GET /api/analytics/overview
    app.get('/api/analytics/overview', (req, res) => {
      // Serve from cache if available
      if (analyticsCache.data) {
        console.log('⚡ [CACHE] Served overview from cache');
        return res.json(analyticsCache.data.overview);
      }
      
      const topLimit = parseInt(req.query.limit, 10) || 20;
      const trendMonths = parseInt(req.query.months, 10) || 12;

      const result = { coverage: {}, macroTopicDistribution: [], topSpecificFocus: [], trendsMonthly: [] };

      // 1) Coverage
      db.get(`
        SELECT 
          COUNT(*) AS total,
          SUM(CASE WHEN macro_topic IS NOT NULL AND TRIM(macro_topic) <> '' THEN 1 ELSE 0 END) AS with_macro
        FROM individual_speeches
      `, [], (err1, cov) => {
        if (err1) return res.status(500).json({ error: err1.message });
        const total = cov?.total || 0;
        const withMacro = cov?.with_macro || 0;
        const pct = total ? Math.round((withMacro / total) * 1000) / 10 : 0;
        result.coverage = { total, with_macro: withMacro, pct_with_macro: pct };

        // 2) Macro Topic Distribution (top N)
        db.all(`
          SELECT macro_topic AS topic, COUNT(*) AS count
          FROM individual_speeches
          WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
          GROUP BY macro_topic
          ORDER BY count DESC
          LIMIT ?
        `, [topLimit], (err2, rowsTopic) => {
          if (err2) return res.status(500).json({ error: err2.message });
          result.macroTopicDistribution = rowsTopic || [];

          // 3) Top Specific Focus (overall top N)
          db.all(`
            SELECT macro_topic AS topic, macro_specific_focus AS focus, COUNT(*) AS count
            FROM individual_speeches
            WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
              AND macro_specific_focus IS NOT NULL AND TRIM(macro_specific_focus) <> ''
            GROUP BY macro_topic, macro_specific_focus
            ORDER BY count DESC
            LIMIT ?
          `, [topLimit], (err3, rowsFocus) => {
            if (err3) return res.status(500).json({ error: err3.message });
            result.topSpecificFocus = rowsFocus || [];

            // 4) Trends for last X months for top 5 topics
            const top5 = (rowsTopic || []).slice(0, 5).map(r => r.topic).filter(Boolean);
            if (top5.length === 0) return res.json(result);

            // Build monthly trend and then trim to last N months
            const placeholders = top5.map(() => '?').join(',');
            db.all(`
              SELECT substr(s.activity_date, 1, 7) AS ym, i.macro_topic AS topic, COUNT(*) AS count
              FROM individual_speeches i
              JOIN sittings s ON s.id = i.sitting_id
              WHERE i.macro_topic IN (${placeholders})
                AND s.activity_date IS NOT NULL
              GROUP BY ym, i.macro_topic
              ORDER BY ym ASC
            `, top5, (err4, rowsTrend) => {
              if (err4) return res.status(500).json({ error: err4.message });
              // Keep only last N months
              const months = Array.from(new Set((rowsTrend || []).map(r => r.ym))).sort();
              const lastMonths = months.slice(-trendMonths);
              result.trendsMonthly = (rowsTrend || []).filter(r => lastMonths.includes(r.ym));
              res.json(result);
            });
          });
        });
      });
    });

    // Helper function to normalize topic names (remove HTML entities, normalize dashes)
    const normalizeTopic = (topic) => {
      if (!topic) return topic;
      let normalized = topic
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");
      
      // Normalize ALL types of dashes, hyphens, and minus signs to regular hyphen-minus
      // U+2010 to U+2015: various dashes
      // U+2011: non-breaking hyphen  
      // U+2013: en-dash
      // U+2014: em-dash (the —)
      // U+2212: minus sign
      normalized = normalized.replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-');
      
      return normalized.trim();
    };

    // Function to warm up analytics cache
    async function warmAnalyticsCache() {
      if (analyticsCache.isWarming) {
        console.log('⚠️ Cache warming already in progress');
        return;
      }

      analyticsCache.isWarming = true;
      analyticsCache.progress = { stage: 'Starting', percent: 0, message: 'Initializing analytics cache...' };
      console.log('🔄 [CACHE] Starting analytics cache warming...');
      
      const cacheData = {};
      
      try {
        // Step 1: Get all normalized topics (10%)
        analyticsCache.progress = { stage: 'topics', percent: 10, message: 'Loading topics...' };
        console.log('📊 [CACHE] Step 1/6: Loading topics');
        
        const allTopicsRows = await new Promise((resolve, reject) => {
          db.all(`
            SELECT DISTINCT i.macro_topic AS topic
            FROM individual_speeches i
            JOIN sittings s ON s.id = i.sitting_id
            WHERE s.activity_date IS NOT NULL
              AND i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
          `, [], (err, rows) => err ? reject(err) : resolve(rows));
        });
        
        const rawTopics = allTopicsRows.map(r => r.topic);
        const normalizedMap = new Map();
        rawTopics.forEach(topic => {
          const normalized = normalizeTopic(topic);
          if (!normalizedMap.has(normalized)) {
            normalizedMap.set(normalized, [topic]);
          } else {
            normalizedMap.get(normalized).push(topic);
          }
        });
        const allTopics = Array.from(normalizedMap.keys());
        cacheData.allTopics = allTopics;
        cacheData.topicVariants = normalizedMap;
        console.log(`✅ [CACHE] Found ${allTopics.length} unique topics`);
        
        // Step 2: Pre-compute time series for ALL topics (monthly and quarterly) (40%)
        analyticsCache.progress = { stage: 'timeseries', percent: 25, message: 'Computing time series data...' };
        console.log('📊 [CACHE] Step 2/6: Computing time series');
        
        for (const interval of ['month', 'quarter']) {
          const periodExpr = interval === 'month' 
            ? `substr(s.activity_date,1,7)` 
            : `substr(s.activity_date,1,4) || '-Q' || ((cast(substr(s.activity_date,6,2) as integer)+2)/3)`;
          
          // Get all variants for SQL
          const allVariants = Array.from(normalizedMap.values()).flat();
          const placeholders = allVariants.map(() => '?').join(',');
          
          const [dataRows, periodRows] = await Promise.all([
            new Promise((resolve, reject) => {
              db.all(`
                SELECT ${periodExpr} AS period, i.macro_topic AS topic, COUNT(*) AS cnt
                FROM individual_speeches i
                JOIN sittings s ON s.id = i.sitting_id
                WHERE s.activity_date IS NOT NULL AND i.macro_topic IN (${placeholders})
                GROUP BY period, i.macro_topic
                ORDER BY period ASC
              `, allVariants, (err, rows) => err ? reject(err) : resolve(rows));
            }),
            new Promise((resolve, reject) => {
              db.all(`
                SELECT DISTINCT ${periodExpr} AS period
                FROM individual_speeches i
                JOIN sittings s ON s.id = i.sitting_id
                WHERE s.activity_date IS NOT NULL
                ORDER BY period ASC
              `, [], (err, rows) => err ? reject(err) : resolve(rows));
            })
          ]);
          
          const labels = periodRows.map(r => r.period);
          
          // 🚀 OPTIMIZATION: Pre-index data by topic|period for O(1) lookups
          const dataIndex = new Map();
          dataRows.forEach(row => {
            const key = `${row.topic}|${row.period}`;
            dataIndex.set(key, (dataIndex.get(key) || 0) + row.cnt);
          });
          
          // Now this is FAST - O(1) lookups instead of O(n) filters!
          const datasets = allTopics.map(normalizedTopic => {
            const variants = normalizedMap.get(normalizedTopic) || [];
            return {
              label: normalizedTopic,
              data: labels.map(p => {
                // Sum counts for all variants using O(1) Map lookups
                return variants.reduce((sum, variant) => {
                  const key = `${variant}|${p}`;
                  return sum + (dataIndex.get(key) || 0);
                }, 0);
              })
            };
          });
          
          cacheData[`timeseries_${interval}`] = { labels, datasets, topics: allTopics };
          console.log(`✅ [CACHE] Computed ${interval} time series: ${labels.length} periods, ${allTopics.length} topics`);
        }
        
        analyticsCache.progress = { stage: 'timeseries', percent: 40, message: 'Time series computed' };
        
        // Step 3: Pre-compute by-group data (60%)
        analyticsCache.progress = { stage: 'groups', percent: 50, message: 'Computing political groups data...' };
        console.log('📊 [CACHE] Step 3/6: Computing by-group');
        
        // Use ALL topics (not just top 10) for comprehensive filtering
        const topTopicsForGroups = allTopics;
        
        const [groups, groupRows] = await Promise.all([
          new Promise((resolve, reject) => {
            db.all(`
              SELECT COALESCE(political_group_std, political_group) AS grp, COUNT(*) AS cnt
              FROM individual_speeches
              WHERE COALESCE(political_group_std, political_group) IS NOT NULL 
                AND TRIM(COALESCE(political_group_std, political_group))<>''
              GROUP BY grp
              ORDER BY cnt DESC LIMIT 10
            `, [], (err, rows) => err ? reject(err) : resolve(rows));
          })
        ]);
        
        const groupsList = groups.map(r => r.grp);
        
        const allTopicVariantsForGroups = Array.from(new Set(
          topTopicsForGroups.flatMap(t => normalizedMap.get(t) || [])
        ));
        
        const groupDataRows = await new Promise((resolve, reject) => {
          const pT = allTopicVariantsForGroups.map(() => '?').join(',');
          const pG = groupsList.map(() => '?').join(',');
          db.all(`
            SELECT i.macro_topic AS topic, COALESCE(i.political_group_std, i.political_group) AS grp, COUNT(*) AS cnt
            FROM individual_speeches i
            WHERE i.macro_topic IN (${pT})
              AND COALESCE(i.political_group_std, i.political_group) IN (${pG})
            GROUP BY i.macro_topic, COALESCE(i.political_group_std, i.political_group)
          `, [...allTopicVariantsForGroups, ...groupsList], (err, rows) => err ? reject(err) : resolve(rows));
        });
        
        // Normalize the rows to use normalized topic names for easier filtering
        const normalizedGroupRows = groupDataRows.map(row => ({
          ...row,
          topic: normalizeTopic(row.topic)
        }));
        
        cacheData.byGroup = { topics: topTopicsForGroups, groups: groupsList, rows: normalizedGroupRows, topicVariants: normalizedMap };
        console.log(`✅ [CACHE] Computed by-group: ${topTopicsForGroups.length} topics × ${groupsList.length} groups`);
        analyticsCache.progress = { stage: 'groups', percent: 60, message: 'Political groups computed' };
        
        // Step 4: Pre-compute by-country data (75%)
        analyticsCache.progress = { stage: 'countries', percent: 65, message: 'Computing countries data...' };
        console.log('📊 [CACHE] Step 4/6: Computing by-country');
        
        // Use ALL topics (not just top 10) for comprehensive filtering
        const topTopicsForCountries = allTopics;
        
        const [countries] = await Promise.all([
          new Promise((resolve, reject) => {
            db.all(`
              SELECT m.country AS country, COUNT(*) AS cnt
              FROM individual_speeches i
              LEFT JOIN meps m ON m.id = i.mep_id
              GROUP BY m.country
              ORDER BY cnt DESC LIMIT 20
            `, [], (err, rows) => err ? reject(err) : resolve(rows));
          })
        ]);
        
        const countriesList = countries.map(r => r.country).filter(Boolean);
        
        const allTopicVariantsForCountries = Array.from(new Set(
          topTopicsForCountries.flatMap(t => normalizedMap.get(t) || [])
        ));
        
        const countryDataRows = await new Promise((resolve, reject) => {
          const pT = allTopicVariantsForCountries.map(() => '?').join(',');
          const pC = countriesList.map(() => '?').join(',');
          db.all(`
            SELECT i.macro_topic AS topic, m.country AS country, COUNT(*) AS cnt
            FROM individual_speeches i
            LEFT JOIN meps m ON m.id = i.mep_id
            WHERE i.macro_topic IN (${pT})
              AND m.country IN (${pC})
            GROUP BY i.macro_topic, m.country
          `, [...allTopicVariantsForCountries, ...countriesList], (err, rows) => err ? reject(err) : resolve(rows));
        });
        
        // Normalize the rows to use normalized topic names for easier filtering
        const normalizedCountryRows = countryDataRows.map(row => ({
          ...row,
          topic: normalizeTopic(row.topic)
        }));
        
        cacheData.byCountry = { topics: topTopicsForCountries, countries: countriesList, rows: normalizedCountryRows, topicVariants: normalizedMap };
        console.log(`✅ [CACHE] Computed by-country: ${topTopicsForCountries.length} topics × ${countriesList.length} countries`);
        analyticsCache.progress = { stage: 'countries', percent: 75, message: 'Countries computed' };
        
        // Step 5: Pre-compute languages (85%)
        analyticsCache.progress = { stage: 'languages', percent: 80, message: 'Computing languages...' };
        console.log('📊 [CACHE] Step 5/6: Computing languages');
        
        const languageRows = await new Promise((resolve, reject) => {
          db.all(`
            SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
            FROM individual_speeches
            GROUP BY UPPER(COALESCE(language,'UNK'))
            ORDER BY cnt DESC
          `, [], (err, rows) => err ? reject(err) : resolve(rows));
        });
        
        cacheData.languages = { rows: languageRows };
        console.log(`✅ [CACHE] Computed languages: ${languageRows.length} languages`);
        analyticsCache.progress = { stage: 'languages', percent: 85, message: 'Languages computed' };
        
        // Step 6: Pre-compute overview data (95%)
        analyticsCache.progress = { stage: 'overview', percent: 90, message: 'Computing overview...' };
        console.log('📊 [CACHE] Step 6/6: Computing overview');
        
        const [coverage, macroTopics, specificFocus] = await Promise.all([
          new Promise((resolve, reject) => {
            db.get(`
              SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN macro_topic IS NOT NULL AND TRIM(macro_topic) <> '' THEN 1 ELSE 0 END) AS with_macro
              FROM individual_speeches
            `, [], (err, row) => err ? reject(err) : resolve(row));
          }),
          new Promise((resolve, reject) => {
            db.all(`
              SELECT macro_topic AS topic, COUNT(*) AS count
              FROM individual_speeches
              WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
              GROUP BY macro_topic
              ORDER BY count DESC LIMIT 20
            `, [], (err, rows) => err ? reject(err) : resolve(rows));
          }),
          new Promise((resolve, reject) => {
            db.all(`
              SELECT macro_topic AS topic, macro_specific_focus AS focus, COUNT(*) AS count
              FROM individual_speeches
              WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
                AND macro_specific_focus IS NOT NULL AND TRIM(macro_specific_focus) <> ''
              GROUP BY macro_topic, macro_specific_focus
              ORDER BY count DESC LIMIT 20
            `, [], (err, rows) => err ? reject(err) : resolve(rows));
          })
        ]);
        
        const total = coverage?.total || 0;
        const withMacro = coverage?.with_macro || 0;
        const pct = total ? Math.round((withMacro / total) * 1000) / 10 : 0;
        
        cacheData.overview = {
          coverage: { total, with_macro: withMacro, pct_with_macro: pct },
          macroTopicDistribution: macroTopics,
          topSpecificFocus: specificFocus
        };
        
        console.log(`✅ [CACHE] Computed overview`);
        
        // Done!
        analyticsCache.data = cacheData;
        analyticsCache.lastUpdated = new Date().toISOString();
        analyticsCache.progress = { stage: 'complete', percent: 100, message: 'Cache ready!' };
        analyticsCache.isWarming = false;
        
        console.log('✅ [CACHE] Analytics cache warming completed successfully!');
        console.log(`📊 [CACHE] Cached: ${allTopics.length} topics, ${cacheData.timeseries_month.labels.length} periods`);
        
      } catch (error) {
        console.error('❌ [CACHE] Error warming cache:', error);
        analyticsCache.isWarming = false;
        analyticsCache.progress = { stage: 'error', percent: 0, message: 'Cache warming failed: ' + error.message };
      }
    }

    // Cache status endpoint
    app.get('/api/analytics/cache-status', (req, res) => {
      res.json({
        ready: analyticsCache.data !== null,
        warming: analyticsCache.isWarming,
        lastUpdated: analyticsCache.lastUpdated,
        progress: analyticsCache.progress
      });
    });

    // GET /api/analytics/time-series?interval=month&from=YYYY-MM&to=YYYY-MM&top=5
    app.get('/api/analytics/time-series', (req, res) => {
      const startTime = Date.now();
      const interval = (req.query.interval || 'month').toLowerCase();
      
      // Serve from cache if available
      if (analyticsCache.data) {
        const cached = analyticsCache.data[`timeseries_${interval}`];
        if (cached) {
          const totalTime = Date.now() - startTime;
          console.log(`⚡ [CACHE] Served time-series from cache in ${totalTime}ms`);
          return res.json(cached);
        }
      }
      const from = req.query.from || null; // 'YYYY-MM' or 'YYYY'
      const to = req.query.to || null;     // 'YYYY-MM' or 'YYYY'
      const top = req.query.top ? Math.max(1, parseInt(req.query.top, 10) || 1) : null; // if missing => ALL
      const returnAll = String(req.query.all || '').toLowerCase() === 'true' || req.query.all === '1';
      // topics can be CSV or JSON array
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }

      let periodExpr = `substr(s.activity_date,1,7)`; // month
      if (interval === 'year') periodExpr = `substr(s.activity_date,1,4)`;
      if (interval === 'quarter') periodExpr = `substr(s.activity_date,1,4) || '-Q' || ((cast(substr(s.activity_date,6,2) as integer)+2)/3)`;

      const where = ['s.activity_date IS NOT NULL'];
      const params = [];
      if (from) { where.push(`${periodExpr} >= ?`); params.push(from); }
      if (to) { where.push(`${periodExpr} <= ?`); params.push(to); }

      const whereSql = where.length ? 'WHERE ' + where.join(' AND ') : '';

      const useTopics = async (topics) => {
        // For SQL query, we need to match both normalized and original forms
        // Get all distinct macro topics from the database
        const allTopicsSql = `
          SELECT DISTINCT i.macro_topic AS topic
          FROM individual_speeches i
          JOIN sittings s ON s.id = i.sitting_id
          ${whereSql}
          AND i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        `;
        db.all(allTopicsSql, params, (errAllTopics, allTopicsRows) => {
          if (errAllTopics) return res.status(500).json({ error: errAllTopics.message });
          
          // Map normalized topics to all their variants in DB
          const topicVariants = new Map();
          topics.forEach(normalizedTopic => {
            const variants = allTopicsRows
              .filter(row => normalizeTopic(row.topic) === normalizedTopic)
              .map(row => row.topic);
            topicVariants.set(normalizedTopic, variants);
          });
          
          // Flatten all variants for the SQL query
          const allVariants = Array.from(topicVariants.values()).flat();
          if (allVariants.length === 0) {
            return res.json({ labels: [], datasets: [], topics });
          }
          
          const placeholders = allVariants.map(()=>'?').join(',');
          const params2 = [...params, ...allVariants];
          const sql = `
            SELECT ${periodExpr} AS period, i.macro_topic AS topic, COUNT(*) AS cnt
            FROM individual_speeches i
            JOIN sittings s ON s.id = i.sitting_id
            ${whereSql} AND i.macro_topic IN (${placeholders})
            GROUP BY period, i.macro_topic
            ORDER BY period ASC
          `;
          db.all(sql, params2, (err, rows) => {
            if (err) return res.status(500).json({ error: err.message });
            
            // Get all periods from the entire database to ensure full timeline
            const allPeriodsSql = `
              SELECT DISTINCT ${periodExpr} AS period
              FROM individual_speeches i
              JOIN sittings s ON s.id = i.sitting_id
              WHERE s.activity_date IS NOT NULL
              ORDER BY period ASC
            `;
            db.all(allPeriodsSql, [], (errPeriods, periodRows) => {
              if (errPeriods) return res.status(500).json({ error: errPeriods.message });
              
              // Use ALL periods from database for complete timeline
              const labels = (periodRows || []).map(r => r.period);
              
              // Aggregate counts by normalized topic
              const datasets = topics.map(normalizedTopic => {
                const variants = topicVariants.get(normalizedTopic) || [];
                return {
                  label: normalizedTopic,
                  data: labels.map(p => {
                    // Sum counts for all variants of this normalized topic
                    const matchingRows = rows.filter(x => 
                      x.period === p && variants.includes(x.topic)
                    );
                    return matchingRows.reduce((sum, r) => sum + r.cnt, 0);
                  })
                };
              });
              const totalTime = Date.now() - startTime;
              console.log(`⏱️ [SERVER] /api/analytics/time-series completed in ${totalTime}ms (${topics.length} topics, ${labels.length} periods)`);
              res.json({ labels, datasets, topics });
            });
          });
        });
      };

      if (topicsFilter && topicsFilter.length) {
        return useTopics(topicsFilter);
      }

      // 1) Determine topics (either ALL in range, or top N)
      const topicSqlAll = `
        SELECT DISTINCT i.macro_topic AS topic
        FROM individual_speeches i
        JOIN sittings s ON s.id = i.sitting_id
        ${whereSql}
        AND i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
      `;
      const topicSqlTop = `
        SELECT i.macro_topic AS topic, COUNT(*) AS cnt
        FROM individual_speeches i
        JOIN sittings s ON s.id = i.sitting_id
        ${whereSql}
        AND i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        GROUP BY i.macro_topic
        ORDER BY cnt DESC
        LIMIT ${top || 50}
      `;

      db.all(returnAll || !top ? topicSqlAll : topicSqlTop, params, (errTop, rows) => {
        if (errTop) return res.status(500).json({ error: errTop.message });
        // Normalize and deduplicate topics
        const rawTopics = (rows||[]).map(r => r.topic).filter(Boolean);
        const normalizedMap = new Map();
        rawTopics.forEach(topic => {
          const normalized = normalizeTopic(topic);
          if (!normalizedMap.has(normalized)) {
            normalizedMap.set(normalized, topic); // Keep first occurrence
          }
        });
        const topics = Array.from(normalizedMap.keys());
        if (topics.length === 0) return res.json({ labels: [], datasets: [] });
        return useTopics(topics);
      });
    });

    // GET /api/analytics/by-group?topTopics=10&topGroups=10&topics=...
    app.get('/api/analytics/by-group', (req, res) => {
      // Check if specific topics are requested
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }
      
      // Serve from cache (with optional filtering)
      if (analyticsCache.data) {
        const cached = analyticsCache.data.byGroup;
        
        if (!topicsFilter || topicsFilter.length === 0) {
          // No filter - return full cache
          console.log('⚡ [CACHE] Served by-group from cache (all topics)');
          return res.json(cached);
        }
        
        // Filter cached data by selected topics (rows are already normalized)
        const filteredRows = cached.rows.filter(row => 
          topicsFilter.includes(row.topic)
        );
        
        const filteredTopics = topicsFilter.filter(t => 
          cached.topics.includes(t)
        );
        
        console.log(`⚡ [CACHE] Served by-group from cache (filtered to ${filteredTopics.length} topics)`);
        return res.json({
          topics: filteredTopics,
          groups: cached.groups,
          rows: filteredRows
        });
      }
      
      // Fallback to database if cache not ready
      const topTopics = Math.max(1, parseInt(req.query.topTopics, 10) || 10);
      const topGroups = Math.max(1, parseInt(req.query.topGroups, 10) || 10);
      
      const processWithTopics = (topics) => {
        // top groups
        db.all(`
          SELECT COALESCE(political_group_std, political_group) AS grp, COUNT(*) AS cnt
          FROM individual_speeches
          WHERE COALESCE(political_group_std, political_group) IS NOT NULL AND TRIM(COALESCE(political_group_std, political_group))<>''
          GROUP BY grp
          ORDER BY cnt DESC
          LIMIT ?
        `, [topGroups], (e2, grows) => {
          if (e2) return res.status(500).json({ error: e2.message });
          const groups = (grows||[]).map(r=>r.grp);
          const placeholdersT = topics.map(()=>'?').join(',');
          const placeholdersG = groups.map(()=>'?').join(',');
          const params = [...topics, ...groups];
          db.all(`
            SELECT i.macro_topic AS topic, COALESCE(i.political_group_std, i.political_group) AS grp, COUNT(*) AS cnt
            FROM individual_speeches i
            WHERE i.macro_topic IN (${placeholdersT})
              AND COALESCE(i.political_group_std, i.political_group) IN (${placeholdersG})
            GROUP BY i.macro_topic, COALESCE(i.political_group_std, i.political_group)
          `, params, (e3, rows) => {
            if (e3) return res.status(500).json({ error: e3.message });
            res.json({ topics, groups, rows });
          });
        });
      };
      
      // If topics filter is provided, use it directly; otherwise get top N
      if (topicsFilter && topicsFilter.length > 0) {
        return processWithTopics(topicsFilter);
      }
      
      // Get top topics
      db.all(`
        SELECT i.macro_topic AS topic, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        GROUP BY i.macro_topic
        ORDER BY cnt DESC
        LIMIT ?
      `, [topTopics], (e1, trows) => {
        if (e1) return res.status(500).json({ error: e1.message });
        const topics = (trows||[]).map(r=>r.topic);
        if (!topics.length) return res.json({ groups: [], topics: [], rows: [] });
        processWithTopics(topics);
      });
    });

    // GET /api/analytics/by-country?topTopics=10&topCountries=20&topics=...
    app.get('/api/analytics/by-country', (req, res) => {
      // Check if specific topics are requested
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }
      
      // Serve from cache (with optional filtering)
      if (analyticsCache.data) {
        const cached = analyticsCache.data.byCountry;
        
        if (!topicsFilter || topicsFilter.length === 0) {
          // No filter - return full cache
          console.log('⚡ [CACHE] Served by-country from cache (all topics)');
          return res.json(cached);
        }
        
        // Filter cached data by selected topics (rows are already normalized)
        const filteredRows = cached.rows.filter(row => 
          topicsFilter.includes(row.topic)
        );
        
        const filteredTopics = topicsFilter.filter(t => 
          cached.topics.includes(t)
        );
        
        console.log(`⚡ [CACHE] Served by-country from cache (filtered to ${filteredTopics.length} topics)`);
        return res.json({
          topics: filteredTopics,
          countries: cached.countries,
          rows: filteredRows
        });
      }
      
      // Fallback to database if cache not ready
      const topTopics = Math.max(1, parseInt(req.query.topTopics, 10) || 10);
      const topCountries = Math.max(1, parseInt(req.query.topCountries, 10) || 20);
      
      const processWithTopics = (topics) => {
        db.all(`
          SELECT m.country AS country, COUNT(*) AS cnt
          FROM individual_speeches i
          LEFT JOIN meps m ON m.id = i.mep_id
          GROUP BY m.country
          ORDER BY cnt DESC
          LIMIT ?
        `, [topCountries], (e2, crows) => {
          if (e2) return res.status(500).json({ error: e2.message });
          const countries = (crows||[]).map(r=>r.country).filter(Boolean);
          const placeholdersT = topics.map(()=>'?').join(',');
          const placeholdersC = countries.map(()=>'?').join(',');
          const params = [...topics, ...countries];
          db.all(`
            SELECT i.macro_topic AS topic, m.country AS country, COUNT(*) AS cnt
            FROM individual_speeches i
            LEFT JOIN meps m ON m.id = i.mep_id
            WHERE i.macro_topic IN (${placeholdersT})
              AND m.country IN (${placeholdersC})
            GROUP BY i.macro_topic, m.country
          `, params, (e3, rows) => {
            if (e3) return res.status(500).json({ error: e3.message });
            res.json({ topics, countries, rows });
          });
        });
      };
      
      // If topics filter is provided, use it directly; otherwise get top N
      if (topicsFilter && topicsFilter.length > 0) {
        return processWithTopics(topicsFilter);
      }
      
      db.all(`
        SELECT i.macro_topic AS topic, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        GROUP BY i.macro_topic
        ORDER BY cnt DESC LIMIT ?
      `, [topTopics], (e1, trows) => {
        if (e1) return res.status(500).json({ error: e1.message });
        const topics = (trows||[]).map(r=>r.topic);
        if (!topics.length) return res.json({ countries: [], topics: [], rows: [] });
        processWithTopics(topics);
      });
    });

    // GET /api/analytics/languages?topics=...
    app.get('/api/analytics/languages', (req, res) => {
      // Check if specific topics are requested
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }
      
      // Serve from cache (with optional filtering)
      if (analyticsCache.data) {
        if (!topicsFilter || topicsFilter.length === 0) {
          // No filter - return full cache
          console.log('⚡ [CACHE] Served languages from cache (all topics)');
          return res.json(analyticsCache.data.languages);
        }
        
        // For filtered languages, we need to query the database with topic filter
        // This is because the cache doesn't store per-topic language breakdown
        console.log('🔍 [QUERY] Computing languages for filtered topics');
      }
      
      // Query database for filtered topics or if cache not ready
      let sql = `
        SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
        FROM individual_speeches
      `;
      let params = [];
      
      if (topicsFilter && topicsFilter.length > 0) {
        // Need to get all variants of the normalized topics
        if (analyticsCache.data) {
          const topicVariants = analyticsCache.data.topicVariants;
          const allVariants = topicsFilter.flatMap(t => topicVariants.get(t) || [t]);
          const placeholders = allVariants.map(()=>'?').join(',');
          sql += ` WHERE macro_topic IN (${placeholders})`;
          params = allVariants;
        } else {
          const placeholders = topicsFilter.map(()=>'?').join(',');
          sql += ` WHERE macro_topic IN (${placeholders})`;
          params = topicsFilter;
        }
      }
      
      sql += ` GROUP BY UPPER(COALESCE(language,'UNK')) ORDER BY cnt DESC`;
      
      db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ rows });
      });
    });

    // GET /api/analytics/top-meps?topic=...&top=20
    app.get('/api/analytics/top-meps', (req, res) => {
      const topic = req.query.topic || null;
      const top = Math.max(1, parseInt(req.query.top, 10) || 20);
      const params = [];
      let where = 'WHERE 1=1';
      if (topic) { where += ' AND i.macro_topic = ?'; params.push(topic); }
      const sql = `
        SELECT m.id, m.label, m.country, COALESCE(i.political_group_std, i.political_group) AS grp, COUNT(*) AS cnt
        FROM individual_speeches i
        LEFT JOIN meps m ON m.id = i.mep_id
        ${where}
        GROUP BY m.id, m.label, m.country, COALESCE(i.political_group_std, i.political_group)
        ORDER BY cnt DESC
        LIMIT ${top}
      `;
      db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ rows });
      });
    });

    // Endpoint: Export speeches to CSV
    app.get('/api/export/speeches', (req, res) => {
      const requestStartTime = Date.now();
      console.log('📊 [EXPORT] ========================================');
      console.log('📊 [EXPORT] Export request received');
      
      const { startDate, endDate, fields, countOnly } = req.query;
      console.log(`📊 [EXPORT] Query params - startDate: ${startDate}, endDate: ${endDate}, fields: ${fields ? fields.substring(0, 50) + '...' : 'default'}, countOnly: ${countOnly}`);
      
      // Build WHERE clause for date filtering
      const params = [];
      let whereClauses = [];
      
      if (startDate) {
        whereClauses.push('s.activity_date >= ?');
        params.push(startDate);
        console.log(`📊 [EXPORT] Adding start date filter: ${startDate}`);
      }
      
      if (endDate) {
        whereClauses.push('s.activity_date <= ?');
        params.push(endDate);
        console.log(`📊 [EXPORT] Adding end date filter: ${endDate}`);
      }
      
      const whereClause = whereClauses.length > 0 ? 'WHERE ' + whereClauses.join(' AND ') : '';
      console.log(`📊 [EXPORT] WHERE clause: ${whereClause || '(none - all data)'}`);
      
      // If only count is requested
      if (countOnly === 'true') {
        console.log('📊 [EXPORT] Count-only request');
        const countSql = `
          SELECT COUNT(*) as count
          FROM individual_speeches i
          LEFT JOIN sittings s ON i.sitting_id = s.id
          ${whereClause}
        `;
        
        console.log('📊 [EXPORT] Executing count query...');
        const queryStartTime = Date.now();
        
        db.get(countSql, params, (err, row) => {
          const queryTime = Date.now() - queryStartTime;
          const totalTime = Date.now() - requestStartTime;
          
          if (err) {
            console.error('❌ [EXPORT] Error counting speeches:', err);
            console.log(`⏱️ [EXPORT] Failed after ${totalTime}ms`);
            return res.status(500).json({ error: err.message });
          }
          console.log(`✅ [EXPORT] Count query completed: ${row.count} speeches`);
          console.log(`⏱️ [EXPORT] Query time: ${queryTime}ms`);
          console.log(`⏱️ [EXPORT] Total count request time: ${totalTime}ms`);
          res.json({ count: row.count });
        });
        return;
      }
      
      // Parse requested fields
      console.log('📊 [EXPORT] Full CSV export request');
      const requestedFields = fields ? fields.split(',') : [
        // Default fields if none specified
        'id', 'sitting_id', 'date', 'speaker_name', 'political_group', 
        'title', 'speech_content', 'language', 'macro_topic', 'specific_focus',
        'topic', 'country', 'mep_id'
      ];
      
      console.log(`📊 [EXPORT] Requested ${requestedFields.length} fields: ${requestedFields.join(', ')}`);
      
      // Map field names to SQL columns
      const fieldMapping = {
        // Basic Information
        'id': 'i.id',
        'sitting_id': 'i.sitting_id',
        'date': 's.activity_date',
        'activity_start_date': 's.activity_start_date',
        'activity_type': 's.activity_type',
        'speech_order': 'i.speech_order',
        'created_at': 'i.created_at',
        
        // Speaker Information
        'speaker_name': 'i.speaker_name',
        'mep_id': 'i.mep_id',
        'country': 'm.country',
        'political_group': 'COALESCE(i.political_group_std, i.political_group)',
        'political_group_raw': 'i.political_group_raw',
        'political_group_std': 'i.political_group_std',
        'political_group_kind': 'i.political_group_kind',
        'political_group_reason': 'i.political_group_reason',
        
        // Content
        'title': 'i.title',
        'speech_content': 'i.speech_content',
        'language': 'i.language',
        'sitting_content': 's.content',
        'sitting_label': 's.label',
        'sitting_type': 's.type',
        'doc_identifier': 's.docIdentifier',
        'notation_id': 's.notationId',
        
        // Topic Classification
        'topic': 'i.topic',
        'macro_topic': 'i.macro_topic',
        'specific_focus': 'i.macro_specific_focus',
        'macro_confidence': 'i.macro_confidence',
        'macro_classified_by': 'i.macro_classified_by',
        'macro_classified_at': 'i.macro_classified_at',
        'macro_classification_cost': 'i.macro_classification_cost'
      };
      
      // Build SELECT clause
      const selectFields = requestedFields
        .filter(f => fieldMapping[f])
        .map(f => `${fieldMapping[f]} as ${f}`);
      
      console.log(`📊 [EXPORT] Mapped to ${selectFields.length} SQL fields`);
      
      if (selectFields.length === 0) {
        console.error('❌ [EXPORT] No valid fields selected');
        return res.status(400).json({ error: 'No valid fields selected' });
      }
      
      const baseSql = `
        SELECT ${selectFields.join(', ')}
        FROM individual_speeches i
        LEFT JOIN sittings s ON i.sitting_id = s.id
        LEFT JOIN meps m ON i.mep_id = m.id
        ${whereClause}
        ORDER BY s.activity_date DESC, i.speech_order ASC
      `;
      
      console.log(`📊 [EXPORT] Executing batch streaming export...`);
      console.log(`📊 [EXPORT] SQL query length: ${baseSql.length} chars`);
      
      // Helper function to escape CSV values
      const escapeCSV = (value) => {
        if (value === null || value === undefined) return '';
        const str = String(value);
        // If contains comma, quote, or newline, wrap in quotes and escape quotes
        if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
          return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
      };
      
      // Set response headers for streaming CSV
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="eu_speeches_export.csv"');
      
      // Write CSV header with BOM for Excel compatibility
      const headers = requestedFields.join(',');
      res.write('\ufeff' + headers + '\n');
      
      console.log('📊 [EXPORT] Starting batch streaming export...');
      const queryStartTime = Date.now();
      
      const BATCH_SIZE = 5000; // Process 5000 rows at a time for better performance
      let offset = 0;
      let totalRowCount = 0;
      let totalBytesWritten = 0;
      let lastLogTime = Date.now();
      let hasError = false;
      
      // Recursive function to process batches with backpressure handling
      function processBatch() {
        if (hasError) return;
        
        const batchSql = baseSql + ` LIMIT ${BATCH_SIZE} OFFSET ${offset}`;
        
        db.all(batchSql, params, (err, rows) => {
          if (err) {
            console.error('❌ [EXPORT] Error fetching batch:', err);
            hasError = true;
            if (!res.headersSent) {
              return res.status(500).json({ error: err.message });
            }
            return res.end();
          }
          
          // If no rows, we're done
          if (rows.length === 0) {
            const queryTime = Date.now() - queryStartTime;
            const totalTime = Date.now() - requestStartTime;
            const sizeMB = (totalBytesWritten / 1024 / 1024).toFixed(2);
            const avgRate = totalRowCount / (queryTime / 1000);
            
            // Finalize the response
            res.end();
            
            console.log(`✅ [EXPORT] Stream completed successfully`);
            console.log(`📊 [EXPORT] Total rows exported: ${totalRowCount}`);
            console.log(`📦 [EXPORT] Total size: ${totalBytesWritten} bytes (${sizeMB} MB)`);
            console.log(`⏱️ [EXPORT] Query + streaming time: ${queryTime}ms (${(queryTime/1000).toFixed(2)}s)`);
            console.log(`📈 [EXPORT] Average rate: ${avgRate.toFixed(1)} rows/sec`);
            console.log('📊 [EXPORT] ========================================');
            return;
          }
          
          // Convert batch to CSV - optimized for speed
          let batchCSV = '';
          for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            for (let j = 0; j < requestedFields.length; j++) {
              if (j > 0) batchCSV += ',';
              batchCSV += escapeCSV(row[requestedFields[j]]);
            }
            batchCSV += '\n';
          }
          
          // Update counters
          totalRowCount += rows.length;
          totalBytesWritten += batchCSV.length;
          offset += rows.length;
          
          // Log progress every 5000 rows
          const now = Date.now();
          if (totalRowCount % 5000 === 0 || now - lastLogTime > 5000) {
            const elapsed = (now - queryStartTime) / 1000;
            const rate = totalRowCount / elapsed;
            const sizeMB = (totalBytesWritten / 1024 / 1024).toFixed(2);
            console.log(`📊 [EXPORT] Progress: ${totalRowCount} rows, ${sizeMB} MB, ${rate.toFixed(1)} rows/sec`);
            lastLogTime = now;
          }
          
          // Write batch with backpressure handling
          const canContinue = res.write(batchCSV);
          
          if (!canContinue) {
            // Buffer is full, wait for drain event
            res.once('drain', () => {
              // Continue processing after drain
              setImmediate(processBatch);
            });
          } else {
            // Continue immediately
            setImmediate(processBatch);
          }
        });
      }
      
      // Start processing batches
      processBatch();
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
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`Server is running on http://localhost:${PORT}`);
      console.log('🚀 [CACHE] Initializing analytics cache in background...');
      
      // Start cache warming in background (don't block server startup)
      setTimeout(() => {
        warmAnalyticsCache().catch(err => {
          console.error('❌ [CACHE] Failed to warm cache:', err);
        });
      }, 1000);
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

// New endpoint: fetch speech content from database (preferred) or fallback to HTML
app.get('/api/speech-html-content', async (req, res) => {
  const { date, speechId } = req.query;
  if (!date) return res.status(400).json({ error: 'Missing date' });
  
  console.log(`🔍 [SPEECH] Fetching content for date: ${date}, speechId: ${speechId || 'all'}`);
  
  try {
    // First, try to get content from database
    if (speechId) {
      // Get specific speech content
      db.get('SELECT content FROM sittings WHERE id = ? AND content != "" AND LENGTH(content) > 100', [speechId], (err, row) => {
        if (err) {
          console.error('❌ [SPEECH] DB error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        if (row && row.content) {
          console.log(`✅ [SPEECH] Found content in database for speech ${speechId} (${row.content.length} chars)`);
          return res.json({ content: row.content });
        }
        
        // Fallback to HTML if no database content
        console.log(`⚠️ [SPEECH] No database content for ${speechId}, falling back to HTML`);
        fetchFromHTML(date, res, speechId);
      });
    } else {
      // Get all speeches for this date and combine their content
      db.all('SELECT content FROM sittings WHERE date = ? AND content != "" AND LENGTH(content) > 100', [date], (err, rows) => {
        if (err) {
          console.error('❌ [SPEECH] DB error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        if (rows && rows.length > 0) {
          const combinedContent = rows.map(row => row.content).join('\n\n---\n\n');
          console.log(`✅ [SPEECH] Found ${rows.length} speeches in database for date ${date} (${combinedContent.length} chars total)`);
          return res.json({ content: combinedContent });
        }
        
        // Fallback to HTML if no database content
        console.log(`⚠️ [SPEECH] No database content for date ${date}, falling back to HTML`);
        fetchFromHTML(date, res);
      });
    }
  } catch (err) {
    console.error('❌ [SPEECH] Error:', err);
    res.status(500).json({ error: 'Failed to fetch content' });
  }
});

// Helper function to fetch from HTML (fallback) - improved with multiple fallback methods
async function fetchFromHTML(date, res, speechId = null) {
  try {
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
      },
      timeout: 8000
    });
    const html = response.data;
    const $ = require('cheerio').load(html);
    
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
        console.log(`🔄 [SPEECH] Trying TOC page for ${date}...`);
        const tocUrl = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}-TOC_EN.html`;
        const tocResponse = await axios.get(tocUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
          },
          timeout: 8000
        });
        const tocHtml = tocResponse.data;
        const $toc = require('cheerio').load(tocHtml);
        const items = $toc('a[href*="ITM-"]').toArray();
        if (items.length > 0) {
          content = `TOC Agenda Items:\n` + items.map(a => $toc(a).text().trim()).join('\n');
          console.log(`📄 [SPEECH] Found ${items.length} TOC items for ${date}`);
        }
      } catch (tocErr) {
        console.log(`⚠️ [SPEECH] TOC fetch also failed for ${date}: ${tocErr.message}`);
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
          console.log(`🔍 [SPEECH] Parsed ${individualSpeeches.length} individual speeches from HTML content`);
          await storeIndividualSpeeches(individualSpeeches);
          console.log(`✅ [SPEECH] Stored ${individualSpeeches.length} individual speeches in database`);
        }
      } catch (parseError) {
        console.error(`❌ [SPEECH] Error parsing individual speeches from HTML:`, parseError.message);
      }
    }
    
    res.json({ content });
  } catch (err) {
    console.error('❌ [SPEECH] HTML fetch failed:', err.toString());
    res.status(500).json({ error: 'Failed to fetch or parse HTML content' });
  }
}

app.get('/api/sittings', (req, res) => {
  db.get('SELECT data, last_updated FROM sittings_cache ORDER BY id DESC LIMIT 1', (err, row) => {
    if (err || !row) {
      return res.status(404).json({ error: 'No cached sittings found.' });
    }
    res.json({ data: JSON.parse(row.data), last_updated: row.last_updated });
  });
});

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
  
  console.log('🎤 Starting comprehensive speech fetch from 2023-01-01...');
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
          console.log(`🔄 Retry ${attempt}/${maxRetries} for offset=${offset}`);
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
            console.log(`📊 Estimated total speeches: ${estimatedTotal}`);
            
            // Show date range from first batch
            const dates = speeches.map(s => s.activity_date).filter(Boolean).sort();
            if (dates.length > 0) {
              console.log(`📅 Date range: ${dates[0]} to ${dates[dates.length - 1]}`);
            }
            console.log('🔄 Starting progress tracking...\n');
          }
          firstBatch = false;
        }
        
        // Show progress bar if we have an estimate
        if (estimatedTotal > 0) {
          const progressBar = createProgressBar(totalFetched, estimatedTotal);
          const rate = totalFetched / ((Date.now() - startTime) / 1000);
          process.stdout.write(`\r🎤 ${progressBar} | Rate: ${rate.toFixed(1)}/sec`);
        } else {
          console.log(`📦 Fetched ${speeches.length} speeches (total: ${totalFetched})`);
        }
        
        if (speeches.length < limit) {
          if (estimatedTotal > 0) {
            console.log('\n✅ Reached end of speeches data');
          } else {
            console.log('✅ Reached end of speeches data');
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
        console.log(`\n⚠️ Attempt ${attempt}/${maxRetries} failed for offset ${offset}: ${error.message}`);
        
        if (error.response?.status === 404) {
          console.log('✅ Reached end of speeches (404)');
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
      console.error(`\n❌ Failed to fetch batch at offset ${offset} after ${maxRetries} attempts:`, batchError.message);
      retryCount++;
      
      if (retryCount >= 5) {
        console.error('❌ Too many consecutive failures, stopping fetch');
        throw new Error(`Failed to fetch speeches after ${retryCount} consecutive batch failures`);
      }
      
      // Skip this batch and continue
      console.log(`⏭️ Skipping batch at offset ${offset} and continuing...`);
      offset += limit;
      // Removed delay for faster fetching
    }
  }
  
  const finalTime = (Date.now() - startTime) / 1000;
  const finalRate = totalFetched / finalTime;
  console.log(`\n🎉 Total speeches fetched: ${all.length} in ${finalTime.toFixed(1)}s (${finalRate.toFixed(1)}/sec)`);
  return all;
}

// Cache all speeches to database
async function cacheAllSpeeches() {
  return new Promise((resolve, reject) => {
    db.serialize(async () => {
      try {
        console.log('🎤 [CACHE] Starting comprehensive speech caching...');
        const allSpeeches = await fetchAllSpeechesFromRemote();
        console.log(`🎤 [CACHE] Fetched ${allSpeeches.length} speeches from API`);
        
        // Clear existing speeches
        db.run('DELETE FROM sittings');
        console.log('🗑️ [CACHE] Cleared existing sittings from database');
        
        // Prepare statement for batch insert
        const stmt = db.prepare(`INSERT OR REPLACE INTO sittings 
          (id, type, label, personId, date, content, docIdentifier, notationId, 
           activity_type, activity_date, activity_start_date, last_updated)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
        
        let processed = 0;
        const cacheStartTime = Date.now();
        const totalSpeeches = allSpeeches.length;
        
        console.log(`💾 Starting to cache ${totalSpeeches} sittings to database...`);
        console.log(`🔍 Will fetch HTML content for speeches with dates...`);
        
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
            console.log(`🔍 Fetching: ${date}`);
            try {
              const htmlContent = await fetchSpeechContentFromHTML(date, speech.id);
              if (htmlContent && htmlContent.length > content.length) {
                content = htmlContent;
                console.log(`✅ ${date}: ${htmlContent.length} chars`);
              } else {
                console.log(`⚠️ ${date}: No content`);
              }
              // Removed delay for faster fetching
            } catch (htmlErr) {
              console.log(`❌ ${date}: ${htmlErr.message}`);
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
            console.error(`❌ [CACHE] Error inserting speech ${speech.id}:`, stmtErr.message);
            // Continue with next speech instead of crashing
          }
          
          processed++;
          if (processed % 50 === 0 || processed === totalSpeeches) {
            const elapsed = (Date.now() - cacheStartTime) / 1000;
            const rate = processed / elapsed;
            const progressBar = createProgressBar(processed, totalSpeeches, 40);
            const estimatedTime = (totalSpeeches - processed) / rate;
            process.stdout.write(`\r💾 ${progressBar} | Rate: ${rate.toFixed(1)}/sec | ETA: ${estimatedTime.toFixed(0)}s`);
          }
        }
        
        stmt.finalize();
        const totalTime = (Date.now() - cacheStartTime) / 1000;
        console.log(`\n✅ [CACHE] Processed all ${allSpeeches.length} speeches in ${totalTime.toFixed(1)} seconds`);
        
        // Update cache status
        const cacheTime = Date.now();
        db.run(`INSERT OR REPLACE INTO cache_status 
          (id, speeches_last_updated, total_speeches) 
          VALUES (1, ?, ?)`, [cacheTime, allSpeeches.length]);
        
        console.log(`🎉 [CACHE] Successfully cached ${allSpeeches.length} sittings to database at ${new Date(cacheTime).toLocaleString()}`);
        resolve(allSpeeches.length);
      } catch (err) {
        console.error('❌ [CACHE] Error caching speeches:', err);
        reject(err);
      }
    });
  });
}

// Efficient incremental refresh: only fetch truly new data
async function fetchNewSpeechesIncremental() {
  return new Promise((resolve, reject) => {
    db.serialize(async () => {
      try {
        console.log('🔄 [EFFICIENT FETCH] Starting efficient incremental fetch...');
        
        // Get the most recent sitting to determine what's new
        db.get(`
          SELECT activity_date, id, docIdentifier, notationId 
          FROM sittings 
          ORDER BY activity_date DESC, id DESC 
          LIMIT 1
        `, async (err, latestSitting) => {
          if (err) {
            console.error('❌ [EFFICIENT FETCH] Error getting latest sitting:', err);
            reject(err);
            return;
          }
          
          if (!latestSitting) {
            console.log('📊 [EFFICIENT FETCH] No existing sittings found, will fetch all data');
            // If no data exists, fetch everything
            const allSpeeches = await fetchAllSpeechesFromRemote();
            console.log(`🎤 [EFFICIENT FETCH] Fetched ${allSpeeches.length} speeches from API (full fetch)`);
            
            // Store all speeches (no duplicates to check)
            let stored = 0;
            for (const speech of allSpeeches) {
              try {
                await storeSpeechInDatabase(speech);
                stored++;
              } catch (storeErr) {
                console.error(`❌ [EFFICIENT FETCH] Error storing speech ${speech.id}:`, storeErr);
              }
            }
            
            console.log(`✅ [EFFICIENT FETCH] Stored ${stored} new speeches (full fetch)`);
            resolve(stored);
            return;
          }
          
          console.log(`📊 [EFFICIENT FETCH] Latest sitting: ${latestSitting.activity_date} (ID: ${latestSitting.id})`);
          
          // Fetch all speeches from API (since API doesn't support date filtering)
          const allSpeeches = await fetchAllSpeechesFromRemote();
          console.log(`🎤 [EFFICIENT FETCH] Fetched ${allSpeeches.length} speeches from API`);
          
          // Get existing speeches with all identifiers to avoid duplicates
          db.all('SELECT id, docIdentifier, notationId FROM sittings', async (err, existingRows) => {
            if (err) {
              console.error('❌ [EFFICIENT FETCH] Error getting existing speeches:', err);
              reject(err);
              return;
            }
            
            // Create multiple lookup sets for robust duplicate detection
            const existingIds = new Set(existingRows.map(row => row.id));
            const existingDocIds = new Set(existingRows.map(row => row.docIdentifier).filter(Boolean));
            const existingNotationIds = new Set(existingRows.map(row => row.notationId).filter(Boolean));
            
            console.log(`📊 [EFFICIENT FETCH] Existing: ${existingIds.size} IDs, ${existingDocIds.size} docIds, ${existingNotationIds.size} notationIds`);
            
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
              console.log('✅ [EFFICIENT FETCH] No new speeches found, database is up to date');
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
            console.log(`💾 Starting to cache ${newSpeeches.length} new speeches...`);
            
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
                console.log(`🔍 Fetching: ${date}`);
                try {
                  const htmlContent = await fetchSpeechContentFromHTML(date, speech.id);
                  if (htmlContent && htmlContent.length > content.length) {
                    content = htmlContent;
                    console.log(`✅ ${date}: ${htmlContent.length} chars`);
                  } else {
                    console.log(`⚠️ ${date}: No content`);
                  }
                } catch (htmlErr) {
                  console.log(`❌ ${date}: ${htmlErr.message}`);
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
                console.error(`❌ [INCREMENTAL] Error inserting speech ${speech.id}:`, stmtErr.message);
                // Continue with next speech instead of crashing
              }
              
              processed++;
              if (processed % 50 === 0 || processed === newSpeeches.length) {
                const elapsed = (Date.now() - incrementalStartTime) / 1000;
                const rate = processed / elapsed;
                const progressBar = createProgressBar(processed, newSpeeches.length, 30);
                process.stdout.write(`\r💾 ${progressBar} | Rate: ${rate.toFixed(1)}/sec`);
              }
            }
            
            stmt.finalize();
            const incrementalTime = (Date.now() - incrementalStartTime) / 1000;
            console.log(`\n✅ [INCREMENTAL] Cached ${newSpeeches.length} new speeches in ${incrementalTime.toFixed(1)} seconds`);
            
            // Update cache status with new total count
            db.get('SELECT COUNT(*) as total FROM sittings', (err, countRow) => {
              if (err) {
                console.error('❌ [INCREMENTAL] Error getting total count:', err);
                reject(err);
                return;
              }
              
              const totalCount = countRow.total;
              const now = Date.now();
              
              db.run(`INSERT OR REPLACE INTO cache_status 
                (id, speeches_last_updated, total_speeches) 
                VALUES (1, ?, ?)`, [now, totalCount]);
              
              console.log(`🎉 [INCREMENTAL] Successfully added ${newSpeeches.length} new speeches. Total: ${totalCount}`);
              resolve(newSpeeches.length);
            });
          });
        });
      } catch (err) {
        console.error('❌ [INCREMENTAL] Error in incremental refresh:', err);
        reject(err);
      }
    });
  });
}

// Add content to existing speeches that don't have content
async function addContentToExistingSpeeches() {
  return new Promise((resolve, reject) => {
    try {
      // Get speeches without content, but only for dates up to today (avoid future dates)
      const today = new Date().toISOString().split('T')[0];
      db.all('SELECT id, date FROM sittings WHERE (content = "" OR content IS NULL OR LENGTH(content) < 100) AND date != "" AND date <= ?', [today], async (err, speeches) => {
        if (err) {
          console.error('❌ [CONTENT] Error getting speeches without content:', err);
          reject(err);
          return;
        }
        
        if (speeches.length === 0) {
          console.log('✅ [CONTENT] All speeches already have content');
          resolve(0);
          return;
        }
        
        console.log(`🔍 [CONTENT] Found ${speeches.length} speeches without content, fetching content...`);
        
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
                    console.error(`❌ [CONTENT] Error updating speech ${speech.id}:`, err);
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
              process.stdout.write(`\r🔍 [CONTENT] ${progressBar} | Rate: ${rate.toFixed(1)}/sec | Success: ${successCount}`);
            }
            
            // Add small delay to be respectful to the server
            await new Promise(resolve => setTimeout(resolve, 500));
            
          } catch (error) {
            console.error(`❌ [CONTENT] Error processing speech ${speech.id}:`, error.message);
            processed++;
          }
        }
        
        const totalTime = (Date.now() - startTime) / 1000;
        console.log(`\n✅ [CONTENT] Completed content fetching: ${successCount}/${speeches.length} speeches updated in ${totalTime.toFixed(1)}s`);
        console.log(`📊 [CONTENT] Success rate: ${((successCount/speeches.length)*100).toFixed(1)}%`);
        
        // If we successfully fetched content for any speeches, parse them automatically
        if (successCount > 0) {
          console.log(`\n🎤 [AUTO-PARSE] Automatically parsing ${successCount} newly fetched sittings...`);
          try {
            await parseAllSpeechesWithContent();
            console.log('✅ [AUTO-PARSE] Successfully parsed all newly fetched sittings');
          } catch (parseErr) {
            console.error('❌ [AUTO-PARSE] Error parsing newly fetched sittings:', parseErr);
            // Don't fail the whole operation if parsing fails
          }
        }
        
        resolve(successCount);
      });
    } catch (err) {
      console.error('❌ [CONTENT] Error in addContentToExistingSpeeches:', err);
      reject(err);
    }
  });
}

// Parse individual speeches from raw sitting content
function parseIndividualSpeeches(rawContent, sittingId) {
  const speeches = [];
  const lines = rawContent.split('\n');
  let currentSpeech = null;
  let speechOrder = 0;
  
  // Common titles that don't have political groups
  const titles = ['President', 'Vice-President', 'Executive Vice-President', 'Commissioner', 'Minister', 'Chair', 'Chairman', 'Chairwoman'];
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Skip empty lines
    if (!line) continue;
    
    // Pattern 1: Name (Political Group). – [SPEECH CONTENT]
    const pattern1 = /^(.+?)\s*\(([^)]+)\)\.\s*–\s*(.*)$/;
    const match1 = line.match(pattern1);
    
    // Pattern 2: Name, on behalf of Group. – [SPEECH CONTENT]
    const pattern2 = /^(.+?),\s*on behalf of\s*(.+?)\.\s*–\s*(.*)$/;
    const match2 = line.match(pattern2);
    
    // Pattern 3: Title. – [SPEECH CONTENT] (for President, Chairman, etc.)
    // This pattern matches simple titles that end with a period and dash
    const pattern3 = /^([A-Z][A-Za-z\s]+)\.\s+–\s*(.*)$/;
    const match3 = line.match(pattern3);
    
    if (match1) {
      // Save previous speech if exists
      if (currentSpeech) {
        speeches.push(currentSpeech);
      }
      
      // Start new speech
      speechOrder++;
      currentSpeech = {
        sitting_id: sittingId,
        speaker_name: match1[1].trim(),
        political_group: match1[2].trim(),
        title: null,
        speech_content: match1[3].trim(),
        speech_order: speechOrder
      };
    } else if (match2) {
      // Save previous speech if exists
      if (currentSpeech) {
        speeches.push(currentSpeech);
      }
      
      // Start new speech
      speechOrder++;
      currentSpeech = {
        sitting_id: sittingId,
        speaker_name: match2[1].trim(),
        political_group: match2[2].trim(),
        title: null,
        speech_content: match2[3].trim(),
        speech_order: speechOrder
      };
    } else if (match3) {
      // Save previous speech if exists
      if (currentSpeech) {
        speeches.push(currentSpeech);
      }
      
      // Start new speech with title
      speechOrder++;
      currentSpeech = {
        sitting_id: sittingId,
        speaker_name: null,
        political_group: null,
        title: match3[1].trim(),
        speech_content: match3[2].trim(),
        speech_order: speechOrder
      };
    } else if (currentSpeech) {
      // Continue current speech content
      currentSpeech.speech_content += ' ' + line;
    }
  }
  
  // Don't forget the last speech
  if (currentSpeech) {
    speeches.push(currentSpeech);
  }
  
  return speeches;
}

// Store individual speeches in database (with duplicate prevention)
async function storeIndividualSpeeches(speeches) {
  return new Promise((resolve, reject) => {
    if (speeches.length === 0) {
      resolve(0);
      return;
    }
    
    const sittingId = speeches[0].sitting_id;
    
    db.serialize(() => {
      // Check if speeches already exist for this sitting
      db.get('SELECT COUNT(*) as count FROM individual_speeches WHERE sitting_id = ?', [sittingId], (err, row) => {
        if (err) {
          console.error('❌ [STORE] Error checking existing speeches:', err);
          reject(err);
          return;
        }
        
        if (row.count > 0) {
          console.log(`⚠️ [STORE] Sitting ${sittingId} already has ${row.count} individual speeches, skipping to prevent duplicates`);
          resolve(0);
          return;
        }
        
        console.log(`🆕 [STORE] No existing speeches found for sitting ${sittingId}, proceeding with insert`);
      
        // Insert new speeches
      const stmt = db.prepare(`INSERT INTO individual_speeches 
        (sitting_id, speaker_name, political_group, title, speech_content, speech_order) 
        VALUES (?, ?, ?, ?, ?, ?)`);
      
      let processed = 0;
      let errors = 0;
      
      for (const speech of speeches) {
        stmt.run(
          speech.sitting_id,
          speech.speaker_name,
          speech.political_group,
          speech.title,
          speech.speech_content,
          speech.speech_order,
          (err) => {
            if (err) {
              console.error(`❌ [PARSE] Error inserting speech ${processed + 1}:`, err);
              errors++;
            }
            processed++;
            
            if (processed === speeches.length) {
              stmt.finalize();
              if (errors > 0) {
                console.log(`⚠️ [PARSE] Stored ${processed - errors}/${processed} individual speeches (${errors} errors)`);
              } else {
                console.log(`✅ [PARSE] Stored ${processed} individual speeches for sitting ${speeches[0]?.sitting_id}`);
              }
              resolve(processed - errors);
            }
          }
        );
      }
    });
  });
  });
}

// Legacy function for backward compatibility
async function fetchAllSittingsFromRemote() {
  return await fetchAllSpeechesFromRemote();
}

// GET /api/cache-status: get current cache status
app.get('/api/cache-status', (req, res) => {
  console.log('📊 [CACHE] Fetching cache status...');
  db.get('SELECT * FROM cache_status WHERE id = 1', (err, row) => {
    if (err) {
      console.error('❌ [CACHE] DB error getting cache status:', err);
      return res.status(500).json({ error: err.toString() });
    }
    
    if (!row) {
      console.log('⚠️ [CACHE] No cache status found, returning defaults');
      return res.json({
        meps_last_updated: 0,
        speeches_last_updated: 0,
        total_speeches: 0
      });
    }
    
    console.log(`✅ [CACHE] Cache status - MEPs: ${row.meps_last_updated ? new Date(row.meps_last_updated).toLocaleString() : 'Never'}, Speeches: ${row.total_speeches} (${row.speeches_last_updated ? new Date(row.speeches_last_updated).toLocaleString() : 'Never'})`);
    
    res.json({
      meps_last_updated: row.meps_last_updated,
      speeches_last_updated: row.speeches_last_updated,
      total_speeches: row.total_speeches
    });
  });
});

// POST /api/refresh-all: refresh all cached data (incremental for speeches)
app.post('/api/refresh-all', async (req, res) => {
  try {
    console.log('🔄 [REFRESH] Starting data refresh...');
    
    // Refresh MEPs (full refresh)
    console.log('👥 [REFRESH] Refreshing MEPs...');
    const meps = await fetchAllMeps();
    console.log(`👥 [REFRESH] Fetched ${meps.length} MEPs from API`);
    
    db.run('DELETE FROM meps');
    console.log('🗑️ [REFRESH] Cleared existing MEPs from database');
    
    const mepStmt = db.prepare(`INSERT OR REPLACE INTO meps 
      (id, label, givenName, familyName, sortLabel, country, politicalGroup, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const m of meps) {
      const pid = parseInt(m.identifier, 10);
      mepStmt.run(pid, m.label, m.givenName, m.familyName, m.sortLabel,
        m['api:country-of-representation'], m['api:political-group'], Date.now());
    }
    mepStmt.finalize();
    console.log(`✅ [REFRESH] Cached ${meps.length} MEPs to database`);
    
    // Refresh speeches (incremental - only new ones)
    console.log('🎤 [REFRESH] Checking for new speeches...');
    const newSpeechCount = await fetchNewSpeechesIncremental();
    
    // Fetch content for any new speeches that don't have content yet
    console.log('🔍 [REFRESH] Fetching content for new speeches...');
    const contentCount = await addContentToExistingSpeeches();
    
    // Update cache status
    const now = Date.now();
    db.get('SELECT total_speeches FROM cache_status WHERE id = 1', (err, row) => {
      const totalSpeeches = row ? row.total_speeches : 0;
      
      db.run(`INSERT OR REPLACE INTO cache_status 
        (id, meps_last_updated, speeches_last_updated, total_speeches) 
        VALUES (1, ?, ?, ?)`, [now, now, totalSpeeches]);
      
      console.log(`📊 [REFRESH] Updated cache status - MEPs: ${new Date(now).toLocaleString()}, Speeches: ${totalSpeeches} (${newSpeechCount} new, ${contentCount} content fetched)`);
      
      console.log('🎉 [REFRESH] Data refresh completed successfully');
      res.json({ 
        success: true, 
        meps_count: meps.length,
        speeches_count: totalSpeeches,
        new_speeches_count: newSpeechCount,
        content_fetched_count: contentCount,
        message: `Data refreshed successfully. Added ${newSpeechCount} new speeches, fetched content for ${contentCount} speeches.`
      });
    });
  } catch (err) {
    console.error('❌ [REFRESH] Error refreshing data:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// Function to check for and remove duplicates
async function checkAndRemoveDuplicates() {
  return new Promise((resolve) => {
    let totalRemoved = 0;
    
    db.serialize(() => {
      // Check for sitting duplicates
      db.all(`
        SELECT activity_date, COUNT(*) as count 
        FROM sittings 
        WHERE LENGTH(content) > 100
        GROUP BY activity_date 
        HAVING COUNT(*) > 1
      `, (err, sittingDuplicates) => {
        if (err) {
          console.error('❌ [DUPLICATE CHECK] Error checking sitting duplicates:', err);
        } else {
          console.log(`🔍 [DUPLICATE CHECK] Found ${sittingDuplicates.length} sitting duplicates`);
          sittingDuplicates.forEach(dup => {
            console.log(`   - ${dup.activity_date}: ${dup.count} copies`);
          });
        }
        
        // Check for speech duplicates
        db.all(`
          SELECT sitting_id, speaker_name, speech_content, COUNT(*) as count
          FROM individual_speeches 
          GROUP BY sitting_id, speaker_name, speech_content
          HAVING COUNT(*) > 1
        `, (err, speechDuplicates) => {
          if (err) {
            console.error('❌ [DUPLICATE CHECK] Error checking speech duplicates:', err);
          } else {
            console.log(`🔍 [DUPLICATE CHECK] Found ${speechDuplicates.length} speech duplicates`);
            
            // Remove speech duplicates
            speechDuplicates.forEach(dup => {
              db.run(`
                DELETE FROM individual_speeches 
                WHERE sitting_id = ? AND speaker_name = ? AND speech_content = ?
                AND id NOT IN (
                  SELECT MIN(id) FROM individual_speeches 
                  WHERE sitting_id = ? AND speaker_name = ? AND speech_content = ?
                )
              `, [dup.sitting_id, dup.speaker_name, dup.speech_content, dup.sitting_id, dup.speaker_name, dup.speech_content], function(err) {
                if (!err && this.changes > 0) {
                  totalRemoved += this.changes;
                  console.log(`   🗑️ Removed ${this.changes} duplicate speeches for ${dup.speaker_name}`);
                }
              });
            });
          }
          
          // Get final counts
          db.get('SELECT COUNT(*) as count FROM sittings WHERE LENGTH(content) > 100', (err, sittingRow) => {
            const sittingCount = sittingRow ? sittingRow.count : 0;
            
            db.get('SELECT COUNT(*) as count FROM individual_speeches', (err, speechRow) => {
              const speechCount = speechRow ? speechRow.count : 0;
              
              console.log(`📊 [DUPLICATE CHECK] Final counts - Sittings: ${sittingCount}, Individual speeches: ${speechCount}`);
              console.log(`🧹 [DUPLICATE CHECK] Total removed: ${totalRemoved} duplicates`);
              
              resolve({
                totalRemoved: totalRemoved,
                sittingCount: sittingCount,
                speechCount: speechCount
              });
            });
          });
        });
      });
    });
  });
}

// Function to link individual speeches to MEPs
async function linkSpeechesToMeps() {
  return new Promise((resolve) => {
    db.all(`
      SELECT DISTINCT speaker_name 
      FROM individual_speeches 
      WHERE speaker_name IS NOT NULL 
      AND mep_id IS NULL
    `, async (err, speakers) => {
      if (err || !speakers) {
        resolve(0);
        return;
      }

      let linkedCount = 0;
      for (const speaker of speakers) {
        const mep = await new Promise((resolve) => {
          db.get(`
            SELECT identifier FROM meps 
            WHERE label LIKE ? OR label LIKE ?
          `, [`%${speaker.speaker_name}%`, `%${speaker.speaker_name.split(' ').reverse().join(' ')}%`], (err, row) => {
            resolve(row);
          });
        });

        if (mep) {
          await new Promise((resolve) => {
            db.run(`
              UPDATE individual_speeches 
              SET mep_id = ? 
              WHERE speaker_name = ? AND mep_id IS NULL
            `, [mep.identifier, speaker.speaker_name], () => {
              linkedCount++;
              resolve();
            });
          });
        }
      }
      resolve(linkedCount);
    });
  });
}

// POST /api/refresh-speeches: refresh only speeches (incremental)
app.post('/api/refresh-speeches', async (req, res) => {
  try {
    console.log('🎤 [REFRESH] Starting perfect incremental refresh...');
    
    // Step 1: Check current database state
    const currentStats = await new Promise((resolve) => {
      db.get(`
        SELECT 
          COUNT(*) as total_sittings,
          COUNT(CASE WHEN LENGTH(content) > 100 THEN 1 END) as sittings_with_content,
          MAX(activity_date) as latest_date
        FROM sittings
      `, (err, row) => {
        resolve(row || { total_sittings: 0, sittings_with_content: 0, latest_date: null });
      });
    });
    
    console.log(`📊 [REFRESH] Current state: ${currentStats.sittings_with_content} sittings with content, latest: ${currentStats.latest_date}`);
    
    // Get all existing sitting IDs to avoid duplicates
    const existingIds = await new Promise((resolve) => {
      db.all('SELECT id FROM sittings', (err, rows) => {
        resolve(rows ? rows.map(r => r.id) : []);
      });
    });
    console.log(`📊 [REFRESH] Existing sitting IDs: ${existingIds.length}`);

    // Step 2: Fetch ALL speeches from API with pagination
    let allSpeeches = [];
    let offset = 0;
    const limit = 1000;
    let hasMore = true;
    let batchCount = 0;
    
    console.log('📡 [REFRESH] Starting API fetch with pagination...');
    
    while (hasMore) {
      batchCount++;
      console.log(`📡 [REFRESH] Fetching batch ${batchCount}: offset=${offset}, limit=${limit}`);
      
      try {
        const response = await axios.get('https://data.europarl.europa.eu/api/v2/speeches', {
          params: {
            format: 'application/ld+json',
            limit: limit,
            offset: offset,
            'search-language': 'EN'
          },
          headers: { 
            Accept: 'application/ld+json',
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)' 
          },
          timeout: 60000
        });
        
        const batchSpeeches = response.data.data || [];
        allSpeeches = allSpeeches.concat(batchSpeeches);
        
        console.log(`   ✅ [REFRESH] Batch ${batchCount}: ${batchSpeeches.length} speeches (total: ${allSpeeches.length})`);
        
        // Check if we got fewer speeches than requested (end of data)
        if (batchSpeeches.length < limit) {
          hasMore = false;
          console.log('   ✅ [REFRESH] Reached end of API data');
        } else {
          offset += limit;
          // Small delay to be respectful to the API
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      } catch (error) {
        console.error(`   ❌ [REFRESH] Error in batch ${batchCount}:`, error.message);
        hasMore = false;
      }
    }
    
    console.log(`📊 [REFRESH] API fetch completed: ${allSpeeches.length} total speeches`);

    // Step 3: Group speeches by date and filter for new ones
    const dateMap = new Map();
    allSpeeches.forEach(speech => {
      const date = speech.activity_date || speech.activity_start_date;
      if (!date) return;
      
      if (!dateMap.has(date)) {
        dateMap.set(date, []);
      }
      dateMap.get(date).push(speech);
    });
    
    console.log(`📅 [REFRESH] Found ${dateMap.size} unique dates in API data`);
    
    // Filter for dates we don't have yet
    const newDates = [];
    for (const [date, speeches] of dateMap) {
      // Check if we have any sitting for this date with content
      const hasContent = await new Promise((resolve) => {
        db.get(`
          SELECT COUNT(*) as count 
          FROM sittings 
          WHERE activity_date = ? AND LENGTH(content) > 100
        `, [date], (err, row) => {
          resolve(row ? row.count > 0 : false);
        });
      });
      
      if (!hasContent) {
        newDates.push({ date, speeches });
      }
    }
    
    console.log(`🆕 [REFRESH] Found ${newDates.length} dates with new content to fetch`);
    
    let fetchedCount = 0;
    let failedCount = 0;
    let parsedCount = 0;
    let totalSpeeches = 0;

    if (newDates.length > 0) {
      // Step 4: Fetch content for new dates
      console.log('📥 [REFRESH] Fetching content for new sittings...');
      
      for (const { date, speeches } of newDates) {
        console.log(`🔍 [REFRESH] Fetching content for ${date}...`);
        
        try {
          // Use the first speech ID as the sitting ID
          const sittingId = speeches[0].id;
          
          // Fetch content using the existing function
          const content = await fetchSpeechContentFromHTML(date, sittingId);
          
          if (content && content.length > 100) {
            // Store the sitting with proper API data structure
            await new Promise((resolve, reject) => {
              const stmt = db.prepare(`
                INSERT OR IGNORE INTO sittings
                (id, type, label, personId, activity_date, content, docIdentifier, notationId, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              `);
              stmt.run(
                sittingId,
                speeches[0].type || 'def/ep-activities/PLENARY_DEBATE_SPEECH', // Use real API type
                speeches[0].label || `Parliamentary Sitting - ${date}`, // Use real API label
                speeches[0].personId || null,
                date,
                content,
                speeches[0].docIdentifier || '',
                speeches[0].notationId || '',
                Date.now()
              );
              stmt.finalize((err) => {
                if (err) reject(err);
                else resolve();
              });
            });
            
            fetchedCount++;
            console.log(`   ✅ [REFRESH] ${date}: ${content.length} chars stored`);
          } else {
            failedCount++;
            console.log(`   ⚠️ [REFRESH] ${date}: No content or too short`);
          }
        } catch (error) {
          failedCount++;
          console.log(`   ❌ [REFRESH] ${date}: ${error.message}`);
        }
        
        // Small delay between requests
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      // Step 5: Parse new sittings into individual speeches
      console.log('🔍 [REFRESH] Parsing new sittings into individual speeches...');
      
      // Get all sittings that need parsing (have content but no individual speeches)
      const sittingsToParse = await new Promise((resolve) => {
        db.all(`
          SELECT s.id, s.content, s.activity_date
          FROM sittings s
          LEFT JOIN individual_speeches i ON s.id = i.sitting_id
          WHERE LENGTH(s.content) > 100
          AND i.sitting_id IS NULL
          GROUP BY s.id
        `, (err, rows) => {
          resolve(rows || []);
        });
      });
      
      console.log(`📊 [REFRESH] Found ${sittingsToParse.length} sittings to parse`);
      
      for (const sitting of sittingsToParse) {
        console.log(`🔍 [REFRESH] Parsing sitting ${sitting.id} (${sitting.activity_date})...`);
        
        try {
          const individualSpeeches = parseIndividualSpeeches(sitting.content, sitting.id);
          
          if (individualSpeeches.length > 0) {
            await storeIndividualSpeeches(individualSpeeches);
            parsedCount++;
            totalSpeeches += individualSpeeches.length;
            console.log(`   ✅ [REFRESH] Parsed ${individualSpeeches.length} individual speeches`);
          } else {
            console.log(`   ⚠️ [REFRESH] No individual speeches found`);
          }
        } catch (error) {
          console.log(`   ❌ [REFRESH] Error parsing: ${error.message}`);
        }
      }

      // Step 6: Link speeches to MEPs
      console.log('🔗 [REFRESH] Linking speeches to MEPs...');
      const linkedCount = await linkSpeechesToMeps();
      console.log(`✅ [REFRESH] Linked ${linkedCount} speeches to MEPs`);
    }

    // Step 7: Check for and remove duplicates (FINAL STEP)
    console.log('🔍 [REFRESH] Final step: Checking for and removing duplicates...');
    const duplicateResult = await checkAndRemoveDuplicates();
    console.log(`🧹 [REFRESH] Duplicate cleanup completed - Removed ${duplicateResult.totalRemoved} duplicates`);

    // Get final count after cleanup
    const finalCount = await new Promise((resolve) => {
      db.get('SELECT COUNT(*) as count FROM sittings WHERE LENGTH(content) > 100', (err, row) => {
        resolve(row ? row.count : 0);
      });
    });

    console.log(`🎉 [REFRESH] Perfect refresh completed - New dates: ${newDates.length}, Fetched: ${fetchedCount}, Parsed: ${parsedCount}, Total speeches: ${totalSpeeches}, Duplicates removed: ${duplicateResult.totalRemoved}, Final total: ${finalCount}`);
    res.json({
      success: true,
      sittings_count: finalCount,
      new_dates_count: newDates.length,
      content_fetched_count: fetchedCount,
      content_failed_count: failedCount,
      sittings_parsed_count: parsedCount,
      individual_speeches_count: totalSpeeches,
      duplicates_removed: duplicateResult.totalRemoved,
      message: `Perfect refresh completed successfully. Added ${newDates.length} new dates, fetched content for ${fetchedCount} sittings, parsed ${parsedCount} sittings into ${totalSpeeches} individual speeches, removed ${duplicateResult.totalRemoved} duplicates.`
    });
  } catch (err) {
    console.error('❌ [REFRESH] Error in perfect refresh:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/refresh-meps: refresh only MEPs
app.post('/api/refresh-meps', async (req, res) => {
  try {
    console.log('Starting MEP refresh...');
    const meps = await fetchAllMeps();
    
    db.run('DELETE FROM meps');
    const stmt = db.prepare(`INSERT OR REPLACE INTO meps 
      (id, label, givenName, familyName, sortLabel, country, politicalGroup, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const m of meps) {
      const pid = parseInt(m.identifier, 10);
      stmt.run(pid, m.label, m.givenName, m.familyName, m.sortLabel,
        m['api:country-of-representation'], m['api:political-group'], Date.now());
    }
    stmt.finalize();
    
    // Update cache status
    db.run(`INSERT OR REPLACE INTO cache_status 
      (id, meps_last_updated) 
      VALUES (1, ?)`, [Date.now()]);
    
    console.log('MEP refresh completed');
    res.json({ 
      success: true, 
      meps_count: meps.length,
      message: 'MEPs refreshed successfully'
    });
  } catch (err) {
    console.error('Error refreshing MEPs:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/refresh-perfect: run the perfect fetch & parse script end-to-end
app.post('/api/refresh-perfect', async (req, res) => {
  try {
    const { execFile } = require('child_process');
    const path = require('path');

    const scriptPath = path.resolve(__dirname, 'perfect-fetch-parse.js');    const startDate = req.body.startDate || '2023-01-01';
    
    console.log(`🔄 [REFRESH PERFECT] Executing ${scriptPath} with start date: ${startDate}...`);

    const child = execFile('node', [scriptPath, startDate], { cwd: __dirname, timeout: 0 });
    
    // Stream stdout in real-time
    child.stdout.on('data', (data) => {
      console.log(data.toString());
    });
    
    // Stream stderr in real-time
    child.stderr.on('data', (data) => {
      console.error(data.toString());
    });
    
    child.on('close', (code) => {
      if (code === 0) {
        console.log('✅ [REFRESH PERFECT] Completed successfully');
        res.json({ success: true, message: 'Perfect fetch & parse completed' });
      } else {
        console.error(`❌ [REFRESH PERFECT] Script exited with code ${code}`);
        res.status(500).json({ success: false, error: `Script exited with code ${code}` });
      }
    });
    
    child.on('error', (error) => {
      console.error('❌ [REFRESH PERFECT] Error:', error);
      res.status(500).json({ success: false, error: error.toString() });
    });
  } catch (err) {
    console.error('❌ [REFRESH PERFECT] Unexpected error:', err);
    res.status(500).json({ success: false, error: err.toString() });
  }
});

// POST /api/refresh-speeches-full: force full refresh of speeches (clears and rebuilds)
app.post('/api/refresh-speeches-full', async (req, res) => {
  try {
    console.log('🎤 [REFRESH] Starting full speech refresh (clearing existing data)...');
    const speechCount = await cacheAllSpeeches();
    
    console.log(`🎉 [REFRESH] Full speech refresh completed - Total: ${speechCount}`);
    res.json({ 
      success: true, 
      speeches_count: speechCount,
      message: `Full speech refresh completed. Total speeches: ${speechCount}`
    });
  } catch (err) {
    console.error('❌ [REFRESH] Error in full speech refresh:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// Legacy endpoint for backward compatibility
app.post('/api/refresh-sittings', async (req, res) => {
  try {
    const all = await fetchAllSittingsFromRemote();
    db.run('INSERT INTO sittings_cache (data, last_updated) VALUES (?, ?)', JSON.stringify(all), Date.now());
    res.json({ success: true, count: all.length });
  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/link-historic-meps: Create historic MEPs and link speeches
app.post('/api/link-historic-meps', async (req, res) => {
  try {
    console.log('🔗 [API] Starting historic MEP creation and speech linking...');
    const results = await createHistoricMepsAndLinkSpeeches();
    res.json({ 
      success: true, 
      ...results,
      message: `Created ${results.createdHistoricMeps} historic MEPs and linked ${results.linkedSpeeches} speeches`
    });
  } catch (err) {
    console.error('❌ [API] Error in historic MEP linking:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// Create historic MEPs from unique speaker names and link speeches
async function createHistoricMepsAndLinkSpeeches() {
  return new Promise((resolve, reject) => {
    console.log('🔗 [HISTORIC-MEPS] Starting historic MEP creation and speech linking...');
    
    // Get all unique speaker names that don't have linked MEPs
    db.all(`
      SELECT DISTINCT speaker_name, political_group, COUNT(*) as speech_count
      FROM individual_speeches 
      WHERE speaker_name IS NOT NULL 
      AND speaker_name != '' 
      AND mep_id IS NULL
      GROUP BY speaker_name, political_group
      ORDER BY speech_count DESC
    `, async (err, uniqueSpeakers) => {
      if (err) {
        console.error('❌ [HISTORIC-MEPS] Error getting unique speakers:', err);
        reject(err);
        return;
      }
      
      console.log(`📊 [HISTORIC-MEPS] Found ${uniqueSpeakers.length} unique speakers without linked MEPs`);
      
      let processedSpeakers = 0;
      let linkedSpeeches = 0;
      let createdHistoricMeps = 0;
      const startTime = Date.now();
      
      // Get the highest MEP ID to start generating IDs for historic MEPs
      db.get('SELECT MAX(id) as maxId FROM meps', async (err, row) => {
        if (err) {
          console.error('❌ [HISTORIC-MEPS] Error getting max MEP ID:', err);
          reject(err);
          return;
        }
        
        let nextMepId = (row.maxId || 1000000) + 1; // Start from high number to avoid conflicts
        
        for (const speaker of uniqueSpeakers) {
          try {
            // Try to find existing MEP by fuzzy name matching
            const nameParts = speaker.speaker_name.trim().split(/\s+/);
            const firstName = nameParts[0];
            const lastName = nameParts[nameParts.length - 1];
            
            // Try exact match first
            const exactMatch = await new Promise((matchResolve) => {
              db.get(`
                SELECT id FROM meps 
                WHERE LOWER(label) = LOWER(?) 
                OR (LOWER(givenName) = LOWER(?) AND LOWER(familyName) = LOWER(?))
              `, [speaker.speaker_name, firstName, lastName], (err, row) => {
                matchResolve(row);
              });
            });
            
            let mepId = exactMatch ? exactMatch.id : null;
            
            // If no exact match, try fuzzy matching
            if (!mepId && nameParts.length >= 2) {
              const fuzzyMatch = await new Promise((matchResolve) => {
                db.get(`
                  SELECT id FROM meps 
                  WHERE (LOWER(label) LIKE LOWER(?) OR LOWER(label) LIKE LOWER(?))
                  OR (LOWER(givenName) LIKE LOWER(?) AND LOWER(familyName) LIKE LOWER(?))
                `, [
                  `%${firstName}%${lastName}%`,
                  `%${lastName}%${firstName}%`,
                  `%${firstName}%`,
                  `%${lastName}%`
                ], (err, row) => {
                  matchResolve(row);
                });
              });
              
              mepId = fuzzyMatch ? fuzzyMatch.id : null;
            }
            
            // If still no match, create historic MEP
            if (!mepId) {
              mepId = nextMepId++;
              
              // Create historic MEP record
              await new Promise((createResolve, createReject) => {
                db.run(`
                  INSERT INTO meps 
                  (id, label, givenName, familyName, sortLabel, country, politicalGroup, is_current, source, last_updated)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `, [
                  mepId,
                  speaker.speaker_name,
                  firstName,
                  lastName,
                  speaker.speaker_name,
                  'Unknown', // We don't have country info from speeches
                  speaker.political_group || 'Unknown',
                  0, // Not current
                  'historic',
                  Date.now()
                ], (err) => {
                  if (err) {
                    console.error(`❌ [HISTORIC-MEPS] Error creating historic MEP for ${speaker.speaker_name}:`, err);
                    createReject(err);
                  } else {
                    createdHistoricMeps++;
                    createResolve();
                  }
                });
              });
            }
            
            // Link speeches to MEP
            await new Promise((linkResolve, linkReject) => {
              db.run(`
                UPDATE individual_speeches 
                SET mep_id = ? 
                WHERE speaker_name = ? AND mep_id IS NULL
              `, [mepId, speaker.speaker_name], function(err) {
                if (err) {
                  console.error(`❌ [HISTORIC-MEPS] Error linking speeches for ${speaker.speaker_name}:`, err);
                  linkReject(err);
                } else {
                  linkedSpeeches += this.changes;
                  linkResolve();
                }
              });
            });
            
            processedSpeakers++;
            
            // Show progress every 50 speakers
            if (processedSpeakers % 50 === 0 || processedSpeakers === uniqueSpeakers.length) {
              const elapsed = (Date.now() - startTime) / 1000;
              const rate = processedSpeakers / elapsed;
              console.log(`🔗 [HISTORIC-MEPS] Progress: ${processedSpeakers}/${uniqueSpeakers.length} speakers | Created: ${createdHistoricMeps} historic MEPs | Linked: ${linkedSpeeches} speeches | Rate: ${rate.toFixed(1)}/sec`);
            }
            
          } catch (error) {
            console.error(`❌ [HISTORIC-MEPS] Error processing speaker ${speaker.speaker_name}:`, error);
            processedSpeakers++;
          }
        }
        
        const totalTime = (Date.now() - startTime) / 1000;
        console.log(`\n✅ [HISTORIC-MEPS] Completed in ${totalTime.toFixed(1)}s:`);
        console.log(`   📊 Processed ${processedSpeakers} unique speakers`);
        console.log(`   👥 Created ${createdHistoricMeps} historic MEP records`);
        console.log(`   🔗 Linked ${linkedSpeeches} speeches to MEPs`);
        
        resolve({
          processedSpeakers,
          createdHistoricMeps,
          linkedSpeeches
        });
      });
    });
  });
}