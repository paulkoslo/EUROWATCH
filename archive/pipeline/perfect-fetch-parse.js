#!/usr/bin/env node

const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');
const cheerio = require('cheerio');

// Database connection
const { DB_PATH } = require('../../core/db');
const db = new sqlite3.Database(DB_PATH);

console.log('🚀 PERFECT FETCH & PARSE SCRIPT');
console.log('================================');
console.log('📅 Script started at:', new Date().toISOString());
console.log('📁 Working directory:', process.cwd());
console.log('📄 Script file:', __filename);

const scriptStartTime = Date.now();

// Parse command line arguments for start date
const startDate = process.argv[2] || '2023-01-01';
console.log('📅 Start date for fetching:', startDate);

// Function to determine session number based on date
function getSessionNumber(date) {
  // European Parliament terms don't start on Jan 1st - they start after elections
  if (date >= '2024-07-16') return 10; // 10th term: 2024-07-16 to present
  if (date >= '2019-07-02' && date < '2024-07-16') return 9;  // 9th term: 2019-07-02 to 2024-07-15
  if (date >= '2014-07-01' && date < '2019-07-02') return 8;  // 8th term: 2014-07-01 to 2019-07-01
  if (date >= '2009-07-14' && date < '2014-07-01') return 7;  // 7th term: 2009-07-14 to 2014-06-30
  if (date >= '2004-07-20' && date < '2009-07-14') return 6;  // 6th term: 2004-07-20 to 2009-07-13
  return 5; // 5th term: 1999-07-20 to 2004-07-19 (fallback)
}

// Function to format date for URL (YYYY-MM-DD -> YYYY-MM-DD with hyphens)
function formatDateForUrl(date) {
  return date; // Keep original format with hyphens
}

async function perfectFetchAndParse() {
  try {
    console.log('🎯 ENTERING perfectFetchAndParse function...');
    console.log('\n📊 STEP 1: CHECKING CURRENT DATABASE STATE');
    console.log('===========================================');
    
    // Get current state
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
    
    console.log(`📊 Current database state:`);
    console.log(`   - Total sittings: ${currentStats.total_sittings}`);
    console.log(`   - Sittings with content: ${currentStats.sittings_with_content}`);
    console.log(`   - Latest date: ${currentStats.latest_date || 'None'}`);
    
    // Get all existing sitting IDs to avoid duplicates
    const existingIds = await new Promise((resolve) => {
      db.all('SELECT id FROM sittings', (err, rows) => {
        resolve(rows ? rows.map(r => r.id) : []);
      });
    });
    console.log(`   - Existing sitting IDs: ${existingIds.length}`);
    
    console.log('\n🎤 STEP 2: FETCHING NEW SITTINGS FROM API');
    console.log('=========================================');
    
    // Fetch ALL speeches from API with pagination
    let allSpeeches = [];
    let offset = 0;
    const limit = 1000;
    let hasMore = true;
    let batchCount = 0;
    
    console.log('📡 Starting API fetch with pagination...');
    console.log(`📡 API URL: https://data.europarl.europa.eu/api/v2/speeches`);
    console.log(`📡 Parameters: format=application/ld+json, limit=${limit}, search-language=EN`);
    
    while (hasMore) {
      batchCount++;
      console.log(`\n📡 BATCH ${batchCount}:`);
      console.log(`   - Offset: ${offset}`);
      console.log(`   - Limit: ${limit}`);
      console.log(`   - Current total: ${allSpeeches.length}`);
      
      try {
        const startTime = Date.now();
        const response = await axios.get('https://data.europarl.europa.eu/api/v2/speeches', {
          params: {
            format: 'application/ld+json',
            limit: limit,
            offset: offset,
            'search-language': 'EN',
            'activity-date-from': startDate
          },
          headers: { 
            Accept: 'application/ld+json',
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)' 
          },
          timeout: 60000
        });
        
        const fetchTime = Date.now() - startTime;
        const batchSpeeches = response.data.data || [];
        allSpeeches = allSpeeches.concat(batchSpeeches);
        
        console.log(`   ✅ SUCCESS: ${batchSpeeches.length} speeches fetched in ${fetchTime}ms`);
        console.log(`   📊 Running total: ${allSpeeches.length} speeches`);
        
        // Show sample of dates from this batch
        if (batchSpeeches.length > 0) {
          const sampleDates = batchSpeeches.slice(0, 3).map(s => s.activity_date || s.activity_start_date).filter(Boolean);
          console.log(`   📅 Sample dates: ${sampleDates.join(', ')}`);
        }
        
        // Check if we got fewer speeches than requested (end of data)
        if (batchSpeeches.length < limit) {
          hasMore = false;
          console.log(`   🏁 END OF DATA: Got ${batchSpeeches.length} < ${limit}, stopping pagination`);
        } else {
          offset += limit;
          console.log(`   ⏭️  Next batch will start at offset: ${offset}`);
          // Small delay to be respectful to the API
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      } catch (error) {
        console.error(`   ❌ ERROR in batch ${batchCount}:`, error.message);
        if (error.response) {
          console.error(`   📊 HTTP Status: ${error.response.status}`);
          console.error(`   📊 Response: ${JSON.stringify(error.response.data).substring(0, 200)}...`);
        }
        hasMore = false;
      }
    }
    
    console.log(`\n📊 API FETCH SUMMARY:`);
    console.log(`   - Total batches: ${batchCount}`);
    console.log(`   - Total speeches fetched: ${allSpeeches.length}`);
    console.log(`   - Final offset: ${offset}`);
    
    console.log('\n📅 STEP 2.5: GROUPING SPEECHES BY DATE');
    console.log('=====================================');
    
    // Group speeches by date and filter for new ones
    const dateMap = new Map();
    let speechesWithoutDate = 0;
    
    allSpeeches.forEach(speech => {
      const date = speech.activity_date || speech.activity_start_date;
      if (!date) {
        speechesWithoutDate++;
        return;
      }
      
      if (!dateMap.has(date)) {
        dateMap.set(date, []);
      }
      dateMap.get(date).push(speech);
    });
    
    console.log(`📅 Date grouping results:`);
    console.log(`   - Total speeches processed: ${allSpeeches.length}`);
    console.log(`   - Speeches without date: ${speechesWithoutDate}`);
    console.log(`   - Unique dates found: ${dateMap.size}`);
    
    // Show sample of dates
    const sampleDates = Array.from(dateMap.keys()).slice(0, 5);
    console.log(`   - Sample dates: ${sampleDates.join(', ')}`);
    
    console.log('\n🔍 STEP 2.6: FILTERING FOR NEW DATES');
    console.log('====================================');
    
    // Filter for dates we don't have yet
    const newDates = [];
    let existingDates = 0;
    
    for (const [date, speeches] of dateMap) {
      console.log(`🔍 Checking date: ${date} (${speeches.length} speeches)`);
      
      // Skip dates before start date
      if (date < startDate) {
        console.log(`   ⏭️  SKIP: ${date} is before start date ${startDate}`);
        continue;
      }
      
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
        console.log(`   ✅ NEW: ${date} needs content (${speeches.length} speeches)`);
      } else {
        existingDates++;
        console.log(`   ⏭️  EXISTS: ${date} already has content`);
      }
    }
    
    console.log(`\n📊 Date filtering summary:`);
    console.log(`   - New dates to fetch: ${newDates.length}`);
    console.log(`   - Existing dates: ${existingDates}`);
    console.log(`   - Total dates checked: ${dateMap.size}`);
    
    if (newDates.length === 0) {
      console.log('✅ No new content to fetch!');
      return;
    }
    
    console.log('\n📥 STEP 3: FETCHING CONTENT FOR NEW SITTINGS');
    console.log('============================================');
    console.log(`📊 Processing ${newDates.length} new dates...`);
    
    let fetchedCount = 0;
    let failedCount = 0;
    let totalContentLength = 0;
    
    for (let i = 0; i < newDates.length; i++) {
      const { date, speeches } = newDates[i];
      console.log(`\n🔍 [${i+1}/${newDates.length}] Fetching content for ${date}:`);
      console.log(`   - Sitting ID: ${speeches[0].id}`);
      console.log(`   - Type: ${speeches[0].type || 'N/A'}`);
      console.log(`   - Label: ${speeches[0].label || 'N/A'}`);
      console.log(`   - DocIdentifier: ${speeches[0].docIdentifier || 'N/A'}`);
      console.log(`   - NotationId: ${speeches[0].notationId || 'N/A'}`);
      
      try {
        const startTime = Date.now();
        
        // Fetch content using the existing function
        const content = await fetchSpeechContentFromHTML(date, speeches[0].id);
        
        const fetchTime = Date.now() - startTime;
        
        if (content && content.length > 100) {
          console.log(`   ✅ CONTENT FETCHED: ${content.length} chars in ${fetchTime}ms`);
          
          // Store the sitting with proper API data structure
          await new Promise((resolve, reject) => {
            const stmt = db.prepare(`
              INSERT OR IGNORE INTO sittings
              (id, type, label, personId, activity_date, content, docIdentifier, notationId, last_updated)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);
            stmt.run(
              speeches[0].id,
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
              if (err) {
                console.log(`   ❌ DB ERROR: ${err.message}`);
                reject(err);
              } else {
                console.log(`   ✅ STORED: Sitting saved to database`);
                resolve();
              }
            });
          });
          
          fetchedCount++;
          totalContentLength += content.length;
          console.log(`   📊 Running totals: ${fetchedCount} fetched, ${totalContentLength} total chars`);
        } else {
          failedCount++;
          console.log(`   ⚠️ NO CONTENT: ${content ? `${content.length} chars (too short)` : 'null/empty'}`);
        }
      } catch (error) {
        failedCount++;
        console.log(`   ❌ ERROR: ${error.message}`);
        if (error.response) {
          console.log(`   📊 HTTP Status: ${error.response.status}`);
        }
      }
      
      // Small delay between requests
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log(`\n📊 CONTENT FETCHING SUMMARY:`);
    console.log(`   - Successfully fetched: ${fetchedCount}`);
    console.log(`   - Failed: ${failedCount}`);
    console.log(`   - Total content length: ${totalContentLength} chars`);
    
    console.log(`\n📊 Content fetch completed:`);
    console.log(`   ✅ Successfully fetched: ${fetchedCount}`);
    console.log(`   ❌ Failed: ${failedCount}`);
    
    console.log('\n🔍 STEP 4: PARSING NEW SITTINGS INTO INDIVIDUAL SPEECHES');
    console.log('======================================================');
    
    let parsedCount = 0;
    let totalSpeeches = 0;
    let failedParseCount = 0;
    
    // Get all sittings that need parsing (have content but no individual speeches)
    console.log('🔍 Finding sittings that need parsing...');
    const sittingsToParse = await new Promise((resolve) => {
      db.all(`
        SELECT s.id, s.content, s.activity_date, LENGTH(s.content) as content_length
        FROM sittings s
        LEFT JOIN individual_speeches i ON s.id = i.sitting_id
        WHERE LENGTH(s.content) > 100
        AND i.sitting_id IS NULL
        GROUP BY s.id
        ORDER BY s.activity_date DESC
      `, (err, rows) => {
        resolve(rows || []);
      });
    });
    
    console.log(`📊 Found ${sittingsToParse.length} sittings to parse`);
    
    if (sittingsToParse.length > 0) {
      console.log(`📅 Sample dates to parse: ${sittingsToParse.slice(0, 3).map(s => s.activity_date).join(', ')}`);
    }
    
    for (let i = 0; i < sittingsToParse.length; i++) {
      const sitting = sittingsToParse[i];
      console.log(`\n🔍 [${i+1}/${sittingsToParse.length}] Parsing sitting:`);
      console.log(`   - ID: ${sitting.id}`);
      console.log(`   - Date: ${sitting.activity_date}`);
      console.log(`   - Content length: ${sitting.content_length} chars`);
      
      try {
        const startTime = Date.now();
        const individualSpeeches = parseIndividualSpeeches(sitting.content, sitting.id);
        const parseTime = Date.now() - startTime;
        
        if (individualSpeeches.length > 0) {
          console.log(`   ✅ PARSED: ${individualSpeeches.length} individual speeches in ${parseTime}ms`);
          
          // Show sample of parsed speeches
          const sampleSpeeches = individualSpeeches.slice(0, 2);
          sampleSpeeches.forEach((speech, idx) => {
            console.log(`   📝 Sample ${idx+1}: ${speech.speaker_name} (${speech.political_group || 'No group'})`);
          });
          
          await storeIndividualSpeeches(individualSpeeches);
          parsedCount++;
          totalSpeeches += individualSpeeches.length;
          console.log(`   ✅ STORED: ${individualSpeeches.length} speeches saved to database`);
        } else {
          failedParseCount++;
          console.log(`   ⚠️ NO SPEECHES: Could not parse any individual speeches from content`);
        }
      } catch (error) {
        failedParseCount++;
        console.log(`   ❌ PARSE ERROR: ${error.message}`);
      }
    }
    
    console.log(`\n📊 PARSING SUMMARY:`);
    console.log(`   - Sittings processed: ${sittingsToParse.length}`);
    console.log(`   - Successfully parsed: ${parsedCount}`);
    console.log(`   - Failed to parse: ${failedParseCount}`);
    console.log(`   - Total individual speeches: ${totalSpeeches}`);
    
    console.log('\n🔗 STEP 5: LINKING SPEECHES TO MEPS');
    console.log('===================================');
    
    console.log('🔍 Starting MEP linking process...');
    const startLinkTime = Date.now();
    const linkedCount = await linkSpeechesToMeps();
    const linkTime = Date.now() - startLinkTime;
    
    console.log(`✅ MEP LINKING COMPLETED:`);
    console.log(`   - Speeches linked: ${linkedCount}`);
    console.log(`   - Time taken: ${linkTime}ms`);
    
    console.log('\n🧹 STEP 6: CHECKING FOR AND REMOVING DUPLICATES');
    console.log('==============================================');
    
    console.log('🔍 Starting duplicate detection and removal...');
    const startDupTime = Date.now();
    const duplicateResult = await checkAndRemoveDuplicates();
    const dupTime = Date.now() - startDupTime;
    
    console.log(`🧹 DUPLICATE CLEANUP COMPLETED:`);
    console.log(`   - Time taken: ${dupTime}ms`);
    console.log(`   - Sittings removed: ${duplicateResult.sittingDuplicatesRemoved || 0}`);
    console.log(`   - Speeches removed: ${duplicateResult.speechDuplicatesRemoved || 0}`);
    console.log(`   - Total duplicates removed: ${duplicateResult.totalRemoved}`);
    console.log(`   - Final sittings: ${duplicateResult.sittingCount}`);
    console.log(`   - Final speeches: ${duplicateResult.speechCount}`);
    
    console.log('\n🎉 PERFECT FETCH & PARSE COMPLETED!');
    console.log('===================================');
    console.log(`📊 FINAL RESULTS:`);
    console.log(`   ✅ New dates fetched: ${fetchedCount}`);
    console.log(`   ✅ Content fetch failures: ${failedCount}`);
    console.log(`   ✅ Sittings parsed: ${parsedCount}`);
    console.log(`   ✅ Parse failures: ${failedParseCount}`);
    console.log(`   ✅ Individual speeches created: ${totalSpeeches}`);
    console.log(`   ✅ MEPs linked: ${linkedCount}`);
    console.log(`   ✅ Duplicates removed: ${duplicateResult.totalRemoved}`);
    console.log(`   📊 Final database state:`);
    console.log(`      - Sittings: ${duplicateResult.sittingCount}`);
    console.log(`      - Individual speeches: ${duplicateResult.speechCount}`);
    
    // Get final database stats
    const finalStats = await new Promise((resolve) => {
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
    
    console.log(`   📊 Database verification:`);
    console.log(`      - Total sittings: ${finalStats.total_sittings}`);
    console.log(`      - Sittings with content: ${finalStats.sittings_with_content}`);
    console.log(`      - Latest date: ${finalStats.latest_date || 'None'}`);
    
    const totalTime = Date.now() - scriptStartTime;
    console.log(`\n⏱️  TOTAL EXECUTION TIME: ${Math.round(totalTime / 1000)}s`);
    console.log('===================================');
    
  } catch (error) {
    console.error('❌ Error in perfect fetch and parse:', error);
  } finally {
    db.close();
  }
}

// Helper function to fetch content from HTML
async function fetchSpeechContentFromHTML(date, sittingId) {
  try {
    const session = getSessionNumber(date);
    const formattedDate = formatDateForUrl(date);
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${formattedDate}_EN.html`;
    
    console.log(`   🔗 Fetching from URL: ${url}`);
    
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
      },
      timeout: 30000
    });
    
    const $ = cheerio.load(response.data);
    
    // Try multiple selectors to find the content
    let content = '';
    
    // Method 1: Look for the main content div
    const mainContent = $('.doc-content, .ep_text, .content, #content, .main-content').first();
    if (mainContent.length > 0) {
      content = mainContent.text().trim();
    }
    
    // Method 2: If no main content, try to get all text from body
    if (!content || content.length < 100) {
      content = $('body').text().trim();
    }
    
    // Method 3: If still no content, try to get all paragraph text
    if (!content || content.length < 100) {
      content = $('p').map((i, el) => $(el).text()).get().join('\n').trim();
    }
    
    return content;
  } catch (error) {
    console.log(`   ❌ Content fetch failed for ${date}: ${error.message}`);
    return null;
  }
}

// Helper function to parse individual speeches
function parseIndividualSpeeches(content, sittingId) {
  const speeches = [];
  
  console.log(`   🔍 Parsing content (${content.length} chars) for sitting ${sittingId}`);
  
  // Split content into lines for better processing
  const lines = content.split('\n');
  let currentSpeech = null;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Skip empty lines
    if (!line) continue;
    
    // Look for the pattern: "Name, Role/Party. – Speech content"
    // The ". –" is ONLY used to separate speakers, never within speeches
    const speechMatch = line.match(/^([^,]+),\s*(.+?)\.\s*–\s*(.+)$/);
    
    if (speechMatch) {
      // Save previous speech if exists
      if (currentSpeech) {
        speeches.push(currentSpeech);
      }
      
      // Start new speech
      const speakerName = speechMatch[1].trim();
      const roleInfo = speechMatch[2].trim();
      const speechContent = speechMatch[3].trim();
      
      // Parse role/party information
      let politicalGroup = null;
      let role = null;
      
      // Check for political group indicators in different languages
      if (roleInfo.includes('on behalf of') || roleInfo.includes('au nom de') || 
          roleInfo.includes('a nome del') || roleInfo.includes('en nombre del') ||
          roleInfo.includes('im Namen der') || roleInfo.includes('au nom du') ||
          roleInfo.includes('fraktion') || roleInfo.includes('gruppo') || 
          roleInfo.includes('grupo') || roleInfo.includes('group') || 
          roleInfo.includes('groupe') || roleInfo.includes('εξ ονόματος') ||
          roleInfo.includes('namens') || roleInfo.includes('w imieniu') ||
          roleInfo.includes('în numele') || roleInfo.includes('for ') ||
          roleInfo.includes('för ') || roleInfo.includes('thar ceann') ||
          roleInfo.includes('u ime') || roleInfo.includes('za skupinu') ||
          roleInfo.includes('em nome') || roleInfo.includes('f\'isem') ||
          roleInfo.includes('(PPE)') || roleInfo.includes('(S&D)') ||
          roleInfo.includes('(ECR)') || roleInfo.includes('(Renew)') ||
          roleInfo.includes('(Verts/ALE)') || roleInfo.includes('(ID)') ||
          roleInfo.includes('(The Left)') || roleInfo.includes('(NI)')) {
        politicalGroup = roleInfo;
      } else {
        role = roleInfo;
      }
      
      currentSpeech = {
        sitting_id: sittingId,
        speaker_name: speakerName,
        political_group: politicalGroup,
        role: role,
        speech_content: speechContent,
        mep_id: null
      };
      
      console.log(`   📝 Found speech: "${speakerName}"${politicalGroup ? ' (' + politicalGroup + ')' : ''}${role ? ' [' + role + ']' : ''}`);
      
    } else if (currentSpeech) {
      // Continue current speech (multiline content)
      // This handles speeches that span multiple lines
      currentSpeech.speech_content += ' ' + line;
    }
  }
  
  // Don't forget the last speech
  if (currentSpeech) {
    speeches.push(currentSpeech);
  }
  
  return speeches;
}

// Helper function to store individual speeches
async function storeIndividualSpeeches(speeches) {
  return new Promise((resolve, reject) => {
    const stmt = db.prepare(`
      INSERT OR IGNORE INTO individual_speeches
      (sitting_id, speaker_name, political_group, speech_content, mep_id)
      VALUES (?, ?, ?, ?, ?)
    `);
    
    let stored = 0;
    for (const speech of speeches) {
      stmt.run(
        speech.sitting_id,
        speech.speaker_name,
        speech.political_group,
        speech.speech_content,
        speech.mep_id
      );
      stored++;
    }
    
    stmt.finalize((err) => {
      if (err) reject(err);
      else resolve(stored);
    });
  });
}

// Helper function to link speeches to MEPs
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

// Helper function to check and remove duplicates
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
          
          // Remove sitting duplicates (keep the one with most content)
          sittingDuplicates.forEach(dup => {
            db.run(`
              DELETE FROM sittings 
              WHERE activity_date = ? 
              AND LENGTH(content) > 100
              AND id NOT IN (
                SELECT id FROM sittings 
                WHERE activity_date = ? AND LENGTH(content) > 100
                ORDER BY LENGTH(content) DESC 
                LIMIT 1
              )
            `, [dup.activity_date, dup.activity_date], function(err) {
              if (!err && this.changes > 0) {
                totalRemoved += this.changes;
                console.log(`   🗑️ Removed ${this.changes} duplicate sittings for ${dup.activity_date}`);
              }
            });
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

// Run the perfect fetch and parse
perfectFetchAndParse();
