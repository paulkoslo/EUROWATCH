#!/usr/bin/env node

const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');
const cheerio = require('cheerio');

// Database connection
const db = new sqlite3.Database('ep_data.db');

console.log('�� PERFECT FETCH & PARSE SCRIPT - YEARLY VERSION');
console.log('================================================');
console.log('📅 Script started at:', new Date().toISOString());

const scriptStartTime = Date.now();

// Parse command line arguments for start and end dates
const startDate = process.argv[2] || '2015-01-01';
const endDate = process.argv[3] || null;
console.log('📅 Start date for fetching:', startDate);
if (endDate) {
  console.log('📅 End date for fetching:', endDate);
}

// Function to determine session number based on date
function getSessionNumber(date) {
  // European Parliament terms with exact dates
  if (date >= '2024-07-16') return 10; // 10th term: 2024-07-16 to present
  if (date >= '2019-07-02' && date < '2024-07-16') return 9;  // 9th term: 2019-07-02 to 2024-07-15
  if (date >= '2014-07-01' && date < '2019-07-02') return 8;  // 8th term: 2014-07-01 to 2019-07-01
  if (date >= '2009-07-14' && date < '2014-07-01') return 7;  // 7th term: 2009-07-14 to 2014-06-30
  if (date >= '2004-07-20' && date < '2009-07-14') return 6;  // 6th term: 2004-07-20 to 2009-07-13
  return 5; // 5th term: 1999-07-20 to 2004-07-19 (fallback)
}

// Function to get all years between start and end date
function getYearsInRange(startDate, endDate) {
  const startYear = parseInt(startDate.split('-')[0]);
  const endYear = endDate ? parseInt(endDate.split('-')[0]) : new Date().getFullYear();
  
  const years = [];
  for (let year = startYear; year <= endYear; year++) {
    years.push(year);
  }
  return years;
}

// Function to get date range for a specific year
function getYearDateRange(year) {
  const yearStart = `${year}-01-01`;
  const yearEnd = `${year}-12-31`;
  return { yearStart, yearEnd };
}

// Function to generate all dates in a year
function generateAllDatesInYear(year) {
  const dates = [];
  
  // Generate all 365/366 days of the year
  const startDate = new Date(year, 0, 1); // January 1st
  const endDate = new Date(year, 11, 31); // December 31st
  
  for (let date = new Date(startDate); date <= endDate; date.setDate(date.getDate() + 1)) {
    const yearStr = date.getFullYear();
    const monthStr = (date.getMonth() + 1).toString().padStart(2, '0');
    const dayStr = date.getDate().toString().padStart(2, '0');
    dates.push(`${yearStr}-${monthStr}-${dayStr}`);
  }
  
  return dates;
}

async function perfectFetchAndParseYearly() {
  try {
    console.log('🎯 ENTERING perfectFetchAndParseYearly function...');
    
    // Get years to process
    const years = getYearsInRange(startDate, endDate);
    console.log(`�� Processing years: ${years.join(', ')}`);
    
    let totalFetched = 0;
    let totalFailed = 0;
    let totalParsed = 0;
    let totalSpeeches = 0;
    
    // Process each year
    for (let i = 0; i < years.length; i++) {
      const year = years[i];
      console.log(`\n🗓️  PROCESSING YEAR ${year} (${i+1}/${years.length})`);
      console.log('='.repeat(50));
      
      const { yearStart, yearEnd } = getYearDateRange(year);
      // Session will be determined per date, not per year
      
      console.log(`�� Year ${year} details:`);
      console.log(`   - Date range: ${yearStart} to ${yearEnd}`);
      console.log(`   - Session will be determined per date (8 for early ${year}, 9 for late ${year})`);
      console.log(`   - URL pattern: CRE-{session}-{date}_EN.html`);
      
      // Check what we already have for this year
      const existingStats = await new Promise((resolve) => {
        db.get(`
          SELECT 
            COUNT(*) as total_sittings,
            COUNT(CASE WHEN LENGTH(content) > 100 THEN 1 END) as sittings_with_content
          FROM sittings
          WHERE activity_date >= ? AND activity_date <= ?
        `, [yearStart, yearEnd], (err, row) => {
          resolve(row || { total_sittings: 0, sittings_with_content: 0 });
        });
      });
      
      console.log(`   - Existing sittings: ${existingStats.total_sittings}`);
      console.log(`   - With content: ${existingStats.sittings_with_content}`);
      
      // Fetch speeches for this year
      const yearResult = await fetchAndParseYear(year, yearStart, yearEnd);
      
      totalFetched += yearResult.fetched;
      totalFailed += yearResult.failed;
      totalParsed += yearResult.parsed;
      totalSpeeches += yearResult.speeches;
      
      console.log(`✅ Year ${year} completed: ${yearResult.fetched} fetched, ${yearResult.failed} failed, ${yearResult.speeches} speeches`);
      
      // Small delay between years
      if (i < years.length - 1) {
        console.log('⏳ Waiting 2 seconds before next year...');
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    
    console.log('\n🎉 YEARLY PROCESSING COMPLETED!');
    console.log('===============================');
    console.log(`📊 FINAL RESULTS:`);
    console.log(`   ✅ Total fetched: ${totalFetched}`);
    console.log(`   ❌ Total failed: ${totalFailed}`);
    console.log(`   �� Total parsed: ${totalParsed}`);
    console.log(`   🎤 Total speeches: ${totalSpeeches}`);
    console.log(`   �� Years processed: ${years.length}`);
    
    const totalTime = Date.now() - scriptStartTime;
    console.log(`\n⏱️  TOTAL EXECUTION TIME: ${Math.round(totalTime / 1000)}s`);
    
  } catch (error) {
    console.error('❌ Error in yearly fetch and parse:', error);
  } finally {
    db.close();
  }
}

// Function to fetch and parse a specific year
async function fetchAndParseYear(year, yearStart, yearEnd) {
  let fetched = 0;
  let failed = 0;
  let parsed = 0;
  let speeches = 0;
  
  try {
    console.log(`\n📅 Fetching HTML content for all days in ${year}...`);
    
    // Generate all dates in the year
    const allDates = generateAllDatesInYear(year);
    console.log(`📅 Generated ${allDates.length} dates to check for ${year}`);
    
    // Process each date
    let processedDates = 0;
    let foundSittings = 0;
    
    for (const date of allDates) {
      processedDates++;
      
      // Show progress every 50 dates
      if (processedDates % 50 === 0 || processedDates === allDates.length) {
        console.log(`   📊 Progress: ${processedDates}/${allDates.length} dates checked, ${foundSittings} sittings found`);
      }
      
      // Check if we already have content for this date
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
        try {
          const dateSession = getSessionNumber(date);
          const content = await fetchSpeechContentFromHTML(date, dateSession);
          
          if (content && content.length > 100) {
            // Store the sitting
            await new Promise((resolve, reject) => {
              const stmt = db.prepare(`
                INSERT OR IGNORE INTO sittings
                (id, type, label, personId, activity_date, content, docIdentifier, notationId, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              `);
              
              stmt.run(
                `direct-${date}`,
                'def/ep-activities/PLENARY_DEBATE_SPEECH',
                `Parliamentary Sitting - ${date}`,
                null,
                date,
                content,
                '',
                '',
                Date.now()
              );
              stmt.finalize((err) => {
                if (err) reject(err);
                else resolve();
              });
            });
            
            fetched++;
            foundSittings++;
            console.log(`   ✅ ${date}: Found sitting (${content.length} chars)`);
          }
        } catch (error) {
          // Silently skip failed dates (most days won't have sittings)
        }
        
        // Small delay between requests to be respectful
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    }
    
    console.log(`📊 Daily scan completed for ${year}: ${foundSittings} sittings found out of ${allDates.length} days checked`);
    
    // Parse new sittings for this year
    console.log(`\n🔍 Parsing new sittings for ${year}...`);
    const sittingsToParse = await new Promise((resolve) => {
      db.all(`
        SELECT s.id, s.content, s.activity_date, LENGTH(s.content) as content_length
        FROM sittings s
        LEFT JOIN individual_speeches i ON s.id = i.sitting_id
        WHERE s.activity_date >= ? AND s.activity_date <= ?
        AND LENGTH(s.content) > 100
        AND i.sitting_id IS NULL
        GROUP BY s.id
        ORDER BY s.activity_date DESC
      `, [yearStart, yearEnd], (err, rows) => {
        resolve(rows || []);
      });
    });
    
    console.log(`�� Found ${sittingsToParse.length} sittings to parse for ${year}`);
    
    for (const sitting of sittingsToParse) {
      try {
        const individualSpeeches = parseIndividualSpeeches(sitting.content, sitting.id);
        
        if (individualSpeeches.length > 0) {
          await storeIndividualSpeeches(individualSpeeches);
          parsed++;
          speeches += individualSpeeches.length;
          console.log(`   ✅ Parsed: ${individualSpeeches.length} speeches from ${sitting.activity_date}`);
        }
      } catch (error) {
        console.log(`   ❌ Parse error for ${sitting.activity_date}: ${error.message}`);
      }
    }
    
  } catch (error) {
    console.error(`❌ Error processing year ${year}:`, error);
  }
  
  return { fetched, failed, parsed, speeches };
}

// Helper function to fetch content from HTML with correct session
async function fetchSpeechContentFromHTML(date, session) {
  try {
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${date}_EN.html`;
    
    console.log(`     �� URL: ${url}`);
    
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
    console.log(`     ❌ Content fetch failed for ${date}: ${error.message}`);
    return null;
  }
}

// Helper function to parse individual speeches (same as original)
function parseIndividualSpeeches(content, sittingId) {
  const speeches = [];
  const lines = content.split('\n');
  let currentSpeech = null;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const speechMatch = line.match(/^([^,]+),\s*(.+?)\.\s*–\s*(.+)$/);
    
    if (speechMatch) {
      if (currentSpeech) {
        speeches.push(currentSpeech);
      }
      
      const speakerName = speechMatch[1].trim();
      const roleInfo = speechMatch[2].trim();
      const speechContent = speechMatch[3].trim();
      
      let politicalGroup = null;
      let role = null;
      
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
      
    } else if (currentSpeech) {
      currentSpeech.speech_content += ' ' + line;
    }
  }
  
  if (currentSpeech) {
    speeches.push(currentSpeech);
  }
  
  return speeches;
}

// Helper function to store individual speeches (same as original)
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

// Run the yearly fetch and parse
perfectFetchAndParseYearly();