#!/usr/bin/env node

const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');
const cheerio = require('cheerio');

const db = new sqlite3.Database('ep_data.db');

console.log('🚀 SIMPLE UPDATE SCRIPT');
console.log('======================');

async function main() {
    try {
        // Step 1: Check what we have
        const currentCount = await new Promise((resolve) => {
            db.get('SELECT COUNT(*) as count FROM sittings WHERE LENGTH(content) > 100', (err, row) => {
                resolve(row ? row.count : 0);
            });
        });
        console.log(`📊 Current sittings with content: ${currentCount}`);

        // Step 2: Get latest date we have
        const latestDate = await new Promise((resolve) => {
            db.get('SELECT MAX(activity_date) as date FROM sittings WHERE LENGTH(content) > 100', (err, row) => {
                resolve(row ? row.date : null);
            });
        });
        console.log(`📅 Latest date we have: ${latestDate}`);

        // Step 3: Fetch new speeches from API
        console.log('🎤 Fetching from API...');
        const response = await axios.get('https://data.europarl.europa.eu/api/v2/speeches', {
            params: {
                format: 'application/ld+json',
                limit: 500,
                offset: 0,
                'search-language': 'EN',
                'activity-date-from': '2023-01-01'
            },
            headers: { 
                Accept: 'application/ld+json',
                'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)' 
            },
            timeout: 45000
        });

        const speeches = response.data.data || [];
        console.log(`📊 Got ${speeches.length} speeches from API`);

        // Step 4: Group by date and filter new ones
        const dateMap = new Map();
        speeches.forEach(speech => {
            const date = speech.activity_date || speech.activity_start_date;
            if (!date) return;
            
            if (!dateMap.has(date)) {
                dateMap.set(date, []);
            }
            dateMap.get(date).push(speech);
        });

        // Filter out dates we already have
        const existingDates = await new Promise((resolve) => {
            db.all('SELECT DISTINCT activity_date FROM sittings WHERE LENGTH(content) > 100', (err, rows) => {
                resolve(rows ? rows.map(r => r.activity_date) : []);
            });
        });

        const newDates = Array.from(dateMap.keys()).filter(date => !existingDates.includes(date));
        console.log(`🆕 Found ${newDates.length} new dates: ${newDates.join(', ')}`);

        if (newDates.length === 0) {
            console.log('✅ No new dates to process');
            db.close();
            return;
        }

        // Step 5: Fetch content for new dates
        let fetchedCount = 0;
        for (const date of newDates) {
            console.log(`📥 Fetching content for ${date}...`);
            
            const content = await fetchContent(date);
            if (content) {
                // Store sitting
                await new Promise((resolve, reject) => {
                    const stmt = db.prepare(`
                        INSERT OR IGNORE INTO sittings 
                        (id, type, label, personId, activity_date, content, docIdentifier, notationId, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `);
                    stmt.run(
                        `sitting-${date}`,
                        'Parliamentary Sitting',
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
                fetchedCount++;
                console.log(`✅ Stored content for ${date} (${content.length} chars)`);
            } else {
                console.log(`❌ Failed to fetch content for ${date}`);
            }
        }

        console.log(`📊 Fetched content for ${fetchedCount} new dates`);

        // Step 6: Parse speeches for new dates
        console.log('🔍 Parsing speeches...');
        let parsedCount = 0;
        for (const date of newDates) {
            const speeches = await new Promise((resolve) => {
                db.all('SELECT id, content FROM sittings WHERE activity_date = ? AND LENGTH(content) > 100', [date], (err, rows) => {
                    resolve(rows || []);
                });
            });

            for (const sitting of speeches) {
                const individualSpeeches = parseSpeeches(sitting.content, sitting.id);
                if (individualSpeeches.length > 0) {
                    await storeSpeeches(individualSpeeches);
                    parsedCount += individualSpeeches.length;
                    console.log(`✅ Parsed ${individualSpeeches.length} speeches for ${date}`);
                }
            }
        }

        console.log(`📊 Parsed ${parsedCount} total speeches`);

        // Step 7: Link to MEPs
        console.log('🔗 Linking speeches to MEPs...');
        const linkCount = await linkSpeeches();
        console.log(`📊 Linked ${linkCount} speeches to MEPs`);

        // Final count
        const finalCount = await new Promise((resolve) => {
            db.get('SELECT COUNT(*) as count FROM sittings WHERE LENGTH(content) > 100', (err, row) => {
                resolve(row ? row.count : 0);
            });
        });

        console.log('🎉 UPDATE COMPLETED!');
        console.log('===================');
        console.log(`✅ New dates processed: ${newDates.length}`);
        console.log(`✅ Content fetched: ${fetchedCount}`);
        console.log(`✅ Speeches parsed: ${parsedCount}`);
        console.log(`✅ Speeches linked: ${linkCount}`);
        console.log(`✅ Total sittings: ${finalCount}`);

    } catch (error) {
        console.error('❌ Error:', error.message);
    } finally {
        db.close();
    }
}

async function fetchContent(date) {
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
    
    try {
        const response = await axios.get(url, {
            timeout: 15000,
            headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)' }
        });

        if (response.data && response.data.length > 100) {
            // Parse HTML to extract text
            const $ = cheerio.load(response.data);
            let textContent = '';
            $('p').each((i, el) => {
                const text = $(el).text().trim();
                if (text) {
                    textContent += text + '\n';
                }
            });
            return textContent.length > 100 ? textContent : response.data;
        }
    } catch (error) {
        console.log(`   ❌ Failed: ${error.response?.status || error.message}`);
    }
    return null;
}

function parseSpeeches(content, sittingId) {
    const speeches = [];
    const lines = content.split('\n');
    let currentSpeech = null;
    let speechOrder = 0;

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;

        // Pattern 1: Name (Group). – Content
        const pattern1 = /^(.+?)\s*\(([^)]+)\)\.\s*–\s*(.*)$/;
        const match1 = line.match(pattern1);

        // Pattern 2: Name, on behalf of Group. – Content  
        const pattern2 = /^(.+?),\s*on behalf of\s*(.+?)\.\s*–\s*(.*)$/;
        const match2 = line.match(pattern2);

        // Pattern 3: Title. – Content
        const pattern3 = /^([A-Z][A-Za-z\s]+)\.\s+–\s*(.*)$/;
        const match3 = line.match(pattern3);

        if (match1) {
            if (currentSpeech) speeches.push(currentSpeech);
            currentSpeech = {
                sitting_id: sittingId,
                speaker_name: match1[1].trim(),
                political_group: match1[2].trim(),
                title: null,
                speech_content: match1[3].trim(),
                speech_order: speechOrder++
            };
        } else if (match2) {
            if (currentSpeech) speeches.push(currentSpeech);
            currentSpeech = {
                sitting_id: sittingId,
                speaker_name: match2[1].trim(),
                political_group: match2[2].trim(),
                title: null,
                speech_content: match2[3].trim(),
                speech_order: speechOrder++
            };
        } else if (match3) {
            if (currentSpeech) speeches.push(currentSpeech);
            currentSpeech = {
                sitting_id: sittingId,
                speaker_name: null,
                political_group: null,
                title: match3[1].trim(),
                speech_content: match3[2].trim(),
                speech_order: speechOrder++
            };
        } else if (currentSpeech) {
            currentSpeech.speech_content += ' ' + line;
        }
    }

    if (currentSpeech) speeches.push(currentSpeech);
    return speeches;
}

async function storeSpeeches(speeches) {
    if (speeches.length === 0) return;

    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`
            INSERT OR IGNORE INTO individual_speeches 
            (sitting_id, speaker_name, political_group, title, speech_content, speech_order)
            VALUES (?, ?, ?, ?, ?, ?)
        `);

        speeches.forEach(speech => {
            stmt.run(
                speech.sitting_id,
                speech.speaker_name,
                speech.political_group,
                speech.title,
                speech.speech_content,
                speech.speech_order
            );
        });

        stmt.finalize((err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

async function linkSpeeches() {
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

main();
