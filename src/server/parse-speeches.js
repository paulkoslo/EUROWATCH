/**
 * Store and bulk-parse individual speeches. Parsing logic lives in core/parse-helpers.js
 * (single source of truth). This module adds DB storage and batch helpers.
 */
const { createProgressBar } = require('./progress');
const { parseIndividualSpeeches } = require('../core/parse-helpers');

// Re-export so callers can require lib/parse-speeches for both parse + store
function parseIndividualSpeechesFromContent(rawContent, sittingId) {
  return parseIndividualSpeeches(rawContent, sittingId);
}

async function storeIndividualSpeeches(db, speeches) {
  return new Promise((resolve, reject) => {
    if (speeches.length === 0) {
      resolve(0);
      return;
    }

    const sittingId = speeches[0].sitting_id;

    db.serialize(() => {
      db.get('SELECT COUNT(*) as count FROM individual_speeches WHERE sitting_id = ?', [sittingId], (err, row) => {
        if (err) {
          console.error('[STORE] Error checking existing speeches:', err);
          reject(err);
          return;
        }

        if (row.count > 0) {
          console.log(`[STORE] Sitting ${sittingId} already has ${row.count} individual speeches, skipping`);
          resolve(0);
          return;
        }

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
                console.error(`[PARSE] Error inserting speech ${processed + 1}:`, err);
                errors++;
              }
              processed++;

              if (processed === speeches.length) {
                stmt.finalize();
                if (errors > 0) {
                  console.log(`[PARSE] Stored ${processed - errors}/${processed} individual speeches (${errors} errors)`);
                } else {
                  console.log(`[PARSE] Stored ${processed} individual speeches for sitting ${speeches[0]?.sitting_id}`);
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

async function parseRecentSpeeches(db) {
  console.log('[PARSE RECENT] Starting parsing of recent speeches...');

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
        console.error('[PARSE RECENT] Error fetching recent speeches:', err);
        reject(err);
        return;
      }

      console.log(`[PARSE RECENT] Found ${speeches.length} recent speeches needing parsing`);

      if (speeches.length === 0) {
        resolve();
        return;
      }

      let processed = 0;
      let successCount = 0;
      let errorCount = 0;

      for (const speech of speeches) {
        try {
          const individualSpeeches = parseIndividualSpeechesFromContent(speech.content, speech.id);
          if (individualSpeeches.length > 0) {
            await storeIndividualSpeeches(db, individualSpeeches);
            successCount++;
          }
        } catch (error) {
          console.error(`[PARSE RECENT] Error parsing ${speech.docIdentifier}:`, error.message);
          errorCount++;
        }
        processed++;
        await new Promise(r => setTimeout(r, 10));
      }

      console.log(`[PARSE RECENT] Completed! Processed ${processed}, success: ${successCount}, errors: ${errorCount}`);
      resolve();
    });
  });
}

async function parseAllSpeechesWithContent(db) {
  console.log('[BULK PARSE] Starting bulk parsing of speeches with content...');

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
        console.error('[BULK PARSE] Error fetching speeches:', err);
        reject(err);
        return;
      }

      if (speeches.length === 0) {
        resolve();
        return;
      }

      let processed = 0;
      let successCount = 0;
      let errorCount = 0;
      const startTime = Date.now();

      for (const speech of speeches) {
        try {
          const existingCount = await new Promise((res) => {
            db.get('SELECT COUNT(*) as count FROM individual_speeches WHERE sitting_id = ?', [speech.id], (e, row) => {
              res(e ? 0 : row.count);
            });
          });

          if (existingCount > 0) {
            processed++;
            continue;
          }

          const individualSpeeches = parseIndividualSpeechesFromContent(speech.content, speech.id);
          if (individualSpeeches.length > 0) {
            await storeIndividualSpeeches(db, individualSpeeches);
            successCount++;
          }
        } catch (error) {
          errorCount++;
        }
        processed++;
        await new Promise(r => setTimeout(r, 10));
      }

      const totalTime = (Date.now() - startTime) / 1000;
      console.log(`\n[BULK PARSE] Completed! Processed ${processed}, success: ${successCount}, errors: ${errorCount}, time: ${totalTime.toFixed(1)}s`);
      resolve();
    });
  });
}

module.exports = {
  parseIndividualSpeeches: parseIndividualSpeechesFromContent,
  storeIndividualSpeeches,
  parseRecentSpeeches,
  parseAllSpeechesWithContent
};
