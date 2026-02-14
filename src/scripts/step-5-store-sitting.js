#!/usr/bin/env node
/**
 * Step 5: Store sitting, speeches with topic mapping, and link MEPs
 *
 * Usage: node scripts/step-5-store-sitting.js --date YYYY-MM-DD --data pipeline-data.json
 *   or:  called programmatically with run(db, payload, log)
 *
 * Expects payload: { date, html, sittingId, topics, speeches, sections, topicMap }
 */

require('dotenv').config();
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const { bestSectionForSpeech } = require('../core/parse-helpers');
const { DB_PATH } = require('../core/db');

async function linkSpeechesToMeps(db, log) {
  return new Promise((resolve, reject) => {
    db.all(`SELECT DISTINCT speaker_name FROM individual_speeches WHERE speaker_name IS NOT NULL AND mep_id IS NULL`, async (err, speakers) => {
      if (err) return reject(err);
      if (!speakers || speakers.length === 0) return resolve(0);
      log(`  Linking ${speakers.length} unlinked speakers to MEPs...`);

      let linkedCount = 0;
      for (const s of speakers) {
        const mep = await new Promise(r => {
          db.get(`SELECT id FROM meps WHERE label LIKE ? OR label LIKE ?`,
            [`%${s.speaker_name}%`, `%${s.speaker_name.split(' ').reverse().join(' ')}%`], (e, row) => r(row));
        });
        if (mep) {
          await new Promise(r => {
            db.run(`UPDATE individual_speeches SET mep_id = ? WHERE speaker_name = ? AND mep_id IS NULL`, [mep.id, s.speaker_name], function(er) {
              if (!er && this.changes > 0) linkedCount++;
              r();
            });
          });
        }
      }
      resolve(linkedCount);
    });
  });
}

async function run(payload, options = {}) {
  const log = options.log || (() => {});
  const db = options.db || new sqlite3.Database(DB_PATH);
  const closeDb = !options.db;

  const { date, html, sittingId, topics, speeches, sections, topicMap } = payload;
  if (!date || !html || !sittingId || !speeches || !sections) {
    throw new Error('payload must include date, html, sittingId, speeches, sections');
  }

  try {
    const replaceExisting = !!options.replaceExisting;
    if (replaceExisting) {
      log('  Replacing existing speeches...');
      await new Promise((resolve, reject) => {
        db.run('DELETE FROM individual_speeches WHERE sitting_id = ?', [sittingId], err => (err ? reject(err) : resolve()));
      });
    }
    log('  Inserting sitting...');
    const sittingRow = [sittingId, 'PLENARY_DEBATE', `Parliamentary Sitting - ${date}`, date, html, Date.now()];
    await new Promise((resolve, reject) => {
      db.run(`INSERT OR REPLACE INTO sittings (id, type, label, activity_date, content, last_updated) VALUES (?, ?, ?, ?, ?, ?)`,
        sittingRow,
        err => err ? reject(err) : resolve());
    });
    await new Promise((resolve, reject) => {
      db.run(`INSERT OR REPLACE INTO speeches (id, type, label, activity_date, content, last_updated) VALUES (?, ?, ?, ?, ?, ?)`,
        sittingRow,
        err => err ? reject(err) : resolve());
    });

    log('  Ensuring schema...');
    await new Promise(r => {
      db.exec(`
        ALTER TABLE individual_speeches ADD COLUMN topic TEXT;
        ALTER TABLE individual_speeches ADD COLUMN macro_topic TEXT;
        ALTER TABLE individual_speeches ADD COLUMN macro_specific_focus TEXT;
        ALTER TABLE individual_speeches ADD COLUMN macro_confidence REAL;
        ALTER TABLE individual_speeches ADD COLUMN language TEXT;
      `, () => r());
    });

    log('  Storing speeches with topic mapping...');
    const stmt = db.prepare(`
      INSERT INTO individual_speeches (sitting_id, speaker_name, political_group, title, speech_content, speech_order, mep_id, topic, macro_topic, macro_specific_focus, macro_confidence)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const topicMapVal = topicMap || {};
    for (const sp of speeches) {
      const match = bestSectionForSpeech(sp, sections);
      let topic = null, macro_topic = null, macro_specific_focus = null, macro_confidence = null;
      if (match) {
        topic = match.section.title;
        const meta = topicMapVal[topic];
        if (meta) {
          macro_topic = meta.macro_topic;
          macro_specific_focus = meta.specific_focus;
          macro_confidence = meta.confidence;
        }
      }
      stmt.run(sp.sitting_id, sp.speaker_name, sp.political_group, sp.title, sp.speech_content, sp.speech_order, sp.mep_id, topic, macro_topic, macro_specific_focus, macro_confidence);
    }
    stmt.finalize();

    log('  Linking speeches to MEPs...');
    const linkedCount = await linkSpeechesToMeps(db, log);

    return { speechesStored: speeches.length, linkedCount };
  } finally {
    if (closeDb) db.close();
  }
}

if (require.main === module) {
  const dateIdx = process.argv.indexOf('--date');
  const dataIdx = process.argv.indexOf('--data');
  const date = dateIdx !== -1 && process.argv[dateIdx + 1] ? process.argv[dateIdx + 1] : null;
  const dataFile = dataIdx !== -1 && process.argv[dataIdx + 1] ? process.argv[dataIdx + 1] : null;

  if (!date || !dataFile) {
    console.error('Usage: node step-5-store-sitting.js --date YYYY-MM-DD --data pipeline-data.json');
    process.exit(1);
  }

  const payload = JSON.parse(require('fs').readFileSync(dataFile, 'utf8'));
  run(payload, { log: console.log })
    .then(r => {
      console.log('Stored:', r);
      process.exit(0);
    })
    .catch(err => {
      console.error('Error:', err.message);
      process.exit(1);
    });
}

module.exports = { run, linkSpeechesToMeps };
