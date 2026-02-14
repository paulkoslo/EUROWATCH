/**
 * Historic MEP creation from speaker names and linking speeches to MEPs.
 * Used by refresh and link endpoints.
 */
function checkAndRemoveDuplicates(db) {
  return new Promise((resolve) => {
    let totalRemoved = 0;
    db.serialize(() => {
      db.all(`
        SELECT activity_date, COUNT(*) as count 
        FROM sittings WHERE LENGTH(content) > 100
        GROUP BY activity_date HAVING COUNT(*) > 1
      `, (err, sittingDuplicates) => {
        if (!err && sittingDuplicates?.length) {
          sittingDuplicates.forEach(dup => console.log(`   - ${dup.activity_date}: ${dup.count} copies`));
        }
        db.all(`
          SELECT sitting_id, speaker_name, speech_content, COUNT(*) as count
          FROM individual_speeches 
          GROUP BY sitting_id, speaker_name, speech_content HAVING COUNT(*) > 1
        `, (err, speechDuplicates) => {
          if (!err && speechDuplicates?.length) {
            speechDuplicates.forEach(dup => {
              db.run(`
                DELETE FROM individual_speeches 
                WHERE sitting_id = ? AND speaker_name = ? AND speech_content = ?
                AND id NOT IN (SELECT MIN(id) FROM individual_speeches 
                  WHERE sitting_id = ? AND speaker_name = ? AND speech_content = ?)
              `, [dup.sitting_id, dup.speaker_name, dup.speech_content, dup.sitting_id, dup.speaker_name, dup.speech_content], function(e) {
                if (!e && this.changes > 0) totalRemoved += this.changes;
              });
            });
          }
          db.get('SELECT COUNT(*) as count FROM sittings WHERE LENGTH(content) > 100', (e1, r1) => {
            db.get('SELECT COUNT(*) as count FROM individual_speeches', (e2, r2) => {
              resolve({ totalRemoved, sittingCount: r1?.count || 0, speechCount: r2?.count || 0 });
            });
          });
        });
      });
    });
  });
}

/** Build in-memory map: normalized label / reversed name -> mep id (for fast lookup). */
function buildMepLabelLookup(meps) {
  const map = new Map();
  for (const m of meps) {
    const id = m.id;
    const label = (m.label || '').trim();
    if (!label) continue;
    const rev = label.split(/\s+/).filter(Boolean).reverse().join(' ');
    const key1 = label.toLowerCase();
    const key2 = rev.toLowerCase();
    if (!map.has(key1)) map.set(key1, id);
    if (key2 !== key1 && !map.has(key2)) map.set(key2, id);
  }
  return map;
}

function linkSpeechesToMeps(db, log = null) {
  return new Promise((resolve, reject) => {
    db.all(`SELECT id, label FROM meps`, (err, meps) => {
      if (err) return reject(err);
      db.all(`SELECT DISTINCT speaker_name FROM individual_speeches WHERE speaker_name IS NOT NULL AND mep_id IS NULL`, async (err2, speakers) => {
        if (err2) return reject(err2);
        if (!speakers?.length) return resolve(0);
        const total = speakers.length;
        if (log) log(`[MEP-DATASET] Step 3: Linking up to ${total} speakers (in-memory match + parallel updates)...`);
        const lookup = buildMepLabelLookup(meps);
        const toLink = [];
        const mepsList = meps;
        for (const speaker of speakers) {
          const name = (speaker.speaker_name || '').trim();
          if (!name) continue;
          const key1 = name.toLowerCase();
          const key2 = name.split(/\s+/).filter(Boolean).reverse().join(' ').toLowerCase();
          let mepId = lookup.get(key1) || lookup.get(key2);
          if (mepId == null && mepsList.length) {
            const first = mepsList.find((m) => {
              const L = (m.label || '').toLowerCase();
              return L && (L.includes(key1) || (key2 && L.includes(key2)));
            });
            if (first) mepId = first.id;
          }
          if (mepId != null) toLink.push({ speaker_name: speaker.speaker_name, mep_id: mepId });
        }
        const BATCH = 50;
        let speechesLinked = 0;
        const updateOne = ({ speaker_name, mep_id }) => new Promise((res, rej) => {
          db.run(`UPDATE individual_speeches SET mep_id = ? WHERE speaker_name = ? AND mep_id IS NULL`, [mep_id, speaker_name], function(e) {
            if (e) rej(e);
            else res(this.changes);
          });
        });
        for (let i = 0; i < toLink.length; i += BATCH) {
          const batch = toLink.slice(i, i + BATCH);
          const changes = await Promise.all(batch.map(updateOne));
          const batchSpeeches = changes.reduce((a, b) => a + b, 0);
          speechesLinked += batchSpeeches;
          const saved = Math.min(i + BATCH, toLink.length);
          if (log) log(`Linking: saved ${saved}/${toLink.length} speakers (${speechesLinked} speeches) to DB`);
        }
        resolve(toLink.length);
      });
    });
  });
}

function createHistoricMepsAndLinkSpeeches(db) {
  return createHistoricMepsOnePerPerson(db);
}

/** Build lookup for createHistoric: label and "givenName familyName" -> id. */
function buildMepCreateLookup(meps) {
  const map = new Map();
  for (const m of meps) {
    const id = m.id;
    const label = (m.label || '').trim().toLowerCase();
    const given = (m.givenName || '').trim().toLowerCase();
    const family = (m.familyName || '').trim().toLowerCase();
    if (label) map.set(label, id);
    if (given && family) {
      map.set(`${given} ${family}`, id);
      map.set(`${family} ${given}`, id);
    }
  }
  return map;
}

/**
 * Create one historic MEP per distinct speaker_name (not per speaker+group).
 * Uses in-memory match then batch INSERT and parallel batch UPDATE.
 */
function createHistoricMepsOnePerPerson(db, log = null) {
  return new Promise((resolve, reject) => {
    db.all(`SELECT id, label, givenName, familyName FROM meps`, (err, meps) => {
      if (err) return reject(err);
      const lookup = buildMepCreateLookup(meps);
      db.get('SELECT MAX(id) as maxId FROM meps', (e, row) => {
        if (e) return reject(e);
        let nextMepId = (row?.maxId || 1000000) + 1;
        db.all(`
          SELECT speaker_name,
                 COUNT(*) as speech_count,
                 MAX(political_group) as political_group
          FROM individual_speeches
          WHERE speaker_name IS NOT NULL AND TRIM(speaker_name) != '' AND mep_id IS NULL
          GROUP BY speaker_name
          ORDER BY speech_count DESC
        `, async (err2, speakers) => {
          if (err2) return reject(err2);
          if (log) log(`[MEP-DATASET] Step 4: Creating historic MEPs for up to ${speakers.length} speakers (in-memory match + batch writes)...`);
          const toLink = [];
          const toCreate = [];
          for (const speaker of speakers) {
            const name = speaker.speaker_name.trim();
            const nameParts = name.split(/\s+/).filter(Boolean);
            const firstName = nameParts[0] || name;
            const lastName = nameParts[nameParts.length - 1] || name;
            const key1 = name.toLowerCase();
            const key2 = nameParts.length >= 2 ? `${firstName.toLowerCase()} ${lastName.toLowerCase()}` : '';
            const key3 = nameParts.length >= 2 ? `${lastName.toLowerCase()} ${firstName.toLowerCase()}` : '';
            let mepId = lookup.get(key1) || (key2 && lookup.get(key2)) || (key3 && lookup.get(key3));
            if (mepId != null) {
              toLink.push({ speaker_name: speaker.speaker_name, mep_id: mepId });
            } else {
              mepId = nextMepId++;
              toCreate.push({
                mep_id: mepId,
                speaker_name: speaker.speaker_name,
                name,
                firstName,
                lastName,
                political_group: speaker.political_group || 'Unknown'
              });
              toLink.push({ speaker_name: speaker.speaker_name, mep_id: mepId });
              lookup.set(key1, mepId);
              if (key2) lookup.set(key2, mepId);
              if (key3) lookup.set(key3, mepId);
            }
          }
          const now = Date.now();
          for (const rec of toCreate) {
            await new Promise((res, rej) => {
              db.run(
                `INSERT INTO meps (id, label, givenName, familyName, sortLabel, country, politicalGroup, is_current, source, last_updated)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [rec.mep_id, rec.name, rec.firstName, rec.lastName, rec.name, 'Unknown', rec.political_group, 0, 'historic', now],
                (e) => (e ? rej(e) : res())
              );
            });
          }
          if (log && toCreate.length) log(`Created ${toCreate.length} historic MEPs; linking ${toLink.length} speakers...`);
          const BATCH = 50;
          let linkedSpeeches = 0;
          const updateOne = (item) => new Promise((res, rej) => {
            db.run(`UPDATE individual_speeches SET mep_id = ? WHERE speaker_name = ? AND mep_id IS NULL`, [item.mep_id, item.speaker_name], function(e) {
              if (e) rej(e);
              else res(this.changes);
            });
          });
          for (let i = 0; i < toLink.length; i += BATCH) {
            const batch = toLink.slice(i, i + BATCH);
            const changes = await Promise.all(batch.map(updateOne));
            linkedSpeeches += changes.reduce((a, b) => a + b, 0);
            const saved = Math.min(i + BATCH, toLink.length);
            if (log) log(`Step 4: saved ${saved}/${toLink.length} (${linkedSpeeches} speeches) to DB`);
          }
          resolve({ processedSpeakers: speakers.length, createdHistoricMeps: toCreate.length, linkedSpeeches });
        });
      });
    });
  });
}

/**
 * Upsert MEPs from API into meps table. Does not delete historic MEPs.
 * Each MEP can have is_current (true/false); defaults to true if missing for backward compatibility.
 */
function upsertApiMeps(db, mepsFromApi) {
  return new Promise((resolve, reject) => {
    const stmt = db.prepare(`
      INSERT OR REPLACE INTO meps (id, label, givenName, familyName, sortLabel, country, politicalGroup, is_current, source, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'api', ?)
    `);
    let n = 0;
    const now = Date.now();
    function next(i) {
      if (i >= mepsFromApi.length) {
        stmt.finalize();
        resolve(n);
        return;
      }
      const m = mepsFromApi[i];
      const pid = parseInt(m.identifier, 10);
      if (Number.isNaN(pid)) {
        next(i + 1);
        return;
      }
      const isCurrent = m.is_current !== false ? 1 : 0;
      stmt.run(pid, m.label || '', m.givenName || '', m.familyName || '', m.sortLabel || m.label || '', m['api:country-of-representation'] || null, m['api:political-group'] || null, isCurrent, now, (err) => {
        if (err) {
          stmt.finalize();
          reject(err);
          return;
        }
        n++;
        next(i + 1);
      });
    }
    next(0);
  });
}

module.exports = { checkAndRemoveDuplicates, linkSpeechesToMeps, createHistoricMepsAndLinkSpeeches, createHistoricMepsOnePerPerson, upsertApiMeps };
