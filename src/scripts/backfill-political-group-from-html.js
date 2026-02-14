/**
 * Re-parse sittings whose content is stored as HTML, then update individual_speeches.political_group
 * (and political_group_raw for normalizer). Run the group normalizer after this to set political_group_std.
 *
 * Usage: node src/scripts/backfill-political-group-from-html.js [--dry-run] [--limit N]
 */

const sqlite3 = require('sqlite3').verbose();
const { DB_PATH } = require('../core/db');
const { parseIndividualSpeeches } = require('../server/parse-speeches');

const db = new sqlite3.Database(DB_PATH);

function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}

function runUpdate(sql, params) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this.changes);
    });
  });
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const limitIdx = args.indexOf('--limit');
  const limit = limitIdx >= 0 && args[limitIdx + 1] ? parseInt(args[limitIdx + 1], 10) : null;

  const htmlLike = await run(
    `SELECT id, activity_date, LENGTH(content) as len FROM sittings 
     WHERE LENGTH(content) > 200 AND (content LIKE '%<p %' OR content LIKE '%<div %')
     ORDER BY activity_date DESC ${limit ? `LIMIT ${limit}` : ''}`
  );

  console.log(`Found ${htmlLike.length} sittings with HTML-like content. Dry run: ${dryRun}\n`);

  let updated = 0;
  let sittingsProcessed = 0;
  let skipped = 0;

  for (const row of htmlLike) {
    const content = (await run('SELECT content FROM sittings WHERE id = ?', [row.id]))[0]?.content;
    if (!content) continue;

    const speeches = parseIndividualSpeeches(content, row.id);
    const existing = await run('SELECT id, speech_order, political_group FROM individual_speeches WHERE sitting_id = ? ORDER BY speech_order', [row.id]);

    if (speeches.length !== existing.length) {
      skipped++;
      if (sittingsProcessed < 3) console.log(`  Skip ${row.id}: parsed ${speeches.length} vs DB ${existing.length}`);
      continue;
    }

    for (let i = 0; i < speeches.length; i++) {
      const sp = speeches[i];
      const rec = existing[i];
      const newGroup = sp.political_group || null;
      const oldGroup = rec.political_group;
      if (newGroup !== oldGroup && (newGroup || oldGroup)) {
        if (!dryRun) {
          await runUpdate(
            'UPDATE individual_speeches SET political_group = ?, political_group_raw = ? WHERE id = ?',
            [newGroup, newGroup, rec.id]
          );
        }
        updated++;
      }
    }
    sittingsProcessed++;
    if (sittingsProcessed % 100 === 0) console.log(`  Processed ${sittingsProcessed} sittings, ${updated} speech rows updated`);
  }

  console.log(`\nDone. Sittings processed: ${sittingsProcessed}, speeches updated: ${updated}, sittings skipped (length mismatch): ${skipped}`);
  if (dryRun) console.log('(Dry run — no changes written. Run without --dry-run to apply.)');
  console.log('Run the group normalizer (--apply) then sync MEP affiliations to backfill political_group_std and meps.politicalGroup.');

  db.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
