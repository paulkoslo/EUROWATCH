/**
 * Analyze MEPs that show as "No affiliation in speeches" / Unknown:
 * - Find such MEPs who have at least one speech
 * - For a few of them, list speeches with raw DB fields (political_group, political_group_std, sitting date)
 * So we can compare with Parliament source and see if we're not parsing or not normalizing.
 *
 * Run: node src/scripts/analyze-unknown-affiliation.js
 */

const sqlite3 = require('sqlite3').verbose();
const { DB_PATH } = require('../core/db');

const db = new sqlite3.Database(DB_PATH);

function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
    });
  });
}

async function main() {
  console.log('=== MEPs with Unknown/No affiliation (would show in pie as "No affiliation in speeches") ===\n');

  // MEPs that have NO speech with political_group_std set (so they get fallback label)
  // and we only care about those who HAVE at least one speech (so we can inspect)
  const unknownWithSpeeches = await run(`
    SELECT m.id, m.label, m.politicalGroup AS mep_political_group,
           COUNT(i.id) AS speech_count,
           SUM(CASE WHEN COALESCE(TRIM(i.political_group), '') != '' THEN 1 ELSE 0 END) AS speeches_with_raw_group,
           SUM(CASE WHEN COALESCE(TRIM(i.political_group_std), '') != '' THEN 1 ELSE 0 END) AS speeches_with_std_group
    FROM meps m
    JOIN individual_speeches i ON i.mep_id = m.id
    WHERE (m.politicalGroup IS NULL OR TRIM(m.politicalGroup) = '')
    GROUP BY m.id
    HAVING speeches_with_std_group = 0
    ORDER BY speech_count DESC
    LIMIT 30
  `);

  console.log(`Found ${unknownWithSpeeches.length} example MEPs (unknown affiliation but have speeches). Top 5:\n`);
  unknownWithSpeeches.slice(0, 5).forEach((m, i) => {
    console.log(`${i + 1}. ${m.label} (id=${m.id})`);
    console.log(`   meps.politicalGroup: ${JSON.stringify(m.mep_political_group)}`);
    console.log(`   speech_count: ${m.speech_count}, with raw political_group: ${m.speeches_with_raw_group}, with political_group_std: ${m.speeches_with_std_group}`);
    console.log('');
  });

  // Pick 3 MEPs and show their speeches with full DB fields
  const toInspect = unknownWithSpeeches.slice(0, 3);
  console.log('--- Detailed speech records for 3 unknown-affiliation MEPs ---\n');

  for (const mep of toInspect) {
    const speeches = await run(`
      SELECT i.id AS speech_id, i.sitting_id, i.speaker_name,
             i.political_group AS raw_political_group,
             i.political_group_std,
             i.political_group_raw,
             i.political_group_kind,
             s.activity_date AS sitting_date,
             substr(i.speech_content, 1, 80) AS content_preview
      FROM individual_speeches i
      LEFT JOIN sittings s ON s.id = i.sitting_id
      WHERE i.mep_id = ?
      ORDER BY s.activity_date DESC
      LIMIT 15
    `, [mep.id]);

    console.log(`\n### ${mep.label} (mep_id=${mep.id}) — ${mep.speech_count} speeches total, showing up to 15\n`);
    speeches.forEach((sp, idx) => {
      console.log(`  Speech ${idx + 1} | sitting: ${sp.sitting_id} | date: ${sp.sitting_date || 'N/A'}`);
      console.log(`    speaker_name: ${JSON.stringify(sp.speaker_name)}`);
      console.log(`    political_group (raw in DB): ${JSON.stringify(sp.raw_political_group)}`);
      console.log(`    political_group_std: ${JSON.stringify(sp.political_group_std)} | political_group_raw: ${JSON.stringify(sp.political_group_raw)} | kind: ${JSON.stringify(sp.political_group_kind)}`);
      console.log(`    content_preview: ${(sp.content_preview || '').replace(/\n/g, ' ')}`);
      console.log('');
    });
  }

  // Summary: how many speeches have empty political_group (parser didn't capture) vs non-empty but no _std (normalizer didn't map)
  const summary = await run(`
    SELECT
      COUNT(*) AS total_speeches_linked,
      SUM(CASE WHEN COALESCE(TRIM(political_group), '') = '' THEN 1 ELSE 0 END) AS raw_empty,
      SUM(CASE WHEN COALESCE(TRIM(political_group), '') != '' AND COALESCE(TRIM(political_group_std), '') = '' THEN 1 ELSE 0 END) AS raw_set_std_empty,
      SUM(CASE WHEN COALESCE(TRIM(political_group_std), '') != '' THEN 1 ELSE 0 END) AS std_set
    FROM individual_speeches
    WHERE mep_id IS NOT NULL
  `);
  console.log('\n--- Summary (all linked speeches) ---');
  console.log(summary[0]);
  console.log('\n  raw_empty = parser did not store political_group');
  console.log('  raw_set_std_empty = parser stored it but normalizer did not set political_group_std');
  console.log('  std_set = have standardized group\n');

  // Sample raw sitting content for one sitting that has "Bordes" (unknown MEP) — see exact line format
  const sampleSitting = await run(
    'SELECT id, activity_date, content FROM sittings WHERE id = ? AND LENGTH(content) > 100',
    ['sitting-2004-04-22']
  );
  if (sampleSitting.length > 0 && sampleSitting[0].content) {
    const lines = sampleSitting[0].content.split('\n').map((l) => l.trim()).filter(Boolean);
    const speakerLines = lines.filter((l) => l.includes('–') || l.includes('−'));
    const bordesLines = lines.filter((l) => /bordes|barón|de palacio/i.test(l));
    console.log('--- Raw line format sample (sitting-2004-04-22) ---');
    console.log('First 8 speaker-style lines (contain "–"):');
    speakerLines.slice(0, 8).forEach((l, i) => console.log(`  ${i + 1}. ${l.slice(0, 120)}${l.length > 120 ? '...' : ''}`));
    console.log('\nLines mentioning Bordes / Barón / De Palacio:');
    bordesLines.slice(0, 5).forEach((l, i) => console.log(`  ${i + 1}. ${l.slice(0, 200)}${l.length > 200 ? '...' : ''}`));
    if (bordesLines.length === 0) {
      console.log('  (none found — content may use different spelling or structure)');
    }
    console.log('');
  }

  db.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
