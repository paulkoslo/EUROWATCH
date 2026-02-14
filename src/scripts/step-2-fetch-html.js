#!/usr/bin/env node
/**
 * Step 2: Fetch HTML for a sitting date
 *
 * Usage: node scripts/step-2-fetch-html.js YYYY-MM-DD
 * Output: prints raw HTML to stdout (or writes path if --out file)
 */

require('dotenv').config();
const fs = require('fs');
const { fetchSittingHTML } = require('../core/parliament-fetch');

async function run(date, options = {}) {
  const log = options.log || (() => {});
  if (!date) throw new Error('Date required (YYYY-MM-DD)');

  log(`  Fetching HTML for ${date}...`);
  const html = await fetchSittingHTML(date);
  if (!html || html.length < 500) throw new Error('HTML fetch failed or content too short');
  log(`  Fetched ${html.length} chars`);
  return html;
}

if (require.main === module) {
  const date = process.argv[2];
  const outIdx = process.argv.indexOf('--out');
  const outFile = outIdx !== -1 && process.argv[outIdx + 1] ? process.argv[outIdx + 1] : null;

  run(date)
    .then(html => {
      if (outFile) {
        fs.writeFileSync(outFile, html);
        console.log(`Written to ${outFile}`);
      } else {
        process.stdout.write(html);
      }
      process.exit(0);
    })
    .catch(err => {
      console.error('Error:', err.message);
      process.exit(1);
    });
}

module.exports = { run };
