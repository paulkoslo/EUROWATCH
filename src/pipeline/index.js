#!/usr/bin/env node
/**
 * EUROWATCH pipeline — single entry point
 *
 *   node pipeline              # refresh (newest sitting)
 *   node pipeline --full       # bulk (1999-07-20 → today)
 *   node pipeline --full --start 1995-01-01 --end 2024-12-31
 *   node pipeline --quiet      # less output
 */

const { runRefresh } = require('./refresh');
const { runBulk } = require('./bulk');

const EARLIEST = '1999-07-20'; // oldest digitized HTML on Europarl

function formatDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function parseArgs() {
  const args = process.argv.slice(2);
  const quiet = args.includes('--quiet');
  const full = args.includes('--full');
  let start = null, end = null;
  const skipExisting = !args.includes('--no-skip-existing');
  const si = args.indexOf('--start'), ei = args.indexOf('--end');
  if (si >= 0 && args[si + 1]) start = args[si + 1];
  if (ei >= 0 && args[ei + 1]) end = args[ei + 1];
  if (full && !start) start = EARLIEST;
  if (full && !end) end = formatDate(new Date());
  return { full, start, end, skipExisting, quiet };
}

async function main() {
  const { full, start, end, skipExisting, quiet } = parseArgs();
  const log = quiet ? (m) => { if (/✓|✗|❌|complete|→/.test(String(m))) console.log(m); } : console.log;

  if (full) {
    const result = await runBulk({ startDate: start, endDate: end, skipExisting, log });
    process.exit(result.processed > 0 || result.failed === 0 ? 0 : 1);
  } else {
    const result = await runRefresh({ log, quiet });
    process.exit(result.success ? 0 : 1);
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
  });
}

// For server: runRefresh is the "test pipeline" action
module.exports = { runRefresh, runBulk };
