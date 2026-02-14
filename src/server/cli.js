/**
 * Handles CLI flags (e.g. --demo-progress, --fetch-content). Returns true if a flag
 * was handled (caller can skip starting the server; handlers call process.exit).
 */
const { demoProgressBars } = require('./progress');
const { fetchSpeechContentFromHTML } = require('./fetch-speech-html');
const { fetchAllMeps } = require('./meps-api');
const { parseIndividualSpeeches, storeIndividualSpeeches, parseRecentSpeeches, parseAllSpeechesWithContent } = require('./parse-speeches');

function handleCli(db) {
  const speechesFetch = require('./speeches-fetch');

  if (process.argv.includes('--demo-progress')) {
    console.log('🎬 Running progress bar demo...\n');
    demoProgressBars();
    console.log('\nDemo completed! Run without --demo-progress to start the server.');
    process.exit(0);
  }

  if (process.argv.includes('--test-content')) {
    console.log('🧪 Testing content fetching...\n');
    (async () => {
      try {
        const testDate = '2025-07-10';
        const testSpeechId = 'eli/dl/event/MTG-PL-2025-07-10-OTH-2017033239347';
        const content = await fetchSpeechContentFromHTML(testDate, testSpeechId);
        console.log(`Content length: ${content.length} characters`);
        console.log(`Content preview: ${content.slice(0, 200)}...`);
      } catch (error) {
        console.error('Test failed:', error);
      }
      process.exit(0);
    })();
    return true;
  }

  if (process.argv.includes('--fetch-content')) {
    console.log('Force fetching content for all speeches...\n');
    (async () => {
      try {
        const successCount = await speechesFetch.addContentToExistingSpeeches(db);
        console.log(`\nContent fetching completed! Updated ${successCount} speeches.`);
      } catch (error) {
        console.error('Content fetching failed:', error);
        process.exit(1);
      }
      process.exit(0);
    })();
    return true;
  }

  if (process.argv.includes('--refetch-all-speeches')) {
    console.log('COMPLETE SITTINGS REFETCH - This will clear and rebuild the entire sittings database...\n');
    (async () => {
      try {
        db.run('DELETE FROM sittings', (err) => {
          if (err) {
            console.error('Error clearing speeches:', err);
            process.exit(1);
          }
          speechesFetch.cacheAllSpeeches(db).then((count) => {
            console.log(`\nCOMPLETE REFETCH COMPLETED! Cached ${count} speeches.`);
            process.exit(0);
          }).catch((error) => {
            console.error('Complete refetch failed:', error);
            process.exit(1);
          });
        });
      } catch (error) {
        console.error('Refetch failed:', error);
        process.exit(1);
      }
    })();
    return true;
  }

  if (process.argv.includes('--parse-speeches')) {
    const sittingId = process.argv[process.argv.indexOf('--parse-speeches') + 1];
    if (!sittingId) {
      console.log('Please provide a sitting ID: node server.js --parse-speeches <sitting_id>');
      process.exit(1);
    }
    console.log(`Parsing individual speeches for sitting: ${sittingId}\n`);
    (async () => {
      try {
        db.get('SELECT content FROM sittings WHERE id = ?', [sittingId], async (err, row) => {
          if (err || !row?.content) {
            console.error(row ? 'No content found for sitting:' : 'Database error:', sittingId);
            process.exit(1);
          }
          const individualSpeeches = parseIndividualSpeeches(row.content, sittingId);
          const stored = await storeIndividualSpeeches(db, individualSpeeches);
          console.log(`\nSuccessfully stored ${stored} individual speeches!`);
          process.exit(0);
        });
      } catch (error) {
        console.error('Parse failed:', error);
        process.exit(1);
      }
    })();
    return true;
  }

  if (process.argv.includes('--parse-all-speeches')) {
    (async () => {
      try {
        await parseAllSpeechesWithContent(db);
        console.log('[BULK PARSE] All speeches parsed successfully!');
      } catch (error) {
        console.error('[BULK PARSE] Error:', error);
        process.exit(1);
      }
      process.exit(0);
    })();
    return true;
  }

  if (process.argv.includes('--parse-recent-speeches')) {
    (async () => {
      try {
        await parseRecentSpeeches(db);
        console.log('[PARSE RECENT] Recent speeches parsed successfully!');
      } catch (error) {
        console.error('[PARSE RECENT] Error:', error);
        process.exit(1);
      }
      process.exit(0);
    })();
    return true;
  }

  return false;
}

module.exports = { handleCli };
