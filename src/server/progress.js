/**
 * Progress bar and demo utilities for CLI and long-running operations.
 * Used by MEP fetch, speech cache, and content fetch to show progress in the terminal.
 */

function createProgressBar(current, total, width = 50) {
  const percentage = Math.round((current / total) * 100);
  const filled = Math.round((current / total) * width);
  const empty = width - filled;
  const bar = '█'.repeat(filled) + '░'.repeat(empty);
  return `[${bar}] ${percentage}% (${current}/${total})`;
}

function demoProgressBars() {
  console.log('🎬 Demo Progress Bars:');
  console.log('MEP Fetch Progress:');
  for (let i = 0; i <= 100; i += 10) {
    const progressBar = createProgressBar(i, 100, 30);
    process.stdout.write(`\r${progressBar} | Rate: ${(i * 2.5).toFixed(1)}/sec`);
    const start = Date.now();
    while (Date.now() - start < 100) { /* wait */ }
  }
  console.log('\nMEP fetch completed!');

  console.log('\nSpeech Fetch Progress:');
  for (let i = 0; i <= 100; i += 5) {
    const progressBar = createProgressBar(i, 100, 50);
    process.stdout.write(`\r${progressBar} | Rate: ${(i * 15.2).toFixed(1)}/sec`);
    const start = Date.now();
    while (Date.now() - start < 50) { /* wait */ }
  }
  console.log('\nSpeech fetch completed!');

  console.log('\nDatabase Caching Progress:');
  for (let i = 0; i <= 100; i += 2) {
    const progressBar = createProgressBar(i, 100, 30);
    process.stdout.write(`\r${progressBar} | Rate: ${(i * 125.8).toFixed(1)}/sec`);
    const start = Date.now();
    while (Date.now() - start < 20) { /* wait */ }
  }
  console.log('\nDatabase caching completed!');
}

module.exports = { createProgressBar, demoProgressBars };
