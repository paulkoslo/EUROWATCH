const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Database connection
const { DB_PATH } = require('../../core/db');
const db = new sqlite3.Database(DB_PATH);

// Language names for better readability
const languageNames = {
  'BG': 'Bulgarian', 'CS': 'Czech', 'DA': 'Danish', 'DE': 'German', 'EL': 'Greek',
  'EN': 'English', 'ES': 'Spanish', 'ET': 'Estonian', 'FI': 'Finnish', 'FR': 'French',
  'HR': 'Croatian', 'HU': 'Hungarian', 'IT': 'Italian', 'LT': 'Lithuanian', 'LV': 'Latvian',
  'MT': 'Maltese', 'NL': 'Dutch', 'PL': 'Polish', 'PT': 'Portuguese', 'RO': 'Romanian',
  'SK': 'Slovak', 'SL': 'Slovenian', 'SV': 'Swedish'
};

// Function to get random speeches for a language
function getRandomSpeeches(language, count = 80) {
  return new Promise((resolve, reject) => {
    db.all(`
      SELECT speaker_name, political_group, speech_content, language
      FROM individual_speeches 
      WHERE language = ?
      ORDER BY RANDOM()
      LIMIT ?
    `, [language, count], (err, rows) => {
      if (err) {
        reject(err);
      } else {
        resolve(rows);
      }
    });
  });
}

// Function to get all languages with their counts
function getAllLanguages() {
  return new Promise((resolve, reject) => {
    db.all(`
      SELECT language, COUNT(*) as count
      FROM individual_speeches 
      WHERE language IS NOT NULL
      GROUP BY language 
      ORDER BY count DESC
    `, [], (err, rows) => {
      if (err) {
        reject(err);
      } else {
        resolve(rows);
      }
    });
  });
}

// Main function to output all samples
async function outputAllLanguageSamples() {
  try {
    console.log('🧪 COMPREHENSIVE LANGUAGE VALIDATION OUTPUT');
    console.log('===========================================');
    console.log('📝 80 random samples from each language');
    console.log('🎯 Please review and assess accuracy\n');
    
    const languages = await getAllLanguages();
    
    console.log('📊 LANGUAGE STATISTICS:');
    languages.forEach((lang, index) => {
      const langName = languageNames[lang.language] || lang.language;
      console.log(`${index + 1}. ${lang.language} (${langName}): ${lang.count} speeches`);
    });
    console.log('\n' + '='.repeat(80) + '\n');
    
    for (const langData of languages) {
      const language = langData.language;
      const count = langData.count;
      const langName = languageNames[language] || language;
      
      // Skip languages with very few speeches
      if (count < 10) {
        console.log(`⏭️ SKIPPING ${language} (${langName}) - only ${count} speeches\n`);
        continue;
      }
      
      console.log(`🌍 LANGUAGE: ${language} (${langName})`);
      console.log(`📊 Total speeches: ${count}`);
      console.log('=' + '='.repeat(60));
      
      const speeches = await getRandomSpeeches(language, Math.min(80, count));
      
      if (speeches.length === 0) {
        console.log(`❌ No speeches found for ${language}\n`);
        continue;
      }
      
      console.log(`📝 Showing ${speeches.length} random samples:\n`);
      
      speeches.forEach((speech, index) => {
        const preview = speech.speech_content ? 
          speech.speech_content.substring(0, 120).replace(/\s+/g, ' ').trim() : 
          'No content';
        const speaker = speech.speaker_name || 'Unknown';
        const group = speech.political_group || 'No group';
        
        console.log(`${(index + 1).toString().padStart(2)}. [${speaker}] (${group})`);
        console.log(`    "${preview}${preview.length >= 120 ? '...' : ''}"`);
        console.log('');
      });
      
      console.log('🔍 ASSESSMENT NEEDED: Are these speeches correctly classified as ' + langName + '?');
      console.log('✅ = Correct, ❌ = Incorrect, ⚠️ = Mixed/Uncertain');
      console.log('\n' + '='.repeat(80) + '\n');
    }
    
    console.log('🎯 VALIDATION COMPLETE');
    console.log('Please review all samples above and provide assessment.');
    console.log('Format: [LANGUAGE_CODE]: [✅/❌/⚠️] [optional notes]');
    console.log('Example: EN: ✅ All samples look correct');
    console.log('Example: EL: ❌ Many non-Greek samples found');
    
  } catch (error) {
    console.error('❌ Error running validation:', error);
  } finally {
    db.close();
  }
}

// Run the validation
outputAllLanguageSamples();