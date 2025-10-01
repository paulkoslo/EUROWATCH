#!/usr/bin/env node

/**
 * Data-Driven Political Groups Normalizer
 * 
 * Sophisticated pattern-based normalizer that:
 * - Preserves raw text in political_group_raw
 * - Writes canonical codes to political_group_std
 * - Classifies types in political_group_kind
 * - Records reasoning in political_group_reason
 * - Produces audit reports
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

// Database connection
const dbPath = path.resolve(__dirname, 'ep_data.db');
const db = new sqlite3.Database(dbPath);

// Canonical political groups (ONLY these are valid political groups)
const CANONICAL_GROUPS = {
  'PPE': 'PPE',
  'S&D': 'S&D', 
  'ECR': 'ECR',
  'ID': 'ID',
  'Verts/ALE': 'Verts/ALE',
  'Renew': 'Renew',
  'The Left': 'The Left',
  'NI': 'NI',
  'PfE': 'PfE',
  'EFDD': 'EFDD',
  'ESN': 'ESN'
};

// Group synonyms and historical names
const GROUP_SYNONYMS = {
  'EPP': 'PPE',
  'PSE': 'S&D',
  'ALDE': 'Renew',
  'RENEW EUROPE': 'Renew',
  'GREENS/EFA': 'Verts/ALE',
  'GREENS': 'Verts/ALE',
  'EFA': 'Verts/ALE',
  'GUE/NGL': 'The Left',
  'GUE': 'The Left',
  'NGL': 'The Left',
  'ENF': 'ID',
  'PATRIOTS FOR EUROPE': 'PfE',
  'PATRIOTS': 'PfE',
  'EUL/NGL': 'The Left',
  'UEN': 'NI', // Historic group - map to NI
  'IND/DEM': 'NI', // Historic group - map to NI
  'EDD': 'NI' // Historic group - map to NI
};

// Institutional markers (multilingual)
const INSTITUTIONAL_MARKERS = [
  // English
  'member of the commission',
  'vice-president of the commission',
  'high representative',
  'president-in-office of the council',
  'president of the eurogroup',
  'executive vice-president',
  'vice president of the commission',
  'vp/hr',
  'vpc/hr',
  
  // German
  'mitglied der kommission',
  'vizepräsident',
  'vizepräsidentin',
  'hohen vertreterin',
  
  // French
  'vice-président de la commission',
  'haut représentant',
  'président en exercice du conseil',
  
  // Spanish
  'vicepresidente de la comisión',
  'alto representante',
  
  // Swedish
  'vice ordförande för kommissionen',
  
  // Danish
  'formand for rådet'
];

// Parliamentary role markers (multilingual)
const PARLIAMENTARY_MARKERS = [
  // English
  'rapporteur',
  'chair of the delegation',
  'vice-chair of the delegation',
  'committee on',
  'special committee',
  'blue-card question',
  'deputising for',
  'winner of the',
  'sakharov prize',
  
  // Swedish
  'föredragande',
  'utskottet för',
  
  // Danish
  'ordfører for udtalelse',
  'udvalget',
  
  // French
  'rapporteur suppléant',
  'commission',
  
  // Portuguese
  'comissão das',
  
  // Polish
  'komisji',
  
  // Croatian
  'odbora za',
  
  // Romanian
  'autorului'
];

// Language-specific suffix patterns to remove
const LANGUAGE_SUFFIXES = [
  ', in writing',
  ', skriftlig',
  ', por escrito', 
  ', írásban',
  ', în scris',
  ', kirjalikult',
  ', písemně',
  ', per iscritto',
  ', schriftlich'
];

// Multilingual "on behalf of" patterns
const ON_BEHALF_PATTERNS = [
  // English
  /on behalf of (?:the )?(.+?)(?:\s+group)?$/i,
  
  // French
  /au nom du groupe (.+)$/i,
  /au nom de (?:la )?(.+)$/i,
  
  // Italian
  /a nome del gruppo (.+)$/i,
  /a nome della (.+)$/i,
  
  // Spanish
  /en nombre del grupo (.+)$/i,
  
  // Portuguese
  /em nome do grupo (.+)$/i,
  
  // German
  /im namen der (.+?)(?:-fraktion)?$/i,
  /im namen der fraktion (.+)$/i,
  
  // Dutch
  /namens de (.+?)(?:-fractie)?$/i,
  /namens de fractie (.+)$/i,
  /namens de groep (.+)$/i,
  
  // Swedish
  /för (.+?)(?:-gruppen)?$/i,
  
  // Danish
  /for (.+?)(?:-gruppen)?$/i,
  
  // Polish
  /w imieniu grupy (.+)$/i,
  
  // Czech/Slovak
  /za skupinu (.+)$/i,
  
  // Romanian
  /în numele grupului (.+)$/i,
  
  // Greek
  /εξ ονόματος της ομάδας (.+)$/i,
  
  // Irish
  /thar ceann an ghrúpa (.+)$/i,
  
  // Croatian
  /u ime kluba (.+?)(?:-a)?$/i,
  /u ime kluba zastupnika (.+?)(?:-a)?$/i,
  
  // Maltese
  /f'isem il-grupp (.+)$/i
];

/**
 * Normalize text for consistent matching
 */
function normalizeText(text) {
  if (!text) return '';
  
  return text
    // NFKC normalization
    .normalize('NFKC')
    // Convert NBSP and zero-width spaces to regular space
    .replace(/[\u00A0\u200B\u200C\u200D\uFEFF]/g, ' ')
    // Unify dashes and quotes
    .replace(/[–—−]/g, '-')
    .replace(/[""]/g, '"')
    .replace(/['']/g, "'")
    // Collapse whitespace
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Remove language-specific suffixes
 */
function removeSuffixes(text) {
  let cleaned = text;
  for (const suffix of LANGUAGE_SUFFIXES) {
    if (cleaned.toLowerCase().endsWith(suffix.toLowerCase())) {
      cleaned = cleaned.slice(0, -suffix.length).trim();
    }
  }
  return cleaned;
}

/**
 * Extract group code from parentheses or trailing patterns
 */
function extractParenthesesCode(text) {
  // Match (CODE) or CODE)
  const parenMatch = text.match(/\(([^)]+)\)|\s+([A-Z&/]+)\)$/);
  if (parenMatch) {
    const code = (parenMatch[1] || parenMatch[2]).trim();
    return normalizeGroupCode(code);
  }
  return null;
}

/**
 * Extract group from "on behalf of" patterns
 */
function extractOnBehalfGroup(text) {
  for (const pattern of ON_BEHALF_PATTERNS) {
    const match = text.match(pattern);
    if (match) {
      const extracted = match[1].trim();
      return normalizeGroupCode(extracted);
    }
  }
  return null;
}

/**
 * Check for institutional markers
 */
function hasInstitutionalMarkers(text) {
  const normalized = text.toLowerCase();
  return INSTITUTIONAL_MARKERS.some(marker => normalized.includes(marker));
}

/**
 * Check for parliamentary role markers
 */
function hasParliamentaryMarkers(text) {
  const normalized = text.toLowerCase();
  return PARLIAMENTARY_MARKERS.some(marker => normalized.includes(marker));
}

/**
 * Normalize group code using synonyms
 */
function normalizeGroupCode(code) {
  if (!code) return null;
  
  const normalized = code.toUpperCase().trim();
  
  // Check canonical groups first
  if (CANONICAL_GROUPS[normalized]) {
    return normalized;
  }
  
  // Check synonyms
  if (GROUP_SYNONYMS[normalized]) {
    return GROUP_SYNONYMS[normalized];
  }
  
  // Check for partial matches
  for (const [synonym, canonical] of Object.entries(GROUP_SYNONYMS)) {
    if (normalized.includes(synonym) || synonym.includes(normalized)) {
      return canonical;
    }
  }
  
  // Check canonical groups for partial matches
  for (const canonical of Object.keys(CANONICAL_GROUPS)) {
    if (normalized.includes(canonical) || canonical.includes(normalized)) {
      return canonical;
    }
  }
  
  return null;
}

/**
 * Find direct group tokens in text (only if not part of a sentence)
 */
function findDirectGroupToken(text) {
  const normalized = text.toLowerCase();
  
  // Don't match tokens in long sentences (>8 words)
  const wordCount = text.split(/\s+/).length;
  if (wordCount > 8) {
    return null;
  }
  
  // Check for direct mentions of group codes (whole word matches)
  for (const canonical of Object.keys(CANONICAL_GROUPS)) {
    const regex = new RegExp(`\\b${canonical.toLowerCase()}\\b`);
    if (regex.test(normalized)) {
      return canonical;
    }
  }
  
  // Check synonyms (whole word matches)
  for (const [synonym, canonical] of Object.entries(GROUP_SYNONYMS)) {
    const regex = new RegExp(`\\b${synonym.toLowerCase()}\\b`);
    if (regex.test(normalized)) {
      return canonical;
    }
  }
  
  return null;
}

/**
 * Main normalization function
 */
function normalizePoliticalGroup(rawText) {
  if (!rawText || rawText.trim() === '') {
    return {
      std: 'NI',
      kind: 'unknown',
      reason: 'empty_input'
    };
  }
  
  // Step 1: Normalize text
  let text = normalizeText(rawText);
  text = removeSuffixes(text);
  
  // Step 2: Check if it's already a canonical group
  const directCanonical = CANONICAL_GROUPS[text];
  if (directCanonical) {
    return {
      std: directCanonical,
      kind: 'group',
      reason: 'direct_canonical'
    };
  }
  
  // Step 3: Check institutional markers (highest priority)
  if (hasInstitutionalMarkers(text)) {
    return {
      std: 'NI', // Institutional roles are Non-Attached for political group purposes
      kind: 'institution',
      reason: 'institutional_markers'
    };
  }
  
  // Step 4: Check parliamentary role markers
  if (hasParliamentaryMarkers(text)) {
    return {
      std: 'NI', // Parliamentary roles are Non-Attached for political group purposes
      kind: 'role',
      reason: 'parliamentary_markers'
    };
  }
  
  // Step 5: Extract from parentheses
  const parenCode = extractParenthesesCode(text);
  if (parenCode) {
    return {
      std: parenCode,
      kind: 'group',
      reason: 'parentheses_extraction'
    };
  }
  
  // Step 6: Extract from "on behalf of" patterns
  const onBehalfGroup = extractOnBehalfGroup(text);
  if (onBehalfGroup) {
    return {
      std: onBehalfGroup,
      kind: 'group',
      reason: 'on_behalf_pattern'
    };
  }
  
  // Step 7: Check if it looks like a sentence (>8 words) - do this BEFORE token matching
  const wordCount = text.split(/\s+/).length;
  if (wordCount > 8) {
    return {
      std: 'NI',
      kind: 'unknown',
      reason: 'looks_like_sentence'
    };
  }
  
  // Step 8: Find direct group tokens (only for shorter text)
  const directToken = findDirectGroupToken(text);
  if (directToken) {
    return {
      std: directToken,
      kind: 'group',
      reason: 'direct_token'
    };
  }
  
  // Step 9: Check if it's a bare code/synonym
  const bareCode = normalizeGroupCode(text);
  if (bareCode) {
    return {
      std: bareCode,
      kind: 'group',
      reason: 'bare_code'
    };
  }
  
  // Step 10: No match found
  return {
    std: 'NI',
    kind: 'unknown',
    reason: 'no_match'
  };
}

/**
 * Add new columns to database if they don't exist
 */
async function ensureColumns() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // Check existing columns
      db.all("PRAGMA table_info(individual_speeches)", (err, columns) => {
        if (err) {
          reject(err);
          return;
        }
        
        const columnNames = columns.map(col => col.name);
        const columnsToAdd = [];
        
        if (!columnNames.includes('political_group_raw')) {
          columnsToAdd.push('political_group_raw TEXT');
        }
        if (!columnNames.includes('political_group_std')) {
          columnsToAdd.push('political_group_std TEXT');
        }
        if (!columnNames.includes('political_group_kind')) {
          columnsToAdd.push('political_group_kind TEXT');
        }
        if (!columnNames.includes('political_group_reason')) {
          columnsToAdd.push('political_group_reason TEXT');
        }
        
        if (columnsToAdd.length === 0) {
          console.log('✅ [SCHEMA] All columns already exist');
          resolve();
          return;
        }
        
        console.log(`⚙️ [SCHEMA] Adding ${columnsToAdd.length} new columns...`);
        
        const addColumnPromises = columnsToAdd.map(columnDef => {
          return new Promise((addResolve, addReject) => {
            db.run(`ALTER TABLE individual_speeches ADD COLUMN ${columnDef}`, (err) => {
              if (err) {
                console.error(`❌ [SCHEMA] Error adding column ${columnDef}:`, err);
                addReject(err);
              } else {
                console.log(`✅ [SCHEMA] Added column: ${columnDef}`);
                addResolve();
              }
            });
          });
        });
        
        Promise.all(addColumnPromises)
          .then(() => resolve())
          .catch(reject);
      });
    });
  });
}

/**
 * Analyze and normalize all political groups
 */
async function analyzeAndNormalize(options = {}) {
  const { apply = false, overwriteLegacy = false, limit = null, fromId = null } = options;
  
  return new Promise((resolve, reject) => {
    console.log('🧹 [NORMALIZER] Starting data-driven political groups analysis...');
    
    // Build query with optional limits
    let query = `
      SELECT DISTINCT political_group, COUNT(*) as usage_count
      FROM individual_speeches 
      WHERE COALESCE(political_group, '') != ''
    `;
    
    if (fromId) query += ` AND id >= ${fromId}`;
    query += ` GROUP BY political_group ORDER BY usage_count DESC`;
    if (limit) query += ` LIMIT ${limit}`;
    
    db.all(query, async (err, distinctGroups) => {
      if (err) {
        console.error('❌ [NORMALIZER] Error fetching distinct groups:', err);
        reject(err);
        return;
      }
      
      console.log(`📊 [NORMALIZER] Found ${distinctGroups.length} distinct political group variations`);
      
      // Analyze each distinct group
      const results = {
        totalDistinct: distinctGroups.length,
        mapped: 0,
        unknown: 0,
        byKind: { group: 0, institution: 0, role: 0, unknown: 0 },
        byStd: {},
        unknowns: [],
        reasons: {}
      };
      
      const mappings = new Map();
      
      for (const group of distinctGroups) {
        const normalized = normalizePoliticalGroup(group.political_group);
        
        mappings.set(group.political_group, {
          ...normalized,
          usage_count: group.usage_count
        });
        
        // Update statistics
        if (normalized.std !== 'NI' || normalized.kind === 'group') {
          results.mapped++;
        } else {
          results.unknown++;
          results.unknowns.push({
            raw: group.political_group,
            count: group.usage_count,
            reason: normalized.reason
          });
        }
        
        results.byKind[normalized.kind]++;
        results.byStd[normalized.std] = (results.byStd[normalized.std] || 0) + group.usage_count;
        results.reasons[normalized.reason] = (results.reasons[normalized.reason] || 0) + 1;
      }
      
      // Print analysis report
      console.log('\n📊 [NORMALIZER] Analysis Results:');
      console.log(`   📈 Coverage: ${results.mapped}/${results.totalDistinct} (${(results.mapped/results.totalDistinct*100).toFixed(1)}%) mapped`);
      console.log(`   🔍 Unknown: ${results.unknown} variations`);
      
      console.log('\n📊 [NORMALIZER] By Kind:');
      Object.entries(results.byKind).forEach(([kind, count]) => {
        console.log(`   ${kind}: ${count} variations`);
      });
      
      console.log('\n📊 [NORMALIZER] By Standard Group (speech count):');
      Object.entries(results.byStd)
        .sort(([,a], [,b]) => b - a)
        .forEach(([std, count]) => {
          console.log(`   ${std}: ${count.toLocaleString()} speeches`);
        });
      
      if (results.unknowns.length > 0) {
        console.log('\n⚠️ [NORMALIZER] Top Unknown Variations:');
        results.unknowns
          .sort((a, b) => b.count - a.count)
          .slice(0, 10)
          .forEach(unknown => {
            console.log(`   "${unknown.raw}" (${unknown.count} uses) - ${unknown.reason}`);
          });
      }
      
      if (apply) {
        console.log('\n🔄 [NORMALIZER] Applying normalizations to database...');
        
        try {
          await applyNormalizations(mappings, overwriteLegacy);
          console.log('✅ [NORMALIZER] Database updated successfully');
        } catch (error) {
          console.error('❌ [NORMALIZER] Error applying normalizations:', error);
          reject(error);
          return;
        }
      } else {
        console.log('\n💡 [NORMALIZER] Dry run complete. Use --apply to update database.');
      }
      
      resolve(results);
    });
  });
}

/**
 * Apply normalizations to database
 */
async function applyNormalizations(mappings, overwriteLegacy = false) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run('BEGIN TRANSACTION');
      
      let processed = 0;
      let totalUpdated = 0;
      const startTime = Date.now();
      
      // First, copy original values to _raw column if not already done
      db.run(`
        UPDATE individual_speeches 
        SET political_group_raw = political_group 
        WHERE political_group_raw IS NULL AND political_group IS NOT NULL
      `, function(err) {
        if (err) {
          console.error('❌ [NORMALIZER] Error copying raw values:', err);
          db.run('ROLLBACK');
          reject(err);
          return;
        }
        
        console.log(`📋 [NORMALIZER] Copied ${this.changes} raw values`);
        
        // Apply all mappings
        const updatePromises = Array.from(mappings.entries()).map(([rawText, normalized]) => {
          return new Promise((updateResolve, updateReject) => {
            const updateQuery = overwriteLegacy 
              ? `UPDATE individual_speeches 
                 SET political_group_std = ?, 
                     political_group_kind = ?, 
                     political_group_reason = ?,
                     political_group = ?
                 WHERE political_group_raw = ?`
              : `UPDATE individual_speeches 
                 SET political_group_std = ?, 
                     political_group_kind = ?, 
                     political_group_reason = ?
                 WHERE political_group_raw = ?`;
            
            const params = overwriteLegacy 
              ? [normalized.std, normalized.kind, normalized.reason, normalized.std, rawText]
              : [normalized.std, normalized.kind, normalized.reason, rawText];
            
            db.run(updateQuery, params, function(err) {
              if (err) {
                console.error(`❌ [NORMALIZER] Error updating "${rawText}":`, err);
                updateReject(err);
              } else {
                if (this.changes > 0) {
                  totalUpdated += this.changes;
                }
                processed++;
                
                if (processed % 50 === 0 || processed === mappings.size) {
                  const elapsed = (Date.now() - startTime) / 1000;
                  const rate = processed / elapsed;
                  console.log(`🔄 [NORMALIZER] Progress: ${processed}/${mappings.size} mappings | ${totalUpdated} records | Rate: ${rate.toFixed(1)}/sec`);
                }
                
                updateResolve();
              }
            });
          });
        });
        
        Promise.all(updatePromises)
          .then(() => {
            db.run('COMMIT', (err) => {
              if (err) {
                console.error('❌ [NORMALIZER] Error committing transaction:', err);
                reject(err);
              } else {
                const totalTime = (Date.now() - startTime) / 1000;
                console.log(`\n✅ [NORMALIZER] Applied ${mappings.size} normalizations in ${totalTime.toFixed(1)}s`);
                console.log(`   🔄 Updated ${totalUpdated} speech records`);
                resolve();
              }
            });
          })
          .catch((error) => {
            db.run('ROLLBACK');
            reject(error);
          });
      });
    });
  });
}

/**
 * Generate audit report
 */
async function generateReport(results, outputFile = null) {
  const report = {
    timestamp: new Date().toISOString(),
    summary: {
      totalDistinctInputs: results.totalDistinct,
      mappedCount: results.mapped,
      unknownCount: results.unknown,
      coveragePercent: (results.mapped / results.totalDistinct * 100).toFixed(1)
    },
    distributionByKind: results.byKind,
    distributionByStandard: results.byStd,
    topUnknowns: results.unknowns.slice(0, 20),
    reasonsUsed: results.reasons,
    suggestedRules: generateSuggestions(results.unknowns)
  };
  
  if (outputFile) {
    fs.writeFileSync(outputFile, JSON.stringify(report, null, 2));
    console.log(`📄 [REPORT] Written to ${outputFile}`);
  }
  
  return report;
}

/**
 * Generate suggestions for unknown entries
 */
function generateSuggestions(unknowns) {
  const suggestions = [];
  
  for (const unknown of unknowns.slice(0, 10)) {
    const text = unknown.raw.toLowerCase();
    let suggestion = 'Unknown';
    
    if (text.includes('commission') || text.includes('council') || text.includes('representative')) {
      suggestion = 'Institution';
    } else if (text.includes('committee') || text.includes('rapporteur') || text.includes('delegation')) {
      suggestion = 'Parliamentary Role';
    } else if (text.includes('group') || text.includes('grupo') || text.includes('groupe')) {
      suggestion = 'Likely Political Group - check for party code';
    }
    
    suggestions.push({
      raw: unknown.raw,
      count: unknown.count,
      suggestion: suggestion
    });
  }
  
  return suggestions;
}

/**
 * CLI argument parsing
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    apply: args.includes('--apply'),
    overwriteLegacy: args.includes('--overwrite-legacy'),
    dryRun: args.includes('--dry-run'),
    report: null,
    limit: null,
    fromId: null
  };
  
  const reportIndex = args.findIndex(arg => arg === '--report');
  if (reportIndex !== -1 && args[reportIndex + 1]) {
    options.report = args[reportIndex + 1];
  }
  
  const limitIndex = args.findIndex(arg => arg === '--limit');
  if (limitIndex !== -1 && args[limitIndex + 1]) {
    options.limit = parseInt(args[limitIndex + 1]);
  }
  
  const fromIdIndex = args.findIndex(arg => arg === '--from-id');
  if (fromIdIndex !== -1 && args[fromIdIndex + 1]) {
    options.fromId = parseInt(args[fromIdIndex + 1]);
  }
  
  return options;
}

/**
 * Run unit tests
 */
function runTests() {
  console.log('🧪 [TESTS] Running unit tests...');
  
  const testCases = [
    // Parentheses extraction
    { input: 'Anna-Michelle (PPE)', expected: { std: 'PPE', kind: 'group', reason: 'parentheses_extraction' } },
    { input: 'Some text ECR)', expected: { std: 'ECR', kind: 'group', reason: 'parentheses_extraction' } },
    
    // On behalf patterns
    { input: 'on behalf of the S&D Group', expected: { std: 'S&D', kind: 'group', reason: 'on_behalf_pattern' } },
    { input: 'au nom du groupe Renew', expected: { std: 'Renew', kind: 'group', reason: 'on_behalf_pattern' } },
    { input: 'im Namen der PPE-Fraktion', expected: { std: 'PPE', kind: 'group', reason: 'on_behalf_pattern' } },
    
    // Institutional detection
    { input: 'Member of the Commission, on behalf of VP/HR', expected: { kind: 'institution' } },
    { input: 'Vice-President of the Commission', expected: { kind: 'institution' } },
    
    // Parliamentary role detection
    { input: 'rapporteur for the Committee on Petitions', expected: { kind: 'role' } },
    { input: 'Chair of the Delegation for Relations', expected: { kind: 'role' } },
    
    // Direct canonical
    { input: 'PPE', expected: { std: 'PPE', kind: 'group', reason: 'direct_canonical' } },
    { input: 'Verts/ALE', expected: { std: 'Verts/ALE', kind: 'group', reason: 'direct_canonical' } },
    
    // Synonyms
    { input: 'ALDE', expected: { std: 'Renew', kind: 'group', reason: 'direct_token' } },
    { input: 'GUE/NGL', expected: { std: 'The Left', kind: 'group', reason: 'direct_token' } },
    
    // Long sentences
    { input: 'I believe the single-use plastics directive is unfairly treating expanded polystyrene', expected: { kind: 'unknown', reason: 'looks_like_sentence' } }
  ];
  
  let passed = 0;
  let failed = 0;
  
  for (const test of testCases) {
    const result = normalizePoliticalGroup(test.input);
    const success = Object.entries(test.expected).every(([key, value]) => result[key] === value);
    
    if (success) {
      passed++;
      console.log(`✅ [TEST] "${test.input}" → ${result.std} (${result.kind})`);
    } else {
      failed++;
      console.log(`❌ [TEST] "${test.input}"`);
      console.log(`   Expected: ${JSON.stringify(test.expected)}`);
      console.log(`   Got: ${JSON.stringify(result)}`);
    }
  }
  
  console.log(`\n🧪 [TESTS] Results: ${passed} passed, ${failed} failed`);
  return failed === 0;
}

/**
 * Main execution
 */
async function main() {
  const options = parseArgs();
  
  console.log('🚀 [MAIN] Political Groups Normalizer');
  console.log(`   Mode: ${options.apply ? 'APPLY' : 'DRY RUN'}`);
  console.log(`   Overwrite Legacy: ${options.overwriteLegacy}`);
  console.log(`   Report: ${options.report || 'console only'}`);
  
  try {
    // Run tests first
    const testsPass = runTests();
    if (!testsPass) {
      console.log('❌ [MAIN] Tests failed, aborting');
      process.exit(1);
    }
    
    // Ensure database schema
    await ensureColumns();
    
    // Analyze and normalize
    const results = await analyzeAndNormalize(options);
    
    // Generate report
    const report = await generateReport(results, options.report);
    
    console.log(`\n🎉 [MAIN] Completed successfully!`);
    console.log(`   📊 ${results.mapped}/${results.totalDistinct} variations mapped (${report.summary.coveragePercent}%)`);
    console.log(`   🎯 Reduced to ${Object.keys(results.byStd).length} canonical groups`);
    
    if (results.unknowns.length > 0) {
      console.log(`   ⚠️ ${results.unknowns.length} unknowns need manual review`);
    }
    
  } catch (error) {
    console.error('❌ [MAIN] Error:', error);
    process.exit(1);
  } finally {
    db.close();
  }
}

// Run if called directly
if (require.main === module) {
  main();
}

module.exports = { 
  normalizePoliticalGroup, 
  analyzeAndNormalize, 
  CANONICAL_GROUPS,
  GROUP_SYNONYMS 
};
