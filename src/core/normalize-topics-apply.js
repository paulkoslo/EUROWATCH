/**
 * Apply macro topic normalization rules to the database.
 * Reads rules (from memory or file) and UPDATEs individual_speeches.macro_topic.
 */

const path = require('path');
const fs = require('fs');

const DATA_DIR = path.join(__dirname, '..', '..', 'data');
const RULES_PATH = path.join(DATA_DIR, 'macro-topic-rules.json');

/**
 * @param {string} [filePath] - Optional path to rules JSON
 * @returns {Array<{ canonical: string, variants: string[] }>}
 */
function loadRules(filePath = RULES_PATH) {
  if (!fs.existsSync(filePath)) return [];
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const data = JSON.parse(raw);
    const arr = Array.isArray(data) ? data : (data.rules ? data.rules : []);
    return arr.filter(
      (r) => r && r.canonical && Array.isArray(r.variants) && r.variants.length > 0
    );
  } catch (e) {
    throw new Error(`Failed to load rules from ${filePath}: ${e.message}`);
  }
}

/**
 * @param {Array<{ canonical: string, variants: string[] }>} rules
 * @param {string} [filePath]
 */
function saveRules(rules, filePath = RULES_PATH) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(rules, null, 2), 'utf8');
}

/**
 * Apply rules to the database: for each rule, set macro_topic = canonical for all speeches with macro_topic IN variants.
 * @param {object} db - SQLite3 database
 * @param {Array<{ canonical: string, variants: string[] }>} rules
 * @param {(msg: string) => void} log
 * @returns {Promise<{ updated: number, byRule: Array<{ canonical: string, updated: number }> }>}
 */
function applyRules(db, rules, log = () => {}) {
  return new Promise((resolve, reject) => {
    if (!rules.length) {
      return resolve({ updated: 0, byRule: [] });
    }

    let totalUpdated = 0;
    const byRule = [];
    const run = (index) => {
      if (index >= rules.length) {
        return resolve({ updated: totalUpdated, byRule });
      }

      const rule = rules[index];
      const placeholders = rule.variants.map(() => '?').join(',');
      const sql = `UPDATE individual_speeches SET macro_topic = ? WHERE macro_topic IN (${placeholders})`;
      const params = [rule.canonical, ...rule.variants];

      db.run(sql, params, function (err) {
        if (err) return reject(err);
        const updated = this.changes;
        totalUpdated += updated;
        byRule.push({ canonical: rule.canonical, updated });
        if (updated > 0) log(`  → ${rule.canonical}: ${updated} speech(es)`);
        run(index + 1);
      });
    };

    log(`[NORMALIZE-APPLY] Applying ${rules.length} rule(s)...`);
    run(0);
  });
}

module.exports = {
  RULES_PATH,
  loadRules,
  saveRules,
  applyRules
};
