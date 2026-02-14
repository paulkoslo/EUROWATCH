/**
 * Sync meps.politicalGroup from individual_speeches.
 * Uses political_group_std (and kind/raw) when set; else raw political_group; else title (role).
 * For each MEP we set politicalGroup to the most frequent affiliation across their speeches.
 */

const { speechToAffiliationDisplay } = require('./affiliation-display');

// Map raw political_group strings (from parser) to display label used in MEP list / chart
const RAW_TO_DISPLAY = {
  'PPE-DE': 'PPE', 'EPP': 'PPE', 'PPE': 'PPE',
  'PSE': 'S&D', 'S&D': 'S&D',
  'ELDR': 'Renew', 'ALDE': 'Renew', 'Renew': 'Renew',
  'GUE/NGL': 'The Left', 'EUL/NGL': 'The Left', 'The Left': 'The Left',
  'Verts/ALE': 'Verts/ALE', 'Greens/EFA': 'Verts/ALE', 'Greens/ALE': 'Verts/ALE',
  'ECR': 'ECR', 'ID': 'ID', 'NI': 'Non-Attached', 'EFDD': 'EFDD', 'ESN': 'ESN', 'PfE': 'PfE',
  'UEN': 'Non-Attached', 'EDD': 'Non-Attached', 'EFD': 'EFDD', 'ENF': 'ID',
  'IND/DEM': 'Non-Attached', 'ITS': 'Non-Attached', 'TDI': 'Non-Attached',
  'rapporteur': 'Committee Rapporteur', 'Committee Chair': 'Committee Chair',
  'European Commission': 'European Commission', 'Council of the EU': 'Council of the EU',
  'High Representative': 'High Representative', 'EU Institution': 'EU Institution'
};

function rawToDisplay(raw) {
  if (!raw || typeof raw !== 'string') return '';
  const t = raw.trim();
  if (!t) return '';
  const key = t in RAW_TO_DISPLAY ? t : t.toUpperCase().replace(/\s+/g, ' ');
  return RAW_TO_DISPLAY[key] || RAW_TO_DISPLAY[t] || t;
}

// Procedural titles that should not be used as affiliation (in writing, blue-card, author, etc.)
const PROCEDURAL_PATTERNS = [
  'in writing', 'par écrit', 'por escrito', 'per iscritto', 'írásban', 'na piśmie', 'în scris',
  'γραπτώς', 'schriftlich', 'písomne', 'napisan', 'schriftelijk', 'kirjallinen', 'kirjalikult',
  'pisno', 'rakstiski', 'skriftlig', 'в писмена форма',
  'author', 'autor', 'auteur', 'auteure', 'autore', 'autora', 'autorka', 'verfasser', 'verfasserin',
  'blue-card', 'blue card', 'carton bleu', 'cartão azul', 'cartellino blu', 'tarjeta azul',
  'blauwe kaart', 'modré karty', 'niebieskiej kartki', 'plave kartice', 'γαλάζια κάρτα',
  'blått kort', 'kékkártyás', 'sinisen kortin', 'mėlynąją', 'albastru', 'blauen karte',
  'blauen kaart', 'creitem', 'stellungnahme des mitberatenden'
];

function isProceduralTitle(title) {
  if (!title || typeof title !== 'string') return true;
  const lower = title.trim().toLowerCase();
  if (!lower) return true;
  return PROCEDURAL_PATTERNS.some(p => lower.includes(p.toLowerCase()));
}

/** Derive display affiliation from speech title (role/party text when no party column). */
function titleToDisplay(title) {
  if (!title || typeof title !== 'string') return '';
  const t = title.trim();
  if (!t) return '';
  if (isProceduralTitle(t)) return '';

  const lower = t.toLowerCase();

  // Party acronyms sometimes appear in title column — use same mapping as raw political_group
  const fromRaw = rawToDisplay(t);
  if (fromRaw) return fromRaw;

  // Rapporteur in many languages (from DB: rapporteur, Berichterstatter, ponente, relatore, relator, sprawozdawca, föredragande, draftsman of the opinion)
  if (lower.includes('rapporteur') || lower.includes('berichterstatter') || lower.includes('ponente') ||
      lower.includes('relator') || lower.includes('sprawozdawca') || lower.includes('föredragande') ||
      lower.includes('draftsman of the opinion')) return 'Committee Rapporteur';

  if (lower.includes('chair') && (lower.includes('delegation') || lower.includes('committee'))) return 'Committee Chair';
  if (lower.includes('delegat')) return 'Delegation Member';

  // Commission / Council / High Rep (Member of the Commission, Vice-President of the Commission, President-in-Office of the Council, etc.)
  if (lower.includes('high representative') || lower.includes('vp/hr') || lower.includes('alto representante')) return 'High Representative';
  if (lower.includes('commission')) return 'European Commission';
  if (lower.includes('council') || lower.includes('ratspräsident') || lower.includes('consejo')) return 'Council of the EU';
  if (lower.includes('institution')) return 'EU Institution';

  return 'Parliamentary Role';
}

/**
 * @param {import('sqlite3').Database} db
 * @param {{ log?: (msg: string) => void }} options
 * @returns {Promise<{ updated: number, skipped: number }>}
 */
function syncMepAffiliationsFromSpeeches(db, options = {}) {
  const log = options.log || (() => {});

  return new Promise((resolve, reject) => {
    db.all(
      `SELECT mep_id, political_group_std, political_group_kind, political_group_raw, political_group, title
       FROM individual_speeches
       WHERE mep_id IS NOT NULL
         AND ( (political_group_std IS NOT NULL AND TRIM(political_group_std) != '')
               OR (political_group IS NOT NULL AND TRIM(political_group) != '')
               OR (title IS NOT NULL AND TRIM(title) != '') )`,
      [],
      (err, rows) => {
        if (err) {
          reject(err);
          return;
        }

        // mep_id -> { displayValue -> count }
        const byMep = {};
        for (const r of rows) {
          let display;
          if (r.political_group_std && String(r.political_group_std).trim()) {
            display = speechToAffiliationDisplay({
              political_group_std: r.political_group_std,
              political_group_kind: r.political_group_kind,
              political_group_raw: r.political_group_raw
            });
          } else if (r.political_group && String(r.political_group).trim()) {
            display = rawToDisplay(r.political_group);
          } else if (r.title && String(r.title).trim()) {
            display = titleToDisplay(r.title);
          }
          if (!display || display === 'Unknown') continue;
          if (!byMep[r.mep_id]) byMep[r.mep_id] = {};
          byMep[r.mep_id][display] = (byMep[r.mep_id][display] || 0) + 1;
        }

        // For each MEP pick the most frequent display value
        const toUpdate = [];
        for (const [mepId, counts] of Object.entries(byMep)) {
          let best = '';
          let bestCount = 0;
          for (const [display, count] of Object.entries(counts)) {
            if (count > bestCount) {
              bestCount = count;
              best = display;
            }
          }
          if (best) toUpdate.push({ mepId: Number(mepId), politicalGroup: best });
        }

        if (toUpdate.length === 0) {
          log('[SYNC-MEP-AFFILIATIONS] No MEPs with speech-derived affiliation; nothing to update.');
          resolve({ updated: 0, skipped: 0 });
          return;
        }

        let done = 0;
        let updated = 0;

        const OTHER_THRESHOLD = 10;

        function runNext() {
          if (done >= toUpdate.length) {
            log(`[SYNC-MEP-AFFILIATIONS] Updated meps.politicalGroup for ${updated} MEPs.`);
            // Collapse affiliations with fewer than OTHER_THRESHOLD members into "Other"
            db.all(
              `SELECT politicalGroup FROM meps
               WHERE politicalGroup IS NOT NULL AND TRIM(politicalGroup) != '' AND politicalGroup != 'Other'
               GROUP BY politicalGroup HAVING COUNT(*) < ?`,
              [OTHER_THRESHOLD],
              (err3, smallGroups) => {
                if (err3) {
                  log(`[SYNC-MEP-AFFILIATIONS] Warning: could not collapse small groups: ${err3.message}`);
                  resolve({ updated, skipped: toUpdate.length - updated });
                  return;
                }
                if (smallGroups.length === 0) {
                  resolve({ updated, skipped: toUpdate.length - updated });
                  return;
                }
                const names = smallGroups.map(r => r.politicalGroup);
                const placeholders = names.map(() => '?').join(',');
                db.run(
                  `UPDATE meps SET politicalGroup = 'Other' WHERE politicalGroup IN (${placeholders})`,
                  names,
                  function (err4) {
                    if (err4) log(`[SYNC-MEP-AFFILIATIONS] Warning: could not set Other: ${err4.message}`);
                    else log(`[SYNC-MEP-AFFILIATIONS] Collapsed ${this.changes} MEPs from small groups into "Other".`);
                    resolve({ updated, skipped: toUpdate.length - updated });
                  }
                );
              }
            );
            return;
          }
          const { mepId, politicalGroup } = toUpdate[done];
          db.run('UPDATE meps SET politicalGroup = ? WHERE id = ?', [politicalGroup, mepId], function (err2) {
            if (err2) {
              reject(err2);
              return;
            }
            if (this.changes > 0) updated += 1;
            done += 1;
            runNext();
          });
        }

        runNext();
      }
    );
  });
}

module.exports = { syncMepAffiliationsFromSpeeches };
