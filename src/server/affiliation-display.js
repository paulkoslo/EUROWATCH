/**
 * Map speech row (political_group_std, political_group_kind, political_group_raw) to the display label
 * used in MEP list and pie chart. Kept in sync with server.js /api/meps roleMap logic.
 * @param {{ political_group_std?: string, political_group_kind?: string, political_group_raw?: string }} g
 * @returns {string}
 */
function speechToAffiliationDisplay(g) {
  let displayValue = g.political_group_std || '';

  if (g.political_group_kind === 'institution') {
    if (g.political_group_raw && g.political_group_raw.includes('Commission')) {
      displayValue = 'European Commission';
    } else if (g.political_group_raw && g.political_group_raw.includes('Council')) {
      displayValue = 'Council of the EU';
    } else if (g.political_group_raw && g.political_group_raw.includes('High Representative')) {
      displayValue = 'High Representative';
    } else {
      displayValue = 'EU Institution';
    }
  } else if (g.political_group_kind === 'role') {
    if (g.political_group_raw && g.political_group_raw.includes('rapporteur')) {
      displayValue = 'Committee Rapporteur';
    } else if (g.political_group_raw && (g.political_group_raw.includes('Chair') || g.political_group_raw.includes('chair'))) {
      displayValue = 'Committee Chair';
    } else if (g.political_group_raw && g.political_group_raw.includes('delegat')) {
      displayValue = 'Delegation Member';
    } else {
      displayValue = 'Parliamentary Role';
    }
  } else if (g.political_group_std === 'NI' && g.political_group_kind === 'group') {
    displayValue = 'Non-Attached';
  }

  return displayValue || 'Unknown';
}

module.exports = { speechToAffiliationDisplay };
