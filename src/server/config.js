/**
 * Server configuration constants.
 * Centralizes PORT and API base URL for the Europarl data API.
 */
const PORT = process.env.PORT || 3000;
const API_BASE = 'https://data.europarl.europa.eu/api/v2';
const LOCALRUN = /^(1|true|yes)$/i.test(String(process.env.LOCALRUN || ''));

module.exports = { PORT, API_BASE, LOCALRUN };
