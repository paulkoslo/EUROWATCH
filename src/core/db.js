/**
 * Central database path. All db access should use this.
 */
const path = require('path');

const DB_PATH = path.join(__dirname, '..', '..', 'data', 'ep_data.db');

module.exports = { DB_PATH };
