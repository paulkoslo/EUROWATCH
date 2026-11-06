#!/usr/bin/env node
/** Compatibility entry point: build a consistent subset as a new immutable release. */
require('dotenv').config({ quiet: true });
const path = require('node:path');
const { prepare } = require('../data/dataset');
const { DB_PATH } = require('../core/db');
const args = process.argv.slice(2);
const index = args.indexOf('--output');
const output = index < 0 ? path.resolve('data/releases', `demo-${Date.now()}`) : args[index + 1];
if (!output) throw new Error('--output requires a new directory');
prepare({ source: DB_PATH, output, from: '2020-01-01' })
  .then(() => console.log(`Demo prepared with matching analytics. Select DATASET_DIR=${path.resolve(output)} to browse it. The original files were not changed.`))
  .catch((error) => { console.error(error.message); process.exitCode = 1; });
