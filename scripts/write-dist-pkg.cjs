'use strict';

/**
 * Drops minimal package.json shims into dist/esm and dist/cjs so Node treats
 * the dual-build outputs as ESM and CommonJS respectively, regardless of the
 * top-level package.json `type` field.
 */

const fs = require('fs');
const path = require('path');

const dist = path.resolve(__dirname, '..', 'dist');

function write(dir, contents) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, 'package.json'),
    JSON.stringify(contents, null, 2) + '\n',
  );
}

write(path.join(dist, 'esm'), { type: 'module', sideEffects: false });
write(path.join(dist, 'cjs'), { type: 'commonjs', sideEffects: false });
