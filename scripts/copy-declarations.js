// WAB-1384: tsc does not emit hand-written .d.ts files, so the published dist referenced ./types, ./types-enhanced and
// ./types/connection without shipping them. Copy every src/**/*.d.ts (tests excluded) next to the compiled output.
'use strict'
const fs = require('fs')
const path = require('path')

const src = path.join(__dirname, '..', 'src')
const dist = path.join(__dirname, '..', 'dist')

function copy(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const from = path.join(dir, entry.name)
        if (entry.isDirectory()) {
            if (entry.name !== '__tests__') copy(from)
        } else if (entry.name.endsWith('.d.ts')) {
            const to = path.join(dist, path.relative(src, from))
            fs.mkdirSync(path.dirname(to), { recursive: true })
            fs.copyFileSync(from, to)
        }
    }
}

copy(src)
