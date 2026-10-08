// WAB-866 child process for the SIGTERM shutdown test. Loads the TypeScript source with
// typescript.transpileModule (no ts-node in this repo), starts one manager, holds one job for
// 1.5 s, and lets the manager's own SIGTERM handler shut it down.
/* eslint-disable @typescript-eslint/no-var-requires */
const fs = require('fs')
const path = require('path')
const ts = require('typescript')

require.extensions['.ts'] = (/** @type {NodeJS.Module} */ module, /** @type {string} */ filename) => {
    const source = fs.readFileSync(filename, 'utf8')
    const { outputText } = ts.transpileModule(source, {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020, esModuleInterop: true },
        fileName: filename,
    })
    const compilable = /** @type {any} */ (module)
    compilable._compile(outputText, filename)
}

const { SharedQueueManager, JobType } = require(path.join(__dirname, '..', '..', 'sharedQueueManager.ts'))

/** @param {string} line */
const say = (line) => process.stdout.write(`${line}\n`)
process.on('exit', (code) => say(`exit ${code}`))

const manager = new SharedQueueManager({
    redis: { connection: process.env.WAB866_REDIS_URL },
    workerId: 'child',
    logLevel: 'none',
    queueConcurrency: { media: 2 },
})
manager.registerInstanceProcessor('inst-child', JobType.MEDIA_DOWNLOAD, async () => {
    say('started')
    await new Promise((resolve) => setTimeout(resolve, 1500))
    say('finished')
    return { ok: true }
})
manager.addJob(JobType.MEDIA_DOWNLOAD, { tag: 'c1' }, 'inst-child').catch((/** @type {Error} */ error) => {
    say(`addJob failed ${error && error.message}`)
    process.exit(2)
})
