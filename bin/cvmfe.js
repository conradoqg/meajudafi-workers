const path = require('path');
if (process.argv[1].includes('snapshot')) process.argv[1] = process.argv[1].replace('arte.js', path.relative(process.cwd(), process.argv0)); // Workaround that shows the correct file path inside the pkg generated file
const yargs = require('yargs');

const cvmDataWorker = require('../lib/worker/cvmDataWorker');
const cvmStatisticWorker = require('../lib/worker/cvmStatisticWorker');
const XPIFundWorker = require('../lib/worker/xpiFundWorker');
const MigrateWorker = require('../lib/worker/migrateWorker');

const createCommandHandler = (func) => {
    return async (argv) => {
        try {
            await func(argv);
            process.exit(0);
        }
        catch (ex) {
            if (ex.toPrint) console.error(ex.toPrint());
            else console.error(ex.stack);
            process.exit(1);
        }
    };
};

yargs
    .example('$0 run cvmDataWorker', 'Download, convert and insert data from CVM to database.')
    .example('$0 run cvmStatisticWorker -b', 'Load CVM data from database and generate financial information.')
    .example('$0 run xpiFundWorker', 'Load XPI data.')
    .example('$0 run migrateWorker', 'Migrate database.')

    .command('run <workerName> [options...]', 'run a worker', (yargs) => {
        return yargs
            .positional('workerName', {
                alias: 'worker',
                describe: 'worker name (cvmDataWorker, cvmStatisticWorker, xpiFundWorker, all)'
            })
            .version(false);
    }, createCommandHandler(async (argv) => {
        const worker = argv.worker;

        if (worker.toLowerCase() == 'cvmDataWorker'.toLowerCase()) {
            await (new cvmDataWorker()).work(argv);
        } else if (worker.toLowerCase() == 'cvmStatisticWorker'.toLowerCase()) {
            await (new cvmStatisticWorker()).work(argv);
        } else if (worker.toLowerCase() == 'xpiFundWorker'.toLowerCase()) {
            await (new XPIFundWorker()).work(argv);
        } else if (worker.toLowerCase() == 'migrateWorker'.toLowerCase()) {
            await (new MigrateWorker()).work(argv);
        } else if (worker.toLowerCase() == 'all'.toLowerCase()) {
            await (new cvmDataWorker()).work(argv);
            await (new cvmStatisticWorker()).work(argv);
            await (new XPIFundWorker()).work(argv);
        }
    }))
    .demandCommand(1)
    .version()
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;