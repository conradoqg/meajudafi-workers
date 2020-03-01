const path = require('path');
if (process.argv[1].includes('snapshot')) process.argv[1] = process.argv[1].replace('arte.js', path.relative(process.cwd(), process.argv0)); // Workaround that shows the correct file path inside the pkg generated file
const yargs = require('yargs');
const package = require('../package.json');

const CVMDataWorker = require('../lib/worker/cvmDataWorker');
const B3DataWorker = require('../lib/worker/b3DataWorker');
const CVMStatisticWorker = require('../lib/worker/cvmStatisticWorker');
const DataImprovementWorker = require('../lib/worker/dataImprovementWorker');
const XPIFundWorker = require('../lib/worker/xpiFundWorker');
const BTGPactualFundWorker = require('../lib/worker/btgPactualFundWorker');
const ModalMaisFundWorker = require('../lib/worker/modalMaisFundWorker');
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

console.log(`CVMFundExplorer v${package.version}`);

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
            await (new CVMDataWorker()).work(argv);
        } else if (worker.toLowerCase() == 'b3DataWorker'.toLowerCase()) {
            await (new B3DataWorker()).work(argv);
        } else if (worker.toLowerCase() == 'cvmStatisticWorker'.toLowerCase()) {
            await (new CVMStatisticWorker()).work(argv);
        } else if (worker.toLowerCase() == 'dataImprovementWorker'.toLowerCase()) {
            await (new DataImprovementWorker()).work(argv);
        } else if (worker.toLowerCase() == 'xpiFundWorker'.toLowerCase()) {
            await (new XPIFundWorker()).work(argv);
        } else if (worker.toLowerCase() == 'btgPactualFundWorker'.toLowerCase()) {
            await (new BTGPactualFundWorker()).work(argv);
        } else if (worker.toLowerCase() == 'modalMaisFundWorker'.toLowerCase()) {
            await (new ModalMaisFundWorker()).work(argv);
        } else if (worker.toLowerCase() == 'migrateWorker'.toLowerCase()) {
            await (new MigrateWorker()).work(argv);
        } else if (worker.toLowerCase() == 'all'.toLowerCase()) {
            await (new CVMDataWorker()).work(argv);
            await (new B3DataWorker()).work(argv);
            await (new CVMStatisticWorker()).work(argv);
            await (new DataImprovementWorker()).work(argv);
            await (new BTGPactualFundWorker()).work(argv);
            await (new ModalMaisFundWorker()).work(argv);
            await (new XPIFundWorker()).work(argv);
        } else {
            console.log(`Worker with name '${worker}' not found!`);
        }
    }))
    .demandCommand(1)
    .version()
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;