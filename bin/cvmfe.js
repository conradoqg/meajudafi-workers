const path = require('path');
if (process.argv[1].includes('snapshot')) process.argv[1] = process.argv[1].replace('arte.js', path.relative(process.cwd(), process.argv0)); // Workaround that shows the correct file path inside the pkg generated file
const yargs = require('yargs');
const cvmDataProcess = require('../lib/cvmDataProcess');
const cvmStatisticProcess = require('../lib/cvmStatisticProcess');
const XPIFundProcess = require('../lib/xpiFundProcess');

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
    .example('$0 run cvmDataProcess', 'Download, convert and insert data from CVM to database.')
    .example('$0 run cvmStatisticProcess -b', 'Load CVM data from database and generate financial information.')

    .command('run <workerName> [options]', 'run a worker', (yargs) => {
        return yargs
            .positional('workerName', {
                alias: 'worker',
                describe: 'worker name (cvmDataProcess, cvmStatisticProcess, all)'
            })
            .version(false);
    }, createCommandHandler(async (argv) => {
        const worker = argv.worker;

        if (worker.toLowerCase() == 'cvmDataProcess'.toLowerCase()) {
            return await cvmDataProcess();
        } else if (worker.toLowerCase() == 'cvmStatisticProcess'.toLowerCase()) {
            return await cvmStatisticProcess();
        } else if (worker.toLowerCase() == 'xpiFundProcess'.toLowerCase()) {
            return await (new XPIFundProcess()).work();
        } else if (worker.toLowerCase() == 'all'.toLowerCase()) {
            await cvmDataProcess();
            await cvmStatisticProcess();
            await (new XPIFundProcess()).work();
        }
    }))
    .demandCommand(1)
    .version()
    .help('h')
    .alias('h', 'help')
    .wrap(yargs.terminalWidth())
    .argv;