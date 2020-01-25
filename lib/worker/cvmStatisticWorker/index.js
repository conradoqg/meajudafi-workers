const Db = require('../../util/db');
const promisePipe = require('promisepipe');
const stream = require('stream');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');

const Worker = require('../worker');
const UI = require('../../util/ui');
const CONFIG = require('../../config');
const createFundCalculatorStream = require('../../stream/createFundCalculatorStream');
const createWaitForPromisesStream = require('../../stream/createWaitForPromisesStream');

const createTotalProgressInfo = () => {
    return (progress) => `CVMStatisticWorker: Calculating statistics of ${progress.total} funds: [${'▇'.repeat(progress.percentage / 2) + '-'.repeat(100 / 2 - progress.percentage / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createTotalFinishInfo = () => {
    return (progress) => `CVMStatisticWorker: Calculating statistics took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s`;
};

class CVMStatisticWorker extends Worker {
    constructor() {
        super();
    }

    async work(options) {
        try {

            let full = false;
            let interestingFundsOnly = false;
            if (options.options && options.options.includes('fullStatistics')) full = true;
            if (options.options && options.options.includes('interestingFundsOnly')) interestingFundsOnly = true;

            const db = new Db();

            await db.takeOnline();

            try {
                const mainClient = await db.pool.connect();

                try {
                    const ui = new UI();

                    ui.start('CVMStatisticWorkerQuery', 'CVMStatisticWorker: Querying funds to calculate', null, progress => `CVMStatisticWorker: Querying funds to calculate took ${prettyMs(progress.elapsed)}`, true);
                    const startData = process.hrtime();

                    let queryfundsCount = null;
                    if (full)
                        queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi GROUP BY cnpj_fundo');
                    else if (interestingFundsOnly)
                        queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi JOIN funds_enhanced ON inf_diario_fi.cnpj_fundo = funds_enhanced.f_cnpj WHERE xf_id IS NOT NULL OR bf_id IS NOT NULL OR mf_id IS NOT NULL GROUP BY cnpj_fundo');
                    else
                        queryfundsCount = await mainClient.query('SELECT cnpj_fundo, MIN(DT_COMPTC) as start FROM inf_diario_fi WHERE pending_statistic_at is not null GROUP BY cnpj_fundo');

                    //const queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi GROUP BY cnpj_fundo LIMIT 200');
                    //queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi WHERE cnpj_fundo=\'18048639000102\' GROUP BY cnpj_fundo');
                    //const queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi WHERE pending_statistic_at is not null AND cnpj_fundo=\'00071477000168\' GROUP BY cnpj_fundo');

                    const fundsCount = parseInt(queryfundsCount.rows.length);

                    ui.update('CVMStatisticWorkerQuery', { elapsed: convertHrtime(process.hrtime(startData)).milliseconds });
                    ui.stop('CVMStatisticWorkerQuery');

                    const progressState = {
                        total: fundsCount,
                        start: process.hrtime(),
                        elapsed: 0,
                        finished: 0,
                        percentage: 0,
                        eta: 0,
                        speed: 0
                    };

                    ui.start('total', 'CVMStatisticWorker: Calculating statistics', createTotalProgressInfo(fundsCount), createTotalFinishInfo());
                    ui.update('total', progressState);

                    const updateUI = () => {
                        progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
                        progressState.speed = progressState.finished / (progressState.elapsed / 100);
                        progressState.eta = ((progressState.elapsed * progressState.total) / progressState.finished) - progressState.elapsed;
                        progressState.percentage = (progressState.finished * 100) / progressState.total;
                        ui.update('total', progressState);
                    };

                    const updateUIProgressStream = stream.Transform({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark,
                        transform: (chunk, e, callback) => {
                            progressState.finished++;
                            updateUI();
                            callback(null, chunk);
                        },
                        flush: (callback) => {
                            updateUI();
                            callback();
                        }
                    });

                    const fundsStream = new stream.Readable({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark
                    });

                    const statisticsPipe = promisePipe(
                        fundsStream,
                        createFundCalculatorStream(db),
                        updateUIProgressStream,
                        createWaitForPromisesStream()
                    );

                    queryfundsCount.rows.forEach(item => fundsStream.push(item));
                    fundsStream.push(null);

                    await statisticsPipe;

                    ui.stop('total');
                    ui.close();
                } finally {
                    mainClient.release();
                }
            } finally {
                await db.takeOffline();
            }
        } catch (ex) {
            console.error('Error: cvmStatisticWorker');            
            console.error(ex.stack);
        }
    }
}

module.exports = CVMStatisticWorker;