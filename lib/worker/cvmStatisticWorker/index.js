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
    return (progress) => `CVMStatisticProcess Overall (${progress.total}): [${'▇'.repeat(progress.percentage) + '-'.repeat(100 - progress.percentage)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createTotalFinishInfo = () => {
    return (progress) => `CVMStatisticProcess Overall took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s`;
};

class CVMStatisticWorker extends Worker {
    constructor() {
        super();
    }

    async work() {
        try {

            const db = new Db();

            await db.takeOnline();

            try {
                const mainClient = await db.pool.connect();

                try {

                    const queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi WHERE pending_statistic_at is not null GROUP BY cnpj_fundo');
                    //const queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi WHERE pending_statistic_at is not null GROUP BY cnpj_fundo LIMIT 300');
                    //const queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi WHERE pending_statistic_at is not null AND cnpj_fundo=\'17489031000150\' GROUP BY cnpj_fundo');
                    //const queryfundsCount = await mainClient.query('SELECT cnpj_fundo FROM inf_diario_fi WHERE pending_statistic_at is not null AND cnpj_fundo=\'00071477000168\' GROUP BY cnpj_fundo');

                    const fundsCount = parseInt(queryfundsCount.rows.length);

                    const progressState = {
                        total: fundsCount,
                        start: process.hrtime(),
                        elapsed: 0,
                        finished: 0,
                        percentage: 0,
                        eta: 0,
                        speed: 0
                    };

                    const ui = new UI();
                    ui.start('total', 'Processing data', createTotalProgressInfo(fundsCount), createTotalFinishInfo());
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
                } catch (ex) {
                    throw ex;
                } finally {
                    mainClient.release();
                }
            } catch (ex) {
                throw ex;
            } finally {
                await db.takeOffline();
            }
        } catch (ex) {
            console.error(ex.stack);
        }
    }
}

module.exports = CVMStatisticWorker;