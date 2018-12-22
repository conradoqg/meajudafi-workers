const Db = require('./util/db');
const promisePipe = require('promisepipe');
const QueryStream = require('pg-query-stream');
const stream = require('stream');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');

const UI = require('./util/ui');
const CONFIG = require('./config');
const createFundCalculatorStream = require('./stream/createFundCalculatorStream');
const createWaitForPromisesStream = require('./stream/createWaitForPromisesStream');

const createTotalProgressInfo = () => {
    return (progress) => `CVMStatisticProcess Overall (${progress.total}): [${'▇'.repeat(progress.percentage) + '-'.repeat(100 - progress.percentage)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createTotalFinishInfo = () => {
    return (progress) => `CVMStatisticProcess Overall took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s`;
};

const main = async () => {
    try {

        const db = new Db();

        await db.takeOnline();

        try {
            const mainClient = await db.pool.connect();

            try {
                const queryfundsCount = await mainClient.query('SELECT COUNT(DISTINCT cnpj_fundo) FROM inf_diario_fi WHERE pending_statistic_at is not null');
                //const queryfundsCount = await mainClient.query('SELECT COUNT(DISTINCT cnpj_fundo) FROM inf_diario_fi  LIMIT 1000');
                const fundsCount = parseInt(queryfundsCount.rows[0].count);

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

                const queryFunds = new QueryStream('SELECT DISTINCT cnpj_fundo FROM inf_diario_fi WHERE pending_statistic_at IS NOT NULL');
                //const queryFunds = new QueryStream('SELECT DISTINCT cnpj_fundo FROM inf_cadastral_fi LIMIT 1000');
                //const queryFunds = new QueryStream('SELECT DISTINCT cnpj_fundo FROM inf_cadastral_fi WHERE cnpj_fundo=\'17489031000150\'');
                const fundsStream = mainClient.query(queryFunds);

                await promisePipe(
                    fundsStream,
                    createFundCalculatorStream(db),
                    updateUIProgressStream,
                    createWaitForPromisesStream()
                );

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
};

module.exports = main;