const Db = require('./db');
const promisePipe = require('promisepipe');
const QueryStream = require('pg-query-stream');
const stream = require('stream');
const prettyMs = require('pretty-ms');
const UI = require('./ui');
const convertHrtime = require('convert-hrtime');

const createAccumulatorStream = require('./stream/createAccumulatorStream');
const createInsertPromiseStream = require('./stream/createInsertPromiseStream');
const createCalculatorStream = require('./stream/createCalculatorStream');

const CONFIG = require('./config');

const createTotalProgressInfo = () => {
    return (progress) => `Overall (${progress.total} files): [${'▇'.repeat(progress.percentage) + '-'.repeat(100 - progress.percentage)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${prettyMs(progress.eta)}`;
};

const createTotalFinishInfo = () => {
    return (progress) => `Overall took ${prettyMs(progress.elapsed)}`;
};

const main = async () => {
    try {

        const db = new Db();

        await db.takeOnline();

        try {
            const mainClient = await db.pool.connect();

            try {
                const queryfundsCount = await mainClient.query('SELECT COUNT(DISTINCT cnpj_fundo) FROM inf_cadastral_fi');
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
                ui.start('total', createTotalProgressInfo(fundsCount), createTotalFinishInfo());
                ui.update('total', progressState);

                const queryFunds = new QueryStream('SELECT DISTINCT cnpj_fundo FROM inf_cadastral_fi');
                const fundsStream = mainClient.query(queryFunds);

                await promisePipe(
                    fundsStream,
                    stream.Transform({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark,
                        transform: async (chunk, e, callback) => {
                            try {
                                const client = await db.pool.connect();
                                try {                                    
                                    await client.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');
                                    const query = new QueryStream(`SELECT * FROM inf_diario_fi LEFT JOIN fbcdata_sgs_12i ON inf_diario_fi.DT_COMPTC = fbcdata_sgs_12i.DATA WHERE CNPJ_FUNDO = '${chunk.cnpj_fundo}' ORDER BY DT_COMPTC ASC`);
                                    const dataStream = client.query(query);

                                    await promisePipe(
                                        dataStream,
                                        createCalculatorStream(),
                                        createAccumulatorStream(),
                                        createInsertPromiseStream(db),
                                        stream.Writable({ objectMode: true, write: (chunk, e, callback) => callback() })
                                    );

                                    progressState.finished++;
                                    progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
                                    progressState.speed = progressState.finished / (progressState.elapsed / 100);
                                    progressState.eta = (progressState.elapsed * progressState.total) / progressState.finished;
                                    progressState.percentage = (progressState.finished * 100) / progressState.total;
                                    ui.update('total', progressState);

                                    await client.query('COMMIT');
                                    callback();
                                } catch (ex) {
                                    await client.query('ROLLBACK');
                                    throw ex;
                                } finally {
                                    client.release();
                                }
                            } catch (ex) {
                                console.error(ex);
                                callback(ex);
                            }
                        }
                    }),
                    stream.Writable({ objectMode: true, write: (chunk, e, callback) => callback() })
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

main();
