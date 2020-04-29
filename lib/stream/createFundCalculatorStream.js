const stream = require('stream');
const promisePipe = require('promisepipe');

const Db = require('../util/db');
const createAccumulatorStream = require('./createAccumulatorStream');
const createInsertPromiseStream = require('./createInsertPromiseStream');
const createCalculatorStream = require('./createCalculatorStream');
const createWaitForPromisesStream = require('./createWaitForPromisesStream');
const CONFIG = require('../config');

const createFundCalculatorStream = (db) => {
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                const client = await db.pool.connect();
                try {
                    await client.query('BEGIN TRANSACTION');

                    const data = await client.query(`
                    SELECT *
                        FROM inf_diario_fi		
                            LEFT JOIN running_days_with_indicators ON inf_diario_fi.dt_comptc = running_days_with_indicators.dt_comptc
                        WHERE cnpj_fundo = '${chunk.cnpj_fundo}'
                        ORDER BY inf_diario_fi.dt_comptc ASC
                    `);

                    const dataStream = new stream.Readable({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark
                    });

                    const accumulatorStream = createAccumulatorStream();
                    const calculatorStream = createCalculatorStream(chunk.start);
                    const insertPromiseStream = createInsertPromiseStream(db);
                    const waitForPromisesStream = createWaitForPromisesStream();

                    const fundPromise = promisePipe(
                        dataStream,
                        calculatorStream,
                        accumulatorStream,
                        insertPromiseStream,
                        waitForPromisesStream
                    );

                    data.rows.forEach(item => dataStream.push(item));
                    dataStream.push(null);

                    callback(null, fundPromise);
                    await fundPromise;                    

                    // Force set to null pending_statistic_at
                    await client.query(`UPDATE inf_diario_fi SET pending_statistic_at = '0001-01-01' WHERE cnpj_fundo = '${chunk.cnpj_fundo}' AND pending_statistic_at IS NOT NULL`);

                    await client.query('COMMIT');
                } catch (ex) {
                    console.error('Error: createFundCalculatorStream: transform 1');
                    console.error(ex);
                    if (!Db.isConnectivityError(ex)) {
                        console.error('Error: createFundCalculatorStream: transform 2');
                        await client.query('ROLLBACK');
                    }
                    throw ex;
                } finally {
                    client.release();
                }
            } catch (ex) {
                console.error('Error: createFundCalculatorStream: transform 3');
                console.error(ex);
                callback(ex);
            }
        }
    });
};

module.exports = createFundCalculatorStream;