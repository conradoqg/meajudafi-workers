const stream = require('stream');
const QueryStream = require('pg-query-stream');
const promisePipe = require('promisepipe');

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
                    await client.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

                    const data = await client.query(`
                    SELECT *
                        FROM inf_diario_fi		
                            LEFT JOIN running_days_with_indicators ON inf_diario_fi.DT_COMPTC = running_days_with_indicators.DT_COMPTC
                        WHERE cnpj_fundo = '${chunk.cnpj_fundo}'
                        ORDER BY inf_diario_fi.DT_COMPTC ASC
                    `);

                    const dataStream = new stream.Readable({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark
                    });

                    const accumulatorStream = createAccumulatorStream();
                    const calculatorStream = createCalculatorStream();
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
                    console.error(ex);
                    if (!isConnectivityError(ex)) {
                        await client.query('ROLLBACK');
                    }
                    throw ex;
                } finally {
                    client.release();
                }
            } catch (ex) {
                console.error(ex);
                callback(ex);
            }
        }
    });
};

module.exports = createFundCalculatorStream;

function isConnectivityError(err) {
    const code = err && typeof err.code === 'string' && err.code;
    const cls = code && code.substr(0, 2); // Error Class
    return code === 'ECONNRESET' || cls === '08' || cls === '57';
    // Code 'ECONNRESET' - Connectivity issue handled by the driver.
    // Class 08 - Connection Exception.
    // Class 57 - Operator Intervention.
    //
    // ERROR CODES: https://www.postgresql.org/docs/9.6/static/errcodes-appendix.html
}
