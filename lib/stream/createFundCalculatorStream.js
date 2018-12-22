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
                    const query = new QueryStream(`
                    SELECT *
                        FROM inf_diario_fi		
                            LEFT JOIN running_days_with_indicators ON inf_diario_fi.DT_COMPTC = running_days_with_indicators.DT_COMPTC
                        WHERE cnpj_fundo = '${chunk.cnpj_fundo}'
                        ORDER BY inf_diario_fi.DT_COMPTC ASC
                    `);
                    const dataStream = client.query(query);

                    const fundPromise = promisePipe(
                        dataStream,
                        createCalculatorStream(),
                        createAccumulatorStream(),
                        createInsertPromiseStream(db),
                        createWaitForPromisesStream()
                    );

                    callback(null, fundPromise);
                    await promisePipe;

                    // Force set to null pending_statistic_at
                    await client.query(`UPDATE inf_diario_fi SET pending_statistic_at = '0001-01-01' WHERE cnpj_fundo = '${chunk.cnpj_fundo}' AND pending_statistic_at IS NOT NULL`);

                    await client.query('COMMIT');
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
    });
};

module.exports = createFundCalculatorStream;