const promisePipe = require('promisepipe');
const stream = require('stream');

const Db = require('../util/db');
const CONFIG = require('../util/config');
const createAccumulatorStream = require('../stream/createAccumulatorStream');
const createInsertPromiseStream = require('../stream/createInsertPromiseStream');
const createCalculatorStream = require('../stream/createCalculatorStream');

const processFund = async (chunk, callback, pool) => {

    let db = null;
    if (pool == null) {        
        db = new Db();
    }
    db && await db.takeOnline();

    try {
        let isClientErrored = false;
        const client = db ? await db.pool.connect() : await pool.connect();
        client.on('error', () => {            
            isClientErrored = true;
        });

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
            const insertPromiseStream = createInsertPromiseStream(client);

            const fundPromise = promisePipe(
                dataStream,
                calculatorStream,
                accumulatorStream,
                insertPromiseStream
            );

            data.rows.forEach(item => dataStream.push(item));
            dataStream.push(null);

            const all = async () => {
                try {
                    await fundPromise;
                } catch (ex) {
                    throw ex.originalError;
                }

                // Force set to null pending_statistic_at
                await client.query(`UPDATE inf_diario_fi SET pending_statistic_at = '0001-01-01' WHERE cnpj_fundo = '${chunk.cnpj_fundo}' AND pending_statistic_at IS NOT NULL`);

                return client.query('COMMIT');
            };

            const allPromise = all();

            callback && callback(null, allPromise);

            await allPromise;
        } catch (ex) {            
            if (!isClientErrored) await client.query('ROLLBACK');            
            throw ex;
        } finally {
            await client.release();
        }
    } finally {
        db && await db.takeOffline(false);
    }
};

module.exports = processFund;