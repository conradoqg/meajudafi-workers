const promisePipe = require('promisepipe');
const stream = require('stream');

const Db = require('../util/db');
const CONFIG = require('../util/config');
const createAccumulatorStream = require('../stream/createAccumulatorStream');
const createInsertPromiseStream = require('../stream/createInsertPromiseStream');
const createCalculatorStream = require('../stream/createCalculatorStream');
const createWaitForPromisesStream = require('../stream/createWaitForPromisesStream');

const processFund = async (chunk, callback, db) => {

    let ownDB = false;
    if (db == null) {
        ownDB = true;
        db = new Db();
    }
    ownDB && await db.takeOnline();    

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
            const insertPromiseStream = createInsertPromiseStream(client);
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

            const all = async () => {
                await fundPromise;

                // Force set to null pending_statistic_at
                await client.query(`UPDATE inf_diario_fi SET pending_statistic_at = '0001-01-01' WHERE cnpj_fundo = '${chunk.cnpj_fundo}' AND pending_statistic_at IS NOT NULL`);

                return client.query('COMMIT');
            };

            const allPromise = all();

            callback && callback(null, allPromise);

            await allPromise;
        } catch (ex) {
            console.error('Error: createFundCalculatorStream: transform 1');
            console.error(ex);
            if (!Db.isConnectivityError(ex)) {
                console.error('Error: createFundCalculatorStream: transform 2');
                await client.query('ROLLBACK');
            }
            throw ex;
        } finally {
            await client.release();
        }
    } catch (ex) {
        console.error('Error: createFundCalculatorStream: transform 3');
        console.error(ex);
        throw ex;
    } finally {
        ownDB && await db.takeOffline(false);
    }
};

module.exports = processFund;