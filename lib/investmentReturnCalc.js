const Db = require('./db');
const promisePipe = require('promisepipe');
const QueryStream = require('pg-query-stream');
const stream = require('stream');
const uuidv1 = require('uuid/v1');

const CONFIG = {
    batchSize: 500,
    highWaterMark: 10
};

const createDailyCalculatorStream = () => {
    let lastChunk = null;
    let firstChunkEver = null;    
    let chunksByMonth = {};
    let chunksByYear = {};
    let dailyReturns = [];

    const getMonthHash = (momentDate) => momentDate.format('YYYY-MM');
    const getYearHash = (momentDate) => momentDate.format('YYYY');

    const processDaily = (stream, chunk, lastChunk, firstChunkEver) => {
        dailyReturns.push((chunk.vl_quota / lastChunk.vl_quota) - 1);
        stream.push({
            table: 'investment_return_daily',
            primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
            fields: {
                id: uuidv1(),
                cnpj_fundo: chunk.cnpj_fundo,
                dt_comptc: chunk.dt_comptc.format('YYYY-MM-DD'),
                return: (chunk.vl_quota / lastChunk.vl_quota) - 1,
                accumulated_total_return: (lastChunk.vl_quota / firstChunkEver.vl_quota) - 1
            }
        });
    };
    const processMonthly = (stream, firstChunkEver, lastChunk, chunksByMonth) => {
        const lastProcessedChunkOfTheLastChunksMonth = chunksByMonth[getMonthHash(lastChunk.dt_comptc.clone().subtract(1, 'month'))] && chunksByMonth[getMonthHash(lastChunk.dt_comptc.clone().subtract(1, 'month'))][chunksByMonth[getMonthHash(lastChunk.dt_comptc.clone().subtract(1, 'month'))].length - 1];
        const firstProcessedChunkOfTheLastChunksMonth = chunksByMonth[getMonthHash(lastChunk.dt_comptc)] && chunksByMonth[getMonthHash(lastChunk.dt_comptc)][0];
        let chunkReference = null;

        if (lastProcessedChunkOfTheLastChunksMonth)
            chunkReference = lastProcessedChunkOfTheLastChunksMonth;
        else if (firstProcessedChunkOfTheLastChunksMonth)
            chunkReference = firstProcessedChunkOfTheLastChunksMonth;

        if (chunkReference) {
            stream.push({
                table: 'investment_return_monthly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: lastChunk.cnpj_fundo,
                    dt_comptc: lastChunk.dt_comptc.clone().endOf('month').format('YYYY-MM-DD'),
                    return: (lastChunk.vl_quota / chunkReference.vl_quota) - 1,
                    accumulated_total_return: (lastChunk.vl_quota / firstChunkEver.vl_quota) - 1
                }
            });
        }
    };
    const processYearly = (stream, firstChunkEver, lastChunk, chunksByYear) => {
        const lastProcessedChunkOfTheLastChunksYear = chunksByYear[getYearHash(lastChunk.dt_comptc.clone().subtract(1, 'year'))] && chunksByYear[getYearHash(lastChunk.dt_comptc.clone().subtract(1, 'year'))][chunksByYear[getYearHash(lastChunk.dt_comptc.clone().subtract(1, 'year'))].length - 1];
        const firstProcessedChunkOfTheLastChunksYear = chunksByYear[getYearHash(lastChunk.dt_comptc)] && chunksByYear[getYearHash(lastChunk.dt_comptc)][0];
        let chunkReference = null;

        if (lastProcessedChunkOfTheLastChunksYear)
            chunkReference = lastProcessedChunkOfTheLastChunksYear;
        else if (firstProcessedChunkOfTheLastChunksYear)
            chunkReference = firstProcessedChunkOfTheLastChunksYear;

        if (chunkReference) {
            stream.push({
                table: 'investment_return_yearly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: lastChunk.cnpj_fundo,
                    dt_comptc: lastChunk.dt_comptc.clone().endOf('year').format('YYYY-MM-DD'),
                    return: (lastChunk.vl_quota / chunkReference.vl_quota) - 1,
                    accumulated_total_return: (lastChunk.vl_quota / firstChunkEver.vl_quota) - 1
                }
            });
        }
    };

    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: function (chunk, e, callback) {
            try {
                if (!firstChunkEver) firstChunkEver = chunk;
                if (!chunksByMonth[getMonthHash(chunk.dt_comptc)]) chunksByMonth[getMonthHash(chunk.dt_comptc)] = [];
                if (!chunksByYear[getYearHash(chunk.dt_comptc)]) chunksByYear[getYearHash(chunk.dt_comptc)] = [];
                chunksByMonth[getMonthHash(chunk.dt_comptc)].push(chunk);
                chunksByYear[getYearHash(chunk.dt_comptc)].push(chunk);

                // Daily
                if (lastChunk != null) {
                    processDaily(this, chunk, lastChunk, firstChunkEver);
                }

                // Monthly
                if (lastChunk != null && lastChunk.dt_comptc.month() != chunk.dt_comptc.month()) {
                    processMonthly(this, firstChunkEver, lastChunk, chunksByMonth);
                }

                // Yearly
                if (lastChunk != null && lastChunk.dt_comptc.year() != chunk.dt_comptc.year()) {
                    processYearly(this, firstChunkEver, lastChunk, chunksByYear);
                }

                lastChunk = chunk;
                callback();
            } catch (ex) {
                console.error(ex.stack);
                callback(ex);
            }
        },
        flush: function (callback) {
            // Monthly
            processMonthly(this, firstChunkEver, lastChunk, chunksByMonth);

            // Yearly
            processYearly(this, firstChunkEver, lastChunk, chunksByYear);

            const dailyReturnSum = dailyReturns.reduce((previous, current) => previous + current, 0);
            const meanReturn = dailyReturnSum / dailyReturns.length;

            const squaredDiffFromMean = dailyReturns.map(item => Math.pow(item - meanReturn, 2), 0);
            const squaredDiffFromMeanSum = squaredDiffFromMean.reduce((previous, current) => previous + current, 0);

            const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / dailyReturns.length);

            const annualizedRisk = standardDeviation * Math.sqrt(252);

            callback();
        }
    });
};

const createAccumulatorStream = () => {
    let valuesPerTableDefinition = {};
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: function (chunk, e, callback) {
            if (!valuesPerTableDefinition[chunk.table]) {
                valuesPerTableDefinition[chunk.table] = {
                    table: chunk.table,
                    primaryKey: chunk.primaryKey,
                    values: []
                };
            }
            valuesPerTableDefinition[chunk.table].values.push(chunk.fields);
            callback();
        },

        flush: function (callback) {
            Object.values(valuesPerTableDefinition).map(tableDefinition => this.push(tableDefinition));
            callback();
        }
    });
};

const createInsertPromiseStream = (db) => {
    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                const client = await db.pool.connect();
                try {
                    let newQuery = db.createUpsertQuery(chunk);

                    const queryPromise = client.query({
                        text: newQuery,
                        rowMode: 'array'
                    });
                    await queryPromise;
                    callback(null, queryPromise);
                } catch (ex) {
                    console.error(ex.stack);
                    callback(ex);
                } finally {
                    client.release();
                }
            } catch (ex) {
                console.error(ex.stack);
                callback(ex);
            }
        }
    });
};

const main = async () => {
    try {

        const db = new Db();

        await db.takeOnline();

        try {
            const client = await db.pool.connect();

            try {
                const fund = 17489031000150;

                const query = new QueryStream(`SELECT * FROM inf_diario_fi WHERE CNPJ_FUNDO = '${fund}' ORDER BY DT_COMPTC ASC`);
                const dataStream = client.query(query);

                await promisePipe(
                    dataStream,
                    createDailyCalculatorStream(),
                    createAccumulatorStream(),
                    createInsertPromiseStream(db),
                    stream.Writable({ objectMode: true, write: (chunk, e, callback) => callback() })
                );
            } finally {
                client.release();
            }

        } catch (ex) {
            console.error(ex.stack);
            throw ex;
        } finally {
            await db.takeOffline();
        }
    } catch (ex) {
        console.error(ex.stack);
    }
};

main();
