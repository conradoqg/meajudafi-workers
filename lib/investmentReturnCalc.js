const Db = require('./db');
const promisePipe = require('promisepipe');
const QueryStream = require('pg-query-stream');
const stream = require('stream');
const uuidv1 = require('uuid/v1');

const CONFIG = {
    batchSize: 500,
    highWaterMark: 10
};

const first = (array) => {
    return array[0];
};

const last = (array) => {
    return array[array.length - 1];
};

const createDailyCalculatorStream = () => {
    let lastChunk = null;
    let firstChunkEver = null;
    let chunksByYearAndMonth = {};
    let returnsByYearAndMonth = {};

    const getMonthHash = (momentDate) => momentDate.format('YYYY-MM');
    const getYearHash = (momentDate) => momentDate.format('YYYY');

    const processDaily = (stream, chunk, chunkBefore, firstChunkEver) => {
        const investimentReturn = (chunk.vl_quota / chunkBefore.vl_quota) - 1;
        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
        returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push(investimentReturn);

        stream.push({
            table: 'investment_return_daily',
            primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
            fields: {
                id: uuidv1(),
                cnpj_fundo: chunk.cnpj_fundo,
                dt_comptc: chunk.dt_comptc.format('YYYY-MM-DD'),
                return: investimentReturn,
                accumulated_total_return: (chunkBefore.vl_quota / firstChunkEver.vl_quota) - 1
            }
        });
    };
    const processMonthly = (stream, firstChunkEver, chunk, chunksByYearAndMonth) => {
        const referenceMonthHash = getMonthHash(chunk.dt_comptc);
        const referenceYearHash = getYearHash(chunk.dt_comptc);
        const referencePreviousMonthHash = getMonthHash(chunk.dt_comptc.clone().subtract(1, 'month'));
        const referencePreviousYearsMonthHash = getYearHash(chunk.dt_comptc.clone().subtract(1, 'month'));

        const lastProcessedChunkOfTheLastChunksMonth =
            chunksByYearAndMonth[referencePreviousYearsMonthHash] &&
            chunksByYearAndMonth[referencePreviousYearsMonthHash][referencePreviousMonthHash] &&
            last(chunksByYearAndMonth[referencePreviousYearsMonthHash][referencePreviousMonthHash]);

        const firstProcessedChunkOfTheLastChunksMonth =
            chunksByYearAndMonth[referenceYearHash] &&
            chunksByYearAndMonth[referenceYearHash][referenceMonthHash] &&
            first(chunksByYearAndMonth[referenceYearHash][referenceMonthHash]);

        let chunkReference = null;

        if (lastProcessedChunkOfTheLastChunksMonth)
            chunkReference = lastProcessedChunkOfTheLastChunksMonth;
        else if (firstProcessedChunkOfTheLastChunksMonth)
            chunkReference = firstProcessedChunkOfTheLastChunksMonth;

        if (chunkReference) {
            let monthsRisk = null;
            let accTotalRisk = null;
            if (returnsByYearAndMonth[referenceYearHash] && returnsByYearAndMonth[referenceYearHash][referenceMonthHash]) {
                {
                    const dailyReturnSum = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc + value, 0);
                    const meanReturn = dailyReturnSum / returnsByYearAndMonth[referenceYearHash][referenceMonthHash].length;
                    const squaredDiffFromMeanSum = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc + Math.pow(value - meanReturn, 2), 0);
                    const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / returnsByYearAndMonth[referenceYearHash][referenceMonthHash].length);
                    monthsRisk = standardDeviation * Math.sqrt(returnsByYearAndMonth[referenceYearHash][referenceMonthHash].length);
                }
                {
                    let entries = 0;
                    const dailyReturnSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                        entries++;
                        return acc3 + value3;
                    }, acc2), acc), 0);
                    const meanReturn = dailyReturnSum / entries;
                    const squaredDiffFromMeanSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                        return acc3 + Math.pow(value3 - meanReturn, 2);
                    }, acc2), acc), 0);
                    const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / entries);
                    accTotalRisk = standardDeviation * Math.sqrt(entries);
                }
            }

            stream.push({
                table: 'investment_return_monthly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: chunk.cnpj_fundo,
                    dt_comptc: chunk.dt_comptc.clone().endOf('month').format('YYYY-MM-DD'),
                    return: (chunk.vl_quota / chunkReference.vl_quota) - 1,
                    accumulated_total_return: (chunk.vl_quota / firstChunkEver.vl_quota) - 1,
                    months_risk: monthsRisk,
                    accumulated_total_risk: accTotalRisk
                }
            });
        }
    };
    const processYearly = (stream, firstChunkEver, chunk, chunksByYearAndMonth) => {
        const referenceYearHash = getYearHash(chunk.dt_comptc);
        const referencePreviousYearsYearHash = getYearHash(chunk.dt_comptc.clone().subtract(1, 'year'));

        const lastProcessedChunkOfTheLastChunksYear =
            chunksByYearAndMonth[referencePreviousYearsYearHash] &&
            last(last(Object.values(chunksByYearAndMonth[referencePreviousYearsYearHash])));

        const firstProcessedChunkOfTheLastChunksYear =
            chunksByYearAndMonth[referenceYearHash] &&
            first(first(Object.values(chunksByYearAndMonth[referenceYearHash])));

        let chunkReference = null;

        if (lastProcessedChunkOfTheLastChunksYear)
            chunkReference = lastProcessedChunkOfTheLastChunksYear;
        else if (firstProcessedChunkOfTheLastChunksYear)
            chunkReference = firstProcessedChunkOfTheLastChunksYear;

        if (chunkReference) {
            let yearsRisk = null;
            let accTotalRisk = null;
            if (returnsByYearAndMonth[referenceYearHash]) {
                {
                    const dailyReturnSum = Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => value.reduce((acc2, value2) => acc2 + value2, acc), 0);
                    const meanReturn = dailyReturnSum / Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => acc + value.length, 0);
                    const squaredDiffFromMeanSum = Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => value.reduce((acc2, value2) => acc2 + Math.pow(value2 - meanReturn, 2), acc), 0);
                    const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => acc + value.length, 0));
                    yearsRisk = standardDeviation * Math.sqrt(Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => acc + value.length, 0));
                }
                {
                    let entries = 0;
                    const dailyReturnSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                        entries++;
                        return acc3 + value3;
                    }, acc2), acc), 0);
                    const meanReturn = dailyReturnSum / entries;
                    const squaredDiffFromMeanSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                        return acc3 + Math.pow(value3 - meanReturn, 2);
                    }, acc2), acc), 0);
                    const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / entries);
                    accTotalRisk = standardDeviation * Math.sqrt(entries);
                }
            }

            stream.push({
                table: 'investment_return_yearly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: chunk.cnpj_fundo,
                    dt_comptc: chunk.dt_comptc.clone().endOf('year').format('YYYY-MM-DD'),
                    return: (chunk.vl_quota / chunkReference.vl_quota) - 1,
                    accumulated_total_return: (chunk.vl_quota / firstChunkEver.vl_quota) - 1,
                    years_risk: yearsRisk,
                    accumulated_total_risk: accTotalRisk
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
                if (!chunksByYearAndMonth[getYearHash(chunk.dt_comptc)]) chunksByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
                if (!chunksByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) chunksByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
                chunksByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push(chunk);

                // Daily
                if (lastChunk != null) {
                    processDaily(this, chunk, lastChunk, firstChunkEver);
                }

                // Monthly
                if (lastChunk != null && lastChunk.dt_comptc.month() != chunk.dt_comptc.month()) {
                    processMonthly(this, firstChunkEver, lastChunk, chunksByYearAndMonth);
                }

                // Yearly
                if (lastChunk != null && lastChunk.dt_comptc.year() != chunk.dt_comptc.year()) {
                    processYearly(this, firstChunkEver, lastChunk, chunksByYearAndMonth);
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
            processMonthly(this, firstChunkEver, lastChunk, chunksByYearAndMonth);

            // Yearly
            processYearly(this, firstChunkEver, lastChunk, chunksByYearAndMonth);

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
