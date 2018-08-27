const Db = require('./db');
const promisePipe = require('promisepipe');
const QueryStream = require('pg-query-stream');
const stream = require('stream');
const uuidv1 = require('uuid/v1');
const moment = require('moment');

const CONFIG = {
    batchSize: 500,
    highWaterMark: 10
};

const createDailyCalculatorStream = () => {
    let lastChunk = null;
    let lastProcessedDayChunkOfEachMonth = {};
    let firstProcessedDayChunkOfEachMonth = {};
    let lastProcessedDayChunkOfEachYear = {};
    let firstProcessedDayChunkOfEachYear = {};

    const getMonthHash = (momentDate) => momentDate.format('YYYY-MM');
    const getYearHash = (momentDate) => momentDate.format('YYYY');



    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: function (chunk, e, callback) {
            try {
                // Daily
                if (lastChunk != null) {
                    this.push({
                        table: 'investment_return_daily',
                        primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                        fields: {
                            id: uuidv1(),
                            cnpj_fundo: chunk.cnpj_fundo,
                            dt_comptc: chunk.dt_comptc.toDateString(),
                            return: (chunk.vl_quota / lastChunk.vl_quota) - 1
                        }
                    });
                }

                // Monthly
                if (lastChunk != null && lastChunk.dt_comptc.getMonth() != chunk.dt_comptc.getMonth()) {
                    const lastProcessedChunkOfTheLastChunksMonth = lastProcessedDayChunkOfEachMonth[getMonthHash(moment(lastChunk.dt_comptc).subtract(1, 'month'))];
                    const firstProcessedChunkOfTheLastChunksMonth = firstProcessedDayChunkOfEachMonth[getMonthHash(moment(lastChunk.dt_comptc))];
                    let chunkReference = null;
                    if (lastProcessedChunkOfTheLastChunksMonth) chunkReference = lastProcessedChunkOfTheLastChunksMonth;
                    else if (firstProcessedChunkOfTheLastChunksMonth) chunkReference = firstProcessedChunkOfTheLastChunksMonth;

                    if (chunkReference) {
                        this.push({
                            table: 'investment_return_monthly',
                            primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                            fields: {
                                id: uuidv1(),
                                cnpj_fundo: lastChunk.cnpj_fundo,
                                dt_comptc: lastChunk.dt_comptc.toDateString(),
                                return: (lastChunk.vl_quota / chunkReference.vl_quota) - 1
                            }
                        });
                    }
                }

                // Yearly
                if (lastChunk != null && lastChunk.dt_comptc.getYear() != chunk.dt_comptc.getYear()) {
                    const lastProcessedChunkOfTheLastChunksYear = lastProcessedDayChunkOfEachYear[getYearHash(moment(lastChunk.dt_comptc).subtract(1, 'year'))];
                    const firstProcessedChunkOfTheLastChunksYear = firstProcessedDayChunkOfEachYear[getYearHash(moment(lastChunk.dt_comptc))];
                    let chunkReference = null;

                    if (lastProcessedChunkOfTheLastChunksYear) chunkReference = lastProcessedChunkOfTheLastChunksYear;
                    else if (firstProcessedChunkOfTheLastChunksYear) chunkReference = firstProcessedChunkOfTheLastChunksYear;

                    if (chunkReference) {
                        this.push({
                            table: 'investment_return_yearly',
                            primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                            fields: {
                                id: uuidv1(),
                                cnpj_fundo: lastChunk.cnpj_fundo,
                                dt_comptc: lastChunk.dt_comptc.toDateString(),
                                return: (lastChunk.vl_quota / chunkReference.vl_quota) - 1
                            }
                        });
                    }
                }

                lastProcessedDayChunkOfEachMonth[getMonthHash(moment(chunk.dt_comptc))] = chunk;
                lastProcessedDayChunkOfEachYear[getYearHash(moment(chunk.dt_comptc))] = chunk;
                if (!firstProcessedDayChunkOfEachMonth[getMonthHash(moment(chunk.dt_comptc))]) firstProcessedDayChunkOfEachMonth[getMonthHash(moment(chunk.dt_comptc))] = chunk;
                if (!firstProcessedDayChunkOfEachYear[getYearHash(moment(chunk.dt_comptc))]) firstProcessedDayChunkOfEachYear[getYearHash(moment(chunk.dt_comptc))] = chunk;
                lastChunk = chunk;
                callback();
            } catch (ex) {
                console.error(ex.stack);
                callback(ex);
            }
        },
        flush: function (callback) {
            {
                // Monthly
                const lastProcessedChunkOfTheLastChunksMonth = lastProcessedDayChunkOfEachMonth[getMonthHash(moment(lastChunk.dt_comptc).subtract(1, 'month'))];
                const firstProcessedChunkOfTheLastChunksMonth = firstProcessedDayChunkOfEachMonth[getMonthHash(moment(lastChunk.dt_comptc))];
                let chunkReference = null;
                if (lastProcessedChunkOfTheLastChunksMonth) chunkReference = lastProcessedChunkOfTheLastChunksMonth;
                else if (firstProcessedChunkOfTheLastChunksMonth) chunkReference = firstProcessedChunkOfTheLastChunksMonth;

                if (chunkReference) {
                    this.push({
                        table: 'investment_return_monthly',
                        primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                        fields: {
                            id: uuidv1(),
                            cnpj_fundo: lastChunk.cnpj_fundo,
                            dt_comptc: lastChunk.dt_comptc.toDateString(),
                            return: (lastChunk.vl_quota / chunkReference.vl_quota) - 1
                        }
                    });
                }
            }

            {
                // Yearly
                const lastProcessedChunkOfTheLastChunksYear = lastProcessedDayChunkOfEachYear[getYearHash(moment(lastChunk.dt_comptc).subtract(1, 'year'))];
                const firstProcessedChunkOfTheLastChunksYear = firstProcessedDayChunkOfEachYear[getYearHash(moment(lastChunk.dt_comptc))];
                let chunkReference = null;

                if (lastProcessedChunkOfTheLastChunksYear) chunkReference = lastProcessedChunkOfTheLastChunksYear;
                else if (firstProcessedChunkOfTheLastChunksYear) chunkReference = firstProcessedChunkOfTheLastChunksYear;

                if (chunkReference) {
                    this.push({
                        table: 'investment_return_yearly',
                        primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                        fields: {
                            id: uuidv1(),
                            cnpj_fundo: lastChunk.cnpj_fundo,
                            dt_comptc: lastChunk.dt_comptc.toDateString(),
                            return: (lastChunk.vl_quota / chunkReference.vl_quota) - 1
                        }
                    });
                }
            }
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