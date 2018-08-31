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
    let returnsByYearAndMonth = {};
    let cdiReturnsByYearAndMonth = {};
    let last252Returns = [];
    let last252CDIReturns = [];
    let last252Consistency = [];

    const getMonthHash = (momentDate) => momentDate.format('YYYY-MM');
    const getYearHash = (momentDate) => momentDate.format('YYYY');

    const processDaily = (stream, chunk, chunkBefore, firstChunkEver) => {
        // Return
        const investment_return = (chunk.vl_quota / chunkBefore.vl_quota) - 1;

        // Accumulated Return
        const accumulated_investment_return = (chunkBefore.vl_quota / firstChunkEver.vl_quota) - 1;

        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
        returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push(investment_return);

        if (!cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)]) cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
        if (!cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
        cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push(chunkBefore.valor / 100);

        if (last252Returns.length >= 252) last252Returns.shift();
        last252Returns.push(investment_return);
        if (last252CDIReturns.length >= 252) last252CDIReturns.shift();
        last252CDIReturns.push(chunkBefore.valor / 100);

        // Consistency
        const last252Return = last252Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        const last252CDIReturn = last252CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        let consistent = false;
        if (last252Return > last252CDIReturn) consistent = true;

        if (last252Consistency.length >= 252) last252Consistency.shift();
        last252Consistency.push(consistent);
        const consistency_1y = (100 * last252Consistency.filter(value => value == true).length) / last252Consistency.length;

        stream.push({
            table: 'investment_return_daily',
            primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
            fields: {
                id: uuidv1(),
                cnpj_fundo: chunk.cnpj_fundo,
                dt_comptc: chunk.dt_comptc.format('YYYY-MM-DD'),
                investment_return,
                accumulated_investment_return,
                consistency_1y
            }
        });
    };
    const processMonthly = (stream, chunk) => {
        const referenceMonthHash = getMonthHash(chunk.dt_comptc);
        const referenceYearHash = getYearHash(chunk.dt_comptc);

        if (returnsByYearAndMonth[referenceYearHash] && returnsByYearAndMonth[referenceYearHash][referenceMonthHash]) {
            // Return
            const investment_return = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc * (1 + (value)), 1) - 1;

            // Accumulated Return
            const accumulated_investment_return = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => acc3 * (1 + value3), acc2), acc), 1) - 1;

            // Risk
            const dailyReturnSum = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc + value, 0);
            const meanReturn = dailyReturnSum / returnsByYearAndMonth[referenceYearHash][referenceMonthHash].length;
            const squaredDiffFromMeanSum = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc + Math.pow(value - meanReturn, 2), 0);
            const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / returnsByYearAndMonth[referenceYearHash][referenceMonthHash].length);
            const months_risk = standardDeviation * Math.sqrt(252);

            // Accumulated Risk
            let accEntries = 0;
            const accDailyReturnSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                accEntries++;
                return acc3 + value3;
            }, acc2), acc), 0);
            const accMeanReturn = accDailyReturnSum / accEntries;
            const accSquaredDiffFromMeanSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                return acc3 + Math.pow(value3 - accMeanReturn, 2);
            }, acc2), acc), 0);
            const accStandardDeviation = Math.sqrt(accSquaredDiffFromMeanSum / accEntries);
            const accumulated_risk = accStandardDeviation * Math.sqrt(252);

            // Sharpe
            const cdi_investiment_return = cdiReturnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc * (1 + (value)), 1) - 1;
            const sharpe = (investment_return - cdi_investiment_return) / months_risk;

            // Accumulated Sharpe
            const cdi_accumulated_investiment_return = Object.values(cdiReturnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => acc3 * (1 + value3), acc2), acc), 1) - 1;
            const accumulated_sharpe = (accumulated_investment_return - cdi_accumulated_investiment_return) / accumulated_risk;

            // Consistency 1Y
            const consistency_1y = (100 * last252Consistency.filter(value => value == true).length) / last252Consistency.length;

            stream.push({
                table: 'investment_return_monthly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: chunk.cnpj_fundo,
                    dt_comptc: chunk.dt_comptc.clone().endOf('month').format('YYYY-MM-DD'),
                    investment_return,
                    accumulated_investment_return,
                    months_risk,
                    accumulated_risk,
                    sharpe,
                    accumulated_sharpe,
                    consistency_1y
                }
            });
        }
    };
    const processYearly = (stream, chunk) => {
        const referenceYearHash = getYearHash(chunk.dt_comptc);

        if (returnsByYearAndMonth[referenceYearHash]) {
            // Return
            const investment_return = Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => value.reduce((acc2, value2) => acc2 * (1 + (value2)), acc), 1) - 1;

            // Accumulated Return
            const accumulated_investment_return = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => acc3 * (1 + value3), acc2), acc), 1) - 1;

            // Risk
            let entries = 0;
            const dailyReturnSum = Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => value.reduce((acc2, value2) => {
                entries++;
                return acc2 + value2;
            }, acc), 0);
            const meanReturn = dailyReturnSum / entries;
            const squaredDiffFromMeanSum = Object.values(returnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => value.reduce((acc2, value2) => acc2 + Math.pow(value2 - meanReturn, 2), acc), 0);
            const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / entries);
            const years_risk = standardDeviation * Math.sqrt(252);

            // Accumulated Risk
            let accEntries = 0;
            const accDailyReturnSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                accEntries++;
                return acc3 + value3;
            }, acc2), acc), 0);
            const accMeanReturn = accDailyReturnSum / accEntries;
            const accSquaredDiffFromMeanSum = Object.values(returnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => {
                return acc3 + Math.pow(value3 - accMeanReturn, 2);
            }, acc2), acc), 0);
            const accStandardDeviation = Math.sqrt(accSquaredDiffFromMeanSum / accEntries);
            const accumulated_risk = accStandardDeviation * Math.sqrt(252);

            // Sharpe
            const cdi_investiment_return = Object.values(cdiReturnsByYearAndMonth[referenceYearHash]).reduce((acc, value) => value.reduce((acc2, value2) => acc2 * (1 + (value2)), acc), 1) - 1;
            const annualizedInvestmentReturn = ((investment_return / accEntries) * 252);
            const annualizedCDIInvestmentReturn = ((cdi_investiment_return / accEntries) * 252);
            const sharpe = (annualizedInvestmentReturn - annualizedCDIInvestmentReturn) / years_risk;

            // Accumulated Sharpe            
            const cdi_accumulated_investiment_return = Object.values(cdiReturnsByYearAndMonth).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce((acc3, value3) => acc3 * (1 + value3), acc2), acc), 1) - 1;
            const annualizedAccInvestmentReturn = ((accumulated_investment_return / accEntries) * 252);
            const annualizedAccCDIInvestmentReturn = ((cdi_accumulated_investiment_return / accEntries) * 252);
            const accumulated_sharpe = (annualizedAccInvestmentReturn - annualizedAccCDIInvestmentReturn) / accumulated_risk;

            // Consistency 1Y
            const consistency_1y = (100 * last252Consistency.filter(value => value == true).length) / last252Consistency.length;

            stream.push({
                table: 'investment_return_yearly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: chunk.cnpj_fundo,
                    dt_comptc: chunk.dt_comptc.clone().endOf('year').format('YYYY-MM-DD'),
                    investment_return,
                    accumulated_investment_return,
                    years_risk,
                    accumulated_risk,
                    sharpe,
                    accumulated_sharpe,
                    consistency_1y
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

                // Monthly
                if (lastChunk != null && lastChunk.dt_comptc.month() != chunk.dt_comptc.month()) {
                    processMonthly(this, lastChunk);
                }

                // Yearly
                if (lastChunk != null && lastChunk.dt_comptc.year() != chunk.dt_comptc.year()) {
                    processYearly(this, lastChunk);
                }

                // Daily
                if (lastChunk != null) {
                    processDaily(this, chunk, lastChunk, firstChunkEver);
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
            processMonthly(this, lastChunk);

            // Yearly
            processYearly(this, lastChunk);

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

                const query = new QueryStream(`SELECT * FROM inf_diario_fi LEFT JOIN fbcdata_sgs_12i ON inf_diario_fi.DT_COMPTC = fbcdata_sgs_12i.DATA WHERE CNPJ_FUNDO = '${fund}' ORDER BY DT_COMPTC ASC`);
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
