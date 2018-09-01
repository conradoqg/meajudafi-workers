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
    let last504Returns = [];
    let last504CDIReturns = [];
    let last504Consistency = [];
    let last756Returns = [];
    let last756CDIReturns = [];
    let last756Consistency = [];

    const getMonthHash = (momentDate) => momentDate.format('YYYY-MM');
    const getYearHash = (momentDate) => momentDate.format('YYYY');

    const dailyReduce = (struct, callbackFn, initialValue) => Object.values(struct).reduce((acc, value) => Object.values(value).reduce((acc2, value2) => value2.reduce(callbackFn, acc2), acc), initialValue);
    const monthlyReduce = (struct, callbackFn, initialValue) => Object.values(struct).reduce((acc, value) => value.reduce(callbackFn, acc), initialValue);

    const processDaily = (stream, chunk, chunkBefore, firstChunkEver) => {
        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
        returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push((chunk.vl_quota / chunkBefore.vl_quota) - 1);

        if (!cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)]) cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
        if (!cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
        cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push(chunkBefore.valor / 100);

        if (last252Returns.length >= 252) last252Returns.shift();
        last252Returns.push((chunk.vl_quota / chunkBefore.vl_quota) - 1);
        if (last252CDIReturns.length >= 252) last252CDIReturns.shift();
        last252CDIReturns.push(chunkBefore.valor / 100);
        if (last504Returns.length >= 252) last504Returns.shift();
        last504Returns.push((chunk.vl_quota / chunkBefore.vl_quota) - 1);
        if (last504CDIReturns.length >= 252) last504CDIReturns.shift();
        last504CDIReturns.push(chunkBefore.valor / 100);
        if (last756Returns.length >= 252) last756Returns.shift();
        last756Returns.push((chunk.vl_quota / chunkBefore.vl_quota) - 1);
        if (last756CDIReturns.length >= 252) last756CDIReturns.shift();
        last756CDIReturns.push(chunkBefore.valor / 100);

        // Return
        const investment_return = (chunk.vl_quota / chunkBefore.vl_quota) - 1;

        // Return 1Y
        const investment_return_1y = last252Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

        // Return 2Y
        const investment_return_2y = last504Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

        // Return 3Y
        const investment_return_3y = last756Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

        // Accumulated Return
        const accumulated_investment_return = (chunk.vl_quota / firstChunkEver.vl_quota) - 1;

        // Risk 1Y
        const sumDailyReturn1Y = last252Returns.reduce((acc, value) => acc + value, 0);
        const meanReturn1Y = sumDailyReturn1Y / last252Returns.length;
        const sumSquaredDiffFromMeanReturn1Y = last252Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn1Y, 2), 0);
        const standardDeviationReturn1Y = Math.sqrt(sumSquaredDiffFromMeanReturn1Y / last252Returns.length);
        const risk_1y = standardDeviationReturn1Y * Math.sqrt(252);

        // Risk 2Y
        const sumDailyReturn2Y = last504Returns.reduce((acc, value) => acc + value, 0);
        const meanReturn2Y = sumDailyReturn2Y / last504Returns.length;
        const sumSquaredDiffFromMeanReturn2Y = last504Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn2Y, 2), 0);
        const standardDeviationReturn2Y = Math.sqrt(sumSquaredDiffFromMeanReturn2Y / last504Returns.length);
        const risk_2y = standardDeviationReturn2Y * Math.sqrt(252);

        // Risk 3Y
        const sumDailyReturn3Y = last756Returns.reduce((acc, value) => acc + value, 0);
        const meanReturn3Y = sumDailyReturn3Y / last756Returns.length;
        const sumSquaredDiffFromMeanReturn3Y = last756Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn3Y, 2), 0);
        const standardDeviationReturn3Y = Math.sqrt(sumSquaredDiffFromMeanReturn3Y / last756Returns.length);
        const risk_3y = standardDeviationReturn3Y * Math.sqrt(252);

        // Accumulated Risk
        let accEntries = 0;
        const accDailyReturnSum = dailyReduce(returnsByYearAndMonth, (acc, value) => {
            accEntries++;
            return acc + value;
        }, 0);
        const accMeanReturn = accDailyReturnSum / accEntries;
        const accSquaredDiffFromMeanSum = dailyReduce(returnsByYearAndMonth, (acc, value) => acc + Math.pow(value - accMeanReturn, 2), 0);
        const accStandardDeviation = Math.sqrt(accSquaredDiffFromMeanSum / accEntries);
        const accumulated_risk = accStandardDeviation * Math.sqrt(252);

        // Sharpe 1Y
        const cdiAccumulatedInvestimentReturn1Y = last252CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        const annualizedAccInvestmentReturn1Y = ((investment_return_1y / last252CDIReturns.length) * 252);
        const annualizedAccCDIInvestmentReturn1Y = ((cdiAccumulatedInvestimentReturn1Y / last252CDIReturns.length) * 252);
        const sharpe_1y = (annualizedAccInvestmentReturn1Y - annualizedAccCDIInvestmentReturn1Y) / risk_1y;

        // Sharpe 2Y
        const cdiAccumulatedInvestimentReturn2Y = last504CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        const annualizedAccInvestmentReturn2Y = ((investment_return_2y / last504CDIReturns.length) * 252);
        const annualizedAccCDIInvestmentReturn2Y = ((cdiAccumulatedInvestimentReturn2Y / last504CDIReturns.length) * 252);
        const sharpe_2y = (annualizedAccInvestmentReturn2Y - annualizedAccCDIInvestmentReturn2Y) / risk_2y;

        // Sharpe 3Y
        const cdiAccumulatedInvestimentReturn3Y = last756CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        const annualizedAccInvestmentReturn3Y = ((investment_return_3y / last756CDIReturns.length) * 252);
        const annualizedAccCDIInvestmentReturn3Y = ((cdiAccumulatedInvestimentReturn3Y / last756CDIReturns.length) * 252);
        const sharpe_3y = (annualizedAccInvestmentReturn3Y - annualizedAccCDIInvestmentReturn3Y) / risk_3y;

        // Accumulated Sharpe
        const cdi_accumulated_investiment_return = dailyReduce(cdiReturnsByYearAndMonth, (acc, value) => acc * (1 + value), 1) - 1;
        const annualizedAccInvestmentReturn = ((accumulated_investment_return / accEntries) * 252);
        const annualizedAccCDIInvestmentReturn = ((cdi_accumulated_investiment_return / accEntries) * 252);
        const accumulated_sharpe = (annualizedAccInvestmentReturn - annualizedAccCDIInvestmentReturn) / accumulated_risk;

        // Consistency 1Y
        const last252Return = last252Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        const last252CDIReturn = last252CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        let consistent1Y = false;
        if (last252Return > last252CDIReturn) consistent1Y = true;

        if (last252Consistency.length >= 252) last252Consistency.shift();
        last252Consistency.push(consistent1Y);
        const consistency_1y = (100 * last252Consistency.filter(value => value == true).length) / last252Consistency.length;        

        // Consistency 2Y
        const last504Return = last504Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        const last504CDIReturn = last504CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        let consistent2Y = false;
        if (last504Return > last504CDIReturn) consistent2Y = true;

        if (last504Consistency.length >= 504) last504Consistency.shift();
        last504Consistency.push(consistent2Y);
        const consistency_2y = (100 * last504Consistency.filter(value => value == true).length) / last504Consistency.length;

        // Consistency 3Y
        const last756Return = last756Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        const last756CDIReturn = last756CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
        let consistent3Y = false;
        if (last756Return > last756CDIReturn) consistent3Y = true;

        if (last756Consistency.length >= 756) last756Consistency.shift();
        last756Consistency.push(consistent3Y);
        const consistency_3y = (100 * last756Consistency.filter(value => value == true).length) / last756Consistency.length;

        stream.push({
            table: 'investment_return_daily',
            primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
            fields: {
                id: uuidv1(),
                cnpj_fundo: chunk.cnpj_fundo,
                dt_comptc: chunk.dt_comptc.format('YYYY-MM-DD'),
                investment_return,
                investment_return_1y,
                investment_return_2y,
                investment_return_3y,
                accumulated_investment_return,
                risk_1y,
                risk_2y,
                risk_3y,
                accumulated_risk,
                sharpe_1y,
                sharpe_2y,
                sharpe_3y,
                accumulated_sharpe,
                consistency_1y,
                consistency_2y,
                consistency_3y
            }
        });
    };
    const processMonthly = (stream, chunk) => {
        const referenceMonthHash = getMonthHash(chunk.dt_comptc);
        const referenceYearHash = getYearHash(chunk.dt_comptc);

        if (returnsByYearAndMonth[referenceYearHash] && returnsByYearAndMonth[referenceYearHash][referenceMonthHash]) {
            // Return
            const investment_return = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc * (1 + (value)), 1) - 1;

            // Return 1Y
            const investment_return_1y = last252Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

            // Return 1Y
            const investment_return_2y = last504Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

            // Return 1Y
            const investment_return_3y = last756Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

            // Accumulated Return
            const accumulated_investment_return = dailyReduce(returnsByYearAndMonth, (acc, value) => acc * (1 + value), 1) - 1;

            // Risk
            const dailyReturnSum = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc + value, 0);
            const meanReturn = dailyReturnSum / returnsByYearAndMonth[referenceYearHash][referenceMonthHash].length;
            const squaredDiffFromMeanSum = returnsByYearAndMonth[referenceYearHash][referenceMonthHash].reduce((acc, value) => acc + Math.pow(value - meanReturn, 2), 0);
            const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / returnsByYearAndMonth[referenceYearHash][referenceMonthHash].length);
            const risk = standardDeviation * Math.sqrt(252);

            // Risk 1Y
            const sumDailyReturn1Y = last252Returns.reduce((acc, value) => acc + value, 0);
            const meanReturn1Y = sumDailyReturn1Y / last252Returns.length;
            const sumSquaredDiffFromMeanReturn1Y = last252Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn1Y, 2), 0);
            const standardDeviationReturn1Y = Math.sqrt(sumSquaredDiffFromMeanReturn1Y / last252Returns.length);
            const risk_1y = standardDeviationReturn1Y * Math.sqrt(252);

            // Risk 2Y
            const sumDailyReturn2Y = last504Returns.reduce((acc, value) => acc + value, 0);
            const meanReturn2Y = sumDailyReturn2Y / last504Returns.length;
            const sumSquaredDiffFromMeanReturn2Y = last504Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn2Y, 2), 0);
            const standardDeviationReturn2Y = Math.sqrt(sumSquaredDiffFromMeanReturn2Y / last504Returns.length);
            const risk_2y = standardDeviationReturn2Y * Math.sqrt(252);

            // Risk 3Y
            const sumDailyReturn3Y = last756Returns.reduce((acc, value) => acc + value, 0);
            const meanReturn3Y = sumDailyReturn3Y / last756Returns.length;
            const sumSquaredDiffFromMeanReturn3Y = last756Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn3Y, 2), 0);
            const standardDeviationReturn3Y = Math.sqrt(sumSquaredDiffFromMeanReturn3Y / last756Returns.length);
            const risk_3y = standardDeviationReturn3Y * Math.sqrt(252);

            // Accumulated Risk
            let accEntries = 0;
            const accDailyReturnSum = dailyReduce(returnsByYearAndMonth, (acc, value) => {
                accEntries++;
                return acc + value;
            }, 0);
            const accMeanReturn = accDailyReturnSum / accEntries;
            const accSquaredDiffFromMeanSum = dailyReduce(returnsByYearAndMonth, (acc, value) => acc + Math.pow(value - accMeanReturn, 2), 0);
            const accStandardDeviation = Math.sqrt(accSquaredDiffFromMeanSum / accEntries);
            const accumulated_risk = accStandardDeviation * Math.sqrt(252);

            // Sharpe
            let sharpeEntries = 0;
            const cdiInvestimentReturn = monthlyReduce(cdiReturnsByYearAndMonth[referenceYearHash], (acc, value) => {
                sharpeEntries++;
                return acc * (1 + (value));
            }, 1) - 1;
            const annualizedInvestmentReturn = ((investment_return / sharpeEntries) * 252);
            const annualizedCDIInvestmentReturn = ((cdiInvestimentReturn / sharpeEntries) * 252);
            const sharpe = (annualizedInvestmentReturn - annualizedCDIInvestmentReturn) / risk;

            // Sharpe 1Y
            const cdiAccumulatedInvestimentReturn1Y = last252CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn1Y = ((investment_return_1y / last252CDIReturns.length) * 252);
            const annualizedAccCDIInvestmentReturn1Y = ((cdiAccumulatedInvestimentReturn1Y / last252CDIReturns.length) * 252);
            const sharpe_1y = (annualizedAccInvestmentReturn1Y - annualizedAccCDIInvestmentReturn1Y) / risk_1y;

            // Sharpe 2Y
            const cdiAccumulatedInvestimentReturn2Y = last504CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn2Y = ((investment_return_2y / last504CDIReturns.length) * 252);
            const annualizedAccCDIInvestmentReturn2Y = ((cdiAccumulatedInvestimentReturn2Y / last504CDIReturns.length) * 252);
            const sharpe_2y = (annualizedAccInvestmentReturn2Y - annualizedAccCDIInvestmentReturn2Y) / risk_2y;

            // Sharpe 3Y
            const cdiAccumulatedInvestimentReturn3Y = last756CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn3Y = ((investment_return_3y / last756CDIReturns.length) * 252);
            const annualizedAccCDIInvestmentReturn3Y = ((cdiAccumulatedInvestimentReturn3Y / last756CDIReturns.length) * 252);
            const sharpe_3y = (annualizedAccInvestmentReturn3Y - annualizedAccCDIInvestmentReturn3Y) / risk_3y;

            // Accumulated Sharpe            
            const cdiAccumulatedInvestimentReturn = dailyReduce(cdiReturnsByYearAndMonth, (acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn = ((accumulated_investment_return / accEntries) * 252);
            const annualizedAccCDIInvestmentReturn = ((cdiAccumulatedInvestimentReturn / accEntries) * 252);
            const accumulated_sharpe = (annualizedAccInvestmentReturn - annualizedAccCDIInvestmentReturn) / accumulated_risk;

            // Consistency 1Y
            const consistency_1y = (100 * last252Consistency.filter(value => value == true).length) / last252Consistency.length;

            // Consistency 2Y
            const consistency_2y = (100 * last504Consistency.filter(value => value == true).length) / last504Consistency.length;

            // Consistency 31Y
            const consistency_3y = (100 * last756Consistency.filter(value => value == true).length) / last756Consistency.length;

            stream.push({
                table: 'investment_return_monthly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: chunk.cnpj_fundo,
                    dt_comptc: chunk.dt_comptc.clone().endOf('month').format('YYYY-MM-DD'),
                    investment_return,
                    investment_return_1y,
                    investment_return_2y,
                    investment_return_3y,
                    accumulated_investment_return,
                    risk,
                    risk_1y,
                    risk_2y,
                    risk_3y,
                    accumulated_risk,
                    sharpe,
                    sharpe_1y,
                    sharpe_2y,
                    sharpe_3y,
                    accumulated_sharpe,
                    consistency_1y,
                    consistency_2y,
                    consistency_3y
                }
            });
        }
    };
    const processYearly = (stream, chunk) => {
        const referenceYearHash = getYearHash(chunk.dt_comptc);

        if (returnsByYearAndMonth[referenceYearHash]) {
            // Return
            const investment_return = monthlyReduce(returnsByYearAndMonth[referenceYearHash], (acc, value) => acc * (1 + (value)), 1) - 1;

            // Return 1Y
            const investment_return_1y = last252Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

            // Return 2Y
            const investment_return_2y = last504Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

            // Return 3Y
            const investment_return_3y = last756Returns.reduce((acc, value) => acc * (1 + value), 1) - 1;

            // Accumulated Return
            const accumulated_investment_return = dailyReduce(returnsByYearAndMonth, (acc, value) => acc * (1 + value), 1) - 1;

            // Risk
            let entries = 0;
            const dailyReturnSum = monthlyReduce(returnsByYearAndMonth[referenceYearHash], (acc, value) => {
                entries++;
                return acc + value;
            }, 0);
            const meanReturn = dailyReturnSum / entries;
            const squaredDiffFromMeanSum = monthlyReduce(returnsByYearAndMonth[referenceYearHash], (acc, value) => acc + Math.pow(value - meanReturn, 2), 0);
            const standardDeviation = Math.sqrt(squaredDiffFromMeanSum / entries);
            const risk = standardDeviation * Math.sqrt(252);

            // Risk 1Y
            const sumDailyReturn1Y = last252Returns.reduce((acc, value) => acc + value, 0);
            const meanReturn1Y = sumDailyReturn1Y / last252Returns.length;
            const sumSquaredDiffFromMeanReturn1Y = last252Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn1Y, 2), 0);
            const standardDeviationReturn1Y = Math.sqrt(sumSquaredDiffFromMeanReturn1Y / last252Returns.length);
            const risk_1y = standardDeviationReturn1Y * Math.sqrt(252);

            // Risk 2Y
            const sumDailyReturn2Y = last504Returns.reduce((acc, value) => acc + value, 0);
            const meanReturn2Y = sumDailyReturn2Y / last504Returns.length;
            const sumSquaredDiffFromMeanReturn2Y = last504Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn2Y, 2), 0);
            const standardDeviationReturn2Y = Math.sqrt(sumSquaredDiffFromMeanReturn2Y / last504Returns.length);
            const risk_2y = standardDeviationReturn2Y * Math.sqrt(252);

            // Risk 3Y
            const sumDailyReturn3Y = last756Returns.reduce((acc, value) => acc + value, 0);
            const meanReturn3Y = sumDailyReturn3Y / last756Returns.length;
            const sumSquaredDiffFromMeanReturn3Y = last756Returns.reduce((acc, value) => acc + Math.pow(value - meanReturn3Y, 2), 0);
            const standardDeviationReturn3Y = Math.sqrt(sumSquaredDiffFromMeanReturn3Y / last756Returns.length);
            const risk_3y = standardDeviationReturn3Y * Math.sqrt(252);

            // Accumulated Risk
            let accEntries = 0;
            const accDailyReturnSum = dailyReduce(returnsByYearAndMonth, (acc, value) => {
                accEntries++;
                return acc + value;
            }, 0);
            const accMeanReturn = accDailyReturnSum / accEntries;
            const accSquaredDiffFromMeanSum = dailyReduce(returnsByYearAndMonth, (acc, value) => acc + Math.pow(value - accMeanReturn, 2), 0);
            const accStandardDeviation = Math.sqrt(accSquaredDiffFromMeanSum / accEntries);
            const accumulated_risk = accStandardDeviation * Math.sqrt(252);

            // Sharpe
            let sharpe_entries = 0;
            const cdiInvestimentReturn = monthlyReduce(cdiReturnsByYearAndMonth[referenceYearHash], (acc, value) => {
                sharpe_entries++;
                return acc * (1 + (value));
            }, 1) - 1;
            const annualizedInvestmentReturn = ((investment_return / sharpe_entries) * 252);
            const annualizedCDIInvestmentReturn = ((cdiInvestimentReturn / sharpe_entries) * 252);
            const sharpe = (annualizedInvestmentReturn - annualizedCDIInvestmentReturn) / risk;

            // Sharpe 1Y
            const cdiAccumulatedInvestimentReturn1Y = last252CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn1Y = ((investment_return_1y / last252CDIReturns.length) * 252);
            const annualizedAccCDIInvestmentReturn1Y = ((cdiAccumulatedInvestimentReturn1Y / last252CDIReturns.length) * 252);
            const sharpe_1y = (annualizedAccInvestmentReturn1Y - annualizedAccCDIInvestmentReturn1Y) / risk_1y;

            // Sharpe 2Y
            const cdiAccumulatedInvestimentReturn2Y = last504CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn2Y = ((investment_return_2y / last504CDIReturns.length) * 252);
            const annualizedAccCDIInvestmentReturn2Y = ((cdiAccumulatedInvestimentReturn2Y / last504CDIReturns.length) * 252);
            const sharpe_2y = (annualizedAccInvestmentReturn2Y - annualizedAccCDIInvestmentReturn2Y) / risk_2y;

            // Sharpe 3Y
            const cdiAccumulatedInvestimentReturn3Y = last756CDIReturns.reduce((acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn3Y = ((investment_return_3y / last756CDIReturns.length) * 252);
            const annualizedAccCDIInvestmentReturn3Y = ((cdiAccumulatedInvestimentReturn3Y / last756CDIReturns.length) * 252);
            const sharpe_3y = (annualizedAccInvestmentReturn3Y - annualizedAccCDIInvestmentReturn3Y) / risk_3y;

            // Accumulated Sharpe            
            const cdiAccumulatedInvestimentReturn = dailyReduce(cdiReturnsByYearAndMonth, (acc, value) => acc * (1 + value), 1) - 1;
            const annualizedAccInvestmentReturn = ((accumulated_investment_return / accEntries) * 252);
            const annualizedAccCDIInvestmentReturn = ((cdiAccumulatedInvestimentReturn / accEntries) * 252);
            const accumulated_sharpe = (annualizedAccInvestmentReturn - annualizedAccCDIInvestmentReturn) / accumulated_risk;

            // Consistency 1Y
            const consistency_1y = (100 * last252Consistency.filter(value => value == true).length) / last252Consistency.length;

            // Consistency 2Y
            const consistency_2y = (100 * last504Consistency.filter(value => value == true).length) / last504Consistency.length;

            // Consistency 3Y
            const consistency_3y = (100 * last756Consistency.filter(value => value == true).length) / last756Consistency.length;

            stream.push({
                table: 'investment_return_yearly',
                primaryKey: ['CNPJ_FUNDO', 'DT_COMPTC'],
                fields: {
                    id: uuidv1(),
                    cnpj_fundo: chunk.cnpj_fundo,
                    dt_comptc: chunk.dt_comptc.clone().endOf('year').format('YYYY-MM-DD'),
                    investment_return,
                    investment_return_1y,
                    investment_return_2y,
                    investment_return_3y,
                    accumulated_investment_return,
                    risk,
                    risk_1y,
                    risk_2y,
                    risk_3y,
                    accumulated_risk,
                    sharpe,
                    sharpe_1y,
                    sharpe_2y,
                    sharpe_3y,
                    accumulated_sharpe,
                    consistency_1y,
                    consistency_2y,
                    consistency_3y
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
