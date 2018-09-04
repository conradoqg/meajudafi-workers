const uuidv1 = require('uuid/v1');
const stream = require('stream');
const CONFIG = require('../config');
const StandardDeviation = require('./../standardDeviation');

const createCalculatorStream = () => {
    let lastChunk = null;
    let returnsByYearAndMonth = {};
    let cdiReturnsByYearAndMonth = {};
    let returns = [];
    let cdiReturns = [];
    let last252Returns = [];
    let last252CDIReturns = [];
    let last252Consistency = [];
    let last504Returns = [];
    let last504CDIReturns = [];
    let last504Consistency = [];
    let last756Returns = [];
    let last756CDIReturns = [];
    let last756Consistency = [];

    const calcSharpeForPeriod = (risk, investment_return, cdi_investment_return, length) => {
        const annualizedAccInvestmentReturn = ((investment_return / length) * 252);
        const annualizedAccCDIInvestmentReturn = ((cdi_investment_return / length) * 252);
        return (annualizedAccInvestmentReturn - annualizedAccCDIInvestmentReturn) / risk;
    };

    const calcConsistencyForPeriod = (investment_return, cdi_investment_return, period, lastConsistency) => {
        let consistencyPoint = 0;
        if (investment_return > cdi_investment_return) consistencyPoint = 1;
        if (lastConsistency.length >= period) consistencyReached -= lastConsistency.shift();
        consistencyReached += consistencyPoint;
        lastConsistency.push(consistencyPoint);
        return (100 * consistencyReached) / lastConsistency.length;
    };

    let chunks = [];
    let chunksByDate = {};
    let last252Chunks = [];
    let last504Chunks = [];
    let last756Chunks = [];
    let consistencyReached = 0;
    let risk = null;
    let risksByMonth = {};
    let risksByYear = {};
    let last252Risk = null;
    let last504Risk = null;
    let last756Risk = null;

    const getMonthHash = (momentDate) => momentDate.format('YYYY-MM');
    const getYearHash = (momentDate) => momentDate.format('YYYY');

    const returnFromQuota = (initialQuota, finalQuota) => (finalQuota == 0) ? 0 : (initialQuota / finalQuota) - 1;

    const processDaily = (stream, chunk, chunkBefore) => {
        let change = returnFromQuota(chunkBefore.vl_quota, chunk.vl_quota);
        let cdiChange = chunkBefore.valor / 100;
        let month = getMonthHash(chunk.dt_comptc);
        let year = getYearHash(chunk.dt_comptc);

        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
        if (!returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
        returnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push(change);

        if (!cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)]) cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)] = {};
        if (!cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)]) cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)] = [];
        cdiReturnsByYearAndMonth[getYearHash(chunk.dt_comptc)][getMonthHash(chunk.dt_comptc)].push(cdiChange);

        returns.push(change);
        cdiReturns.push(cdiChange);

        if (last252Returns.length >= 252) last252Risk.removeMeasurement(last252Returns.shift());
        last252Returns.push(change);
        if (last252CDIReturns.length >= 252) last252CDIReturns.shift();
        last252CDIReturns.push(cdiChange);
        if (last504Returns.length >= 504) last504Risk.removeMeasurement(last504Returns.shift());
        last504Returns.push(change);
        if (last504CDIReturns.length >= 504) last504CDIReturns.shift();
        last504CDIReturns.push(cdiChange);
        if (last756Returns.length >= 756) last756Risk.removeMeasurement(last756Returns.shift());
        last756Returns.push(change);
        if (last756CDIReturns.length >= 756) last756CDIReturns.shift();
        last756CDIReturns.push(cdiChange);

        // Return
        const investment_return = change;
        const cdi_investment_return = cdiChange;

        // Return 1Y
        const investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
        const cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);

        // Return 2Y        
        const investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
        const cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);

        // Return 3Y        
        const investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
        const cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);

        // Accumulated Return        
        const accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
        const cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);

        // Months Risk
        if (!risksByMonth[month]) risksByMonth[month] = new StandardDeviation(change);
        else risksByMonth[month].addMeasurement(change);

        // Years Risk
        if (!risksByYear[year]) risksByYear[year] = new StandardDeviation(change);
        else risksByYear[year].addMeasurement(change);

        // Risk 1Y
        if (last252Risk == null) last252Risk = new StandardDeviation(change);
        else last252Risk.addMeasurement(change);
        const risk_1y = last252Risk.get() * Math.sqrt(252);

        // Risk 2Y
        if (last504Risk == null) last504Risk = new StandardDeviation(change);
        else last504Risk.addMeasurement(change);
        const risk_2y = last504Risk.get() * Math.sqrt(252);

        // Risk 3Y
        if (last756Risk == null) last756Risk = new StandardDeviation(change);
        else last756Risk.addMeasurement(change);
        const risk_3y = last756Risk.get() * Math.sqrt(252);

        // Accumulated Risk
        if (risk == null) risk = new StandardDeviation(change);
        else risk.addMeasurement(change);
        const accumulated_risk = risk.get() * Math.sqrt(252);

        // Sharpe 1Y     
        const sharpe_1y = calcSharpeForPeriod(risk_1y, investment_return_1y, cdi_investment_return_1y, last252CDIReturns.length);

        // Sharpe 2Y                
        const sharpe_2y = calcSharpeForPeriod(risk_2y, investment_return_2y, cdi_investment_return_2y, last504CDIReturns.length);

        // Sharpe 3Y        
        const sharpe_3y = calcSharpeForPeriod(risk_3y, investment_return_3y, cdi_investment_return_3y, last756CDIReturns.length);

        // Accumulated Sharpe        
        const accumulated_sharpe = calcSharpeForPeriod(accumulated_risk, accumulated_investment_return, cdi_accumulated_investment_return, returns.length);

        // Consistency 1Y        
        const consistency_1y = calcConsistencyForPeriod(investment_return_1y, cdi_investment_return_1y, 252, last252Consistency);

        // Consistency 2Y
        const consistency_2y = calcConsistencyForPeriod(investment_return_2y, cdi_investment_return_2y, 504, last504Consistency);

        // Consistency 3Y
        const consistency_3y = calcConsistencyForPeriod(investment_return_3y, cdi_investment_return_3y, 756, last756Consistency);

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

    const lastItem = (array) => array[array.length - 1];
    const firstItem = (array) => array[0];

    const lastMonthOrFirstDay = (chunk, year, month, lastYear, lastMonth) => {
        if (chunksByDate[lastYear] && chunksByDate[lastYear][lastMonth]) return lastItem(chunksByDate[lastYear][lastMonth]);
        else return firstItem(chunksByDate[year][month]);
    };

    const lastYearOrFirstDay = (chunk, year, lastYear) => {
        let toReturn = null;
        if (chunksByDate[lastYear]) toReturn = lastItem(lastItem(Object.values(chunksByDate[lastYear])));
        else toReturn = firstItem(firstItem(Object.values(chunksByDate[year])));
        return toReturn;
    };

    const processMonthly = (stream, chunk) => {
        const monthBefore = chunk.dt_comptc.clone().subtract(1, 'month');
        const lastMonth = getMonthHash(monthBefore);
        const lastYear = getYearHash(monthBefore);
        const month = getMonthHash(chunk.dt_comptc);
        const year = getYearHash(chunk.dt_comptc);

        if (risk) {
            // Return            
            const investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).vl_quota, chunk.vl_quota);
            const cdi_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).cdi_quota, chunk.cdi_quota);

            // Return 1Y            
            const investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            const cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);

            // Return 2Y
            const investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            const cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);

            // Return 3Y
            const investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            const cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);

            // Accumulated Return
            const accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            const cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);

            // Months Risk            
            const risk_total = risksByMonth[month].get() * Math.sqrt(252);

            // Risk 1Y            
            const risk_1y = last252Risk.get() * Math.sqrt(252);

            // Risk 2Y            
            const risk_2y = last504Risk.get() * Math.sqrt(252);

            // Risk 3Y            
            const risk_3y = last756Risk.get() * Math.sqrt(252);

            // Accumulated Risk
            const accumulated_risk = risk.get() * Math.sqrt(252);

            // Sharpe
            const sharpe = calcSharpeForPeriod(risk_total, investment_return, cdi_investment_return, chunksByDate[year][month].length);

            // Sharpe 1Y
            const sharpe_1y = calcSharpeForPeriod(risk_1y, investment_return_1y, cdi_investment_return_1y, last252CDIReturns.length);

            // Sharpe 2Y
            const sharpe_2y = calcSharpeForPeriod(risk_2y, investment_return_2y, cdi_investment_return_2y, last504CDIReturns.length);

            // Sharpe 3Y
            const sharpe_3y = calcSharpeForPeriod(risk_3y, investment_return_3y, cdi_investment_return_3y, last756CDIReturns.length);

            // Accumulated Sharpe            
            const accumulated_sharpe = calcSharpeForPeriod(accumulated_risk, accumulated_investment_return, cdi_accumulated_investment_return, returns.length);

            // Consistency 1Y        
            const consistency_1y = calcConsistencyForPeriod(investment_return_1y, cdi_investment_return_1y, 252, last252Consistency);

            // Consistency 2Y
            const consistency_2y = calcConsistencyForPeriod(investment_return_2y, cdi_investment_return_2y, 504, last504Consistency);

            // Consistency 3Y
            const consistency_3y = calcConsistencyForPeriod(investment_return_3y, cdi_investment_return_3y, 756, last756Consistency);

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
                    risk: risk_total,
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
        const lastYear = getYearHash(chunk.dt_comptc.clone().subtract(1, 'year'));
        const year = getYearHash(chunk.dt_comptc);

        if (returnsByYearAndMonth[year]) {
            // Return            
            const investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).vl_quota, chunk.vl_quota);
            const cdi_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).cdi_quota, chunk.cdi_quota);

            // Return 1Y            
            const investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            const cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);

            // Return 2Y
            const investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            const cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);

            // Return 3Y
            const investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            const cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);

            // Accumulated Return            
            const accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            const cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);

            // Risk            
            const risk_total = risksByYear[year].get() * Math.sqrt(252);

            // Risk 1Y            
            const risk_1y = last252Risk.get() * Math.sqrt(252);

            // Risk 2Y            
            const risk_2y = last504Risk.get() * Math.sqrt(252);

            // Risk 3Y            
            const risk_3y = last756Risk.get() * Math.sqrt(252);

            // Accumulated Risk
            const accumulated_risk = risk.get() * Math.sqrt(252);

            // Sharpe
            const sharpe = calcSharpeForPeriod(risk_total, investment_return, cdi_investment_return, Object.values(chunksByDate[year]).reduce((acc, month) => acc + month.length, 0));

            // Sharpe 1Y
            const sharpe_1y = calcSharpeForPeriod(risk_1y, investment_return_1y, cdi_investment_return_1y, last252CDIReturns.length);

            // Sharpe 2Y
            const sharpe_2y = calcSharpeForPeriod(risk_2y, investment_return_2y, cdi_investment_return_2y, last504CDIReturns.length);

            // Sharpe 3Y
            const sharpe_3y = calcSharpeForPeriod(risk_3y, investment_return_3y, cdi_investment_return_3y, last756CDIReturns.length);

            // Accumulated Sharpe            
            const accumulated_sharpe = calcSharpeForPeriod(accumulated_risk, accumulated_investment_return, cdi_accumulated_investment_return, returns.length);

            // Consistency 1Y        
            const consistency_1y = calcConsistencyForPeriod(investment_return_1y, cdi_investment_return_1y, 252, last252Consistency);

            // Consistency 2Y
            const consistency_2y = calcConsistencyForPeriod(investment_return_2y, cdi_investment_return_2y, 504, last504Consistency);

            // Consistency 3Y
            const consistency_3y = calcConsistencyForPeriod(investment_return_3y, cdi_investment_return_3y, 756, last756Consistency);

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
                    risk: risk_total,
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
                if (chunks.length == 0) chunk.cdi_quota = 1;
                else chunk.cdi_quota = (chunks[chunks.length - 1].cdi_quota * (1 + (chunks[chunks.length - 1].valor / 100)));
                chunks.push(chunk);
                if (last252Chunks.length >= 252) last252Chunks.shift();
                last252Chunks.push(chunk);
                if (last504Chunks.length >= 504) last504Chunks.shift();
                last504Chunks.push(chunk);
                if (last756Chunks.length >= 756) last756Chunks.shift();
                last756Chunks.push(chunk);

                const month = getMonthHash(chunk.dt_comptc);
                const year = getYearHash(chunk.dt_comptc);
                if (!chunksByDate[year]) chunksByDate[year] = {};
                if (!chunksByDate[year][month]) chunksByDate[year][month] = [];
                chunksByDate[year][month].push(chunk);

                if (lastChunk != null) {
                    // Monthly
                    if (lastChunk.dt_comptc.month() != chunk.dt_comptc.month()) {
                        processMonthly(this, lastChunk);
                    }

                    // Yearly
                    if (lastChunk.dt_comptc.year() != chunk.dt_comptc.year()) {
                        processYearly(this, lastChunk);
                    }

                    // Daily
                    processDaily(this, chunk, lastChunk);
                }

                lastChunk = chunk;
                callback();
            } catch (ex) {
                console.error(ex.stack);
                callback(ex);
            }
        },
        flush: function (callback) {
            try {
                if (lastChunk != null) {
                    // Monthly
                    processMonthly(this, lastChunk);

                    // Yearly
                    processYearly(this, lastChunk);
                }

                callback();
            } catch (ex) {
                console.error(ex.stack);
                callback(ex);
            }
        }
    });
};

module.exports = createCalculatorStream;